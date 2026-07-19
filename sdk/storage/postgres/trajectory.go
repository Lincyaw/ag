package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lincyaw/ag/sdk"
)

type TrajectoryStore struct {
	pool      *pgxpool.Pool
	namespace string
}

func newTrajectoryStore(
	pool *pgxpool.Pool,
	namespace string,
) *TrajectoryStore {
	return &TrajectoryStore{pool: pool, namespace: namespace}
}

func (store *TrajectoryStore) Create(
	ctx context.Context,
	trajectory sdk.Trajectory,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareNewTrajectory(trajectory, time.Now().UTC())
	if err != nil {
		return err
	}
	trajectory = prepared.Trajectory
	environmentJSON, err := trajectoryEnvironmentJSON(
		trajectory.Environment,
	)
	if err != nil {
		return fmt.Errorf("encode trajectory environment: %w", err)
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var inheritedCount uint64
	if trajectory.ParentID != "" {
		branch, err := store.loadBranch(
			ctx,
			tx,
			trajectory.ParentID,
			trajectory.ParentEntryID,
		)
		if err != nil {
			return fmt.Errorf(
				"resolve trajectory %q fork point: %w",
				trajectory.ID,
				err,
			)
		}
		prepared, err = prepareNewTrajectoryFork(prepared, branch)
		if err != nil {
			return err
		}
		trajectory = prepared.Trajectory
		inheritedCount = prepared.InheritedEntryCount
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO ag_trajectories (
			namespace,
			id,
			schema_version,
			parent_id,
			parent_entry_id,
			created_at,
			updated_at,
			head,
			checkpoint,
			environment,
			inherited_entry_count,
			owned_entry_count
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, 0
		)`,
		store.namespace,
		trajectory.ID,
		trajectory.SchemaVersion,
		trajectory.ParentID,
		trajectory.ParentEntryID,
		trajectory.CreatedAt,
		trajectory.UpdatedAt,
		trajectory.Head,
		trajectory.Checkpoint,
		environmentJSON,
		inheritedCount,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf(
				"%w: %s",
				sdk.ErrTrajectoryExists,
				trajectory.ID,
			)
		}
		return fmt.Errorf(
			"insert PostgreSQL trajectory %q: %w",
			trajectory.ID,
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit PostgreSQL trajectory %q create: %w",
			trajectory.ID,
			err,
		)
	}
	return nil
}

func (store *TrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...sdk.TrajectoryEntry,
) (string, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	metadata, err := store.appendTrajectoryInTx(
		ctx,
		tx,
		sdk.TrajectoryAppendCommit{
			TrajectoryID: id,
			ExpectedHead: expectedHead,
			Entries:      entries,
		},
	)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return metadata.Head, nil
}

func (store *TrajectoryStore) appendTrajectoryInTx(
	ctx context.Context,
	tx pgx.Tx,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory, _, ownedCount, err := store.loadStoredTrajectory(
		ctx,
		tx,
		commit.TrajectoryID,
		true,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.Execution != nil && !trajectory.Execution.Terminal() {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has active execution %s",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
			trajectory.Execution.ID,
		)
	}
	if _, err := store.appendEntries(
		ctx,
		tx,
		trajectory,
		ownedCount,
		commit.ExpectedHead,
		commit.Entries,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return store.metadataInTx(ctx, tx, commit.TrajectoryID)
}

func (store *TrajectoryStore) LoadMetadata(
	ctx context.Context,
	id string,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory, inheritedCount, ownedCount, err :=
		store.loadStoredTrajectory(ctx, store.pool, id, false)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadata(
		trajectory,
		int(inheritedCount+ownedCount),
		int(ownedCount),
	), nil
}

func (store *TrajectoryStore) LoadEntry(
	ctx context.Context,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := sdk.ValidateResourceName(
		"trajectory entry",
		entryID,
	); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	entry, found, err := store.loadEntry(
		ctx,
		store.pool,
		id,
		entryID,
	)
	if err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if !found {
		return sdk.TrajectoryEntry{}, fmt.Errorf(
			"%w: trajectory %s entry %s",
			sdk.ErrTrajectoryEntryNotFound,
			id,
			entryID,
		)
	}
	return entry, nil
}

func (store *TrajectoryStore) LoadBranch(
	ctx context.Context,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return nil, err
	}
	return store.loadBranch(ctx, store.pool, id, head)
}

func (store *TrajectoryStore) FindLatest(
	ctx context.Context,
	id string,
	head string,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool, error) {
	if err := validateTrajectoryKind(kind); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if _, _, _, err := store.loadStoredTrajectory(
		ctx,
		store.pool,
		id,
		false,
	); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	return latestEntry(
		head,
		kind,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.loadEntry(ctx, store.pool, id, entryID)
		},
	)
}

func (store *TrajectoryStore) Load(
	ctx context.Context,
	id string,
) (sdk.Trajectory, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory, inheritedCount, ownedCount, err :=
		store.loadStoredTrajectory(ctx, store.pool, id, false)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory.Entries = make(
		[]sdk.TrajectoryEntry,
		0,
		int(inheritedCount+ownedCount),
	)
	if trajectory.ParentID != "" {
		inherited, err := store.loadBranch(
			ctx,
			store.pool,
			trajectory.ParentID,
			trajectory.ParentEntryID,
		)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = append(
			trajectory.Entries,
			inherited...,
		)
	}
	rows, err := store.pool.Query(
		ctx,
		`SELECT `+trajectoryEntryColumns+`
		 FROM ag_trajectory_entries
		 WHERE namespace = $1 AND trajectory_id = $2
		 ORDER BY ordinal`,
		store.namespace,
		id,
	)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	defer rows.Close()
	for rows.Next() {
		entry, err := scanPostgresTrajectoryEntry(rows)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = append(trajectory.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	return trajectory, nil
}

func (store *TrajectoryStore) List(
	ctx context.Context,
) ([]sdk.TrajectorySummary, error) {
	rows, err := store.pool.Query(
		ctx,
		`SELECT
			t.schema_version,
			t.id,
			t.parent_id,
			t.parent_entry_id,
			t.created_at,
			t.updated_at,
			t.head,
			t.checkpoint,
			COALESCE(e.execution_id, ''),
			COALESCE(e.state, ''),
			t.inherited_entry_count + t.owned_entry_count,
			t.owned_entry_count
		 FROM ag_trajectories t
		 LEFT JOIN ag_trajectory_executions e
		   ON e.namespace = t.namespace
		  AND e.trajectory_id = t.id
		 WHERE t.namespace = $1
		 ORDER BY t.created_at, t.id`,
		store.namespace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]sdk.TrajectorySummary, 0)
	for rows.Next() {
		item, err := scanTrajectorySummary(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func scanTrajectorySummary(scanner interface {
	Scan(...any) error
}) (sdk.TrajectorySummary, error) {
	var item sdk.TrajectorySummary
	if err := scanner.Scan(
		&item.SchemaVersion,
		&item.ID,
		&item.ParentID,
		&item.ParentEntryID,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.Head,
		&item.Checkpoint,
		&item.ExecutionID,
		&item.ExecutionState,
		&item.EntryCount,
		&item.OwnedEntryCount,
	); err != nil {
		return sdk.TrajectorySummary{}, err
	}
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
	return item, nil
}

func (store *TrajectoryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.TrajectoryPage, error) {
	request, err := normalizePageRequest(request)
	if err != nil {
		return sdk.TrajectoryPage{}, err
	}
	statement := `SELECT
		t.schema_version,
		t.id,
		t.parent_id,
		t.parent_entry_id,
		t.created_at,
		t.updated_at,
		t.head,
		t.checkpoint,
		COALESCE(e.execution_id, ''),
		COALESCE(e.state, ''),
		t.inherited_entry_count + t.owned_entry_count,
		t.owned_entry_count
	 FROM ag_trajectories t
	 LEFT JOIN ag_trajectory_executions e
	   ON e.namespace = t.namespace
	  AND e.trajectory_id = t.id
	 WHERE t.namespace = $1`
	args := []any{store.namespace}
	if request.After != "" {
		var createdAt time.Time
		if err := store.pool.QueryRow(
			ctx,
			`SELECT created_at
			 FROM ag_trajectories
			 WHERE namespace = $1 AND id = $2`,
			store.namespace,
			request.After,
		).Scan(&createdAt); errors.Is(err, pgx.ErrNoRows) {
			return sdk.TrajectoryPage{}, fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		} else if err != nil {
			return sdk.TrajectoryPage{}, err
		}
		statement += ` AND (
			t.created_at > $2
			OR (t.created_at = $2 AND t.id > $3)
		)`
		args = append(args, createdAt.UTC(), request.After)
	}
	statement += fmt.Sprintf(
		` ORDER BY t.created_at, t.id LIMIT $%d`,
		len(args)+1,
	)
	args = append(args, request.Limit+1)
	rows, err := store.pool.Query(ctx, statement, args...)
	if err != nil {
		return sdk.TrajectoryPage{}, err
	}
	defer rows.Close()
	items := make([]sdk.TrajectorySummary, 0, request.Limit+1)
	for rows.Next() {
		item, err := scanTrajectorySummary(rows)
		if err != nil {
			return sdk.TrajectoryPage{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return sdk.TrajectoryPage{}, err
	}
	next := ""
	if len(items) > request.Limit {
		items = items[:request.Limit]
		next = items[len(items)-1].ID
	}
	return sdk.TrajectoryPage{Items: items, Next: next}, nil
}

func (store *TrajectoryStore) Delete(
	ctx context.Context,
	id string,
) error {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	trajectory, _, _, err := store.loadStoredTrajectory(
		ctx,
		tx,
		id,
		true,
	)
	if err != nil {
		return err
	}
	if trajectory.Execution != nil && !trajectory.Execution.Terminal() {
		return fmt.Errorf(
			"%w: trajectory %s execution %s is active",
			sdk.ErrTrajectoryExecution,
			id,
			trajectory.Execution.ID,
		)
	}
	var childID string
	err = tx.QueryRow(
		ctx,
		`SELECT id
		 FROM ag_trajectories
		 WHERE namespace = $1 AND parent_id = $2
		 LIMIT 1`,
		store.namespace,
		id,
	).Scan(&childID)
	if err == nil {
		return fmt.Errorf(
			"%w: trajectory %s has live forks",
			sdk.ErrTrajectoryReferenced,
			id,
		)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	tag, err := tx.Exec(
		ctx,
		`DELETE FROM ag_trajectories
		 WHERE namespace = $1 AND id = $2`,
		store.namespace,
		id,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
	}
	return tx.Commit(ctx)
}

func (store *TrajectoryStore) loadEntry(
	ctx context.Context,
	query queryer,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	entry, err := scanPostgresTrajectoryEntry(query.QueryRow(
		ctx,
		`SELECT `+trajectoryEntryColumns+`
		 FROM ag_trajectory_entries
		 WHERE namespace = $1
		   AND trajectory_id = $2
		   AND entry_id = $3`,
		store.namespace,
		id,
		entryID,
	))
	if err == nil {
		return entry, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return sdk.TrajectoryEntry{}, false, err
	}
	trajectory, _, _, err := store.loadStoredTrajectory(
		ctx,
		query,
		id,
		false,
	)
	if err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if trajectory.ParentID == "" {
		return sdk.TrajectoryEntry{}, false, nil
	}
	return store.entryOnBranch(
		ctx,
		query,
		trajectory.ParentID,
		trajectory.ParentEntryID,
		entryID,
	)
}

func (store *TrajectoryStore) entryOnBranch(
	ctx context.Context,
	query queryer,
	id string,
	head string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	seen := make(map[string]struct{})
	for cursor := head; cursor != ""; {
		if _, cycle := seen[cursor]; cycle {
			return sdk.TrajectoryEntry{}, false, fmt.Errorf(
				"trajectory %q contains a cycle at %q",
				id,
				cursor,
			)
		}
		seen[cursor] = struct{}{}
		entry, found, err := store.loadEntry(
			ctx,
			query,
			id,
			cursor,
		)
		if err != nil {
			return sdk.TrajectoryEntry{}, false, err
		}
		if !found {
			return sdk.TrajectoryEntry{}, false, fmt.Errorf(
				"trajectory %q branch references unknown entry %q",
				id,
				cursor,
			)
		}
		if cursor == entryID {
			return entry, true, nil
		}
		cursor = entry.ParentID
	}
	return sdk.TrajectoryEntry{}, false, nil
}

func (store *TrajectoryStore) loadBranch(
	ctx context.Context,
	query queryer,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if _, _, _, err := store.loadStoredTrajectory(
		ctx,
		query,
		id,
		false,
	); err != nil {
		return nil, err
	}
	result := make([]sdk.TrajectoryEntry, 0)
	seen := make(map[string]struct{})
	for cursor := head; cursor != ""; {
		if _, cycle := seen[cursor]; cycle {
			return nil, fmt.Errorf(
				"trajectory %q contains a cycle at %q",
				id,
				cursor,
			)
		}
		seen[cursor] = struct{}{}
		entry, found, err := store.loadEntry(
			ctx,
			query,
			id,
			cursor,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf(
				"trajectory %q branch references unknown entry %q",
				id,
				cursor,
			)
		}
		result = append(result, entry)
		cursor = entry.ParentID
	}
	slices.Reverse(result)
	return result, nil
}
