package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lincyaw/ag/sdk"
)

type duckDBTrajectoryStore struct {
	db        *sql.DB
	namespace string
	writeMu   sync.RWMutex
	closeOnce sync.Once
	closeErr  error
}

// Store is the DuckDB implementation of sdk.TrajectoryStore.
type Store = duckDBTrajectoryStore

// NewTrajectoryStore opens a namespaced DuckDB trajectory store.
func NewTrajectoryStore(
	path string,
	namespace string,
) (*Store, error) {
	return newDuckDBTrajectoryStore(path, namespace)
}

func newDuckDBTrajectoryStore(
	path string,
	namespace string,
) (*duckDBTrajectoryStore, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open DuckDB trajectory store: %w", err)
	}
	maxConnections := max(4, runtime.GOMAXPROCS(0))
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(min(4, maxConnections))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping DuckDB trajectory store: %w", err)
	}
	if err := initDuckDBTrajectorySchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf(
			"restrict DuckDB trajectory file permissions: %w",
			err,
		)
	}
	return &duckDBTrajectoryStore{
		db:        db,
		namespace: namespace,
	}, nil
}

func (store *duckDBTrajectoryStore) Ping(ctx context.Context) error {
	if store == nil || store.db == nil {
		return errors.New("DuckDB trajectory store is not initialized")
	}
	return store.db.PingContext(ctx)
}

func (store *duckDBTrajectoryStore) Close() error {
	if store == nil {
		return nil
	}
	store.closeOnce.Do(func() {
		store.closeErr = store.db.Close()
	})
	return store.closeErr
}

func (store *duckDBTrajectoryStore) Create(
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
	environmentJSON, err := duckDBEnvironmentJSON(trajectory.Environment)
	if err != nil {
		return fmt.Errorf("encode trajectory environment: %w", err)
	}

	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin DuckDB trajectory create: %w", err)
	}
	defer tx.Rollback()
	if _, _, _, err := store.loadStoredTrajectory(
		ctx,
		tx,
		trajectory.ID,
	); err == nil {
		return fmt.Errorf("%w: %s", sdk.ErrTrajectoryExists, trajectory.ID)
	} else if !errors.Is(err, sdk.ErrTrajectoryNotFound) {
		return err
	}
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
	_, err = tx.ExecContext(
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
			environment_json,
			inherited_entry_count,
			owned_entry_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		store.namespace,
		trajectory.ID,
		trajectory.SchemaVersion,
		trajectory.ParentID,
		trajectory.ParentEntryID,
		trajectory.CreatedAt.UTC(),
		trajectory.UpdatedAt.UTC(),
		trajectory.Head,
		trajectory.Checkpoint,
		environmentJSON,
		inheritedCount,
	)
	if err != nil {
		return mapDuckDBTrajectoryWriteError(
			fmt.Errorf("insert DuckDB trajectory %q: %w", trajectory.ID, err),
		)
	}
	if err := tx.Commit(); err != nil {
		return mapDuckDBTrajectoryWriteError(
			fmt.Errorf("commit DuckDB trajectory %q create: %w", trajectory.ID, err),
		)
	}
	return nil
}

func (store *duckDBTrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...sdk.TrajectoryEntry,
) (string, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin DuckDB trajectory append: %w", err)
	}
	defer tx.Rollback()
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
	if err := tx.Commit(); err != nil {
		return "", mapDuckDBTrajectoryWriteError(err)
	}
	return metadata.Head, nil
}

func (store *duckDBTrajectoryStore) appendTrajectoryInTx(
	ctx context.Context,
	tx *sql.Tx,
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
	return store.metadataInTransaction(ctx, tx, commit.TrajectoryID)
}

func (store *duckDBTrajectoryStore) LoadMetadata(
	ctx context.Context,
	id string,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory, inheritedCount, ownedCount, err :=
		store.loadStoredTrajectory(ctx, store.db, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadata(
		trajectory,
		int(inheritedCount+ownedCount),
		int(ownedCount),
	), nil
}

func (store *duckDBTrajectoryStore) LoadEntry(
	ctx context.Context,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := sdk.ValidateResourceName("trajectory entry", entryID); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	entry, found, err := store.loadEntry(ctx, store.db, id, entryID)
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

func (store *duckDBTrajectoryStore) LoadBranch(
	ctx context.Context,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return store.loadBranch(ctx, store.db, id, head)
}

func (store *duckDBTrajectoryStore) FindLatest(
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
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if _, _, _, err := store.loadStoredTrajectory(
		ctx,
		store.db,
		id,
	); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	return latestEntry(
		head,
		kind,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.loadEntry(ctx, store.db, id, entryID)
		},
	)
}

func (store *duckDBTrajectoryStore) Load(
	ctx context.Context,
	id string,
) (sdk.Trajectory, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.Trajectory{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory, inheritedCount, ownedCount, err :=
		store.loadStoredTrajectory(ctx, store.db, id)
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
			store.db,
			trajectory.ParentID,
			trajectory.ParentEntryID,
		)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = append(trajectory.Entries, inherited...)
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBTrajectoryEntryColumns+`
		 FROM ag_trajectory_entries
		 WHERE namespace = ? AND trajectory_id = ?
		 ORDER BY ordinal`,
		store.namespace,
		id,
	)
	if err != nil {
		return sdk.Trajectory{}, fmt.Errorf(
			"load DuckDB trajectory %q entries: %w",
			id,
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		entry, err := scanDuckDBTrajectoryEntry(rows)
		if err != nil {
			return sdk.Trajectory{}, fmt.Errorf(
				"scan DuckDB trajectory %q entry: %w",
				id,
				err,
			)
		}
		trajectory.Entries = append(trajectory.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	return trajectory, nil
}

func (store *duckDBTrajectoryStore) List(
	ctx context.Context,
) ([]sdk.TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(
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
		 WHERE t.namespace = ?
		 ORDER BY t.created_at, t.id`,
		store.namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("list DuckDB trajectories: %w", err)
	}
	defer rows.Close()
	result := make([]sdk.TrajectorySummary, 0)
	for rows.Next() {
		var item sdk.TrajectorySummary
		if err := rows.Scan(
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
			return nil, fmt.Errorf("scan DuckDB trajectory summary: %w", err)
		}
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *duckDBTrajectoryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.TrajectoryPage, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryPage{}, err
	}
	if request.Limit == 0 {
		request.Limit = sdk.DefaultPageSize
	}
	if request.Limit < 0 {
		return sdk.TrajectoryPage{}, errors.New(
			"page limit cannot be negative",
		)
	}
	if request.Limit > sdk.MaxPageSize {
		return sdk.TrajectoryPage{}, fmt.Errorf(
			"page limit %d exceeds maximum %d",
			request.Limit,
			sdk.MaxPageSize,
		)
	}
	query := `SELECT
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
	 WHERE t.namespace = ?`
	args := []any{store.namespace}
	if request.After != "" {
		var createdAt time.Time
		if err := store.db.QueryRowContext(
			ctx,
			`SELECT created_at
			 FROM ag_trajectories
			 WHERE namespace = ? AND id = ?`,
			store.namespace,
			request.After,
		).Scan(&createdAt); errors.Is(err, sql.ErrNoRows) {
			return sdk.TrajectoryPage{}, fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		} else if err != nil {
			return sdk.TrajectoryPage{}, err
		}
		query += ` AND (
			t.created_at > ?
			OR (t.created_at = ? AND t.id > ?)
		)`
		args = append(
			args,
			createdAt.UTC(),
			createdAt.UTC(),
			request.After,
		)
	}
	query += ` ORDER BY t.created_at, t.id LIMIT ?`
	args = append(args, request.Limit+1)
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return sdk.TrajectoryPage{}, err
	}
	defer rows.Close()
	items := make([]sdk.TrajectorySummary, 0, request.Limit+1)
	for rows.Next() {
		var item sdk.TrajectorySummary
		if err := rows.Scan(
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
			return sdk.TrajectoryPage{}, err
		}
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
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

func (store *duckDBTrajectoryStore) Delete(
	ctx context.Context,
	id string,
) error {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	trajectory, _, _, err := store.loadStoredTrajectory(ctx, tx, id)
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
	var children int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT count(*)
		 FROM ag_trajectories
		 WHERE namespace = ? AND parent_id = ?`,
		store.namespace,
		id,
	).Scan(&children); err != nil {
		return err
	}
	if children > 0 {
		return fmt.Errorf(
			"%w: trajectory %s has live forks",
			sdk.ErrTrajectoryReferenced,
			id,
		)
	}
	for _, statement := range []string{
		`DELETE FROM ag_trajectory_entries
		 WHERE namespace = ? AND trajectory_id = ?`,
		`DELETE FROM ag_trajectory_executions
		 WHERE namespace = ? AND trajectory_id = ?`,
		`DELETE FROM ag_trajectories
		 WHERE namespace = ? AND id = ?`,
	} {
		if _, err := tx.ExecContext(
			ctx,
			statement,
			store.namespace,
			id,
		); err != nil {
			return mapDuckDBTrajectoryWriteError(err)
		}
	}
	return mapDuckDBTrajectoryWriteError(tx.Commit())
}

func (store *duckDBTrajectoryStore) loadEntry(
	ctx context.Context,
	queryer duckDBQueryer,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	row := queryer.QueryRowContext(
		ctx,
		`SELECT `+duckDBTrajectoryEntryColumns+`
		 FROM ag_trajectory_entries
		 WHERE namespace = ?
		   AND trajectory_id = ?
		   AND entry_id = ?`,
		store.namespace,
		id,
		entryID,
	)
	entry, err := scanDuckDBTrajectoryEntry(row)
	if err == nil {
		return entry, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return sdk.TrajectoryEntry{}, false, fmt.Errorf(
			"load DuckDB trajectory %q entry %q: %w",
			id,
			entryID,
			err,
		)
	}
	trajectory, _, _, err := store.loadStoredTrajectory(ctx, queryer, id)
	if err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if trajectory.ParentID == "" {
		return sdk.TrajectoryEntry{}, false, nil
	}
	return store.entryOnBranch(
		ctx,
		queryer,
		trajectory.ParentID,
		trajectory.ParentEntryID,
		entryID,
	)
}

func (store *duckDBTrajectoryStore) entryOnBranch(
	ctx context.Context,
	queryer duckDBQueryer,
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
			queryer,
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

func (store *duckDBTrajectoryStore) loadBranch(
	ctx context.Context,
	queryer duckDBQueryer,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if _, _, _, err := store.loadStoredTrajectory(
		ctx,
		queryer,
		id,
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
			queryer,
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

func mapDuckDBTrajectoryWriteError(err error) error {
	if err == nil {
		return nil
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "transaction conflict") ||
		strings.Contains(lower, "duplicate key") ||
		strings.Contains(lower, "constraint error") {
		return fmt.Errorf("%w: %v", sdk.ErrTrajectoryConflict, err)
	}
	return err
}
