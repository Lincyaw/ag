package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const duckDBCrashHelperPath = "AG_DUCKDB_CRASH_HELPER_PATH"

func TestDuckDBTrajectorySurvivesAbruptProcessExit(t *testing.T) {
	if path := os.Getenv(duckDBCrashHelperPath); path != "" {
		store, err := newDuckDBTrajectoryStore(path, "default")
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := store.Create(
			ctx,
			sdk.Trajectory{ID: "abrupt-exit"},
		); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginExecution(
			ctx,
			"abrupt-exit",
			"",
			sdk.TrajectoryExecutionStart{
				ID:       "abrupt-execution",
				Provider: "test-provider",
				MaxTurns: 2,
			},
			trajectoryTestEntry(
				"abrupt-input",
				"",
				sdk.TrajectoryKindUserMessage,
				`{"role":"user","content":"survive"}`,
			),
		); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}

	t.Parallel()
	path := filepath.Join(t.TempDir(), "abrupt.duckdb")
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestDuckDBTrajectorySurvivesAbruptProcessExit$",
	)
	command.Env = append(
		os.Environ(),
		duckDBCrashHelperPath+"="+path,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("abrupt DuckDB helper: %v\n%s", err, output)
	}
	reopened, err := newDuckDBTrajectoryStore(path, "default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close abrupt-exit DuckDB store: %v", err)
		}
	})
	metadata, err := reopened.LoadMetadata(t.Context(), "abrupt-exit")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Head != "abrupt-input" ||
		metadata.Execution == nil ||
		metadata.Execution.ID != "abrupt-execution" ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		t.Fatalf("abrupt-exit metadata = %#v", metadata)
	}
}

func TestDuckDBTrajectoryStorePersistsIndexedRecoverableExecution(
	t *testing.T,
) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "trajectory.duckdb")
	store, err := newDuckDBTrajectoryStore(path, "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(
		ctx,
		sdk.Trajectory{ID: "duckdb-recovery"},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginExecution(
		ctx,
		"duckdb-recovery",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "duckdb-execution",
			Provider: "indexed-provider",
			System:   "durable system",
			MaxTurns: 4,
		},
		trajectoryTestEntry(
			"duckdb-input",
			"",
			sdk.TrajectoryKindUserMessage,
			`{"role":"user","content":"resume from DuckDB"}`,
		),
	); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimExecution(
		ctx,
		"duckdb-recovery",
		"terminated-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := trajectoryTestEntry(
		"duckdb-provider-request",
		"duckdb-input",
		sdk.TrajectoryKindProviderRequest,
		`{"request":"detail"}`,
	)
	request.Fields.ExecutionID = claimed.ID
	request.Fields.OperationKey = "stable-indexed-operation"
	if _, err := store.CommitExecution(
		ctx,
		sdk.TrajectoryExecutionCommit{
			TrajectoryID: "duckdb-recovery",
			ExecutionID:  claimed.ID,
			LeaseToken:   claimed.LeaseToken,
			ExpectedHead: "duckdb-input",
			Entries:      []sdk.TrajectoryEntry{request},
		},
	); err != nil {
		t.Fatal(err)
	}
	analyzed, err := store.AnalyzeEntries(
		ctx,
		sdk.TrajectoryEntryQuery{
			OperationKey: "stable-indexed-operation",
			Kind:         sdk.TrajectoryKindProviderRequest,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(analyzed) != 1 ||
		analyzed[0].ID != "duckdb-provider-request" ||
		analyzed[0].Fields.Provider != "test-provider" {
		t.Fatalf("analyzed entries = %#v", analyzed)
	}
	var indexCount int
	indexDB, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer indexDB.Close()
	if err := indexDB.QueryRowContext(
		ctx,
		`SELECT count(*)
		 FROM duckdb_indexes()
		 WHERE index_name IN (
		   'ag_trajectory_entries_execution_idx',
		   'ag_trajectory_entries_operation_idx',
		   'ag_trajectory_executions_recovery_idx'
		 )`,
	).Scan(&indexCount); err != nil {
		t.Fatal(err)
	}
	if indexCount != 3 {
		t.Fatalf("trajectory analysis/recovery indexes = %d, want 3", indexCount)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if info.Size() == 0 {
		t.Fatal("DuckDB trajectory file is empty")
	}

	reopened, err := newDuckDBTrajectoryStore(path, "default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened DuckDB trajectory store: %v", err)
		}
	})
	metadata, err := reopened.LoadMetadata(ctx, "duckdb-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.State != sdk.TrajectoryExecutionRunning ||
		metadata.Execution.ID != "duckdb-execution" {
		t.Fatalf("reopened execution metadata = %#v", metadata.Execution)
	}
	recoverable, err := reopened.ListRecoverable(
		ctx,
		time.Now().UTC().Add(2*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 ||
		recoverable[0].ID != "duckdb-recovery" {
		t.Fatalf("recoverable after reopen = %#v", recoverable)
	}
}

func BenchmarkTrajectoryAppendAfterLargeHistory(b *testing.B) {
	for _, backend := range []string{"file", "duckdb"} {
		b.Run(backend, func(b *testing.B) {
			ctx := context.Background()
			var store sdk.TrajectoryStore
			switch backend {
			case "file":
				created, err := NewFileTrajectoryStore(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				store = created
			case "duckdb":
				created, err := newDuckDBTrajectoryStore(
					filepath.Join(b.TempDir(), "benchmark.duckdb"),
					"default",
				)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := created.Close(); err != nil {
						b.Errorf("close DuckDB benchmark: %v", err)
					}
				})
				store = created
			}
			if err := store.Create(
				ctx,
				sdk.Trajectory{ID: "benchmark"},
			); err != nil {
				b.Fatal(err)
			}
			head := ""
			seed := make([]sdk.TrajectoryEntry, 1000)
			for index := range seed {
				id := fmt.Sprintf("seed-%04d", index)
				seed[index] = trajectoryTestEntry(
					id,
					head,
					sdk.TrajectoryKindUserMessage,
					`{"text":"seed"}`,
				)
				head = id
			}
			var err error
			head, err = store.Append(ctx, "benchmark", "", seed...)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				id := fmt.Sprintf("append-%08d", index)
				head, err = store.Append(
					ctx,
					"benchmark",
					head,
					trajectoryTestEntry(
						id,
						head,
						sdk.TrajectoryKindUserMessage,
						`{"text":"append"}`,
					),
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDuckDBIndexedTrajectoryAnalysis(b *testing.B) {
	ctx := context.Background()
	store, err := newDuckDBTrajectoryStore(
		filepath.Join(b.TempDir(), "analysis.duckdb"),
		"default",
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close DuckDB analysis benchmark: %v", err)
		}
	})
	if err := store.Create(
		ctx,
		sdk.Trajectory{ID: "analysis"},
	); err != nil {
		b.Fatal(err)
	}
	head := ""
	seed := make([]sdk.TrajectoryEntry, 5000)
	for index := range seed {
		id := fmt.Sprintf("request-%05d", index)
		entry := trajectoryTestEntry(
			id,
			head,
			sdk.TrajectoryKindProviderRequest,
			`{"request":"seed"}`,
		)
		entry.Fields.OperationKey = fmt.Sprintf("operation-%05d", index)
		seed[index] = entry
		head = id
	}
	if _, err := store.Append(ctx, "analysis", "", seed...); err != nil {
		b.Fatal(err)
	}
	query := sdk.TrajectoryEntryQuery{
		OperationKey: "operation-04999",
		Kind:         sdk.TrajectoryKindProviderRequest,
		Limit:        1,
	}
	b.ResetTimer()
	for range b.N {
		entries, err := store.AnalyzeEntries(ctx, query)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 1 {
			b.Fatalf("analysis entries = %d", len(entries))
		}
	}
}

func TestDuckDBStorageDriverIsDurableAndNamespaceIsolated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "private", "agent-state.duckdb")
	uri := (&url.URL{
		Scheme:   "duckdb",
		Path:     path,
		RawQuery: url.Values{"namespace": {"tenant-a"}}.Encode(),
	}).String()
	backend, err := NewDefaultStorageRegistry().Open(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close first DuckDB namespace: %v", err)
		}
	})
	if backend.Namespace() != "tenant-a" {
		t.Fatalf("namespace = %q", backend.Namespace())
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	} else if permissions := info.Mode().Perm(); permissions != 0o700 {
		t.Fatalf("DuckDB state directory permissions = %o, want 700", permissions)
	}
	capabilities := backend.Capabilities()
	if !capabilities.Durable ||
		capabilities.MultiProcessSafe ||
		!capabilities.AtomicState ||
		!capabilities.NamespaceIsolation {
		t.Fatalf("DuckDB capabilities = %#v", capabilities)
	}
	if _, ok := backend.(sdk.AtomicStateBackend); !ok {
		t.Fatalf("DuckDB backend does not implement AtomicStateBackend")
	}
	if err := backend.Trajectories().Create(
		ctx,
		sdk.Trajectory{ID: "tenant-trajectory"},
	); err != nil {
		t.Fatal(err)
	}

	otherURI := (&url.URL{
		Scheme:   "duckdb",
		Path:     path,
		RawQuery: url.Values{"namespace": {"tenant-b"}}.Encode(),
	}).String()
	other, err := NewDefaultStorageRegistry().Open(ctx, otherURI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(context.Background()); err != nil {
			t.Errorf("close other DuckDB namespace: %v", err)
		}
	})
	if _, err := other.Trajectories().LoadMetadata(
		ctx,
		"tenant-trajectory",
	); !errors.Is(err, sdk.ErrTrajectoryNotFound) {
		t.Fatalf("other namespace load error = %v", err)
	}
	if _, err := backend.Trajectories().LoadMetadata(
		ctx,
		"tenant-trajectory",
	); err != nil {
		t.Fatalf("first namespace lost its trajectory: %v", err)
	}
}

func TestDuckDBStateBackendStoresDeliveriesInDuckDB(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "agent-state.duckdb")
	backend, err := NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := backend.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	delivery := sdk.Delivery{
		ID:           "duckdb-delivery-1",
		Plugin:       "observer",
		Subscription: "events",
		Partition:    "events/session",
		Event: sdk.Event{
			ID:      "duckdb-event-1",
			Name:    sdk.EventAgentStart,
			Payload: []byte(`{}`),
		},
		CreatedAt: base,
	}
	if err := outbox.Enqueue(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	firstLease, err := outbox.Lease(ctx, base, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(ctx); err != nil {
		t.Fatal(err)
	}
	deliveryJSON := filepath.Join(
		path+".state",
		"namespaces",
		"default",
		"deliveries",
		sdk.HostOutboxQueue,
		"deliveries.json",
	)
	if _, err := os.Stat(deliveryJSON); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DuckDB delivery unexpectedly used file sidecar %q: %v", deliveryJSON, err)
	}

	reopened, err := NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(context.Background()); err != nil {
			t.Errorf("close reopened DuckDB backend: %v", err)
		}
	})
	reopenedOutbox, err := reopened.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopenedOutbox.Lease(
		ctx,
		base.Add(30*time.Second),
		time.Minute,
	); !errors.Is(err, sdk.ErrNoDelivery) {
		t.Fatalf("unexpired DuckDB delivery lease was redelivered: %v", err)
	}
	secondLease, err := reopenedOutbox.Lease(
		ctx,
		base.Add(time.Minute),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if secondLease.Attempt != 2 || secondLease.Sequence != 1 ||
		secondLease.LeaseToken == firstLease.LeaseToken {
		t.Fatalf("recovered DuckDB delivery = %#v, first = %#v", secondLease, firstLease)
	}
	if err := reopenedOutbox.Ack(
		ctx,
		firstLease.ID,
		firstLease.LeaseToken,
		base.Add(time.Minute),
	); !errors.Is(err, sdk.ErrDeliveryLease) {
		t.Fatalf("stale DuckDB delivery ack error = %v", err)
	}
	if err := reopenedOutbox.Ack(
		ctx,
		secondLease.ID,
		secondLease.LeaseToken,
		base.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	listed, err := reopenedOutbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].State != sdk.DeliveryDelivered {
		t.Fatalf("DuckDB deliveries = %#v", listed)
	}
}

func TestDuckDBAtomicCommitRollsBackTrajectoryAndOutboxOnConflict(
	t *testing.T,
) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "agent-state.duckdb")
	backend, err := NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close DuckDB atomic backend: %v", err)
		}
	})
	atomicBackend, ok := backend.(sdk.AtomicStateBackend)
	if !ok {
		t.Fatalf("DuckDB backend does not implement AtomicStateBackend")
	}
	trajectoryID := "duckdb-atomic-trajectory"
	if err := backend.Trajectories().Create(
		ctx,
		sdk.Trajectory{ID: trajectoryID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Trajectories().BeginExecution(
		ctx,
		trajectoryID,
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "duckdb-atomic-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		trajectoryTestEntry(
			"duckdb-atomic-input",
			"",
			sdk.TrajectoryKindUserMessage,
			`{"role":"user","content":"atomic"}`,
		),
	); err != nil {
		t.Fatal(err)
	}
	execution, err := backend.Trajectories().ClaimExecution(
		ctx,
		trajectoryID,
		"duckdb-atomic-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := backend.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(
		ctx,
		duckDBTestDelivery("duckdb-atomic-conflict", "original-event"),
	); err != nil {
		t.Fatal(err)
	}
	checkpoint := trajectoryTestEntry(
		"duckdb-atomic-checkpoint",
		"duckdb-atomic-input",
		sdk.TrajectoryKindCheckpoint,
		`{"checkpoint":"state"}`,
	)
	mutation := sdk.ExecutionMutationCommit{
		Trajectory: sdk.TrajectoryExecutionCommit{
			TrajectoryID: trajectoryID,
			ExecutionID:  execution.ID,
			LeaseToken:   execution.LeaseToken,
			ExpectedHead: "duckdb-atomic-input",
			Entries:      []sdk.TrajectoryEntry{checkpoint},
		},
		Outbox: []sdk.StateMutationDeliveries{{
			Queue: sdk.HostOutboxQueue,
			Deliveries: []sdk.Delivery{
				duckDBTestDelivery(
					"duckdb-atomic-conflict",
					"different-event",
				),
			},
		}},
	}
	if _, err := atomicBackend.CommitExecution(ctx, mutation); err == nil {
		t.Fatal("conflicting DuckDB atomic outbox unexpectedly committed")
	}
	metadata, err := backend.Trajectories().LoadMetadata(ctx, trajectoryID)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Head != "duckdb-atomic-input" {
		t.Fatalf("DuckDB trajectory head after rollback = %q", metadata.Head)
	}
	outboxItems, err := outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(outboxItems) != 1 ||
		outboxItems[0].ID != "duckdb-atomic-conflict" ||
		outboxItems[0].Event.ID != "original-event" {
		t.Fatalf("DuckDB outbox after rollback = %#v", outboxItems)
	}

	mutation.Outbox[0].Deliveries[0] = duckDBTestDelivery(
		"duckdb-atomic-result",
		"result-event",
	)
	result, err := atomicBackend.CommitExecution(ctx, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if result.Trajectory.Head != "duckdb-atomic-checkpoint" {
		t.Fatalf("DuckDB atomic result = %#v", result)
	}
	outboxItems, err = outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(outboxItems) != 2 {
		t.Fatalf("committed DuckDB outbox = %#v", outboxItems)
	}
}

func TestDuckDBStateBackendStoresOperationsInDuckDB(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "agent-state.duckdb")
	backend, err := NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	invocation := sdk.Invocation{
		ID:          "child",
		RootID:      "root",
		ParentID:    "parent",
		GroupID:     "group",
		SessionID:   "session",
		ExecutionID: "execution",
	}
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             "duckdb-operation-1",
			IdempotencyKey: "stable-key",
		},
		Kind:             sdk.OperationKindTool,
		Resource:         "file",
		ResourceRevision: "rev1",
		Input:            []byte(`{"path":"a"}`),
		Invocation:       invocation,
	}
	submitted, created, err := backend.Operations().Submit(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("initial DuckDB operation submit was not created")
	}
	replay := record
	replay.Operation.ID = "ignored-operation-id"
	replayed, created, err := backend.Operations().Submit(ctx, replay)
	if err != nil {
		t.Fatal(err)
	}
	if created || replayed.Operation.ID != submitted.Operation.ID {
		t.Fatalf("DuckDB operation replay = %#v, created = %v", replayed, created)
	}
	rootOperations, err := backend.Operations().ListByInvocationRoot(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	if len(rootOperations) != 1 ||
		rootOperations[0].Operation.ID != submitted.Operation.ID {
		t.Fatalf("DuckDB operations by invocation root = %#v", rootOperations)
	}
	claimTime := time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)
	claimed, err := backend.Operations().Claim(
		ctx,
		submitted.Operation.ID,
		"worker-a",
		claimTime,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Execution == nil || claimed.Execution.Token == "" {
		t.Fatalf("DuckDB operation claim has no lease: %#v", claimed)
	}
	if err := backend.Close(ctx); err != nil {
		t.Fatal(err)
	}
	operationJSON := filepath.Join(
		path+".state",
		"namespaces",
		"default",
		"operations",
		"operations.json",
	)
	if _, err := os.Stat(operationJSON); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DuckDB operation unexpectedly used file sidecar %q: %v", operationJSON, err)
	}

	reopened, err := NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(context.Background()); err != nil {
			t.Errorf("close reopened DuckDB backend: %v", err)
		}
	})
	loaded, err := reopened.Operations().Get(ctx, submitted.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Operation.State != sdk.OperationRunning ||
		loaded.Execution == nil ||
		loaded.Execution.Token != claimed.Execution.Token {
		t.Fatalf("reopened DuckDB operation = %#v, claimed = %#v", loaded, claimed)
	}
	completed, err := reopened.Operations().Complete(
		ctx,
		submitted.Operation.ID,
		claimed.Execution.Token,
		sdk.OperationSucceeded,
		[]byte(`{"ok":true}`),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Operation.State != sdk.OperationSucceeded ||
		string(completed.Operation.Output) != `{"ok":true}` {
		t.Fatalf("completed DuckDB operation = %#v", completed)
	}
	if _, err := reopened.Operations().Fail(
		ctx,
		submitted.Operation.ID,
		claimed.Operation.Revision,
		"late failure",
	); !errors.Is(err, sdk.ErrOperationConflict) {
		t.Fatalf("stale DuckDB operation mutation error = %v", err)
	}
}

func duckDBTestDelivery(id string, eventID string) sdk.Delivery {
	return sdk.Delivery{
		ID:           id,
		Plugin:       "observer",
		Subscription: "events",
		Event: sdk.Event{
			ID:      eventID,
			Name:    sdk.EventAgentStart,
			Payload: []byte(`{}`),
		},
	}
}

func TestDuckDBStateBackendMigratesLegacyOperationSidecar(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "agent-state.duckdb")
	sidecarDirectory := filepath.Join(
		path+".state",
		"namespaces",
		"default",
		"operations",
	)
	legacy, err := NewFileOperationStore(sidecarDirectory)
	if err != nil {
		t.Fatal(err)
	}
	submitted, _, err := legacy.Submit(
		ctx,
		sdk.OperationRecord{
			Operation: sdk.Operation{
				ID:             "legacy-operation-1",
				IdempotencyKey: "legacy-key",
			},
			Kind:     sdk.OperationKindTool,
			Resource: "file",
			Input:    []byte(`{"path":"legacy"}`),
			Invocation: sdk.Invocation{
				ID:          "legacy-child",
				RootID:      "legacy-root",
				SessionID:   "legacy-session",
				ExecutionID: "legacy-execution",
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := legacy.Claim(
		ctx,
		submitted.Operation.ID,
		"legacy-worker",
		time.Date(2026, 7, 19, 14, 0, 0, 0, time.UTC),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}

	backend, err := NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close migrated DuckDB backend: %v", err)
		}
	})
	loaded, err := backend.Operations().Get(ctx, submitted.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Operation.Revision != claimed.Operation.Revision ||
		loaded.Operation.State != sdk.OperationRunning ||
		loaded.Execution == nil ||
		loaded.Execution.Token != claimed.Execution.Token {
		t.Fatalf("migrated DuckDB operation = %#v, legacy = %#v", loaded, claimed)
	}
	rootOperations, err := backend.Operations().ListByInvocationRoot(
		ctx,
		"legacy-root",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootOperations) != 1 ||
		rootOperations[0].Operation.ID != submitted.Operation.ID {
		t.Fatalf("migrated DuckDB root operations = %#v", rootOperations)
	}
	markerPath := filepath.Join(sidecarDirectory, ".migrated-to-duckdb")
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("DuckDB operation migration marker missing: %v", err)
	}
}
