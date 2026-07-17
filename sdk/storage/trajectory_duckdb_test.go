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
		capabilities.AtomicState ||
		!capabilities.NamespaceIsolation {
		t.Fatalf("DuckDB capabilities = %#v", capabilities)
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
