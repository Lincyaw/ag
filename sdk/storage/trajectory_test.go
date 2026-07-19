package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/filestate"
	"github.com/lincyaw/ag/sdk"
)

func TestTrajectoryStoresValidateTrajectoryIDs(t *testing.T) {
	t.Parallel()
	const invalidID = "invalid/id"
	want := sdk.ValidateResourceName("trajectory", invalidID)
	for name, factory := range trajectoryStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := factory(t).LoadMetadata(t.Context(), invalidID)
			if err == nil || err.Error() != want.Error() {
				t.Fatalf("LoadMetadata() error = %v, want %v", err, want)
			}
		})
	}
}

func TestTrajectoryStoresValidateForkReferences(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		parentID   string
		parentHead string
		want       error
	}{
		{
			name:       "parent trajectory",
			parentID:   "../parent",
			parentHead: "fork-point",
			want: sdk.ValidateResourceName(
				"trajectory parent",
				"../parent",
			),
		},
		{
			name:       "parent entry",
			parentID:   "parent",
			parentHead: "../fork-point",
			want: sdk.ValidateResourceName(
				"trajectory parent entry",
				"../fork-point",
			),
		},
	}
	for storeName, factory := range trajectoryStoreFactories() {
		for _, test := range tests {
			t.Run(storeName+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				store := factory(t)
				err := store.Create(t.Context(), sdk.Trajectory{
					ID:            "child",
					ParentID:      test.parentID,
					ParentEntryID: test.parentHead,
				})
				if err == nil || err.Error() != test.want.Error() {
					t.Fatalf("Create() error = %v, want %v", err, test.want)
				}
			})
		}
	}
}

func TestTrajectoryStoresPreserveBranchesAndRejectConcurrentLostUpdates(
	t *testing.T,
) {
	t.Parallel()
	for name, factory := range trajectoryStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := factory(t)
			if err := store.Create(ctx, sdk.Trajectory{ID: "session-main"}); err != nil {
				t.Fatal(err)
			}

			head, err := store.Append(
				ctx,
				"session-main",
				"",
				trajectoryTestEntry("user-1", "", sdk.TrajectoryKindUserMessage, `{"text":"hello"}`),
				trajectoryTestEntry("checkpoint-1", "user-1", sdk.TrajectoryKindCheckpoint, `{"turn":1}`),
				trajectoryTestEntry("partial-tool", "checkpoint-1", sdk.TrajectoryKindToolCall, `{"name":"write"}`),
			)
			if err != nil {
				t.Fatal(err)
			}
			if head != "partial-tool" {
				t.Fatalf("head = %q", head)
			}

			rollback := trajectoryTestEntry(
				"rollback-1",
				"checkpoint-1",
				sdk.TrajectoryKindRollback,
				`{"from":"partial-tool","to":"checkpoint-1"}`,
			)
			head, err = store.Append(ctx, "session-main", head, rollback)
			if err != nil {
				t.Fatal(err)
			}

			const writers = 32
			var successes atomic.Int32
			var conflicts atomic.Int32
			var wait sync.WaitGroup
			start := make(chan struct{})
			for index := range writers {
				wait.Add(1)
				go func(index int) {
					defer wait.Done()
					<-start
					entry := trajectoryTestEntry(
						fmt.Sprintf("writer-%02d", index),
						head,
						sdk.TrajectoryKindUserMessage,
						fmt.Sprintf(`{"writer":%d}`, index),
					)
					if _, appendErr := store.Append(
						ctx,
						"session-main",
						head,
						entry,
					); appendErr == nil {
						successes.Add(1)
					} else if errors.Is(appendErr, sdk.ErrTrajectoryConflict) {
						conflicts.Add(1)
					} else {
						t.Errorf("writer %d: %v", index, appendErr)
					}
				}(index)
			}
			close(start)
			wait.Wait()
			if successes.Load() != 1 || conflicts.Load() != writers-1 {
				t.Fatalf(
					"successes=%d conflicts=%d",
					successes.Load(),
					conflicts.Load(),
				)
			}

			trajectory, err := store.Load(ctx, "session-main")
			if err != nil {
				t.Fatal(err)
			}
			if len(trajectory.Entries) != 5 {
				t.Fatalf("entries=%d, want 5", len(trajectory.Entries))
			}
			rollbackBranch, err := trajectory.Branch("rollback-1")
			if err != nil {
				t.Fatal(err)
			}
			gotIDs := make([]string, 0, len(rollbackBranch))
			for _, entry := range rollbackBranch {
				gotIDs = append(gotIDs, entry.ID)
			}
			wantIDs := []string{"user-1", "checkpoint-1", "rollback-1"}
			if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
				t.Fatalf("rollback branch = %v, want %v", gotIDs, wantIDs)
			}
			for _, entry := range rollbackBranch {
				if entry.ID == "partial-tool" {
					t.Fatal("abandoned partial entry remained on rollback branch")
				}
			}

			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			before := trajectory.Head
			_, err = store.Append(
				cancelled,
				trajectory.ID,
				before,
				trajectoryTestEntry("cancelled", before, sdk.TrajectoryKindTerminal, `{}`),
			)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled append error = %v", err)
			}
			after, err := store.Load(ctx, trajectory.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Head != before || len(after.Entries) != len(trajectory.Entries) {
				t.Fatal("cancelled append mutated trajectory")
			}
		})
	}
}

func TestTrajectoryStoresRoundTripEntryAudit(t *testing.T) {
	t.Parallel()
	for name, factory := range trajectoryStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			store := factory(t)
			if err := store.Create(ctx, sdk.Trajectory{ID: "audit-roundtrip"}); err != nil {
				t.Fatal(err)
			}
			blockStep := 0
			entry := trajectoryTestEntry(
				"audited-entry",
				"",
				sdk.TrajectoryKindProviderRequest,
				`{"request":"detail"}`,
			)
			entry.Audit = []sdk.EventAudit{{
				EventID:    "event-1",
				EventName:  sdk.EventBeforeProvider,
				Generation: 3,
				InputHash:  "sha256:input",
				OutputHash: "sha256:output",
				Steps: []sdk.HookAuditStep{{
					Index:         0,
					Plugin:        "policy",
					PluginVersion: "1.0.0",
					Hook:          "rewrite-system",
					Priority:      sdk.PriorityPre,
					Sequence:      7,
					FailurePolicy: sdk.FailurePolicyFailClosed,
					Duration:      time.Millisecond,
					InputHash:     "sha256:input",
					OutputHash:    "sha256:output",
					PatchFields:   []string{"system"},
					Overwrites:    []string{"system"},
					Block: &sdk.BlockSummary{
						Reason: "blocked",
						Kind:   "policy",
					},
					Action: &sdk.ActionSummary{
						Kind:         sdk.ActionStop,
						CauseCode:    "policy_stop",
						CauseFinal:   true,
						MessageCount: 2,
					},
					Outcome: sdk.HookAuditBlocked,
				}},
				Resolution: sdk.EffectResolution{
					Outcome:     sdk.EffectResolutionBlocked,
					Block:       &sdk.BlockSummary{Reason: "blocked", Kind: "policy"},
					BlockStep:   &blockStep,
					PatchFields: []string{"system"},
				},
			}}
			wantAudit := sdk.CloneEventAudits(entry.Audit)
			if _, err := store.Append(ctx, "audit-roundtrip", "", entry); err != nil {
				t.Fatal(err)
			}
			loaded, err := store.LoadEntry(ctx, "audit-roundtrip", entry.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(loaded.Audit, wantAudit) {
				t.Fatalf("loaded audit = %#v, want %#v", loaded.Audit, wantAudit)
			}
			loaded.Audit[0].Steps[0].PatchFields[0] = "mutated"
			reloaded, err := store.LoadEntry(ctx, "audit-roundtrip", entry.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(reloaded.Audit, wantAudit) {
				t.Fatalf("reloaded audit after caller mutation = %#v", reloaded.Audit)
			}
		})
	}
}

func TestTrajectoryStoresUseCopyOnWriteForksAndTargetedReads(t *testing.T) {
	t.Parallel()
	for name, factory := range trajectoryStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			store := factory(t)
			if err := store.Create(ctx, sdk.Trajectory{ID: "source"}); err != nil {
				t.Fatal(err)
			}
			sourceHead, err := store.Append(
				ctx,
				"source",
				"",
				trajectoryTestEntry(
					"source-message",
					"",
					sdk.TrajectoryKindUserMessage,
					`{"text":"shared"}`,
				),
				trajectoryTestEntry(
					"source-checkpoint",
					"source-message",
					sdk.TrajectoryKindCheckpoint,
					`{"messages":[]}`,
				),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Create(ctx, sdk.Trajectory{
				ID:            "fork",
				ParentID:      "source",
				ParentEntryID: sourceHead,
			}); err != nil {
				t.Fatal(err)
			}

			metadata, err := store.LoadMetadata(ctx, "fork")
			if err != nil {
				t.Fatal(err)
			}
			if metadata.Head != sourceHead ||
				metadata.Checkpoint != sourceHead ||
				metadata.EntryCount != 2 ||
				metadata.OwnedEntryCount != 0 {
				t.Fatalf("initial fork metadata = %#v", metadata)
			}

			if _, err := store.Append(
				ctx,
				"source",
				sourceHead,
				trajectoryTestEntry(
					"source-later",
					sourceHead,
					sdk.TrajectoryKindUserMessage,
					`{"text":"source only"}`,
				),
			); err != nil {
				t.Fatal(err)
			}
			forkHead, err := store.Append(
				ctx,
				"fork",
				sourceHead,
				trajectoryTestEntry(
					"fork-message",
					sourceHead,
					sdk.TrajectoryKindUserMessage,
					`{"text":"fork only"}`,
				),
			)
			if err != nil {
				t.Fatal(err)
			}

			branch, err := store.LoadBranch(ctx, "fork", forkHead)
			if err != nil {
				t.Fatal(err)
			}
			gotIDs := make([]string, 0, len(branch))
			for _, entry := range branch {
				gotIDs = append(gotIDs, entry.ID)
			}
			wantIDs := []string{
				"source-message",
				"source-checkpoint",
				"fork-message",
			}
			if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
				t.Fatalf("fork branch = %v, want %v", gotIDs, wantIDs)
			}
			inherited, err := store.LoadEntry(ctx, "fork", "source-message")
			if err != nil {
				t.Fatal(err)
			}
			local, err := store.LoadEntry(ctx, "fork", "fork-message")
			if err != nil {
				t.Fatal(err)
			}
			if inherited.TrajectoryID != "source" ||
				inherited.Ordinal != 1 ||
				inherited.Depth != 1 {
				t.Fatalf("inherited entry fields = %#v", inherited)
			}
			if local.TrajectoryID != "fork" ||
				local.Ordinal != 1 ||
				local.Depth != 3 {
				t.Fatalf("local entry fields = %#v", local)
			}
			checkpoint, found, err := store.FindLatest(
				ctx,
				"fork",
				forkHead,
				sdk.TrajectoryKindCheckpoint,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !found || checkpoint.ID != sourceHead {
				t.Fatalf("latest checkpoint = %#v found=%v", checkpoint, found)
			}

			metadata, err = store.LoadMetadata(ctx, "fork")
			if err != nil {
				t.Fatal(err)
			}
			if metadata.EntryCount != 3 || metadata.OwnedEntryCount != 1 {
				t.Fatalf("fork metadata after append = %#v", metadata)
			}
			if err := store.Delete(ctx, "source"); !errors.Is(
				err,
				sdk.ErrTrajectoryReferenced,
			) {
				t.Fatalf("delete referenced source error = %v", err)
			}
			if err := store.Delete(ctx, "fork"); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(ctx, "source"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTrajectoryStoresCommitRecoverableExecutionAtomically(t *testing.T) {
	t.Parallel()
	for name, factory := range trajectoryStoreFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			store := factory(t)
			if err := store.Create(ctx, sdk.Trajectory{ID: "transactional"}); err != nil {
				t.Fatal(err)
			}
			metadata, err := store.BeginExecution(
				ctx,
				"transactional",
				"",
				sdk.TrajectoryExecutionStart{
					ID:       "execution-1",
					Provider: "test-provider",
					System:   "test-system",
					MaxTurns: 4,
				},
				trajectoryTestEntry(
					"input-1",
					"",
					sdk.TrajectoryKindUserMessage,
					`{"role":"user","content":"continue durably"}`,
				),
			)
			if err != nil {
				t.Fatal(err)
			}
			if metadata.Head != "input-1" ||
				metadata.Execution == nil ||
				metadata.Execution.State != sdk.TrajectoryExecutionPending ||
				metadata.Execution.InputEntryID != "input-1" {
				t.Fatalf("execution begin metadata = %#v", metadata)
			}
			input, err := store.LoadEntry(ctx, "transactional", "input-1")
			if err != nil {
				t.Fatal(err)
			}
			if input.Fields.ExecutionID != "execution-1" {
				t.Fatalf(
					"durable input execution ID = %q",
					input.Fields.ExecutionID,
				)
			}
			if err := store.Delete(
				ctx,
				"transactional",
			); !errors.Is(err, sdk.ErrTrajectoryExecution) {
				t.Fatalf("delete active trajectory error = %v", err)
			}
			if _, err := store.Append(
				ctx,
				"transactional",
				"input-1",
				trajectoryTestEntry(
					"unfenced",
					"input-1",
					sdk.TrajectoryKindAgentStart,
					`{}`,
				),
			); !errors.Is(err, sdk.ErrTrajectoryExecution) {
				t.Fatalf("unfenced append error = %v", err)
			}

			past := time.Now().UTC().Add(-time.Minute)
			first, err := store.ClaimExecution(
				ctx,
				"transactional",
				"worker-1",
				past,
				time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			second, err := store.ClaimExecution(
				ctx,
				"transactional",
				"worker-2",
				time.Now().UTC(),
				time.Minute,
			)
			if err != nil {
				t.Fatal(err)
			}
			if first.LeaseToken == second.LeaseToken ||
				second.Owner != "worker-2" ||
				second.Revision <= first.Revision {
				t.Fatalf("reclaimed execution first=%#v second=%#v", first, second)
			}
			if _, err := store.CommitExecution(
				ctx,
				sdk.TrajectoryExecutionCommit{
					TrajectoryID: "transactional",
					ExecutionID:  first.ID,
					LeaseToken:   first.LeaseToken,
					ExpectedHead: "input-1",
					State:        sdk.TrajectoryExecutionSucceeded,
				},
			); !errors.Is(err, sdk.ErrTrajectoryFence) {
				t.Fatalf("stale execution commit error = %v", err)
			}

			checkpoint := trajectoryTestEntry(
				"checkpoint-1",
				"input-1",
				sdk.TrajectoryKindCheckpoint,
				`{"messages":[]}`,
			)
			checkpoint.Fields.ExecutionID = second.ID
			committed, err := store.CommitExecution(
				ctx,
				sdk.TrajectoryExecutionCommit{
					TrajectoryID: "transactional",
					ExecutionID:  second.ID,
					LeaseToken:   second.LeaseToken,
					ExpectedHead: "input-1",
					Entries:      []sdk.TrajectoryEntry{checkpoint},
					State:        sdk.TrajectoryExecutionSucceeded,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if committed.Head != "checkpoint-1" ||
				committed.Checkpoint != "checkpoint-1" ||
				committed.Execution == nil ||
				committed.Execution.State != sdk.TrajectoryExecutionSucceeded ||
				committed.EntryCount != 2 {
				t.Fatalf("atomic execution commit = %#v", committed)
			}
			if recoverable, err := store.ListRecoverable(
				ctx,
				time.Now().UTC(),
			); err != nil {
				t.Fatal(err)
			} else if len(recoverable) != 0 {
				t.Fatalf("terminal recoverable executions = %#v", recoverable)
			}
		})
	}
}

func TestFileTrajectoryStoreFindsPendingExecutionAfterReopen(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	directory := t.TempDir()
	first, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Create(ctx, sdk.Trajectory{ID: "recoverable"}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.BeginExecution(
		ctx,
		"recoverable",
		"",
		sdk.TrajectoryExecutionStart{
			ID: "execution", MaxTurns: 2,
		},
		trajectoryTestEntry(
			"input",
			"",
			sdk.TrajectoryKindUserMessage,
			`{"role":"user","content":"resume me"}`,
		),
	); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	recoverable, err := reopened.ListRecoverable(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 ||
		recoverable[0].ID != "recoverable" ||
		recoverable[0].Execution == nil ||
		recoverable[0].Execution.ID != "execution" {
		t.Fatalf("recoverable executions after reopen = %#v", recoverable)
	}
}

func TestFileTrajectoryStoreSurvivesReopenWithLineage(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	first, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := first.Create(ctx, sdk.Trajectory{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(
		ctx,
		"source",
		"",
		trajectoryTestEntry("source-checkpoint", "", sdk.TrajectoryKindCheckpoint, `{"messages":[]}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := first.Create(ctx, sdk.Trajectory{
		ID:            "fork",
		ParentID:      "source",
		ParentEntryID: "source-checkpoint",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(
		ctx,
		"fork",
		"source-checkpoint",
		trajectoryTestEntry(
			"fork-message",
			"source-checkpoint",
			sdk.TrajectoryKindUserMessage,
			`{"text":"new path"}`,
		),
	); err != nil {
		t.Fatal(err)
	}

	second, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := second.Load(ctx, "fork")
	if err != nil {
		t.Fatal(err)
	}
	if fork.ParentID != "source" || fork.ParentEntryID != "source-checkpoint" {
		t.Fatalf("lost fork lineage: %#v", fork)
	}
	if fork.Head != "fork-message" || fork.Checkpoint != "source-checkpoint" {
		t.Fatalf("fork pointers = head %q checkpoint %q", fork.Head, fork.Checkpoint)
	}
	branch, err := second.LoadBranch(ctx, "fork", fork.Head)
	if err != nil {
		t.Fatal(err)
	}
	if len(branch) != 2 ||
		branch[0].ID != "source-checkpoint" ||
		branch[0].TrajectoryID != "source" ||
		branch[1].ID != "fork-message" ||
		branch[1].TrajectoryID != "fork" {
		t.Fatalf("fork branch = %#v", branch)
	}
	if err := second.Delete(ctx, "source"); !errors.Is(
		err,
		sdk.ErrTrajectoryReferenced,
	) {
		t.Fatalf("delete referenced source error = %v", err)
	}
	summaries, err := second.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 || summaries[1].ID != "fork" {
		t.Fatalf("summaries = %#v", summaries)
	}
}

func TestFileTrajectoryStoreReadsSchemaV1AsFixedEntryEnvelope(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	now := time.Now().UTC()
	legacy := sdk.Trajectory{
		SchemaVersion: 1,
		ID:            "legacy",
		CreatedAt:     now,
		UpdatedAt:     now,
		Head:          "legacy-checkpoint",
		Entries: []sdk.TrajectoryEntry{{
			ID:             "legacy-checkpoint",
			Kind:           sdk.TrajectoryKindCheckpoint,
			Timestamp:      now,
			PayloadVersion: 1,
			Payload:        json.RawMessage(`{"messages":[]}`),
		}},
	}
	if err := filestate.WriteJSON(
		t.Context(),
		directory,
		filepath.Join(directory, "legacy.json"),
		"legacy trajectory",
		legacy,
	); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.LoadMetadata(t.Context(), legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.SchemaVersion != 1 ||
		metadata.Checkpoint != legacy.Head ||
		metadata.EntryCount != 1 {
		t.Fatalf("legacy metadata = %#v", metadata)
	}
	entry, err := store.LoadEntry(t.Context(), legacy.ID, legacy.Head)
	if err != nil {
		t.Fatal(err)
	}
	if entry.TrajectoryID != legacy.ID ||
		entry.Ordinal != 1 ||
		entry.Depth != 1 {
		t.Fatalf("normalized legacy entry = %#v", entry)
	}
}

func TestMemoryTrajectoryStoreKeepsEntriesIncremental(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := newMemoryTrajectoryStore()
	if err := store.Create(ctx, sdk.Trajectory{ID: "incremental"}); err != nil {
		t.Fatal(err)
	}
	head := ""
	for index := range 1000 {
		entryID := fmt.Sprintf("entry-%04d", index)
		next, err := store.Append(
			ctx,
			"incremental",
			head,
			trajectoryTestEntry(
				entryID,
				head,
				sdk.TrajectoryKindUserMessage,
				`{"text":"value"}`,
			),
		)
		if err != nil {
			t.Fatal(err)
		}
		head = next
	}

	store.mu.RLock()
	stored := store.trajectories["incremental"]
	headerEntries := len(stored.trajectory.Entries)
	entryCount := len(stored.entries)
	orderCount := len(stored.order)
	store.mu.RUnlock()
	if headerEntries != 0 || entryCount != 1000 || orderCount != 1000 {
		t.Fatalf(
			"stored header entries=%d map=%d order=%d",
			headerEntries,
			entryCount,
			orderCount,
		)
	}

	metadata, err := store.LoadMetadata(ctx, "incremental")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Head != head ||
		metadata.EntryCount != 1000 ||
		metadata.OwnedEntryCount != 1000 {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func trajectoryStoreFactories() map[string]func(*testing.T) sdk.TrajectoryStore {
	factories := map[string]func(*testing.T) sdk.TrajectoryStore{
		"memory": func(*testing.T) sdk.TrajectoryStore {
			return NewMemoryTrajectoryStore()
		},
		"file": func(t *testing.T) sdk.TrajectoryStore {
			store, err := NewFileTrajectoryStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"duckdb": func(t *testing.T) sdk.TrajectoryStore {
			store, err := newDuckDBTrajectoryStore(
				filepath.Join(t.TempDir(), "trajectories.duckdb"),
				"default",
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Errorf("close DuckDB trajectory store: %v", err)
				}
			})
			return store
		},
	}
	if dsn := os.Getenv("AG_TEST_POSTGRES_DSN"); dsn != "" {
		factories["postgres"] = func(t *testing.T) sdk.TrajectoryStore {
			parsed, err := url.Parse(dsn)
			if err != nil {
				t.Fatal(err)
			}
			query := parsed.Query()
			query.Set("namespace", "contract-"+sdk.NewID())
			parsed.RawQuery = query.Encode()
			backend, err := NewDefaultStorageRegistry().Open(
				t.Context(),
				parsed.String(),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := backend.Close(
					context.Background(),
				); err != nil {
					t.Errorf(
						"close PostgreSQL trajectory store: %v",
						err,
					)
				}
			})
			return backend.Trajectories()
		}
	}
	return factories
}

func trajectoryTestEntry(
	id string,
	parent string,
	kind sdk.TrajectoryKind,
	payload string,
) sdk.TrajectoryEntry {
	fields := sdk.TrajectoryEntryFields{}
	switch kind {
	case sdk.TrajectoryKindProviderRequest:
		turn := 0
		fields.Turn = &turn
		fields.Provider = "test-provider"
		fields.OperationKey = id + "-operation"
	case sdk.TrajectoryKindProviderResponse:
		turn := 0
		isError := false
		fields.Turn = &turn
		fields.Provider = "test-provider"
		fields.IsError = &isError
	case sdk.TrajectoryKindToolCall:
		turn := 0
		fields.Turn = &turn
		fields.ToolName = "test-tool"
		fields.ToolCallID = id
		fields.OperationKey = id + "-operation"
	case sdk.TrajectoryKindToolResult:
		turn := 0
		isError := false
		fields.Turn = &turn
		fields.ToolName = "test-tool"
		fields.ToolCallID = id
		fields.IsError = &isError
	case sdk.TrajectoryKindDecision:
		turn := 0
		fields.Turn = &turn
	}
	return sdk.TrajectoryEntry{
		ID:        id,
		ParentID:  parent,
		Kind:      kind,
		Timestamp: time.Now().UTC(),
		Fields:    fields,
		Payload:   json.RawMessage(payload),
	}
}
