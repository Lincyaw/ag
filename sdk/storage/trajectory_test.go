package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/lincyaw/ag/sdk"
)

func TestTrajectoryStoresPreserveBranchesAndRejectConcurrentLostUpdates(
	t *testing.T,
) {
	t.Parallel()
	factories := map[string]func(*testing.T) TrajectoryStore{
		"memory": func(*testing.T) TrajectoryStore {
			return NewMemoryTrajectoryStore()
		},
		"file": func(t *testing.T) TrajectoryStore {
			store, err := NewFileTrajectoryStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := factory(t)
			if err := store.Create(ctx, Trajectory{ID: "session-main"}); err != nil {
				t.Fatal(err)
			}

			head, err := store.Append(
				ctx,
				"session-main",
				"",
				trajectoryTestEntry("user-1", "", TrajectoryKindUserMessage, `{"text":"hello"}`),
				trajectoryTestEntry("checkpoint-1", "user-1", TrajectoryKindCheckpoint, `{"turn":1}`),
				trajectoryTestEntry("partial-tool", "checkpoint-1", TrajectoryKindToolCall, `{"name":"write"}`),
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
				TrajectoryKindRollback,
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
						TrajectoryKindUserMessage,
						fmt.Sprintf(`{"writer":%d}`, index),
					)
					if _, appendErr := store.Append(
						ctx,
						"session-main",
						head,
						entry,
					); appendErr == nil {
						successes.Add(1)
					} else if errors.Is(appendErr, ErrTrajectoryConflict) {
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
				trajectoryTestEntry("cancelled", before, TrajectoryKindTerminal, `{}`),
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

func TestFileTrajectoryStoreSurvivesReopenWithLineage(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	first, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := first.Create(ctx, Trajectory{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(
		ctx,
		"source",
		"",
		trajectoryTestEntry("source-checkpoint", "", TrajectoryKindCheckpoint, `{"messages":[]}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := first.Create(ctx, Trajectory{
		ID:            "fork",
		ParentID:      "source",
		ParentEntryID: "source-checkpoint",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(
		ctx,
		"fork",
		"",
		trajectoryTestEntry("fork-message", "", TrajectoryKindUserMessage, `{"text":"new path"}`),
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
	summaries, err := second.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 || summaries[1].ID != "fork" {
		t.Fatalf("summaries = %#v", summaries)
	}
}

func trajectoryTestEntry(
	id string,
	parent string,
	kind string,
	payload string,
) TrajectoryEntry {
	return TrajectoryEntry{
		ID:        id,
		ParentID:  parent,
		Kind:      kind,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(payload),
	}
}
