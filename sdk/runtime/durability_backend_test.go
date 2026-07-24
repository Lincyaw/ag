package runtime

// Durability tests cover state-backend execution boundaries.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func newTestStateBackend() sdk.StateBackend {
	return sdkstorage.NewMemoryStateBackend()
}

func testStateBackendWithStores(
	trajectories sdk.TrajectoryStore,
	operations sdk.OperationStore,
) sdk.StateBackend {
	backend := &testStateBackend{StateBackend: newTestStateBackend()}
	if trajectories != nil {
		backend.trajectories = trajectories
	}
	if operations != nil {
		backend.operations = operations
	}
	return backend
}

type testStateBackend struct {
	sdk.StateBackend
	trajectories sdk.TrajectoryStore
	operations   sdk.OperationStore
}

func (backend *testStateBackend) Trajectories() sdk.TrajectoryStore {
	if backend.trajectories != nil {
		return backend.trajectories
	}
	return backend.StateBackend.Trajectories()
}

func (backend *testStateBackend) Operations() sdk.OperationStore {
	if backend.operations != nil {
		return backend.operations
	}
	return backend.StateBackend.Operations()
}

type atomicTestBackend struct {
	sdk.StateBackend
	appends int
	starts  int
	commits int
	cancels int
	outbox  []sdk.StateMutationDeliveries
}

func (backend *atomicTestBackend) Capabilities() sdk.StorageCapabilities {
	capabilities := backend.StateBackend.Capabilities()
	return capabilities
}

func (backend *atomicTestBackend) AppendTrajectory(
	ctx context.Context,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryAppendResult, error) {
	backend.appends++
	head, err := backend.Trajectories().Append(
		ctx,
		commit.TrajectoryID,
		commit.ExpectedHead,
		commit.Entries...,
	)
	if err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		ctx,
		commit.TrajectoryID,
	)
	if err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	if metadata.Head != head {
		return sdk.TrajectoryAppendResult{}, fmt.Errorf(
			"trajectory append head = %q, metadata head = %q",
			head,
			metadata.Head,
		)
	}
	if err := backend.enqueueOutbox(ctx, commit.Outbox); err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	return sdk.TrajectoryAppendResult{Trajectory: metadata}, nil
}

func (backend *atomicTestBackend) StartExecution(
	ctx context.Context,
	commit sdk.ExecutionStartCommit,
) (sdk.ExecutionMutationResult, error) {
	backend.starts++
	metadata, err := backend.Trajectories().BeginExecution(
		ctx,
		commit.TrajectoryID,
		commit.ExpectedHead,
		commit.Start,
		commit.Input,
	)
	if err != nil {
		return sdk.ExecutionMutationResult{Trajectory: metadata}, err
	}
	if err := backend.enqueueOutbox(ctx, commit.Outbox); err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	return sdk.ExecutionMutationResult{Trajectory: metadata}, nil
}

func (backend *atomicTestBackend) CommitExecution(
	ctx context.Context,
	commit sdk.ExecutionMutationCommit,
) (sdk.ExecutionMutationResult, error) {
	backend.commits++
	metadata, err := backend.Trajectories().CommitExecution(
		ctx,
		commit.Trajectory,
	)
	if err != nil {
		return sdk.ExecutionMutationResult{Trajectory: metadata}, err
	}
	if err := backend.enqueueOutbox(ctx, commit.Outbox); err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	return sdk.ExecutionMutationResult{Trajectory: metadata}, nil
}

func (backend *atomicTestBackend) CancelExecution(
	ctx context.Context,
	commit sdk.ExecutionCancelCommit,
) (sdk.ExecutionCancelResult, error) {
	backend.cancels++
	result, err := backend.Trajectories().CancelExecution(
		ctx,
		commit.TrajectoryCommit(),
	)
	if err != nil {
		return sdk.ExecutionCancelResult{Trajectory: result.Trajectory}, err
	}
	if result.Changed {
		if err := backend.enqueueOutbox(ctx, commit.Outbox); err != nil {
			return sdk.ExecutionCancelResult{}, err
		}
	}
	return sdk.ExecutionCancelResult{
		Trajectory: result.Trajectory,
		Changed:    result.Changed,
	}, nil
}

func (backend *atomicTestBackend) enqueueOutbox(
	ctx context.Context,
	outbox []sdk.StateMutationDeliveries,
) error {
	for _, group := range outbox {
		cloned := sdk.CloneStateMutationDeliveries(group)
		store, err := backend.Deliveries(group.Queue)
		if err != nil {
			return err
		}
		if err := store.Enqueue(ctx, cloned.Deliveries...); err != nil {
			return err
		}
		backend.outbox = append(
			backend.outbox,
			sdk.CloneStateMutationDeliveries(cloned),
		)
	}
	return nil
}

func TestRuntimeRoutesExecutionCommitThroughAtomicBackend(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	if err := store.Create(
		ctx,
		sdk.Trajectory{ID: "atomic-runtime"},
	); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "atomic-runtime-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"role":"user"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"atomic-runtime",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "atomic-runtime-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ClaimExecution(
		ctx,
		"atomic-runtime",
		"atomic-runtime-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := runtime.commitTrajectoryExecution(
		ctx,
		sdk.TrajectoryExecutionCommit{
			TrajectoryID: "atomic-runtime",
			ExecutionID:  execution.ID,
			LeaseToken:   execution.LeaseToken,
			ExpectedHead: input.ID,
			Entries: []sdk.TrajectoryEntry{{
				ID:        "atomic-runtime-checkpoint",
				ParentID:  input.ID,
				Kind:      sdk.TrajectoryKindCheckpoint,
				Timestamp: time.Now().UTC(),
				Payload:   json.RawMessage(`{"checkpoint":true}`),
			}},
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if backend.commits != 1 ||
		metadata.Head != "atomic-runtime-checkpoint" {
		t.Fatalf(
			"atomic commits=%d metadata=%#v",
			backend.commits,
			metadata,
		)
	}
}

func TestRuntimeHostOutboxRequiresAtomicBackend(t *testing.T) {
	t.Parallel()
	delivery := sdk.Delivery{
		ID:           "non-atomic-delivery",
		Plugin:       "observer",
		Subscription: "events",
		Event: sdk.Event{
			ID:      "non-atomic-event",
			Name:    sdk.EventTrajectoryAppend,
			Payload: json.RawMessage(`{}`),
		},
	}
	if outbox, err := (&Runtime{}).atomicMutationHostOutbox(
		[]sdk.Delivery{delivery},
	); err == nil || outbox != nil {
		t.Fatalf("non-atomic outbox = %#v, err = %v", outbox, err)
	}

	runtime := &Runtime{
		atomicState: &atomicTestBackend{StateBackend: newTestStateBackend()},
	}
	outbox, err := runtime.atomicMutationHostOutbox([]sdk.Delivery{delivery})
	if err != nil {
		t.Fatal(err)
	}
	if len(outbox) != 1 ||
		outbox[0].Queue != sdk.HostOutboxQueue ||
		len(outbox[0].Deliveries) != 1 ||
		outbox[0].Deliveries[0].ID != delivery.ID {
		t.Fatalf("atomic outbox = %#v", outbox)
	}
}

func TestAtomicTrajectoryAppendPlansSubscriberOutbox(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		DeliveryPoll:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "atomic-trajectory-observer",
			Version:     "1.0.0",
			Description: "observes atomic trajectory append events",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("trajectory-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "trajectory-events",
				Events: []string{sdk.EventTrajectoryAppend},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, sdk.Trajectory{ID: "atomic-outbox"}); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "atomic-outbox-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"role":"user"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"atomic-outbox",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "atomic-outbox-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ClaimExecution(
		ctx,
		"atomic-outbox",
		"atomic-outbox-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{
		runtime:        runtime,
		config:         SessionConfig{ID: "atomic-outbox"},
		head:           input.ID,
		executionID:    execution.ID,
		executionToken: execution.LeaseToken,
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if err := session.appendTrajectory(
		ctx,
		lease.snapshot,
		sdk.TrajectoryKindCheckpoint,
		map[string]bool{"checkpoint": true},
	); err != nil {
		t.Fatal(err)
	}
	if backend.commits != 1 || len(backend.outbox) != 1 {
		t.Fatalf(
			"commits=%d outbox=%#v",
			backend.commits,
			backend.outbox,
		)
	}
	group := backend.outbox[0]
	if group.Queue != sdk.HostOutboxQueue || len(group.Deliveries) != 1 {
		t.Fatalf("outbox group = %#v", group)
	}
	delivery := group.Deliveries[0]
	if delivery.Event.Name != sdk.EventTrajectoryAppend ||
		delivery.Event.SessionID != "atomic-outbox" {
		t.Fatalf("delivery event = %#v", delivery.Event)
	}
	var payload sdk.TrajectoryEventPayload
	if err := json.Unmarshal(delivery.Event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.TrajectoryID != "atomic-outbox" ||
		payload.EntryKind != sdk.TrajectoryKindCheckpoint ||
		payload.Generation != lease.snapshot.generation {
		t.Fatalf("trajectory event payload = %#v", payload)
	}
}

func TestAtomicRollbackTrajectoryPlansSubscriberOutbox(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		DeliveryPoll:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "atomic-rollback-observer",
			Version:     "1.0.0",
			Description: "observes atomic rollback",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("rollback-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "rollback-events",
				Events: []string{sdk.EventTrajectoryRollback},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, sdk.Trajectory{ID: "atomic-rollback"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(
		ctx,
		"atomic-rollback",
		"",
		sdk.TrajectoryEntry{
			ID:        "atomic-rollback-checkpoint",
			Kind:      sdk.TrajectoryKindCheckpoint,
			Timestamp: time.Now().UTC(),
			Payload: json.RawMessage(
				`{"messages":[],"turns":0,"tool_calls":0,"action":{}}`,
			),
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RollbackTrajectory(
		ctx,
		"atomic-rollback",
		"atomic-rollback-checkpoint",
	); err != nil {
		t.Fatal(err)
	}
	if backend.appends != 1 || len(backend.outbox) != 1 {
		t.Fatalf(
			"appends=%d outbox=%#v",
			backend.appends,
			backend.outbox,
		)
	}
	group := backend.outbox[0]
	if group.Queue != sdk.HostOutboxQueue || len(group.Deliveries) != 1 {
		t.Fatalf("outbox group = %#v", group)
	}
	delivery := group.Deliveries[0]
	if delivery.Event.Name != sdk.EventTrajectoryRollback ||
		delivery.Event.SessionID != "atomic-rollback" {
		t.Fatalf("delivery event = %#v", delivery.Event)
	}
	var payload sdk.TrajectoryEventPayload
	if err := json.Unmarshal(delivery.Event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.TrajectoryID != "atomic-rollback" ||
		payload.EntryKind != sdk.TrajectoryKindRollback ||
		payload.From != "atomic-rollback-checkpoint" ||
		payload.To != "atomic-rollback-checkpoint" {
		t.Fatalf("rollback event payload = %#v", payload)
	}
	trajectory, err := store.Load(ctx, "atomic-rollback")
	if err != nil {
		t.Fatal(err)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	if branch[len(branch)-1].Kind != sdk.TrajectoryKindRollback {
		t.Fatalf("rollback branch = %#v", branch)
	}
}

func TestAtomicExecutionStartPlansSubscriberOutbox(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		DeliveryPoll:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "atomic-start-observer",
			Version:     "1.0.0",
			Description: "observes atomic execution start",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("start-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "start-events",
				Events: []string{sdk.EventTrajectoryAppend},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(
		ctx,
		SessionConfig{ID: "atomic-start", MaxTurns: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if submission.Execution().ID == "" {
		t.Fatalf("submission execution = %#v", submission.Execution())
	}
	if backend.starts != 1 || backend.commits != 0 || len(backend.outbox) != 1 {
		t.Fatalf(
			"starts=%d commits=%d outbox=%#v",
			backend.starts,
			backend.commits,
			backend.outbox,
		)
	}
	group := backend.outbox[0]
	if group.Queue != sdk.HostOutboxQueue || len(group.Deliveries) != 1 {
		t.Fatalf("outbox group = %#v", group)
	}
	delivery := group.Deliveries[0]
	if delivery.Event.Name != sdk.EventTrajectoryAppend ||
		delivery.Event.SessionID != "atomic-start" {
		t.Fatalf("delivery event = %#v", delivery.Event)
	}
	var payload sdk.TrajectoryEventPayload
	if err := json.Unmarshal(delivery.Event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.TrajectoryID != "atomic-start" ||
		payload.EntryKind != sdk.TrajectoryKindUserMessage ||
		payload.EntryID == "" {
		t.Fatalf("trajectory event payload = %#v", payload)
	}
}

func TestAtomicFailedExecutionPlansCompletionOutboxInMutationOrder(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		DeliveryPoll:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "atomic-agent-end-observer",
			Version:     "1.0.0",
			Description: "observes atomic agent completion",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("agent-end-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name: "agent-end-events",
				Events: []string{
					sdk.EventTrajectoryAppend,
					sdk.EventTrajectoryRestore,
					sdk.EventAgentEnd,
				},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, sdk.Trajectory{ID: "atomic-agent-end"}); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "atomic-agent-end-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"role":"user","content":"hello"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"atomic-agent-end",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "atomic-agent-end-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ClaimExecution(
		ctx,
		"atomic-agent-end",
		"atomic-agent-end-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{
		runtime:        runtime,
		config:         SessionConfig{ID: "atomic-agent-end"},
		head:           input.ID,
		executionID:    execution.ID,
		executionToken: execution.LeaseToken,
	}
	failure := errors.New("provider failed")
	result := Result{
		Messages: []sdk.Message{{
			Role:    sdk.RoleUser,
			Content: "hello",
		}},
		Cause: sdk.Cause{
			Code:   "provider_error",
			Detail: failure.Error(),
		},
	}
	if err := session.failExecution(ctx, failure, result); err != nil {
		t.Fatal(err)
	}
	if backend.commits != 1 || len(backend.outbox) != 1 {
		t.Fatalf(
			"commits=%d outbox=%#v",
			backend.commits,
			backend.outbox,
		)
	}
	group := backend.outbox[0]
	if group.Queue != sdk.HostOutboxQueue || len(group.Deliveries) != 3 {
		t.Fatalf("outbox group = %#v", group)
	}
	eventNames := []string{
		group.Deliveries[0].Event.Name,
		group.Deliveries[1].Event.Name,
		group.Deliveries[2].Event.Name,
	}
	wantEventNames := []string{
		sdk.EventTrajectoryAppend,
		sdk.EventTrajectoryRestore,
		sdk.EventAgentEnd,
	}
	if fmt.Sprint(eventNames) != fmt.Sprint(wantEventNames) {
		t.Fatalf("outbox event order = %v, want %v", eventNames, wantEventNames)
	}
	delivery := group.Deliveries[2]
	if delivery.Event.Name != sdk.EventAgentEnd ||
		delivery.Event.SessionID != "atomic-agent-end" {
		t.Fatalf("delivery event = %#v", delivery.Event)
	}
	var payload sdk.AgentEndPayload
	if err := json.Unmarshal(delivery.Event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Cause.Code != "provider_error" ||
		payload.Cause.Detail != failure.Error() {
		t.Fatalf("agent end payload = %#v", payload)
	}
	trajectory, err := store.Load(ctx, "atomic-agent-end")
	if err != nil {
		t.Fatal(err)
	}
	if trajectory.Execution == nil ||
		trajectory.Execution.State != sdk.TrajectoryExecutionFailed {
		t.Fatalf("execution = %#v", trajectory.Execution)
	}
	var terminalEntry sdk.TrajectoryEntry
	for _, entry := range trajectory.Entries {
		if entry.Kind == sdk.TrajectoryKindTerminal {
			terminalEntry = entry
			break
		}
	}
	if terminalEntry.ID == "" || terminalEntry.ParentID != input.ID {
		t.Fatalf("terminal entry = %#v", terminalEntry)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	if branch[len(branch)-1].Kind != sdk.TrajectoryKindRestore {
		t.Fatalf("failed execution head branch = %#v", branch)
	}
	for _, entry := range branch {
		if entry.ID == terminalEntry.ID {
			t.Fatalf("terminal entry %q remained on active branch", entry.ID)
		}
	}
}

func TestAtomicCancelExecutionPlansCompletionOutboxInMutationOrder(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		DeliveryPoll:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "atomic-cancel-observer",
			Version:     "1.0.0",
			Description: "observes atomic cancellation",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("cancel-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name: "cancel-events",
				Events: []string{
					sdk.EventTrajectoryAppend,
					sdk.EventTrajectoryRestore,
					sdk.EventAgentEnd,
				},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, sdk.Trajectory{ID: "atomic-cancel"}); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "atomic-cancel-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"role":"user","content":"stop"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"atomic-cancel",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "atomic-cancel-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ClaimExecution(
		ctx,
		"atomic-cancel",
		"remote-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := runtime.CancelExecution(
		ctx,
		"atomic-cancel",
		execution.ID,
		"runtime requested cancellation",
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled view = %#v", cancelled)
	}
	if backend.cancels != 1 || len(backend.outbox) != 1 {
		t.Fatalf(
			"cancels=%d outbox=%#v",
			backend.cancels,
			backend.outbox,
		)
	}
	group := backend.outbox[0]
	if group.Queue != sdk.HostOutboxQueue || len(group.Deliveries) != 3 {
		t.Fatalf("outbox group = %#v", group)
	}
	eventNames := []string{
		group.Deliveries[0].Event.Name,
		group.Deliveries[1].Event.Name,
		group.Deliveries[2].Event.Name,
	}
	wantEventNames := []string{
		sdk.EventTrajectoryAppend,
		sdk.EventTrajectoryRestore,
		sdk.EventAgentEnd,
	}
	if fmt.Sprint(eventNames) != fmt.Sprint(wantEventNames) {
		t.Fatalf("outbox event order = %v, want %v", eventNames, wantEventNames)
	}
	delivery := group.Deliveries[2]
	if delivery.Event.Name != sdk.EventAgentEnd ||
		delivery.Event.SessionID != "atomic-cancel" {
		t.Fatalf("delivery event = %#v", delivery.Event)
	}
	var payload sdk.AgentEndPayload
	if err := json.Unmarshal(delivery.Event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Cause.Code != "cancelled" ||
		payload.Cause.Detail != "runtime requested cancellation" ||
		!payload.Cause.Final {
		t.Fatalf("agent end payload = %#v", payload)
	}
	trajectory, err := store.Load(ctx, "atomic-cancel")
	if err != nil {
		t.Fatal(err)
	}
	var terminalEntry sdk.TrajectoryEntry
	for _, entry := range trajectory.Entries {
		if entry.Kind == sdk.TrajectoryKindTerminal {
			terminalEntry = entry
			break
		}
	}
	if terminalEntry.ID == "" || terminalEntry.ParentID != input.ID {
		t.Fatalf("terminal entry = %#v", terminalEntry)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	if len(branch) == 0 ||
		branch[len(branch)-1].Kind != sdk.TrajectoryKindRestore {
		t.Fatalf("cancelled execution head branch = %#v", branch)
	}
	for _, entry := range branch {
		if entry.ID == terminalEntry.ID {
			t.Fatalf("terminal entry %q remained on active branch", entry.ID)
		}
	}
	if _, err := runtime.CancelExecution(
		ctx,
		"atomic-cancel",
		execution.ID,
		"duplicate cancellation",
	); err != nil {
		t.Fatal(err)
	}
	if backend.cancels != 2 || len(backend.outbox) != 1 {
		t.Fatalf(
			"idempotent cancel cancels=%d outbox=%#v",
			backend.cancels,
			backend.outbox,
		)
	}
}
