package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestEventDispatchRejectsNilSnapshot(t *testing.T) {
	t.Parallel()
	if _, err := preparePostCommitEventPlan(
		nil,
		sdk.EventAgentEnd,
		postCommitSessionSubject("nil-snapshot"),
		sdk.AgentEndPayload{},
		postCommitDeliveryBoundaryHostOutbox,
	); err == nil {
		t.Fatal("prepare post-commit event with nil snapshot succeeded")
	}
	if _, err := (&Runtime{}).dispatchPreparedEvent(
		context.Background(),
		nil,
		sdk.Event{Name: sdk.EventAgentEnd},
		emitEventDispatchOptions(),
	); err == nil {
		t.Fatal("dispatch prepared event with nil snapshot succeeded")
	}
	if deliveries := planSubscriberDeliveries(
		nil,
		sdk.Event{Name: sdk.EventAgentEnd},
		time.Now(),
	); deliveries != nil {
		t.Fatalf("nil snapshot subscriber deliveries = %#v", deliveries)
	}
	if subscriberDeliveryStableBeforeDispatch(nil, sdk.EventAgentEnd) {
		t.Fatal("nil snapshot has stable subscriber delivery")
	}
}

func TestDispatchAuditRecordsOrderedHookChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	var secondHookInput sdk.BeforeAgentStartPayload
	skippedCalled := false
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "audit-chain",
			Version:     "1.0.0",
			Description: "records hook audit ordering",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.HookResource("patch-pre"),
				sdk.HookResource("patch-normal"),
				sdk.HookResource("block-post"),
				sdk.HookResource("skipped-post"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return errors.Join(
				registrar.RegisterHook(sdk.HookFunc{
					HookSpec: sdk.HookSpec{
						Name:     "patch-pre",
						Event:    sdk.EventBeforeAgentStart,
						Priority: sdk.PriorityPre,
					},
					HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
						return sdk.Patch(map[string]any{"system": "pre"})
					},
				}),
				registrar.RegisterHook(sdk.HookFunc{
					HookSpec: sdk.HookSpec{
						Name:     "patch-normal",
						Event:    sdk.EventBeforeAgentStart,
						Priority: sdk.PriorityNormal,
					},
					HandleFunc: func(_ context.Context, event sdk.Event) (sdk.Effect, error) {
						if err := json.Unmarshal(event.Payload, &secondHookInput); err != nil {
							return sdk.Effect{}, err
						}
						return sdk.Patch(map[string]any{"system": "normal"})
					},
				}),
				registrar.RegisterHook(sdk.HookFunc{
					HookSpec: sdk.HookSpec{
						Name:     "block-post",
						Event:    sdk.EventBeforeAgentStart,
						Priority: sdk.PriorityPost,
					},
					HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
						return sdk.BlockWith("blocked by audit test", "policy"), nil
					},
				}),
				registrar.RegisterHook(sdk.HookFunc{
					HookSpec: sdk.HookSpec{
						Name:     "skipped-post",
						Event:    sdk.EventBeforeAgentStart,
						Priority: sdk.PriorityPost,
					},
					HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
						skippedCalled = true
						return sdk.Effect{}, nil
					},
				}),
			)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}

	result, err := runtime.Emit(
		ctx,
		sdk.EventBeforeAgentStart,
		"audit-chain-session",
		sdk.BeforeAgentStartPayload{System: "original"},
	)
	if err != nil {
		t.Fatal(err)
	}
	var final sdk.BeforeAgentStartPayload
	if err := json.Unmarshal(result.Event.Payload, &final); err != nil {
		t.Fatal(err)
	}
	if secondHookInput.System != "pre" || final.System != "normal" {
		t.Fatalf(
			"hook reducer payloads: second=%q final=%q",
			secondHookInput.System,
			final.System,
		)
	}
	if skippedCalled {
		t.Fatal("hook after blocking hook was invoked")
	}

	audit := result.Audit
	if audit.EventName != sdk.EventBeforeAgentStart ||
		audit.InputHash == "" ||
		audit.OutputHash == "" ||
		audit.InputHash == audit.OutputHash {
		t.Fatalf("audit hashes/event = %#v", audit)
	}
	if len(audit.Steps) != 4 {
		t.Fatalf("audit steps = %#v", audit.Steps)
	}
	wantOutcomes := []sdk.HookAuditOutcome{
		sdk.HookAuditPatched,
		sdk.HookAuditPatched,
		sdk.HookAuditBlocked,
		sdk.HookAuditSkipped,
	}
	for index, want := range wantOutcomes {
		if audit.Steps[index].Index != index ||
			audit.Steps[index].Outcome != want {
			t.Fatalf("audit step %d = %#v, want outcome %q", index, audit.Steps[index], want)
		}
	}
	if !reflect.DeepEqual(audit.Steps[1].Overwrites, []string{"system"}) {
		t.Fatalf("second patch overwrites = %#v", audit.Steps[1].Overwrites)
	}
	if audit.Steps[1].InputHash != audit.Steps[0].OutputHash {
		t.Fatalf("hook input/output chain is not connected: %#v", audit.Steps)
	}
	if audit.Resolution.Outcome != sdk.EffectResolutionBlocked ||
		audit.Resolution.Block == nil ||
		audit.Resolution.Block.Reason != "blocked by audit test" ||
		audit.Resolution.BlockStep == nil ||
		*audit.Resolution.BlockStep != 2 ||
		!reflect.DeepEqual(audit.Resolution.PatchFields, []string{"system"}) {
		t.Fatalf("audit resolution = %#v", audit.Resolution)
	}
}

func TestPromptPersistsHookAuditOnTrajectoryEntries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(trajectories, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	beforeProvider := sdk.TypedHook[sdk.BeforeProviderPayload](
		sdk.HookSpec{Name: "rewrite-system", Event: sdk.EventBeforeProvider},
		func(context.Context, sdk.BeforeProviderPayload) (sdk.Effect, error) {
			return sdk.Patch(map[string]any{"system": "audited system"})
		},
	)
	decide := sdk.TypedHook[sdk.DecidePayload](
		sdk.HookSpec{Name: "policy-stop", Event: sdk.EventDecide},
		func(context.Context, sdk.DecidePayload) (sdk.Effect, error) {
			return sdk.Stop(sdk.Cause{Code: "policy_stop"}), nil
		},
	)
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "trajectory-audit",
			Version:     "1.0.0",
			Description: "persists hook audit on trajectory entries",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("observer-provider"),
				sdk.HookResource("rewrite-system"),
				sdk.HookResource("policy-stop"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return errors.Join(
				registrar.RegisterProvider(observerProvider{}),
				registrar.RegisterHook(beforeProvider),
				registrar.RegisterHook(decide),
			)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "trajectory-audit-session",
		Provider: "observer-provider",
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := session.Prompt(ctx, "record audit")
	if err != nil {
		t.Fatal(err)
	}
	if result.Cause.Code != "policy_stop" {
		t.Fatalf("result cause = %#v", result.Cause)
	}
	trajectory, err := trajectories.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	providerRequest, ok := findBranchEntry(branch, sdk.TrajectoryKindProviderRequest)
	if !ok {
		t.Fatalf("provider request entry not found in branch %#v", branch)
	}
	decision, ok := findBranchEntry(branch, sdk.TrajectoryKindDecision)
	if !ok {
		t.Fatalf("decision entry not found in branch %#v", branch)
	}

	if len(providerRequest.Audit) != 1 ||
		providerRequest.Audit[0].EventName != sdk.EventBeforeProvider ||
		len(providerRequest.Audit[0].Steps) != 1 ||
		providerRequest.Audit[0].Steps[0].Hook != "rewrite-system" ||
		providerRequest.Audit[0].Steps[0].Outcome != sdk.HookAuditPatched ||
		!reflect.DeepEqual(
			providerRequest.Audit[0].Resolution.PatchFields,
			[]string{"system"},
		) {
		t.Fatalf("provider request audit = %#v", providerRequest.Audit)
	}
	if len(decision.Audit) != 1 ||
		decision.Audit[0].EventName != sdk.EventDecide ||
		len(decision.Audit[0].Steps) != 1 ||
		decision.Audit[0].Resolution.Outcome != sdk.EffectResolutionAction ||
		decision.Audit[0].Resolution.Action == nil ||
		decision.Audit[0].Resolution.Action.CauseCode != "policy_stop" ||
		decision.Audit[0].Resolution.ActionRule != "last_stop" ||
		!reflect.DeepEqual(decision.Audit[0].Resolution.ActionSteps, []int{0}) {
		t.Fatalf("decision audit = %#v", decision.Audit)
	}
}

func TestExecutionEventSubscriberEnqueueFailureDoesNotAbortDispatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: enqueueFailingStateBackend{
			StateBackend: newTestStateBackend(),
			err:          context.DeadlineExceeded,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	mountExecutionSubscriber(t, runtime, sdk.EventBeforeAgentStart)
	mountExecutionSubscriber(t, runtime, sdk.EventDecide)

	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := lease.snapshot
	lease.release()
	_, mutableResult, err := dispatchMutableExecutionEvent(
		runtime,
		ctx,
		snapshot,
		sdk.EventBeforeAgentStart,
		"enqueue-failure-session",
		sdk.BeforeAgentStartPayload{System: "keep running"},
	)
	if err != nil {
		t.Fatalf("dispatch mutable event returned enqueue failure: %v", err)
	}
	if mutableResult.Event.ID == "" ||
		mutableResult.Event.Name != sdk.EventBeforeAgentStart {
		t.Fatalf("dispatch mutable result = %#v", mutableResult)
	}

	decisionResult, err := runtime.dispatchExecutionEvent(
		ctx,
		snapshot,
		sdk.EventDecide,
		"enqueue-failure-session",
		sdk.DecidePayload{Default: sdk.Action{Kind: sdk.ActionStop}},
	)
	if err != nil {
		t.Fatalf("dispatch execution event returned enqueue failure: %v", err)
	}
	if decisionResult.Event.ID == "" ||
		decisionResult.Event.Name != sdk.EventDecide {
		t.Fatalf("dispatch execution result = %#v", decisionResult)
	}
}

func TestExecutionEventSubscriberEnqueueOutlivesRequestCancellation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	mountExecutionSubscriber(t, runtime, sdk.EventDecide)

	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := lease.snapshot
	lease.release()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	result, err := runtime.dispatchExecutionEvent(
		cancelled,
		snapshot,
		sdk.EventDecide,
		"detached-enqueue-session",
		sdk.DecidePayload{Default: sdk.Action{Kind: sdk.ActionStop}},
	)
	if err != nil {
		t.Fatalf("dispatch execution event: %v", err)
	}
	deliveries, err := runtime.delivery.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 ||
		deliveries[0].Event.ID != result.Event.ID ||
		deliveries[0].Event.Name != sdk.EventDecide {
		t.Fatalf("detached execution deliveries = %#v", deliveries)
	}
}

func TestEmitReturnsSubscriberEnqueueFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: enqueueFailingStateBackend{
			StateBackend: newTestStateBackend(),
			err:          context.DeadlineExceeded,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	mountExecutionSubscriber(t, runtime, sdk.EventAgentStart)

	_, err = runtime.Emit(
		ctx,
		sdk.EventAgentStart,
		"strict-emit-session",
		sdk.AgentStartPayload{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Emit() error = %v, want context deadline", err)
	}
}

func TestEmitSubscriberEnqueueUsesRequestCancellation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	mountExecutionSubscriber(t, runtime, sdk.EventAgentStart)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = runtime.Emit(
		cancelled,
		sdk.EventAgentStart,
		"strict-cancelled-emit",
		sdk.AgentStartPayload{},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Emit() error = %v, want context canceled", err)
	}
	deliveries, err := runtime.delivery.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("cancelled emit deliveries = %#v", deliveries)
	}
}

func TestEmitUsesConfiguredSubscriberEnqueueTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: enqueueDeadlineStateBackend{
			StateBackend: newTestStateBackend(),
			minimum:      1500 * time.Millisecond,
		},
		DeliveryEnqueueTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	mountExecutionSubscriber(t, runtime, sdk.EventAgentStart)

	if _, err := runtime.Emit(
		ctx,
		sdk.EventAgentStart,
		"configured-enqueue-timeout",
		sdk.AgentStartPayload{},
	); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
}

type enqueueFailingStateBackend struct {
	sdk.StateBackend
	err error
}

func (backend enqueueFailingStateBackend) Deliveries(
	name string,
) (sdk.DeliveryStore, error) {
	store, err := backend.StateBackend.Deliveries(name)
	if err != nil {
		return nil, err
	}
	return enqueueFailingDeliveryStore{
		DeliveryStore: store,
		err:           backend.err,
	}, nil
}

type enqueueFailingDeliveryStore struct {
	sdk.DeliveryStore
	err error
}

func (store enqueueFailingDeliveryStore) Enqueue(
	context.Context,
	...sdk.Delivery,
) error {
	return store.err
}

type enqueueDeadlineStateBackend struct {
	sdk.StateBackend
	minimum time.Duration
}

func (backend enqueueDeadlineStateBackend) Deliveries(
	name string,
) (sdk.DeliveryStore, error) {
	store, err := backend.StateBackend.Deliveries(name)
	if err != nil {
		return nil, err
	}
	return enqueueDeadlineDeliveryStore{
		DeliveryStore: store,
		minimum:       backend.minimum,
	}, nil
}

type enqueueDeadlineDeliveryStore struct {
	sdk.DeliveryStore
	minimum time.Duration
}

func (store enqueueDeadlineDeliveryStore) Enqueue(
	ctx context.Context,
	deliveries ...sdk.Delivery,
) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("subscriber enqueue deadline missing")
	}
	if time.Until(deadline) < store.minimum {
		return errors.New("subscriber enqueue deadline shorter than configured budget")
	}
	return store.DeliveryStore.Enqueue(ctx, deliveries...)
}

func mountExecutionSubscriber(
	t *testing.T,
	runtime *Runtime,
	eventName string,
) {
	t.Helper()
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "enqueue-failure-observer-" + eventName,
			Version:     "1.0.0",
			Description: "observes event enqueue boundary",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("observe-" + eventName)},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "observe-" + eventName,
				Events: []string{eventName},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatalf("mount subscriber: %v", err)
	}
}

func TestResolveActionAuditRules(t *testing.T) {
	t.Parallel()
	injected, resolution := resolveAction(
		sdk.Action{Kind: sdk.ActionStep},
		[]sdk.Action{
			{
				Kind: sdk.ActionInject,
				Messages: []sdk.Message{{
					Role:    sdk.RoleUser,
					Content: "first",
				}},
			},
			{
				Kind: sdk.ActionInject,
				Messages: []sdk.Message{{
					Role:    sdk.RoleUser,
					Content: "second",
				}},
			},
		},
		[]int{2, 3, 5},
	)
	if injected.Kind != sdk.ActionInject ||
		len(injected.Messages) != 2 ||
		resolution.ActionRule != "inject_merge" ||
		!reflect.DeepEqual(resolution.ActionSteps, []int{2, 3}) {
		t.Fatalf("inject resolution action=%#v resolution=%#v", injected, resolution)
	}

	stopped, resolution := resolveAction(
		sdk.Action{Kind: sdk.ActionStep},
		[]sdk.Action{
			{
				Kind: sdk.ActionInject,
				Messages: []sdk.Message{{
					Role:    sdk.RoleUser,
					Content: "ignored context",
				}},
			},
			{Kind: sdk.ActionStop, Cause: &sdk.Cause{Code: "first_stop"}},
			{
				Kind: sdk.ActionInject,
				Messages: []sdk.Message{{
					Role:    sdk.RoleUser,
					Content: "also ignored",
				}},
			},
			{Kind: sdk.ActionStop, Cause: &sdk.Cause{Code: "second_stop"}},
		},
		[]int{7, 8, 9, 10},
	)
	if stopped.Kind != sdk.ActionStop ||
		stopped.Cause == nil ||
		stopped.Cause.Code != "second_stop" ||
		resolution.ActionRule != "last_stop" ||
		!reflect.DeepEqual(resolution.ActionSteps, []int{10}) {
		t.Fatalf("stop resolution action=%#v resolution=%#v", stopped, resolution)
	}

	final, resolution := resolveAction(
		sdk.Action{
			Kind: sdk.ActionStop,
			Cause: &sdk.Cause{
				Code:  "max_turns",
				Final: true,
			},
		},
		[]sdk.Action{{
			Kind: sdk.ActionInject,
			Messages: []sdk.Message{{
				Role:    sdk.RoleUser,
				Content: "too late",
			}},
		}},
		[]int{9},
	)
	if final.Kind != sdk.ActionStop ||
		final.Cause == nil ||
		final.Cause.Code != "max_turns" ||
		resolution.ActionRule != "default_final_stop" ||
		len(resolution.ActionSteps) != 0 {
		t.Fatalf("final resolution action=%#v resolution=%#v", final, resolution)
	}
}

func findBranchEntry(
	branch []sdk.TrajectoryEntry,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool) {
	for _, entry := range branch {
		if entry.Kind == kind {
			return entry, true
		}
	}
	return sdk.TrajectoryEntry{}, false
}
