// Package runtime implements the agent-execution core over SDK ports. It owns
// sessions, composition, orchestration, and durable execution coordination,
// while applications select concrete storage, discovery, and transport
// adapters.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/lincyaw/ag/sdk"

const (
	defaultRuntimeVersion           = "development"
	defaultDeliveryWorkers          = 2
	defaultDeliveryLease            = 30 * time.Second
	defaultDeliveryPoll             = 25 * time.Millisecond
	defaultDeliveryEnqueueTimeout   = 5 * time.Second
	defaultDeliveryTimeout          = 5 * time.Second
	defaultDeliveryMaxAttempts      = 8
	defaultPluginCloseTimeout       = 10 * time.Second
	defaultHookTimeout              = time.Second
	defaultOperationPoll            = 100 * time.Millisecond
	defaultOperationCancelTimeout   = 2 * time.Second
	defaultOperationLease           = 30 * time.Second
	defaultTrajectoryExecutionLease = 30 * time.Second
)

type StorageOwnership string

const (
	StorageOwned    StorageOwnership = "owned"
	StorageBorrowed StorageOwnership = "borrowed"
)

// AgentForkPolicy controls whether a forked agent session may create another
// forked child session.
type AgentForkPolicy string

const (
	// AgentForkPolicyAllowNested keeps nested forked sessions available as a
	// general SDK extension.
	AgentForkPolicyAllowNested AgentForkPolicy = "allow_nested"
	// AgentForkPolicyDenyNested rejects fork requests issued from a forked
	// child session, matching Claude Code's recursive fork policy.
	AgentForkPolicyDenyNested AgentForkPolicy = "deny_nested"
)

type RuntimeConfig struct {
	RuntimeVersion   string
	Logger           *slog.Logger
	Tracer           trace.Tracer
	Meter            metric.Meter
	Storage          sdk.StateBackend
	StorageOwnership StorageOwnership
	// AgentForkPolicy controls runtime policy for forked child agents. The
	// default keeps the SDK trajectory model general; hosts that need exact
	// Claude Code compatibility can deny nested forks without changing storage.
	AgentForkPolicy AgentForkPolicy
	// EventObserver receives a cloned copy of each dispatched event after
	// hooks and subscriber enqueueing. It is for host-side UI/diagnostics and
	// does not participate in the runtime composition contract. Runtime close
	// cancels observer contexts and waits only within the finalization boundary.
	EventObserver          func(context.Context, sdk.Event)
	DeliveryWorkers        int
	DeliveryLease          time.Duration
	DeliveryPoll           time.Duration
	DeliveryEnqueueTimeout time.Duration
	DeliveryTimeout        time.Duration
	DeliveryMaxAttempts    int
	PluginCloseTimeout     time.Duration
	HookTimeout            time.Duration
	OperationPoll          time.Duration
	OperationCancelTimeout time.Duration
	OperationLease         time.Duration
	TrajectoryLease        time.Duration
}

type Runtime struct {
	mu                  sync.Mutex
	current             atomic.Pointer[registrySnapshot]
	closed              bool
	closeDone           chan struct{}
	closeErr            error
	nextSequence        uint64
	version             string
	logger              *slog.Logger
	tracer              trace.Tracer
	mounts              metric.Int64Counter
	unmounts            metric.Int64Counter
	events              metric.Int64Counter
	hooks               metric.Int64Counter
	pluginCloseTimeout  time.Duration
	hookTimeout         time.Duration
	agentForkPolicy     AgentForkPolicy
	storage             sdk.StateBackend
	atomicState         sdk.AtomicStateBackend
	closeStorage        bool
	trajectories        sdk.TrajectoryStore
	contextInjections   sdk.ContextInjectionStore
	observer            eventObserverRuntime
	trajectoryExecution trajectoryExecutionRuntime
	operation           operationRuntime
	delivery            deliveryRuntime
}

type runtimeStoragePorts struct {
	trajectories      sdk.TrajectoryStore
	operations        sdk.OperationStore
	contextInjections sdk.ContextInjectionStore
	delivery          sdk.DeliveryStore
	atomicState       sdk.AtomicStateBackend
}

func normalizeRuntimeConfig(config RuntimeConfig) (RuntimeConfig, error) {
	if strings.TrimSpace(config.RuntimeVersion) == "" {
		config.RuntimeVersion = defaultRuntimeVersion
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Tracer == nil {
		config.Tracer = otel.Tracer(instrumentationName)
	}
	if config.Meter == nil {
		config.Meter = otel.Meter(instrumentationName)
	}
	if config.Storage == nil {
		return RuntimeConfig{}, errors.New("runtime state backend is required")
	}
	if config.StorageOwnership == "" {
		config.StorageOwnership = StorageOwned
	}
	switch config.StorageOwnership {
	case StorageOwned, StorageBorrowed:
	default:
		return RuntimeConfig{}, fmt.Errorf(
			"unknown storage ownership %q",
			config.StorageOwnership,
		)
	}
	if config.AgentForkPolicy == "" {
		config.AgentForkPolicy = AgentForkPolicyAllowNested
	}
	switch config.AgentForkPolicy {
	case AgentForkPolicyAllowNested, AgentForkPolicyDenyNested:
	default:
		return RuntimeConfig{}, fmt.Errorf(
			"unknown agent fork policy %q",
			config.AgentForkPolicy,
		)
	}
	if config.DeliveryWorkers == 0 {
		config.DeliveryWorkers = defaultDeliveryWorkers
	}
	if config.DeliveryLease == 0 {
		config.DeliveryLease = defaultDeliveryLease
	}
	if config.DeliveryPoll == 0 {
		config.DeliveryPoll = defaultDeliveryPoll
	}
	if config.DeliveryEnqueueTimeout == 0 {
		config.DeliveryEnqueueTimeout = defaultDeliveryEnqueueTimeout
	}
	if config.DeliveryTimeout == 0 {
		config.DeliveryTimeout = defaultDeliveryTimeout
	}
	if config.DeliveryMaxAttempts == 0 {
		config.DeliveryMaxAttempts = defaultDeliveryMaxAttempts
	}
	if config.PluginCloseTimeout == 0 {
		config.PluginCloseTimeout = defaultPluginCloseTimeout
	}
	if config.HookTimeout == 0 {
		config.HookTimeout = defaultHookTimeout
	}
	if config.OperationPoll == 0 {
		config.OperationPoll = defaultOperationPoll
	}
	if config.OperationCancelTimeout == 0 {
		config.OperationCancelTimeout = defaultOperationCancelTimeout
	}
	if config.OperationLease == 0 {
		config.OperationLease = defaultOperationLease
	}
	if config.TrajectoryLease == 0 {
		config.TrajectoryLease = defaultTrajectoryExecutionLease
	}
	if config.DeliveryWorkers < 1 || config.DeliveryLease <= 0 ||
		config.DeliveryPoll <= 0 || config.DeliveryEnqueueTimeout <= 0 ||
		config.DeliveryTimeout <= 0 || config.DeliveryMaxAttempts < 1 ||
		config.PluginCloseTimeout <= 0 || config.HookTimeout <= 0 ||
		config.OperationPoll <= 0 || config.OperationCancelTimeout <= 0 ||
		config.OperationLease <= 0 || config.TrajectoryLease <= 0 {
		return RuntimeConfig{}, errors.New(
			"runtime lifecycle settings must be positive",
		)
	}
	if config.DeliveryTimeout >= config.DeliveryLease {
		return RuntimeConfig{}, errors.New(
			"delivery timeout must be shorter than the delivery lease",
		)
	}
	return config, nil
}

func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	return NewRuntimeContext(context.Background(), config)
}

func NewRuntimeContext(
	ctx context.Context,
	config RuntimeConfig,
) (*Runtime, error) {
	// Ownership of Storage transfers only when construction succeeds.
	if ctx == nil {
		ctx = context.Background()
	}
	config, err := normalizeRuntimeConfig(config)
	if err != nil {
		return nil, err
	}

	mounts, err := config.Meter.Int64Counter(
		"agentm.plugin.mounts",
		metric.WithDescription("Plugin mounts committed by the runtime."),
	)
	if err != nil {
		return nil, fmt.Errorf("create plugin mounts counter: %w", err)
	}
	unmounts, err := config.Meter.Int64Counter(
		"agentm.plugin.unmounts",
		metric.WithDescription("Plugin unmounts committed by the runtime."),
	)
	if err != nil {
		return nil, fmt.Errorf("create plugin unmounts counter: %w", err)
	}
	events, err := config.Meter.Int64Counter(
		"agentm.events",
		metric.WithDescription("Events dispatched by the runtime."),
	)
	if err != nil {
		return nil, fmt.Errorf("create events counter: %w", err)
	}
	hooks, err := config.Meter.Int64Counter(
		"agentm.hooks",
		metric.WithDescription("Plugin hooks invoked by the runtime."),
	)
	if err != nil {
		return nil, fmt.Errorf("create hooks counter: %w", err)
	}
	if err := config.Storage.Health(ctx); err != nil {
		return nil, fmt.Errorf("state backend health: %w", err)
	}
	storage, err := resolveRuntimeStoragePorts(config.Storage)
	if err != nil {
		return nil, err
	}
	runtimeContext := lifecycle.Detached(ctx)
	deliveryContext, cancelDeliveries := context.WithCancel(runtimeContext)
	operationContext, cancelOperations := context.WithCancel(runtimeContext)
	trajectoryContext, cancelTrajectories := context.WithCancel(runtimeContext)
	observerContext, cancelObservers := context.WithCancel(runtimeContext)
	runtime := &Runtime{
		version:            config.RuntimeVersion,
		logger:             config.Logger,
		tracer:             config.Tracer,
		closeDone:          make(chan struct{}),
		mounts:             mounts,
		unmounts:           unmounts,
		events:             events,
		hooks:              hooks,
		pluginCloseTimeout: config.PluginCloseTimeout,
		hookTimeout:        config.HookTimeout,
		agentForkPolicy:    config.AgentForkPolicy,
		storage:            config.Storage,
		atomicState:        storage.atomicState,
		closeStorage:       config.StorageOwnership == StorageOwned,
		trajectories:       storage.trajectories,
		contextInjections:  storage.contextInjections,
		observer: eventObserverRuntime{
			observe: config.EventObserver,
			context: observerContext,
			cancel:  cancelObservers,
		},
		trajectoryExecution: trajectoryExecutionRuntime{
			context:  trajectoryContext,
			cancel:   cancelTrajectories,
			hosts:    newHostedExecutionRegistry(),
			lease:    config.TrajectoryLease,
			workerID: "trajectory-runtime-" + sdk.NewID(),
		},
		operation: operationRuntime{
			store:         storage.operations,
			context:       operationContext,
			cancel:        cancelOperations,
			inflight:      operationworker.NewInflight(operationContext),
			poll:          config.OperationPoll,
			cancelTimeout: config.OperationCancelTimeout,
			lease:         config.OperationLease,
			workerID:      "runtime-" + sdk.NewID(),
		},
		delivery: deliveryRuntime{
			store:          storage.delivery,
			workers:        config.DeliveryWorkers,
			lease:          config.DeliveryLease,
			poll:           config.DeliveryPoll,
			enqueueTimeout: config.DeliveryEnqueueTimeout,
			timeout:        config.DeliveryTimeout,
			maxAttempts:    config.DeliveryMaxAttempts,
			context:        deliveryContext,
			cancel:         cancelDeliveries,
		},
	}
	runtime.current.Store(initialSnapshot())
	return runtime, nil
}

func resolveRuntimeStoragePorts(
	backend sdk.StateBackend,
) (runtimeStoragePorts, error) {
	capabilities := backend.Capabilities()
	if !capabilities.OperationFencing {
		return runtimeStoragePorts{}, errors.New(
			"state backend must advertise operation fencing",
		)
	}
	if !capabilities.NamedQueues {
		return runtimeStoragePorts{}, errors.New(
			"state backend must advertise named delivery queues",
		)
	}
	atomicState, err := resolveAtomicStateBackend(backend, capabilities)
	if err != nil {
		return runtimeStoragePorts{}, err
	}
	trajectories := backend.Trajectories()
	operations := backend.Operations()
	contextInjections := backend.ContextInjections()
	deliveryStore, err := backend.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		return runtimeStoragePorts{}, fmt.Errorf("open host outbox: %w", err)
	}
	if trajectories == nil || operations == nil ||
		contextInjections == nil || deliveryStore == nil {
		return runtimeStoragePorts{}, errors.New(
			"state backend returned a nil store",
		)
	}
	return runtimeStoragePorts{
		trajectories:      trajectories,
		operations:        operations,
		contextInjections: contextInjections,
		delivery:          deliveryStore,
		atomicState:       atomicState,
	}, nil
}

func resolveAtomicStateBackend(
	backend sdk.StateBackend,
	capabilities sdk.StorageCapabilities,
) (sdk.AtomicStateBackend, error) {
	atomicState, implementsAtomicState := backend.(sdk.AtomicStateBackend)
	switch {
	case capabilities.AtomicState && !implementsAtomicState:
		return nil, errors.New(
			"state backend advertises atomic state without implementing AtomicStateBackend",
		)
	case !capabilities.AtomicState && implementsAtomicState:
		return nil, errors.New(
			"state backend implements AtomicStateBackend without advertising atomic state",
		)
	case capabilities.AtomicState:
		return atomicState, nil
	default:
		return nil, nil
	}
}

// RequestClose starts shutdown once without waiting for cleanup to finish.
// Supervisors can use this to ask active runtime-owned work to unwind, then
// wait on the goroutine that owns the borrowed host.
func (runtime *Runtime) RequestClose(ctx context.Context) {
	if runtime == nil {
		return
	}
	runtime.startClose(ctx)
}

func (runtime *Runtime) startClose(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		ctx = context.Background()
	}
	closeCtx := lifecycle.Detached(ctx)
	runtime.mu.Lock()
	var states []*mountState
	if !runtime.closed {
		runtime.closed = true
		current := runtime.current.Load()
		states = make([]*mountState, 0, len(current.plugins))
		for _, state := range current.plugins {
			states = append(states, state)
			state.retire(closeCtx)
		}
		empty := initialSnapshot()
		empty.generation = current.generation + 1
		runtime.current.Store(empty)
	}
	done := runtime.closeDone
	runtime.mu.Unlock()

	if states != nil {
		runtime.trajectoryExecution.stop()
		runtime.delivery.stop()
		runtime.operation.stop()
		runtime.observer.stop()
		go runtime.finishClose(closeCtx, states)
	}
	return done
}

// Close starts shutdown once. A caller timeout stops waiting, not cleanup, so
// a later call can wait for the same shutdown and receive its final error.
func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := runtime.startClose(ctx)

	select {
	case <-done:
		return runtime.closeErr
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return runtime.closeErr
	}
}

func (runtime *Runtime) finishClose(ctx context.Context, states []*mountState) {
	runtime.closeErr = runtime.finishCloseError(ctx, states)
	close(runtime.closeDone)
}

func (runtime *Runtime) finishCloseError(
	ctx context.Context,
	states []*mountState,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf(
				"runtime close panic: %v\n%s",
				recovered,
				debug.Stack(),
			)
			runtime.logger.ErrorContext(
				ctx,
				"runtime close panic",
				"panic", recovered,
			)
			err = errors.Join(err, panicErr)
		}
	}()
	var errs []error
	runtime.delivery.waitDurableStopped()
	runtime.operation.waitDurableStopped()
	runtime.trajectoryExecution.waitDurableStopped()
	if err := runtime.observer.waitBestEffortStopped(
		ctx,
		lifecycle.DefaultFinalizationTimeout,
	); err != nil {
		errs = append(errs, err)
	}
	for _, state := range states {
		<-state.done
		if err := state.closeError(); err != nil {
			errs = append(errs, fmt.Errorf(
				"close plugin %q: %w",
				state.manifest.Name,
				err,
			))
		}
	}
	if runtime.storage != nil && runtime.closeStorage {
		if err := runtime.storage.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close state backend: %w", err))
		}
	}
	return errors.Join(errs...)
}
