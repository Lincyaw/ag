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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/lincyaw/ag/sdk"

type StorageOwnership string

const (
	StorageOwned    StorageOwnership = "owned"
	StorageBorrowed StorageOwnership = "borrowed"
)

type RuntimeConfig struct {
	RuntimeVersion      string
	Logger              *slog.Logger
	Tracer              trace.Tracer
	Meter               metric.Meter
	Storage             sdk.StateBackend
	StorageOwnership    StorageOwnership
	DeliveryWorkers     int
	DeliveryLease       time.Duration
	DeliveryPoll        time.Duration
	DeliveryTimeout     time.Duration
	DeliveryMaxAttempts int
	HookTimeout         time.Duration
	OperationPoll       time.Duration
	OperationLease      time.Duration
	TrajectoryLease     time.Duration
}

type Runtime struct {
	mu                 sync.Mutex
	current            atomic.Pointer[registrySnapshot]
	closed             bool
	closeDone          chan struct{}
	closeErr           error
	nextSequence       uint64
	version            string
	logger             *slog.Logger
	tracer             trace.Tracer
	mounts             metric.Int64Counter
	unmounts           metric.Int64Counter
	events             metric.Int64Counter
	hooks              metric.Int64Counter
	hookTimeout        time.Duration
	storage            sdk.StateBackend
	atomicState        sdk.AtomicStateBackend
	closeStorage       bool
	trajectories       sdk.TrajectoryStore
	trajectoryLease    time.Duration
	trajectoryWorkerID string
	trajectoryContext  context.Context
	cancelTrajectories context.CancelFunc
	trajectoryWait     sync.WaitGroup
	operation          operationRuntime
	delivery           deliveryRuntime
}

func normalizeRuntimeConfig(config RuntimeConfig) (RuntimeConfig, error) {
	if strings.TrimSpace(config.RuntimeVersion) == "" {
		config.RuntimeVersion = "development"
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
	if config.DeliveryWorkers == 0 {
		config.DeliveryWorkers = 2
	}
	if config.DeliveryLease == 0 {
		config.DeliveryLease = 30 * time.Second
	}
	if config.DeliveryPoll == 0 {
		config.DeliveryPoll = 25 * time.Millisecond
	}
	if config.DeliveryTimeout == 0 {
		config.DeliveryTimeout = 5 * time.Second
	}
	if config.DeliveryMaxAttempts == 0 {
		config.DeliveryMaxAttempts = 8
	}
	if config.HookTimeout == 0 {
		config.HookTimeout = time.Second
	}
	if config.OperationPoll == 0 {
		config.OperationPoll = 100 * time.Millisecond
	}
	if config.OperationLease == 0 {
		config.OperationLease = 30 * time.Second
	}
	if config.TrajectoryLease == 0 {
		config.TrajectoryLease = 30 * time.Second
	}
	if config.DeliveryWorkers < 1 || config.DeliveryLease <= 0 ||
		config.DeliveryPoll <= 0 || config.DeliveryTimeout <= 0 ||
		config.DeliveryMaxAttempts < 1 || config.HookTimeout <= 0 ||
		config.OperationPoll <= 0 || config.OperationLease <= 0 ||
		config.TrajectoryLease <= 0 {
		return RuntimeConfig{}, errors.New(
			"delivery and operation settings must be positive",
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
	// Ownership of Storage transfers only when construction succeeds.
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
	if err := config.Storage.Health(context.Background()); err != nil {
		return nil, fmt.Errorf("state backend health: %w", err)
	}
	var atomicState sdk.AtomicStateBackend
	if config.Storage.Capabilities().AtomicState {
		var ok bool
		atomicState, ok = config.Storage.(sdk.AtomicStateBackend)
		if !ok {
			return nil, errors.New(
				"state backend advertises atomic state without implementing AtomicStateBackend",
			)
		}
	}
	trajectories := config.Storage.Trajectories()
	operations := config.Storage.Operations()
	deliveryStore, err := config.Storage.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		return nil, fmt.Errorf("open host outbox: %w", err)
	}
	if trajectories == nil || operations == nil || deliveryStore == nil {
		return nil, errors.New("state backend returned a nil store")
	}
	deliveryContext, cancelDeliveries := context.WithCancel(context.Background())
	operationContext, cancelOperations := context.WithCancel(context.Background())
	trajectoryContext, cancelTrajectories :=
		context.WithCancel(context.Background())
	runtime := &Runtime{
		version:            config.RuntimeVersion,
		logger:             config.Logger,
		tracer:             config.Tracer,
		closeDone:          make(chan struct{}),
		mounts:             mounts,
		unmounts:           unmounts,
		events:             events,
		hooks:              hooks,
		hookTimeout:        config.HookTimeout,
		storage:            config.Storage,
		atomicState:        atomicState,
		closeStorage:       config.StorageOwnership == StorageOwned,
		trajectories:       trajectories,
		trajectoryLease:    config.TrajectoryLease,
		trajectoryWorkerID: "trajectory-runtime-" + sdk.NewID(),
		trajectoryContext:  trajectoryContext,
		cancelTrajectories: cancelTrajectories,
		operation: operationRuntime{
			store:    operations,
			context:  operationContext,
			cancel:   cancelOperations,
			cancels:  make(map[string]context.CancelFunc),
			poll:     config.OperationPoll,
			lease:    config.OperationLease,
			workerID: "runtime-" + sdk.NewID(),
		},
		delivery: deliveryRuntime{
			store:       deliveryStore,
			workers:     config.DeliveryWorkers,
			lease:       config.DeliveryLease,
			poll:        config.DeliveryPoll,
			timeout:     config.DeliveryTimeout,
			maxAttempts: config.DeliveryMaxAttempts,
			context:     deliveryContext,
			cancel:      cancelDeliveries,
		},
	}
	runtime.current.Store(initialSnapshot())
	return runtime, nil
}

// Close starts shutdown once. A caller timeout stops waiting, not cleanup, so
// a later call can wait for the same shutdown and receive its final error.
func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}

	runtime.mu.Lock()
	var states []*mountState
	if !runtime.closed {
		runtime.closed = true
		current := runtime.current.Load()
		states = make([]*mountState, 0, len(current.plugins))
		for _, state := range current.plugins {
			states = append(states, state)
			state.retire()
		}
		empty := initialSnapshot()
		empty.generation = current.generation + 1
		runtime.current.Store(empty)
	}
	done := runtime.closeDone
	runtime.mu.Unlock()

	if states != nil {
		runtime.cancelTrajectories()
		runtime.delivery.cancel()
		runtime.operation.cancel()
		go runtime.finishClose(states)
	}

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

func (runtime *Runtime) finishClose(states []*mountState) {
	runtime.delivery.wait.Wait()
	runtime.operation.wait.Wait()
	runtime.trajectoryWait.Wait()
	var errs []error
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
		if err := runtime.storage.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("close state backend: %w", err))
		}
	}
	runtime.closeErr = errors.Join(errs...)
	close(runtime.closeDone)
}
