package sdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/lincyaw/ag/sdk"

type RuntimeConfig struct {
	Logger              *slog.Logger
	Tracer              trace.Tracer
	Meter               metric.Meter
	Trajectories        TrajectoryStore
	Operations          OperationStore
	Outbox              OutboxStore
	DeliveryWorkers     int
	DeliveryLease       time.Duration
	DeliveryPoll        time.Duration
	DeliveryTimeout     time.Duration
	DeliveryMaxAttempts int
	OperationPoll       time.Duration
}

type Runtime struct {
	mu                  sync.Mutex
	current             atomic.Pointer[registrySnapshot]
	closed              bool
	nextSequence        uint64
	logger              *slog.Logger
	tracer              trace.Tracer
	mounts              metric.Int64Counter
	unmounts            metric.Int64Counter
	events              metric.Int64Counter
	hooks               metric.Int64Counter
	trajectories        TrajectoryStore
	operations          OperationStore
	operationContext    context.Context
	cancelOperations    context.CancelFunc
	operationMu         sync.Mutex
	operationCancels    map[string]context.CancelFunc
	operationWait       sync.WaitGroup
	outbox              OutboxStore
	deliveryWorkers     int
	deliveryLease       time.Duration
	deliveryPoll        time.Duration
	deliveryTimeout     time.Duration
	deliveryMaxAttempts int
	deliveryContext     context.Context
	cancelDeliveries    context.CancelFunc
	deliveryOnce        sync.Once
	deliveryWait        sync.WaitGroup
	operationPoll       time.Duration
}

func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Tracer == nil {
		config.Tracer = otel.Tracer(instrumentationName)
	}
	if config.Meter == nil {
		config.Meter = otel.Meter(instrumentationName)
	}
	if config.Trajectories == nil {
		config.Trajectories = NewMemoryTrajectoryStore()
	}
	if config.Operations == nil {
		config.Operations = NewMemoryOperationStore()
	}
	if config.Outbox == nil {
		config.Outbox = NewMemoryOutboxStore()
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
	if config.OperationPoll == 0 {
		config.OperationPoll = 100 * time.Millisecond
	}
	if config.DeliveryWorkers < 1 || config.DeliveryLease <= 0 ||
		config.DeliveryPoll <= 0 || config.DeliveryTimeout <= 0 ||
		config.DeliveryMaxAttempts < 1 || config.OperationPoll <= 0 {
		return nil, errors.New("delivery and operation settings must be positive")
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
	deliveryContext, cancelDeliveries := context.WithCancel(context.Background())
	operationContext, cancelOperations := context.WithCancel(context.Background())
	runtime := &Runtime{
		logger:              config.Logger,
		tracer:              config.Tracer,
		mounts:              mounts,
		unmounts:            unmounts,
		events:              events,
		hooks:               hooks,
		trajectories:        config.Trajectories,
		operations:          config.Operations,
		operationContext:    operationContext,
		cancelOperations:    cancelOperations,
		operationCancels:    make(map[string]context.CancelFunc),
		outbox:              config.Outbox,
		deliveryWorkers:     config.DeliveryWorkers,
		deliveryLease:       config.DeliveryLease,
		deliveryPoll:        config.DeliveryPoll,
		deliveryTimeout:     config.DeliveryTimeout,
		deliveryMaxAttempts: config.DeliveryMaxAttempts,
		deliveryContext:     deliveryContext,
		cancelDeliveries:    cancelDeliveries,
		operationPoll:       config.OperationPoll,
	}
	runtime.current.Store(initialSnapshot())
	return runtime, nil
}

func (runtime *Runtime) Trajectories() TrajectoryStore {
	if runtime == nil {
		return nil
	}
	return runtime.trajectories
}

func (runtime *Runtime) Operations() OperationStore {
	if runtime == nil {
		return nil
	}
	return runtime.operations
}

func (runtime *Runtime) Outbox() OutboxStore {
	if runtime == nil {
		return nil
	}
	return runtime.outbox
}

type Mount struct {
	runtime *Runtime
	state   *mountState
}

func (mount *Mount) Name() string {
	if mount == nil || mount.state == nil {
		return ""
	}
	return mount.state.manifest.Name
}

func (mount *Mount) Unmount(ctx context.Context) error {
	if mount == nil || mount.runtime == nil || mount.state == nil {
		return nil
	}
	return mount.runtime.unmount(ctx, mount.state)
}

func (mount *Mount) Done() <-chan struct{} {
	if mount == nil || mount.state == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return mount.state.done
}

func (runtime *Runtime) Mount(
	ctx context.Context,
	source Source,
) (*Mount, error) {
	if source == nil {
		return nil, errors.New("plugin source is nil")
	}

	ctx, span := runtime.tracer.Start(
		ctx,
		"plugin.mount",
		trace.WithAttributes(
			attribute.String("plugin.source", sourceDescription(source)),
		),
	)
	defer span.End()

	connection, err := source.Open(ctx)
	if err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf("open plugin source %s: %w", sourceDescription(source), err)
	}
	closeOnError := func(cause error) (*Mount, error) {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return nil, errors.Join(cause, connection.Close(closeCtx))
	}

	manifest := connection.Manifest()
	if err := manifest.Validate(); err != nil {
		recordSpanError(span, err)
		return closeOnError(fmt.Errorf("validate plugin manifest: %w", err))
	}
	span.SetAttributes(
		attribute.String("plugin.name", manifest.Name),
		attribute.String("plugin.version", manifest.Version),
	)

	staged := newStagingRegistrar()
	if err := connection.Install(ctx, staged); err != nil {
		recordSpanError(span, err)
		return closeOnError(fmt.Errorf(
			"install plugin %q into staging registry: %w",
			manifest.Name,
			err,
		))
	}
	if err := staged.validateManifest(manifest); err != nil {
		recordSpanError(span, err)
		return closeOnError(err)
	}
	runtime.wrapSynchronousResources(staged)

	state := newMountState(manifest, sourceDescription(source), connection)
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return closeOnError(errors.New("runtime is closed"))
	}
	next := runtime.current.Load().clone()
	if err := next.add(state, staged, &runtime.nextSequence); err != nil {
		runtime.mu.Unlock()
		recordSpanError(span, err)
		return closeOnError(err)
	}
	next.generation++
	runtime.current.Store(next)
	runtime.mu.Unlock()
	if len(staged.subscribers) > 0 {
		runtime.startDeliveryWorkers()
	}

	runtime.mounts.Add(
		ctx,
		1,
		metric.WithAttributes(attribute.String("plugin.name", manifest.Name)),
	)
	runtime.logger.InfoContext(
		ctx,
		"plugin mounted",
		"plugin",
		manifest.Name,
		"version",
		manifest.Version,
		"generation",
		next.generation,
		"source",
		state.source,
	)
	if _, emitErr := runtime.Emit(
		ctx,
		EventPluginMounted,
		"",
		PluginLifecyclePayload{
			Name:       manifest.Name,
			Version:    manifest.Version,
			Source:     state.source,
			Generation: next.generation,
		},
	); emitErr != nil {
		runtime.logger.WarnContext(
			ctx,
			"plugin mounted event failed",
			"plugin",
			manifest.Name,
			"error",
			emitErr,
		)
	}
	return &Mount{runtime: runtime, state: state}, nil
}

func (runtime *Runtime) MountRegistered(
	ctx context.Context,
	registry *PluginRegistry,
	nameOrURI string,
) (*Mount, error) {
	if registry == nil {
		return nil, errors.New("plugin registry is nil")
	}
	source, err := registry.Resolve(ctx, nameOrURI)
	if err != nil {
		return nil, err
	}
	return runtime.Mount(ctx, source)
}

func (runtime *Runtime) unmount(
	ctx context.Context,
	state *mountState,
) error {
	ctx, span := runtime.tracer.Start(
		ctx,
		"plugin.unmount",
		trace.WithAttributes(attribute.String("plugin.name", state.manifest.Name)),
	)
	defer span.End()

	runtime.mu.Lock()
	current := runtime.current.Load()
	active, exists := current.plugins[state.manifest.Name]
	if !exists || active != state {
		runtime.mu.Unlock()
		return nil
	}
	next := current.without(state)
	if err := next.validateDependencies(); err != nil {
		runtime.mu.Unlock()
		recordSpanError(span, err)
		return fmt.Errorf("unmount plugin %q: %w", state.manifest.Name, err)
	}
	next.generation++
	runtime.current.Store(next)
	state.retire()
	runtime.mu.Unlock()

	runtime.unmounts.Add(
		ctx,
		1,
		metric.WithAttributes(attribute.String("plugin.name", state.manifest.Name)),
	)
	runtime.logger.InfoContext(
		ctx,
		"plugin unmounted",
		"plugin",
		state.manifest.Name,
		"generation",
		next.generation,
	)
	if _, emitErr := runtime.Emit(
		ctx,
		EventPluginUnmounted,
		"",
		PluginLifecyclePayload{
			Name:       state.manifest.Name,
			Version:    state.manifest.Version,
			Source:     state.source,
			Generation: next.generation,
		},
	); emitErr != nil {
		runtime.logger.WarnContext(
			ctx,
			"plugin unmounted event failed",
			"plugin",
			state.manifest.Name,
			"error",
			emitErr,
		)
	}
	return nil
}

func (runtime *Runtime) Close(ctx context.Context) error {
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil
	}
	runtime.closed = true
	current := runtime.current.Load()
	states := make([]*mountState, 0, len(current.plugins))
	for _, state := range current.plugins {
		states = append(states, state)
		state.retire()
	}
	empty := initialSnapshot()
	empty.generation = current.generation + 1
	runtime.current.Store(empty)
	runtime.mu.Unlock()
	runtime.cancelDeliveries()
	runtime.cancelOperations()

	workersDone := make(chan struct{})
	go func() {
		runtime.deliveryWait.Wait()
		close(workersDone)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-workersDone:
	}
	operationsDone := make(chan struct{})
	go func() {
		runtime.operationWait.Wait()
		close(operationsDone)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-operationsDone:
	}

	var errs []error
	for _, state := range states {
		select {
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		case <-state.done:
			if err := state.closeError(); err != nil {
				errs = append(errs, fmt.Errorf(
					"close plugin %q: %w",
					state.manifest.Name,
					err,
				))
			}
		}
	}
	return errors.Join(errs...)
}

type MountedPlugin struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Description string   `json:"description"`
	Source      string   `json:"source"`
	Registers   []string `json:"registers"`
}

type CatalogSnapshot struct {
	Generation   uint64           `json:"generation"`
	Plugins      []MountedPlugin  `json:"plugins"`
	Providers    []ProviderSpec   `json:"providers"`
	Tools        []ToolSpec       `json:"tools"`
	Hooks        []HookSpec       `json:"hooks"`
	Subscribers  []SubscriberSpec `json:"subscribers"`
	Capabilities []CapabilitySpec `json:"capabilities"`
	Events       []EventContract  `json:"events"`
}

func (runtime *Runtime) Catalog() CatalogSnapshot {
	snapshot := runtime.current.Load()
	result := CatalogSnapshot{Generation: snapshot.generation}
	for _, state := range snapshot.plugins {
		result.Plugins = append(result.Plugins, MountedPlugin{
			Name:        state.manifest.Name,
			Version:     state.manifest.Version,
			Description: state.manifest.Description,
			Source:      state.source,
			Registers:   append([]string(nil), state.manifest.Registers...),
		})
	}
	for _, provider := range snapshot.providers {
		result.Providers = append(result.Providers, provider.provider.Spec())
	}
	for _, tool := range snapshot.tools {
		result.Tools = append(result.Tools, tool.tool.Spec())
	}
	for _, hooks := range snapshot.hooks {
		for _, hook := range hooks {
			result.Hooks = append(result.Hooks, hook.spec)
		}
	}
	for _, subscriber := range snapshot.subscribers {
		spec := subscriber.spec
		spec.Events = append([]string(nil), spec.Events...)
		result.Subscribers = append(result.Subscribers, spec)
	}
	for _, capability := range snapshot.capabilities {
		result.Capabilities = append(
			result.Capabilities,
			capability.capability.Spec(),
		)
	}
	for _, event := range snapshot.events {
		contract := event.contract
		contract.MutableFields = append([]string(nil), contract.MutableFields...)
		result.Events = append(result.Events, contract)
	}
	sortCatalog(&result)
	return result
}

func sortCatalog(catalog *CatalogSnapshot) {
	slices.SortFunc(catalog.Plugins, func(left, right MountedPlugin) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Providers, func(left, right ProviderSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Tools, func(left, right ToolSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Hooks, func(left, right HookSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Subscribers, func(left, right SubscriberSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Capabilities, func(left, right CapabilitySpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Events, func(left, right EventContract) int {
		return strings.Compare(left.Name, right.Name)
	})
}

type snapshotLease struct {
	snapshot *registrySnapshot
	mounts   []*mountState
}

func (runtime *Runtime) acquireSnapshot() (*snapshotLease, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, errors.New("runtime is closed")
	}
	snapshot := runtime.current.Load()
	mounts := make([]*mountState, 0, len(snapshot.plugins))
	for _, state := range snapshot.plugins {
		state.acquire()
		mounts = append(mounts, state)
	}
	return &snapshotLease{snapshot: snapshot, mounts: mounts}, nil
}

func (lease *snapshotLease) release() {
	if lease == nil {
		return
	}
	for _, state := range lease.mounts {
		state.release()
	}
}

type mountState struct {
	manifest   Manifest
	source     string
	connection Connection
	mu         sync.Mutex
	refs       int
	retired    bool
	closing    bool
	closeErr   error
	done       chan struct{}
}

func newMountState(
	manifest Manifest,
	source string,
	connection Connection,
) *mountState {
	return &mountState{
		manifest:   manifest,
		source:     source,
		connection: connection,
		done:       make(chan struct{}),
	}
}

func (state *mountState) acquire() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.refs++
}

func (state *mountState) release() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.refs--
	if state.refs < 0 {
		panic("sdk: negative plugin mount reference count")
	}
	state.startCloseLocked()
}

func (state *mountState) retire() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.retired = true
	state.startCloseLocked()
}

func (state *mountState) startCloseLocked() {
	if !state.retired || state.refs != 0 || state.closing {
		return
	}
	state.closing = true
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := state.connection.Close(ctx)
		state.mu.Lock()
		state.closeErr = err
		close(state.done)
		state.mu.Unlock()
	}()
}

func (state *mountState) closeError() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.closeErr
}
