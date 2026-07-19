package runtime

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Mount is the lifecycle handle for one published plugin composition.
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
	source sdk.Source,
) (*Mount, error) {
	if source == nil {
		return nil, errors.New("plugin source is nil")
	}
	description := sourceDescription(source)

	ctx, span := runtime.tracer.Start(
		ctx,
		"plugin.mount",
		trace.WithAttributes(
			attribute.String("plugin.source", description),
		),
	)
	defer span.End()

	connection, err := source.Open(ctx)
	if err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf(
			"open plugin source %s: %w",
			description,
			err,
		)
	}
	pluginName := ""
	closeOnError := func(cause error) (*Mount, error) {
		closeCtx, cancel := lifecycle.WithDetachedTimeout(ctx, 5*time.Second)
		defer cancel()
		return nil, errors.Join(
			cause,
			closePluginConnection(closeCtx, pluginName, connection),
		)
	}

	manifest := sdk.CloneManifest(connection.Manifest())
	pluginName = manifest.Name
	if err := manifest.Validate(); err != nil {
		recordSpanError(span, err)
		return closeOnError(fmt.Errorf("validate plugin manifest: %w", err))
	}
	span.SetAttributes(
		attribute.String("plugin.name", manifest.Name),
		attribute.String("plugin.version", manifest.Version),
	)

	staged := plugincontract.NewAgentRegistrar()
	if err := connection.Install(ctx, staged); err != nil {
		recordSpanError(span, err)
		return closeOnError(fmt.Errorf(
			"install plugin %q into staging registry: %w",
			manifest.Name,
			err,
		))
	}
	if err := staged.ValidateManifest(manifest); err != nil {
		recordSpanError(span, err)
		return closeOnError(err)
	}
	state := newMountState(manifest, description, connection)
	runtime.adaptSynchronousResources(staged, state)
	var eventLease *snapshotLease
	var lifecycleEvents postCommitEventBundle
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return closeOnError(errors.New("runtime is closed"))
	}
	next := runtime.current.Load().clone()
	if err := next.add(
		state,
		staged,
		&runtime.nextSequence,
		runtime.hookTimeout,
	); err != nil {
		runtime.mu.Unlock()
		recordSpanError(span, err)
		return closeOnError(err)
	}
	next.generation++
	eventLease, err = acquireMountsLocked(next.plugins)
	if err != nil {
		runtime.mu.Unlock()
		recordSpanError(span, err)
		return closeOnError(err)
	}
	eventLease.snapshot = next
	lifecycleEvent, err := runtime.preparePluginLifecycleEventPlan(
		eventLease.snapshot,
		sdk.EventPluginMounted,
		sdk.PluginLifecyclePayload{
			Name:       manifest.Name,
			Version:    manifest.Version,
			Source:     state.source,
			Generation: next.generation,
		},
	)
	if err != nil {
		runtime.mu.Unlock()
		eventLease.release()
		recordSpanError(span, err)
		return closeOnError(err)
	}
	lifecycleEvents = postCommitEventBundle{lifecycleEvent}
	runtime.current.Store(next)
	if len(staged.Subscribers) > 0 {
		runtime.startDeliveryWorkersLocked()
	}
	runtime.mu.Unlock()
	defer eventLease.release()

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
	lifecycleEvents.dispatch(ctx, runtime)
	return &Mount{runtime: runtime, state: state}, nil
}

func sourceDescription(source sdk.Source) string {
	if source == nil {
		return "<nil>"
	}
	if value := source.String(); value != "" {
		return value
	}
	return fmt.Sprintf("%T", source)
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

	var eventLease *snapshotLease
	var lifecycleEvents postCommitEventBundle
	runtime.mu.Lock()
	current := runtime.current.Load()
	active, exists := current.plugins[state.manifest.Name]
	if !exists || active != state {
		runtime.mu.Unlock()
		return nil
	}
	next := current.without(state)
	if err := next.validateComposition(); err != nil {
		runtime.mu.Unlock()
		recordSpanError(span, err)
		return fmt.Errorf("unmount plugin %q: %w", state.manifest.Name, err)
	}
	next.generation++
	eventLease, err := acquireMountsLocked(next.plugins)
	if err != nil {
		runtime.mu.Unlock()
		recordSpanError(span, err)
		return fmt.Errorf("unmount plugin %q: %w", state.manifest.Name, err)
	}
	eventLease.snapshot = next
	lifecycleEvent, err := runtime.preparePluginLifecycleEventPlan(
		eventLease.snapshot,
		sdk.EventPluginUnmounted,
		sdk.PluginLifecyclePayload{
			Name:       state.manifest.Name,
			Version:    state.manifest.Version,
			Source:     state.source,
			Generation: next.generation,
		},
	)
	if err != nil {
		runtime.mu.Unlock()
		eventLease.release()
		recordSpanError(span, err)
		return fmt.Errorf("unmount plugin %q: %w", state.manifest.Name, err)
	}
	lifecycleEvents = postCommitEventBundle{lifecycleEvent}
	runtime.current.Store(next)
	state.retire(ctx)
	runtime.mu.Unlock()
	defer eventLease.release()

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
	lifecycleEvents.dispatch(ctx, runtime)
	return nil
}

type snapshotLease struct {
	snapshot *registrySnapshot
	mounts   []*mountState
	mu       sync.Mutex
	released bool
}

func (runtime *Runtime) acquireSnapshot() (*snapshotLease, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, errors.New("runtime is closed")
	}
	// Current snapshot reads are linearized with composition mutations so new
	// callers cannot retain a snapshot that has just been unpublished.
	snapshot := runtime.current.Load()
	return acquireRegistrySnapshotLocked(snapshot)
}

func (runtime *Runtime) acquireRegistrySnapshot(
	snapshot *registrySnapshot,
) (*snapshotLease, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, errors.New("runtime is closed")
	}
	return acquireRegistrySnapshotLocked(snapshot)
}

func acquireRegistrySnapshotLocked(
	snapshot *registrySnapshot,
) (*snapshotLease, error) {
	if snapshot == nil {
		return nil, errors.New("runtime registry snapshot is nil")
	}
	lease, err := acquireMountsLocked(snapshot.plugins)
	if err != nil {
		return nil, err
	}
	lease.snapshot = snapshot
	return lease, nil
}

func (runtime *Runtime) acquireMounts(
	states ...*mountState,
) (*snapshotLease, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, errors.New("runtime is closed")
	}
	index := make(map[string]*mountState, len(states))
	for _, state := range states {
		if state != nil {
			index[state.manifest.Name] = state
		}
	}
	return acquireMountsLocked(index)
}

func acquireMountsLocked(
	index map[string]*mountState,
) (*snapshotLease, error) {
	mounts := make([]*mountState, 0, len(index))
	for _, state := range index {
		if err := state.acquire(); err != nil {
			for _, acquired := range mounts {
				acquired.release()
			}
			return nil, err
		}
		mounts = append(mounts, state)
	}
	return &snapshotLease{mounts: mounts}, nil
}

func (lease *snapshotLease) release() {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	if lease.released {
		lease.mu.Unlock()
		return
	}
	lease.released = true
	mounts := lease.mounts
	lease.mu.Unlock()
	for _, state := range mounts {
		state.release()
	}
}

var errMountReferenceUnderflow = errors.New(
	"sdk: plugin mount reference released more than acquired",
)

type mountState struct {
	manifest   sdk.Manifest
	source     string
	connection sdk.Connection
	mu         sync.Mutex
	refs       int
	retired    bool
	closing    bool
	closeCtx   context.Context
	closeErr   error
	done       chan struct{}
}

func newMountState(
	manifest sdk.Manifest,
	source string,
	connection sdk.Connection,
) *mountState {
	return &mountState{
		manifest:   manifest,
		source:     source,
		connection: connection,
		done:       make(chan struct{}),
	}
}

func (state *mountState) acquire() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing {
		return fmt.Errorf(
			"plugin %q mount is closing",
			state.manifest.Name,
		)
	}
	state.refs++
	return nil
}

func (state *mountState) release() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.refs == 0 {
		if !errors.Is(state.closeErr, errMountReferenceUnderflow) {
			state.closeErr = errors.Join(
				state.closeErr,
				errMountReferenceUnderflow,
			)
		}
		state.startCloseLocked()
		return
	}
	state.refs--
	state.startCloseLocked()
}

func (state *mountState) retire(ctx context.Context) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.retired = true
	state.closeCtx = lifecycle.Detached(ctx)
	state.startCloseLocked()
}

func (state *mountState) startCloseLocked() {
	if !state.retired || state.refs != 0 || state.closing {
		return
	}
	state.closing = true
	closeCtx := state.closeCtx
	if closeCtx == nil {
		closeCtx = context.Background()
	}
	go func() {
		ctx, cancel := context.WithTimeout(closeCtx, 10*time.Second)
		defer cancel()
		err := closePluginConnection(ctx, state.manifest.Name, state.connection)
		state.mu.Lock()
		state.closeErr = errors.Join(state.closeErr, err)
		close(state.done)
		state.mu.Unlock()
	}()
}

func closePluginConnection(
	ctx context.Context,
	pluginName string,
	connection sdk.Connection,
) (err error) {
	if pluginName == "" {
		pluginName = "<unknown>"
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"close plugin %q connection panic: %v\n%s",
				pluginName,
				recovered,
				debug.Stack(),
			)
		}
	}()
	return connection.Close(ctx)
}

func (state *mountState) closeError() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.closeErr
}
