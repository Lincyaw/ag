package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

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
	closeOnError := func(cause error) (*Mount, error) {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return nil, errors.Join(cause, connection.Close(closeCtx))
	}

	manifest := connection.Manifest()
	manifest.Requires = slices.Clone(manifest.Requires)
	manifest.Conflicts = slices.Clone(manifest.Conflicts)
	manifest.Registers = slices.Clone(manifest.Registers)
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
	runtime.wrapSynchronousResources(staged, manifest)

	state := newMountState(manifest, description, connection)
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
	runtime.current.Store(next)
	if len(staged.subscribers) > 0 {
		runtime.startDeliveryWorkersLocked()
	}
	runtime.mu.Unlock()

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
		sdk.EventPluginMounted,
		"",
		sdk.PluginLifecyclePayload{
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
		sdk.EventPluginUnmounted,
		"",
		sdk.PluginLifecyclePayload{
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
	manifest   sdk.Manifest
	source     string
	connection sdk.Connection
	mu         sync.Mutex
	refs       int
	retired    bool
	closing    bool
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
