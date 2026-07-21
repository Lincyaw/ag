package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/lincyaw/ag/gateway"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func GatewayRuntimeBuilder(
	config appconfig.Config,
	logger *slog.Logger,
	tracer trace.Tracer,
	meter metric.Meter,
	version string,
) gateway.RuntimeBuilder {
	return func(
		ctx context.Context,
		spec gateway.RuntimeBuildSpec,
		state sdk.StateBackend,
	) (*agentruntime.Runtime, error) {
		runtime, err := agentruntime.NewRuntimeContext(
			ctx,
			agentruntime.RuntimeConfig{
				RuntimeVersion:   version,
				Logger:           logger,
				Tracer:           tracer,
				Meter:            meter,
				Storage:          state,
				StorageOwnership: agentruntime.StorageBorrowed,
				EventObserver:    spec.EventObserver,
			},
		)
		if err != nil {
			return nil, err
		}
		fail := func(cause error) (*agentruntime.Runtime, error) {
			closeCtx, cancel := closeContext(ctx)
			defer cancel()
			return nil, errors.Join(cause, runtime.Close(closeCtx))
		}
		if spec.Interactions != nil {
			if _, err := runtime.Mount(
				ctx,
				sdk.Local(gateway.NewInteractionPlugin(spec.Interactions)),
			); err != nil {
				return fail(err)
			}
			if _, err := runtime.Mount(
				ctx,
				sdk.Local(gateway.NewPermissionPlugin(
					spec.Interactions,
					spec.Permissions,
				)),
			); err != nil {
				return fail(err)
			}
		}
		sessionConfig, err := gatewaySessionConfig(config, spec)
		if err != nil {
			return fail(err)
		}
		plan, err := BuildPluginPlan(
			ctx,
			sessionConfig,
			logger,
			tracer,
			meter,
		)
		if err != nil {
			return fail(err)
		}
		bound := make(map[string]struct{}, len(spec.Plugins))
		for _, binding := range spec.Plugins {
			bound[binding.Name] = struct{}{}
		}
		for _, name := range plan.Mounts {
			if _, replaced := bound[name]; replaced {
				continue
			}
			source, err := plan.Catalog.Resolve(ctx, name)
			if err != nil {
				return fail(err)
			}
			if _, err := runtime.Mount(ctx, source); err != nil {
				return fail(err)
			}
		}

		for _, binding := range spec.Plugins {
			source, err := plan.Catalog.Resolve(ctx, binding.URI)
			if err != nil {
				return fail(err)
			}
			if _, err := runtime.Mount(ctx, expectedManifestSource{
				Source: source, expected: binding.Manifest,
			}); err != nil {
				return fail(fmt.Errorf(
					"mount session plugin %s@%s: %w",
					binding.Name,
					binding.InstanceID,
					err,
				))
			}
		}
		return runtime, nil
	}
}

func gatewaySessionConfig(
	config appconfig.Config,
	spec gateway.RuntimeBuildSpec,
) (appconfig.Config, error) {
	if len(spec.RuntimeConfig) > 0 {
		var profile appconfig.TrajectoryRuntimeProfile
		if err := json.Unmarshal(spec.RuntimeConfig, &profile); err != nil {
			return appconfig.Config{}, fmt.Errorf(
				"decode trajectory runtime profile: %w",
				err,
			)
		}
		config = profile.Apply(config)
	}
	if spec.WorkspaceRoot != "" {
		config.Workspace.Root = spec.WorkspaceRoot
	}
	if spec.Model != "" {
		config.ApplyModelReference(spec.Model)
	}
	if spec.AutoCompact != nil {
		config.Compact.Enabled = *spec.AutoCompact
	}
	return config, nil
}

type expectedManifestSource struct {
	sdk.Source
	expected sdk.Manifest
}

func (source expectedManifestSource) Open(
	ctx context.Context,
) (sdk.Connection, error) {
	connection, err := source.Source.Open(ctx)
	if err != nil {
		return nil, err
	}
	actual := connection.Manifest()
	if reflect.DeepEqual(actual, source.expected) {
		return connection, nil
	}
	closeCtx, cancel := closeContext(ctx)
	defer cancel()
	return nil, errors.Join(
		fmt.Errorf(
			"discovered plugin manifest changed from %s@%s to %s@%s",
			source.expected.Name,
			source.expected.Version,
			actual.Name,
			actual.Version,
		),
		connection.Close(closeCtx),
	)
}
