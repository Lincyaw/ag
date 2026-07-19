package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/lincyaw/ag/gateway"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/pluginrpc"
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
	catalog := sdk.NewPluginRegistry()
	catalogErr := pluginrpc.RegisterDrivers(
		catalog,
		pluginrpc.ClientConfig{},
	)
	return func(
		ctx context.Context,
		spec gateway.RuntimeBuildSpec,
		state sdk.StateBackend,
	) (*agentruntime.Runtime, error) {
		if catalogErr != nil {
			return nil, catalogErr
		}
		runtime, err := agentruntime.NewRuntimeContext(
			ctx,
			agentruntime.RuntimeConfig{
				RuntimeVersion:   version,
				Logger:           logger,
				Tracer:           tracer,
				Meter:            meter,
				Storage:          state,
				StorageOwnership: agentruntime.StorageBorrowed,
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
		bound := make(map[string]struct{}, len(spec.Plugins))
		for _, binding := range spec.Plugins {
			bound[binding.Name] = struct{}{}
		}
		mountLocal := func(plugin sdk.Plugin) error {
			if _, replaced := bound[plugin.Manifest().Name]; replaced {
				return nil
			}
			_, err := runtime.Mount(ctx, sdk.Local(plugin))
			return err
		}
		localPlugins, err := configuredLocalPlugins(config, logger, tracer, meter)
		if err != nil {
			return fail(err)
		}
		for _, plugin := range localPlugins {
			if err := mountLocal(plugin); err != nil {
				return fail(err)
			}
		}

		for _, binding := range spec.Plugins {
			source, err := catalog.Resolve(ctx, binding.URI)
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
