package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/internal/telemetry"
	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/plugins/bash"
	fileplugin "github.com/lincyaw/ag/plugins/file"
	"github.com/lincyaw/ag/plugins/openai"
	otelplugin "github.com/lincyaw/ag/plugins/otel"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type runningRuntime struct {
	runtime   *sdk.Runtime
	telemetry *telemetry.Runtime
}

func startRuntime(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
	version string,
) (*runningRuntime, error) {
	logger, err := logging.New(logging.Config{
		Level: config.Logging.Level, Format: config.Logging.Format, Writer: stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("configure logging: %w", err)
	}
	observability, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName: "ag", ServiceVersion: version, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("configure OpenTelemetry: %w", err)
	}
	logger = logging.WithHandler(logger, observability.LogHandler)
	cleanupTelemetry := func(cause error) (*runningRuntime, error) {
		closeCtx, cancel := closeContext()
		defer cancel()
		return nil, errors.Join(cause, observability.Shutdown(closeCtx))
	}
	trajectories, err := sdk.NewFileTrajectoryStore(filepath.Join(
		config.State.Directory,
		"trajectories",
	))
	if err != nil {
		return cleanupTelemetry(err)
	}
	outbox, err := sdk.NewFileOutboxStore(filepath.Join(config.State.Directory, "outbox"))
	if err != nil {
		return cleanupTelemetry(err)
	}
	operations, err := sdk.NewFileOperationStore(filepath.Join(config.State.Directory, "operations"))
	if err != nil {
		return cleanupTelemetry(err)
	}
	runtime, err := sdk.NewRuntime(sdk.RuntimeConfig{
		Logger: logger, Tracer: observability.Tracer, Meter: observability.Meter,
		Trajectories: trajectories, Operations: operations, Outbox: outbox,
	})
	if err != nil {
		return cleanupTelemetry(err)
	}
	running := &runningRuntime{runtime: runtime, telemetry: observability}
	registry, names, err := buildRegistry(
		config,
		logger,
		observability.Tracer,
		observability.Meter,
	)
	if err != nil {
		running.close(stderr)
		return nil, err
	}
	for _, name := range names {
		if _, err := runtime.MountRegistered(ctx, registry, name); err != nil {
			running.close(stderr)
			return nil, fmt.Errorf("mount plugin %q: %w", name, err)
		}
	}
	return running, nil
}

func (running *runningRuntime) close(stderr io.Writer) {
	if running == nil {
		return
	}
	ctx, cancel := closeContext()
	defer cancel()
	var err error
	if running.runtime != nil {
		err = errors.Join(err, running.runtime.Close(ctx))
	}
	if running.telemetry != nil {
		err = errors.Join(err, running.telemetry.Shutdown(ctx))
	}
	if err != nil {
		fmt.Fprintf(stderr, "ag: shutdown: %v\n", err)
	}
}

func closeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func buildRegistry(
	config appconfig.Config,
	logger *slog.Logger,
	tracer trace.Tracer,
	meter metric.Meter,
) (*sdk.PluginRegistry, []string, error) {
	registry := sdk.NewPluginRegistry()
	if err := pluginrpc.RegisterDrivers(registry, pluginrpc.ClientConfig{
		RegistryURI: config.Plugins.RegistryURI,
	}); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, 4+len(config.Plugins.Remote))
	registerLocal := func(plugin sdk.Plugin) error {
		manifest := plugin.Manifest()
		if err := registry.Register(sdk.PluginReference{
			Name: manifest.Name, Description: manifest.Description, Source: sdk.Local(plugin),
		}); err != nil {
			return err
		}
		names = append(names, manifest.Name)
		return nil
	}

	if config.Observability.Enabled {
		plugin, err := otelplugin.New(otelplugin.Config{Logger: logger, Tracer: tracer, Meter: meter})
		if err != nil {
			return nil, nil, err
		}
		if err := registerLocal(plugin); err != nil {
			return nil, nil, err
		}
	}
	if config.OpenAI.Enabled {
		if err := registerLocal(openai.New(openai.Config{
			Model: config.OpenAI.Model, BaseURL: config.OpenAI.BaseURL,
			MaxRetries: config.OpenAI.MaxRetries,
		})); err != nil {
			return nil, nil, err
		}
	}
	if config.Workspace.Enabled {
		if err := registerLocal(fileplugin.New(fileplugin.Config{
			Root: config.Workspace.Root, EnableWrite: config.Workspace.EnableWrite,
			MaxReadBytes:  config.Workspace.MaxReadBytes,
			MaxWriteBytes: config.Workspace.MaxWriteBytes,
			MaxEntries:    config.Workspace.MaxEntries,
		})); err != nil {
			return nil, nil, err
		}
	}
	if config.Bash.Enabled {
		if err := registerLocal(bash.New(bash.Config{
			Root: config.Workspace.Root, Shell: config.Bash.Shell,
			DefaultTimeout: config.Bash.DefaultTimeout,
			MaxTimeout:     config.Bash.MaxTimeout,
			MaxOutputBytes: config.Bash.MaxOutputBytes,
			Environment:    config.Bash.Environment,
		})); err != nil {
			return nil, nil, err
		}
	}
	for _, raw := range config.Plugins.Remote {
		name, uri, ok := strings.Cut(raw, "=")
		name, uri = strings.TrimSpace(name), strings.TrimSpace(uri)
		if !ok || name == "" || uri == "" {
			return nil, nil, fmt.Errorf(
				"remote plugin %q must be name=grpc[s]://host:port",
				raw,
			)
		}
		if err := registry.Register(sdk.PluginReference{Name: name, URI: uri}); err != nil {
			return nil, nil, err
		}
		names = append(names, name)
	}
	return registry, names, nil
}
