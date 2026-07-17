package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
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
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type runningRuntime struct {
	runtime   *agentruntime.Runtime
	telemetry *telemetry.Runtime
	logger    *slog.Logger
	logFile   io.Closer
}

func startRuntime(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
	version string,
) (*runningRuntime, error) {
	logger, logFile, err := openConfiguredLogger(config.Logging, stderr)
	if err != nil {
		return nil, fmt.Errorf("configure logging: %w", err)
	}
	observability, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName: "ag", ServiceVersion: version, Logger: logger,
	})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("configure OpenTelemetry: %w", err),
			logFile.Close(),
		)
	}
	logger = logging.WithHandler(logger, observability.LogHandler)
	cleanupTelemetry := func(cause error) (*runningRuntime, error) {
		closeCtx, cancel := closeContext()
		defer cancel()
		return nil, errors.Join(
			cause,
			observability.Shutdown(closeCtx),
			logFile.Close(),
		)
	}
	storage, err := openStateBackend(ctx, config)
	if err != nil {
		return cleanupTelemetry(fmt.Errorf("configure state backend: %w", err))
	}
	runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{
		RuntimeVersion: version,
		Logger:         logger,
		Tracer:         observability.Tracer,
		Meter:          observability.Meter,
		Storage:        storage,
	})
	if err != nil {
		closeCtx, cancel := closeContext()
		closeErr := storage.Close(closeCtx)
		cancel()
		return cleanupTelemetry(errors.Join(err, closeErr))
	}
	running := &runningRuntime{
		runtime: runtime, telemetry: observability,
		logger: logger, logFile: logFile,
	}
	registry, names, err := buildRegistry(
		ctx,
		config,
		logger,
		observability.Tracer,
		observability.Meter,
	)
	if err != nil {
		running.close()
		return nil, err
	}
	for _, name := range names {
		source, resolveErr := registry.Resolve(ctx, name)
		if resolveErr != nil {
			running.close()
			return nil, fmt.Errorf("resolve plugin %q: %w", name, resolveErr)
		}
		if _, err := runtime.Mount(ctx, source); err != nil {
			running.close()
			return nil, fmt.Errorf("mount plugin %q: %w", name, err)
		}
	}
	return running, nil
}

func openStateBackend(
	ctx context.Context,
	config appconfig.Config,
) (sdk.StateBackend, error) {
	namespace := strings.TrimSpace(config.State.Namespace)
	rawURI := strings.TrimSpace(config.State.BackendURI)
	if rawURI == "" {
		directory, err := filepath.Abs(config.State.Directory)
		if err != nil {
			return nil, fmt.Errorf("resolve state directory: %w", err)
		}
		rawURI = (&url.URL{Scheme: "file", Path: directory}).String()
	}
	if namespace != "" {
		parsed, err := url.Parse(rawURI)
		if err != nil {
			return nil, fmt.Errorf("parse state backend URI: %w", err)
		}
		query := parsed.Query()
		if existing := strings.TrimSpace(query.Get("namespace")); existing != "" &&
			existing != namespace {
			return nil, fmt.Errorf(
				"state namespace %q conflicts with backend URI namespace %q",
				namespace,
				existing,
			)
		}
		query.Set("namespace", namespace)
		parsed.RawQuery = query.Encode()
		rawURI = parsed.String()
	}
	return sdkstorage.NewDefaultStorageRegistry().Open(ctx, rawURI)
}

func (running *runningRuntime) close() {
	if running == nil {
		return
	}
	var err error
	if running.runtime != nil {
		ctx, cancel := closeContext()
		err = errors.Join(err, running.runtime.DrainDeliveries(ctx))
		cancel()
		ctx, cancel = closeContext()
		err = errors.Join(err, running.runtime.Close(ctx))
		cancel()
	}
	if running.telemetry != nil {
		ctx, cancel := closeContext()
		err = errors.Join(err, running.telemetry.Shutdown(ctx))
		cancel()
	}
	if err != nil && running.logger != nil {
		running.logger.Error("runtime shutdown failed", "error", err)
	}
	if running.logFile != nil {
		_ = running.logFile.Close()
	}
}

func closeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func buildRegistry(
	ctx context.Context,
	config appconfig.Config,
	logger *slog.Logger,
	tracer trace.Tracer,
	meter metric.Meter,
) (*sdk.PluginRegistry, []string, error) {
	catalog := sdk.NewPluginRegistry()
	if err := pluginrpc.RegisterDrivers(
		catalog,
		pluginrpc.ClientConfig{},
	); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, 4+len(config.Plugins.Remote))
	registerLocal := func(plugin sdk.Plugin) error {
		manifest := plugin.Manifest()
		if err := catalog.Register(sdk.PluginReference{
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
	var directory registry.Directory
	defer func() {
		if directory != nil {
			_ = directory.Close(context.Background())
		}
	}()
	for _, raw := range config.Plugins.Remote {
		name, uri, mapped := strings.Cut(raw, "=")
		name, uri = strings.TrimSpace(name), strings.TrimSpace(uri)
		var reference sdk.PluginReference
		if mapped {
			if name == "" || uri == "" {
				return nil, nil, fmt.Errorf(
					"remote plugin %q must be name=grpc[s]://host:port",
					raw,
				)
			}
			reference = sdk.PluginReference{Name: name, URI: uri}
		} else {
			selector, selectorErr := parsePluginSelector(raw)
			if selectorErr != nil {
				return nil, nil, fmt.Errorf(
					"parse remote plugin selector %q: %w",
					raw,
					selectorErr,
				)
			}
			if directory == nil {
				opened, openErr := openPluginDirectory(
					ctx,
					config.Plugins,
				)
				if openErr != nil {
					return nil, nil, openErr
				}
				directory = opened
			}
			instance, selectErr := selectPluginInstance(
				ctx,
				directory,
				config.Plugins.RegistryNamespace,
				raw,
			)
			if selectErr != nil {
				return nil, nil, selectErr
			}
			reference = sdk.PluginReference{
				Name:        selector.Name,
				URI:         instance.URI,
				Description: instance.Manifest.Description,
				Labels:      instance.Labels,
			}
		}
		if err := catalog.Register(reference); err != nil {
			return nil, nil, err
		}
		names = append(names, reference.Name)
	}
	return catalog, names, nil
}
