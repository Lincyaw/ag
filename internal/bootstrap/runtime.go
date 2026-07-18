package bootstrap

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
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type RunningRuntime struct {
	Runtime   *agentruntime.Runtime
	telemetry *telemetry.Runtime
	logger    *slog.Logger
	logFile   io.Closer
}

type PluginPlan struct {
	Catalog *sdk.PluginRegistry
	Mounts  []string
}

func StartRuntime(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
	version string,
	eventSink EventSink,
) (*RunningRuntime, error) {
	logger, logFile, err := OpenConfiguredLogger(config.Logging, stderr)
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
	cleanupTelemetry := func(cause error) (*RunningRuntime, error) {
		closeCtx, cancel := closeContext()
		defer cancel()
		return nil, errors.Join(
			cause,
			observability.Shutdown(closeCtx),
			logFile.Close(),
		)
	}
	storage, err := OpenStateBackend(ctx, config)
	if err != nil {
		return cleanupTelemetry(fmt.Errorf("configure state backend: %w", err))
	}
	runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{
		RuntimeVersion: version,
		Logger:         logger,
		Tracer:         observability.Tracer,
		Meter:          observability.Meter,
		Storage:        storage,
		EventObserver:  eventObserver(eventSink),
	})
	if err != nil {
		closeCtx, cancel := closeContext()
		closeErr := storage.Close(closeCtx)
		cancel()
		return cleanupTelemetry(errors.Join(err, closeErr))
	}
	running := &RunningRuntime{
		Runtime: runtime, telemetry: observability,
		logger: logger, logFile: logFile,
	}
	plan, err := BuildPluginPlan(
		ctx,
		config,
		logger,
		observability.Tracer,
		observability.Meter,
	)
	if err != nil {
		running.Close()
		return nil, err
	}
	if err := plan.Mount(ctx, runtime); err != nil {
		running.Close()
		return nil, err
	}
	return running, nil
}

func (plan PluginPlan) Mount(
	ctx context.Context,
	runtime *agentruntime.Runtime,
) error {
	for _, name := range plan.Mounts {
		source, resolveErr := plan.Catalog.Resolve(ctx, name)
		if resolveErr != nil {
			return fmt.Errorf("resolve plugin %q: %w", name, resolveErr)
		}
		if _, err := runtime.Mount(ctx, source); err != nil {
			return fmt.Errorf("mount plugin %q: %w", name, err)
		}
	}
	return nil
}

func OpenStateBackend(
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

func RollbackTrajectory(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
	backend sdk.StateBackend,
	trajectoryID string,
	checkpointID string,
) error {
	logger, logFile, err := OpenConfiguredLogger(config.Logging, stderr)
	if err != nil {
		return err
	}
	defer logFile.Close()
	runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{
		Logger:           logger,
		Storage:          backend,
		StorageOwnership: agentruntime.StorageBorrowed,
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := closeContext()
		defer cancel()
		_ = runtime.Close(closeCtx)
	}()
	return runtime.RollbackTrajectory(ctx, trajectoryID, checkpointID)
}

func (running *RunningRuntime) Close() {
	if running == nil {
		return
	}
	var err error
	if running.Runtime != nil {
		ctx, cancel := closeContext()
		err = errors.Join(err, running.Runtime.DrainDeliveries(ctx))
		cancel()
		ctx, cancel = closeContext()
		err = errors.Join(err, running.Runtime.Close(ctx))
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

func BuildPluginPlan(
	ctx context.Context,
	config appconfig.Config,
	logger *slog.Logger,
	tracer trace.Tracer,
	meter metric.Meter,
) (PluginPlan, error) {
	catalog := sdk.NewPluginRegistry()
	if err := pluginrpc.RegisterDrivers(
		catalog,
		pluginrpc.ClientConfig{},
	); err != nil {
		return PluginPlan{}, err
	}
	plan := PluginPlan{
		Catalog: catalog,
		Mounts:  make([]string, 0, 4+len(config.Plugins.Remote)),
	}
	registerPlugin := func(plugin sdk.Plugin) error {
		manifest := plugin.Manifest()
		if err := catalog.Register(sdk.PluginReference{
			Name: manifest.Name, Description: manifest.Description, Source: sdk.Local(plugin),
		}); err != nil {
			return err
		}
		plan.Mounts = append(plan.Mounts, manifest.Name)
		return nil
	}
	localPlugins, err := configuredLocalPlugins(config, logger, tracer, meter)
	if err != nil {
		return PluginPlan{}, err
	}
	for _, plugin := range localPlugins {
		if err := registerPlugin(plugin); err != nil {
			return PluginPlan{}, err
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
				return PluginPlan{}, fmt.Errorf(
					"remote plugin %q must be name=grpc[s]://host:port",
					raw,
				)
			}
			reference = sdk.PluginReference{Name: name, URI: uri}
		} else {
			selector, selectorErr := ParsePluginSelector(raw)
			if selectorErr != nil {
				return PluginPlan{}, fmt.Errorf(
					"parse remote plugin selector %q: %w",
					raw,
					selectorErr,
				)
			}
			if directory == nil {
				opened, openErr := OpenPluginDirectory(
					ctx,
					config.Plugins,
				)
				if openErr != nil {
					return PluginPlan{}, openErr
				}
				directory = opened
			}
			instance, selectErr := SelectPluginInstance(
				ctx,
				directory,
				config.Plugins.RegistryNamespace,
				raw,
			)
			if selectErr != nil {
				return PluginPlan{}, selectErr
			}
			reference = sdk.PluginReference{
				Name:        selector.Name,
				URI:         instance.URI,
				Description: instance.Manifest.Description,
				Labels:      instance.Labels,
			}
		}
		if err := catalog.Register(reference); err != nil {
			return PluginPlan{}, err
		}
		plan.Mounts = append(plan.Mounts, reference.Name)
	}
	return plan, nil
}
