package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/lincyaw/ag/gateway"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/internal/telemetry"
)

type RunningGateway struct {
	Service     *gateway.Service
	Root        string
	RegistryURI string
	Logger      *slog.Logger
	telemetry   *telemetry.Runtime
	logFile     io.Closer
}

func StartGateway(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
	version string,
) (*RunningGateway, error) {
	logger, logFile, err := OpenConfiguredLogger(
		config.Logging,
		stderr,
	)
	if err != nil {
		return nil, fmt.Errorf("configure logging: %w", err)
	}
	observability, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "ag-gateway",
		ServiceVersion: version,
		Logger:         logger,
	})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("configure OpenTelemetry: %w", err),
			logFile.Close(),
		)
	}
	logger = logging.WithHandler(logger, observability.LogHandler)
	directory, registryURI, err := OpenGatewayPluginDirectory(ctx, config)
	if err != nil {
		return nil, closeGatewayStartup(ctx, err, logFile, observability, nil, nil, nil, nil, nil, nil)
	}
	root, err := filepath.Abs(config.Gateway.Directory)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			fmt.Errorf("resolve gateway directory: %w", err),
			logFile,
			observability,
			directory,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	stateResolution, err := ResolveStateBackend(config)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			err,
			logFile,
			observability,
			directory,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	controlStores, err := openGatewayControlStores(
		ctx, stateResolution.URI, root,
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			err,
			logFile,
			observability,
			directory,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	sessionStore := controlStores.sessions
	eventStore := controlStores.events
	inputStore := controlStores.inputs
	interactions, err := gateway.NewInteractionManager(
		controlStores.interactions,
		eventStore,
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			errors.Join(
				err,
				controlStores.interactions.Close(context.Background()),
			),
			logFile,
			observability,
			directory,
			sessionStore,
			eventStore,
			nil,
			nil,
			nil,
		)
	}
	stateFactory, err := gateway.NewStorageSessionStateFactory(
		stateResolution.URI,
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			eventStore,
			interactions,
			inputStore,
			nil,
		)
	}
	executions, err := gateway.NewRuntimeExecutionBackend(
		gateway.RuntimeExecutionConfig{
			States:          stateFactory,
			Events:          eventStore,
			Interactions:    interactions,
			ValidateSession: gateway.PluginBindingValidator(directory),
			Build: GatewayRuntimeBuilder(
				config,
				logger,
				observability.Tracer,
				observability.Meter,
				version,
			),
			Logger: logger,
		},
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			eventStore,
			interactions,
			inputStore,
			nil,
		)
	}
	service, err := gateway.NewService(gateway.ServiceConfig{
		Store: sessionStore, Events: eventStore, Inputs: inputStore,
		Interactions:     interactions,
		Directory:        directory,
		Executions:       executions,
		DefaultNamespace: config.Plugins.RegistryNamespace,
		DefaultProvider:  config.Agent.Provider,
		DefaultSystem:    config.Agent.System,
		DefaultMaxTurns:  config.Agent.MaxTurns,
	})
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			eventStore,
			interactions,
			inputStore,
			executions,
		)
	}
	return &RunningGateway{
		Service:     service,
		Root:        root,
		RegistryURI: registryURI,
		Logger:      logger,
		telemetry:   observability,
		logFile:     logFile,
	}, nil
}

type gatewayControlStores struct {
	sessions     gateway.SessionStore
	events       gateway.EventStore
	inputs       gateway.InputStore
	interactions gateway.InteractionStore
}

// openGatewayControlStores keeps the whole gateway control plane in the same
// SQL URI and namespace as trajectory state. File stores are compatibility
// adapters only for legacy non-SQL state drivers.
func openGatewayControlStores(
	ctx context.Context,
	rawURI string,
	root string,
) (gatewayControlStores, error) {
	scheme, _, _ := strings.Cut(strings.TrimSpace(rawURI), ":")
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "sqlite", "postgres", "postgresql":
		stores, err := gateway.NewGORMGatewayStores(
			ctx,
			rawURI,
			gateway.LegacyGatewayStoreDirectories{
				Sessions:     filepath.Join(root, "control"),
				Inputs:       filepath.Join(root, "inputs"),
				Interactions: filepath.Join(root, "interactions"),
			},
		)
		if err != nil {
			return gatewayControlStores{}, err
		}
		return gatewayControlStores{
			sessions: stores.Sessions, events: stores.Events,
			inputs: stores.Inputs, interactions: stores.Interactions,
		}, nil
	case "duckdb", "file", "memory":
		return openLegacyGatewayControlStores(root)
	default:
		return gatewayControlStores{}, fmt.Errorf(
			"gateway control backend does not support state URI scheme %q",
			scheme,
		)
	}
}

func openLegacyGatewayControlStores(root string) (gatewayControlStores, error) {
	sessions, err := gateway.NewFileSessionStore(filepath.Join(root, "control"))
	if err != nil {
		return gatewayControlStores{}, err
	}
	events, err := gateway.NewFileEventStore(filepath.Join(root, "events"))
	if err != nil {
		_ = sessions.Close(context.Background())
		return gatewayControlStores{}, err
	}
	inputs, err := gateway.NewFileInputStore(filepath.Join(root, "inputs"))
	if err != nil {
		_ = events.Close(context.Background())
		_ = sessions.Close(context.Background())
		return gatewayControlStores{}, err
	}
	interactions, err := gateway.NewFileInteractionStore(
		filepath.Join(root, "interactions"),
	)
	if err != nil {
		_ = inputs.Close(context.Background())
		_ = events.Close(context.Background())
		_ = sessions.Close(context.Background())
		return gatewayControlStores{}, err
	}
	return gatewayControlStores{
		sessions: sessions, events: events,
		inputs: inputs, interactions: interactions,
	}, nil
}

func (running *RunningGateway) Close(ctx context.Context) error {
	if running == nil {
		return nil
	}
	var err error
	if running.Service != nil {
		err = errors.Join(err, running.Service.Close(ctx))
	}
	if running.telemetry != nil {
		err = errors.Join(err, running.telemetry.Shutdown(ctx))
	}
	if running.logFile != nil {
		err = errors.Join(err, running.logFile.Close())
	}
	return err
}

func closeGatewayStartup(
	ctx context.Context,
	cause error,
	logFile io.Closer,
	observability *telemetry.Runtime,
	directory gateway.PluginDirectory,
	sessionStore gateway.SessionStore,
	eventStore gateway.EventStore,
	interactions *gateway.InteractionManager,
	inputStore gateway.InputStore,
	executions gateway.ExecutionBackend,
) error {
	closeCtx, cancel := closeContext(ctx)
	defer cancel()
	return errors.Join(
		cause,
		closeGatewayResource(closeCtx, executions),
		closeGatewayResource(closeCtx, inputStore),
		closeGatewayResource(closeCtx, interactions),
		closeGatewayResource(closeCtx, eventStore),
		closeGatewayResource(closeCtx, sessionStore),
		closeGatewayResource(closeCtx, directory),
		observability.Shutdown(closeCtx),
		logFile.Close(),
	)
}

type contextCloser interface {
	Close(context.Context) error
}

func closeGatewayResource(ctx context.Context, resource contextCloser) error {
	if resource == nil {
		return nil
	}
	return resource.Close(ctx)
}
