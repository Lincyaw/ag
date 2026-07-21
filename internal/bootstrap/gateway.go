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
	sessionStore, err := gateway.NewFileSessionStore(
		filepath.Join(root, "control"),
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
	eventStore, err := openGatewayEventStore(
		ctx,
		stateResolution.URI,
		filepath.Join(root, "events"),
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			nil,
			nil,
			nil,
			nil,
		)
	}
	interactionStore, err := gateway.NewFileInteractionStore(
		filepath.Join(root, "interactions"),
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx, err, logFile, observability, directory,
			sessionStore, eventStore, nil, nil, nil,
		)
	}
	interactions, err := gateway.NewInteractionManager(
		interactionStore,
		eventStore,
	)
	if err != nil {
		return nil, closeGatewayStartup(
			ctx,
			errors.Join(err, interactionStore.Close(context.Background())),
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
	inputStore, err := gateway.NewFileInputStore(
		filepath.Join(root, "inputs"),
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

// openGatewayEventStore keeps gateway events in the same SQL database and
// deployment namespace as trajectory state. File storage remains only for
// legacy state drivers which cannot host the GORM event tables.
func openGatewayEventStore(
	ctx context.Context,
	rawURI string,
	legacyDirectory string,
) (gateway.EventStore, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil {
		return nil, fmt.Errorf("parse gateway event backend URI: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
	case "sqlite", "postgres", "postgresql":
		return gateway.NewGORMEventStore(ctx, rawURI)
	case "duckdb", "file", "memory":
		return gateway.NewFileEventStore(legacyDirectory)
	default:
		return nil, fmt.Errorf(
			"gateway event backend does not support state URI scheme %q",
			parsed.Scheme,
		)
	}
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
