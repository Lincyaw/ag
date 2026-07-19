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
	if strings.TrimSpace(config.Plugins.RegistryURI) == "" {
		return nil, errors.New(
			"gateway requires a plugin registry; set plugins.registry_uri or --registry-uri",
		)
	}
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
	directory, err := OpenPluginDirectory(ctx, config.Plugins)
	if err != nil {
		return nil, closeGatewayStartup(err, logFile, observability, nil, nil, nil)
	}
	root, err := filepath.Abs(config.Gateway.Directory)
	if err != nil {
		return nil, closeGatewayStartup(
			fmt.Errorf("resolve gateway directory: %w", err),
			logFile,
			observability,
			directory,
			nil,
			nil,
		)
	}
	sessionStore, err := gateway.NewFileSessionStore(
		filepath.Join(root, "control"),
	)
	if err != nil {
		return nil, closeGatewayStartup(
			err,
			logFile,
			observability,
			directory,
			nil,
			nil,
		)
	}
	stateFactory, err := gateway.NewDuckDBSessionStateFactory(
		filepath.Join(root, "state"),
	)
	if err != nil {
		return nil, closeGatewayStartup(
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			nil,
		)
	}
	executions, err := gateway.NewRuntimeExecutionBackend(
		gateway.RuntimeExecutionConfig{
			States: stateFactory,
			Build: GatewayRuntimeBuilder(
				config,
				logger,
				observability.Tracer,
				observability.Meter,
				version,
			),
		},
	)
	if err != nil {
		return nil, closeGatewayStartup(
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			nil,
		)
	}
	service, err := gateway.NewService(gateway.ServiceConfig{
		Store: sessionStore, Directory: directory,
		Executions:       executions,
		DefaultNamespace: config.Plugins.RegistryNamespace,
		DefaultProvider:  config.Agent.Provider,
		DefaultSystem:    config.Agent.System,
		DefaultMaxTurns:  config.Agent.MaxTurns,
	})
	if err != nil {
		return nil, closeGatewayStartup(
			err,
			logFile,
			observability,
			directory,
			sessionStore,
			executions,
		)
	}
	return &RunningGateway{
		Service:     service,
		Root:        root,
		RegistryURI: config.Plugins.RegistryURI,
		Logger:      logger,
		telemetry:   observability,
		logFile:     logFile,
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
	cause error,
	logFile io.Closer,
	observability *telemetry.Runtime,
	directory gateway.PluginDirectory,
	sessionStore gateway.SessionStore,
	executions gateway.ExecutionBackend,
) error {
	closeCtx, cancel := closeContext()
	defer cancel()
	return errors.Join(
		cause,
		closeGatewayResource(closeCtx, executions),
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
