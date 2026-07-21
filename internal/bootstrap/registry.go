package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/internal/telemetry"
	"github.com/lincyaw/ag/registry"
)

type RunningRegistry struct {
	Directory    registry.Directory
	Backend      string
	Capabilities registry.Capabilities
	Logger       *slog.Logger
	telemetry    *telemetry.Runtime
	logFile      io.Closer
}

func StartRegistry(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
	version string,
) (*RunningRegistry, error) {
	logger, logFile, err := OpenConfiguredLogger(
		config.Logging,
		stderr,
	)
	if err != nil {
		return nil, fmt.Errorf("configure logging: %w", err)
	}
	observability, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "ag-registry",
		ServiceVersion: version,
		Logger:         logger,
		Disabled:       !config.Observability.Enabled,
	})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("configure OpenTelemetry: %w", err),
			logFile.Close(),
		)
	}
	logger = logging.WithHandler(logger, observability.LogHandler)
	directory, err := registry.NewDefaultBackendRegistry().Open(
		ctx,
		config.Registry.BackendURI,
	)
	if err != nil {
		closeCtx, cancel := closeContext(ctx)
		defer cancel()
		return nil, errors.Join(
			fmt.Errorf("open registry backend: %w", err),
			observability.Shutdown(closeCtx),
			logFile.Close(),
		)
	}
	return &RunningRegistry{
		Directory:    directory,
		Backend:      directory.String(),
		Capabilities: directory.Capabilities(),
		Logger:       logger,
		telemetry:    observability,
		logFile:      logFile,
	}, nil
}

func (running *RunningRegistry) Close(ctx context.Context) error {
	if running == nil {
		return nil
	}
	var err error
	if running.Directory != nil {
		err = errors.Join(err, running.Directory.Close(ctx))
	}
	if running.telemetry != nil {
		err = errors.Join(err, running.telemetry.Shutdown(ctx))
	}
	if running.logFile != nil {
		err = errors.Join(err, running.logFile.Close())
	}
	return err
}
