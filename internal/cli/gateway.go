package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	gatewaymanager "github.com/lincyaw/ag/gateway/manager"
	"github.com/lincyaw/ag/gatewayrpc"
	"github.com/lincyaw/ag/internal/bootstrap"
	appconfig "github.com/lincyaw/ag/internal/config"
	"google.golang.org/grpc"
)

const privateGatewayListen = "127.0.0.1:0"

func (application *app) serveGateway(
	ctx context.Context,
	config appconfig.Config,
) (returnErr error) {
	running, err := bootstrap.StartGateway(
		ctx,
		config,
		application.stderr,
		application.version,
	)
	if err != nil {
		return err
	}
	var (
		listener      net.Listener
		server        *grpc.Server
		serveDone     chan error
		serveObserved bool
	)
	defer func() {
		var cleanupErr error
		if server != nil {
			gracefulDone := make(chan struct{})
			go func() {
				server.GracefulStop()
				close(gracefulDone)
			}()
			drainContext, cancelDrain := context.WithTimeout(
				context.Background(), config.Gateway.ShutdownTimeout,
			)
			drainErr := running.Service.Drain(drainContext)
			cancelDrain()
			if drainErr != nil {
				running.Logger.Warn(
					"gateway graceful drain reached its termination boundary",
					"error", drainErr,
				)
				if !errors.Is(drainErr, context.Canceled) &&
					!errors.Is(drainErr, context.DeadlineExceeded) {
					cleanupErr = errors.Join(cleanupErr, drainErr)
				}
			}
			// Long-lived attached views can keep gRPC GracefulStop open after
			// execution work has drained. Disconnect them before forced cleanup.
			server.Stop()
			select {
			case <-gracefulDone:
			case <-time.After(time.Second):
				cleanupErr = errors.Join(
					cleanupErr,
					errors.New("gateway gRPC graceful stop did not finish after forced stop"),
				)
			}
			if !serveObserved {
				serveErr := <-serveDone
				if serveErr != nil &&
					!errors.Is(serveErr, grpc.ErrServerStopped) &&
					!errors.Is(serveErr, net.ErrClosed) {
					cleanupErr = errors.Join(cleanupErr, serveErr)
				}
			}
		} else if listener != nil {
			if closeErr := listener.Close(); closeErr != nil &&
				!errors.Is(closeErr, net.ErrClosed) {
				cleanupErr = errors.Join(cleanupErr, closeErr)
			}
		}
		closeContext, cancel := context.WithTimeout(
			context.Background(), config.Gateway.ShutdownTimeout,
		)
		cleanupErr = errors.Join(cleanupErr, running.Close(closeContext))
		cancel()
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	listener, err = net.Listen("tcp", privateGatewayListen)
	if err != nil {
		return fmt.Errorf("listen for gateway RPC: %w", err)
	}
	server, err = gatewayrpc.NewGRPCServer(running.Service, 0)
	if err != nil {
		return err
	}
	serveDone = make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case serveErr := <-serveDone:
		serveObserved = true
		if serveErr == nil {
			return errors.New("gateway RPC server stopped before readiness")
		}
		return fmt.Errorf("serve gateway RPC before readiness: %w", serveErr)
	default:
	}

	recovered, recoverErr := running.Service.RecoverSessions(ctx)
	if recoverErr != nil {
		running.Logger.WarnContext(
			ctx,
			"some gateway executions could not be scheduled for recovery",
			"scheduled", len(recovered), "error", recoverErr,
		)
	}
	target, err := gatewayRPCTarget(listener.Addr().String())
	if err != nil {
		return err
	}
	ready := gatewaymanager.Ready{
		Target: target, Listen: listener.Addr().String(),
		Directory: running.Root, Registry: running.RegistryURI,
		RecoveredExecutions: len(recovered), PID: os.Getpid(),
	}
	ready.Executable, ready.ExecutableSHA256, err = gatewaymanager.CurrentExecutableIdentity()
	if err != nil {
		return err
	}
	if err := application.writeGatewayReady(ready); err != nil {
		return fmt.Errorf("write gateway ready record: %w", err)
	}
	running.Logger.InfoContext(
		ctx,
		"gateway ready",
		"target", ready.Target,
		"registry", ready.Registry,
		"recovered_executions", ready.RecoveredExecutions,
	)

	select {
	case <-ctx.Done():
		return nil
	case serveErr := <-serveDone:
		serveObserved = true
		if serveErr == nil || errors.Is(serveErr, grpc.ErrServerStopped) ||
			errors.Is(serveErr, net.ErrClosed) {
			return nil
		}
		return fmt.Errorf("serve gateway RPC: %w", serveErr)
	}
}

func runManagedGatewayChild(
	ctx context.Context,
	args []string,
	configPath string,
	stdout io.Writer,
	stderr io.Writer,
	version string,
) int {
	if len(args) != 0 {
		writeTextError(stderr, errors.New(
			"private gateway child does not accept command-line arguments",
		))
		return exitUsage
	}
	loaded, err := appconfig.Load(appconfig.LoadOptions{ConfigFile: configPath})
	if err != nil {
		writeTextError(stderr, fmt.Errorf("load private gateway config: %w", err))
		return exitRuntime
	}
	application := &app{
		version: version, stdout: stdout, stderr: stderr,
		output: outputJSON, progress: progressNever, color: colorNever,
	}
	if err := application.serveGateway(ctx, loaded.Config); err != nil {
		writeTextError(stderr, err)
		return exitRuntime
	}
	return exitOK
}

func gatewayRPCTarget(listen string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(listen))
	if err != nil {
		return "", fmt.Errorf("derive gateway RPC target from %q: %w", listen, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "grpc://" + net.JoinHostPort(host, port), nil
}

func (application *app) writeGatewayReady(value gatewaymanager.Ready) error {
	return application.render(value, func(writer io.Writer) error {
		if _, err := fmt.Fprintln(writer, "Gateway ready"); err != nil {
			return err
		}
		return writeSection(
			writer,
			"Endpoint",
			[2]string{"Target", value.Target},
			[2]string{"Listen", value.Listen},
			[2]string{"Directory", value.Directory},
			[2]string{"Registry", value.Registry},
			[2]string{
				"Recovered executions", fmt.Sprint(value.RecoveredExecutions),
			},
			[2]string{"PID", fmt.Sprint(value.PID)},
		)
	})
}
