package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"github.com/lincyaw/ag/gateway"
	"github.com/lincyaw/ag/internal/bootstrap"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type gatewayReady struct {
	URL                 string `json:"url"`
	Listen              string `json:"listen"`
	Directory           string `json:"directory"`
	Registry            string `json:"registry"`
	RecoveredExecutions int    `json:"recovered_executions"`
	PID                 int    `json:"pid"`
}

func (application *app) gatewayCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "gateway",
		Short: "Manage durable user sessions and asynchronous executions",
	}
	serve := &cobra.Command{
		Use:   "serve",
		Short: "Serve the multi-session HTTP gateway",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := application.load(command)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			return application.serveGateway(
				command.Context(),
				loaded.Config,
			)
		},
	}
	serve.Flags().String(
		"gateway-listen",
		"",
		"HTTP listen address.",
	)
	serve.Flags().String(
		"gateway-dir",
		"",
		"Durable gateway session and execution directory.",
	)
	serve.Flags().Duration(
		"read-header-timeout",
		0,
		"Maximum time to read HTTP request headers.",
	)
	serve.Flags().Duration(
		"idle-timeout",
		0,
		"HTTP keep-alive idle timeout.",
	)
	serve.Flags().Duration(
		"shutdown-timeout",
		0,
		"Graceful shutdown deadline.",
	)
	addGatewayRuntimeConfigFlags(serve.Flags())
	command.AddCommand(serve)
	return command
}

func addGatewayRuntimeConfigFlags(flags *pflag.FlagSet) {
	flags.String("system", "", "Default system prompt for new sessions.")
	flags.String("provider", "", "Default provider for new sessions.")
	flags.Int("max-turns", 0, "Default maximum model turns per message.")
	flags.Bool("openai", true, "Mount the local OpenAI provider.")
	flags.String("model", "", "OpenAI model ID.")
	flags.String("base-url", "", "Trusted OpenAI-compatible base URL.")
	flags.Int("max-retries", 0, "OpenAI request retry count.")
	flags.Bool("file", true, "Mount the local file plugin.")
	flags.String("cwd", "", "Root for local file and bash plugins.")
	flags.Bool("write", false, "Enable atomic writes in the local file plugin.")
	flags.Int64("max-read-bytes", 0, "Maximum bytes per file read.")
	flags.Int64("max-write-bytes", 0, "Maximum bytes per file write.")
	flags.Int("max-entries", 0, "Maximum entries per directory listing.")
	flags.Bool("bash", false, "Mount the local bash plugin.")
	flags.String("shell", "", "Absolute shell path for the bash plugin.")
	flags.Duration("bash-timeout", 0, "Default bash operation timeout.")
	flags.Duration("bash-max-timeout", 0, "Maximum bash operation timeout.")
	flags.Int64(
		"bash-max-output-bytes",
		0,
		"Maximum retained bytes per bash output stream.",
	)
	flags.Bool("compact", true, "Mount automatic prompt compaction.")
	flags.Bool("tree", true, "Mount the local workspace_tree plugin.")
	flags.Int("tree-max-entries", 0, "Maximum entries returned by workspace_tree.")
	flags.Int("tree-max-depth", 0, "Maximum directory depth for workspace_tree.")
	flags.String("registry-uri", "", "Remote lease registry grpc[s] URI.")
	flags.String(
		"registry-namespace",
		"",
		"Registry namespace used for session plugin discovery.",
	)
}

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
		server        *http.Server
		serveDone     chan error
		serveObserved bool
	)
	defer func() {
		var cleanupErr error
		if server != nil {
			closeCtx, cancel := context.WithTimeout(
				context.Background(),
				config.Gateway.ShutdownTimeout,
			)
			shutdownErr := server.Shutdown(closeCtx)
			cancel()
			if shutdownErr != nil {
				shutdownErr = errors.Join(shutdownErr, server.Close())
			}
			cleanupErr = errors.Join(cleanupErr, shutdownErr)
			if !serveObserved {
				serveErr := <-serveDone
				if serveErr != nil &&
					!errors.Is(serveErr, http.ErrServerClosed) &&
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
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			config.Gateway.ShutdownTimeout,
		)
		cleanupErr = errors.Join(
			cleanupErr,
			running.Close(closeCtx),
		)
		cancel()
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	handler, err := gateway.NewHTTPHandler(
		running.Service,
		gateway.HeaderAuthenticator,
	)
	if err != nil {
		return err
	}
	listener, err = net.Listen("tcp", config.Gateway.Listen)
	if err != nil {
		return fmt.Errorf("listen for gateway HTTP: %w", err)
	}
	server = &http.Server{
		Handler: otelhttp.NewHandler(
			handler,
			"ag.gateway.http",
		),
		ReadHeaderTimeout: config.Gateway.ReadHeaderTimeout,
		IdleTimeout:       config.Gateway.IdleTimeout,
	}
	serveDone = make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case serveErr := <-serveDone:
		serveObserved = true
		if serveErr == nil {
			return errors.New("gateway HTTP server stopped before readiness")
		}
		return fmt.Errorf(
			"serve gateway HTTP before readiness: %w",
			serveErr,
		)
	default:
	}

	recovered, recoverErr := running.Service.RecoverSessions(ctx)
	if recoverErr != nil {
		running.Logger.WarnContext(
			ctx,
			"some gateway executions could not be scheduled for recovery",
			"scheduled",
			len(recovered),
			"error",
			recoverErr,
		)
	}
	ready := gatewayReady{
		URL:    "http://" + listener.Addr().String(),
		Listen: listener.Addr().String(), Directory: running.Root,
		Registry:            running.RegistryURI,
		RecoveredExecutions: len(recovered),
		PID:                 os.Getpid(),
	}
	if err := application.writeGatewayReady(ready); err != nil {
		return fmt.Errorf("write gateway ready record: %w", err)
	}
	running.Logger.InfoContext(
		ctx,
		"gateway ready",
		"url",
		ready.URL,
		"registry",
		ready.Registry,
		"recovered_executions",
		ready.RecoveredExecutions,
	)

	select {
	case <-ctx.Done():
		return nil
	case serveErr := <-serveDone:
		serveObserved = true
		if serveErr == nil ||
			errors.Is(serveErr, http.ErrServerClosed) ||
			errors.Is(serveErr, net.ErrClosed) {
			return nil
		}
		return fmt.Errorf("serve gateway HTTP: %w", serveErr)
	}
}

func (application *app) writeGatewayReady(value gatewayReady) error {
	return application.render(value, func(writer io.Writer) error {
		if _, err := fmt.Fprintln(writer, "Gateway ready"); err != nil {
			return err
		}
		return writeSection(
			writer,
			"Endpoint",
			[2]string{"URL", value.URL},
			[2]string{"Listen", value.Listen},
			[2]string{"Directory", value.Directory},
			[2]string{"Registry", value.Registry},
			[2]string{
				"Recovered executions",
				fmt.Sprint(value.RecoveredExecutions),
			},
			[2]string{"PID", fmt.Sprint(value.PID)},
		)
	})
}
