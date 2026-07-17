package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/internal/telemetry"
	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/registry"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type registryReady struct {
	URI          string                `json:"uri"`
	Listen       string                `json:"listen"`
	Backend      string                `json:"backend"`
	Capabilities registry.Capabilities `json:"capabilities"`
	PID          int                   `json:"pid"`
}

func (application *app) registryCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "registry",
		Short: "Run the plugin registration and discovery control plane",
	}
	serve := &cobra.Command{
		Use:   "serve",
		Short: "Serve the plugin registry over gRPC",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := application.load(command)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			return application.serveRegistry(
				command.Context(),
				loaded.Config,
			)
		},
	}
	serve.Flags().String("listen", "", "TCP listen address.")
	serve.Flags().String(
		"advertise-uri",
		"",
		"Client-facing grpc[s] URI (required for wildcard listen addresses).",
	)
	serve.Flags().String(
		"registry-backend",
		"",
		"Directory backend URI (memory://local or file:///path).",
	)
	serve.Flags().String("tls-cert", "", "TLS certificate PEM file.")
	serve.Flags().String("tls-key", "", "TLS private key PEM file.")
	serve.Flags().Int(
		"max-message-bytes",
		0,
		"Maximum gRPC request or response size.",
	)
	command.AddCommand(serve)
	return command
}

func (application *app) serveRegistry(
	ctx context.Context,
	config appconfig.Config,
) (returnErr error) {
	logger, err := logging.New(logging.Config{
		Level:  config.Logging.Level,
		Format: config.Logging.Format,
		Writer: application.stderr,
	})
	if err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}
	observability, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "ag-registry",
		ServiceVersion: application.version,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("configure OpenTelemetry: %w", err)
	}
	logger = logging.WithHandler(logger, observability.LogHandler)
	directory, err := registry.NewDefaultBackendRegistry().Open(
		ctx,
		config.Registry.BackendURI,
	)
	if err != nil {
		return errors.Join(
			fmt.Errorf("open registry backend: %w", err),
			shutdownTelemetry(observability),
		)
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
			stopGRPCServer(server)
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
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		cleanupErr = errors.Join(
			cleanupErr,
			directory.Close(closeCtx),
			observability.Shutdown(closeCtx),
		)
		cancel()
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	listener, err = net.Listen("tcp", config.Registry.Listen)
	if err != nil {
		return fmt.Errorf("listen for registry RPC: %w", err)
	}
	serverOptions, scheme, err := registryTLSOptions(config.Registry)
	if err != nil {
		return err
	}
	uri, err := registryAdvertiseURI(
		config.Registry.AdvertiseURI,
		listener.Addr().String(),
		scheme,
	)
	if err != nil {
		return err
	}
	server, err = pluginrpc.NewRegistryGRPCServer(
		directory,
		config.Registry.MaxMessageBytes,
		serverOptions...,
	)
	if err != nil {
		return fmt.Errorf("create registry RPC server: %w", err)
	}
	serveDone = make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case serveErr := <-serveDone:
		serveObserved = true
		if serveErr == nil {
			return errors.New("registry RPC server stopped before readiness")
		}
		return fmt.Errorf(
			"serve registry RPC before readiness: %w",
			serveErr,
		)
	default:
	}

	ready := registryReady{
		URI:          uri,
		Listen:       listener.Addr().String(),
		Backend:      directory.String(),
		Capabilities: directory.Capabilities(),
		PID:          os.Getpid(),
	}
	if err := application.writeRegistryReady(ready); err != nil {
		return fmt.Errorf("write registry ready record: %w", err)
	}
	logger.InfoContext(
		ctx,
		"plugin registry ready",
		"uri",
		uri,
		"backend",
		directory.String(),
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
		return fmt.Errorf("serve registry RPC: %w", serveErr)
	}
}

func registryTLSOptions(
	config appconfig.Registry,
) ([]grpc.ServerOption, string, error) {
	if config.TLSCertFile == "" {
		return nil, "grpc", nil
	}
	certificate, err := tls.LoadX509KeyPair(
		config.TLSCertFile,
		config.TLSKeyFile,
	)
	if err != nil {
		return nil, "", fmt.Errorf("load registry TLS identity: %w", err)
	}
	return []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificate},
			MinVersion:   tls.VersionTLS12,
		})),
	}, "grpcs", nil
}

func registryAdvertiseURI(
	configured string,
	listenAddress string,
	scheme string,
) (string, error) {
	advertised := strings.TrimSpace(configured)
	if advertised == "" {
		host, _, err := net.SplitHostPort(listenAddress)
		if err != nil {
			return "", fmt.Errorf(
				"parse registry listen address %q: %w",
				listenAddress,
				err,
			)
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			return "", errors.New(
				"--advertise-uri is required for a wildcard listen address",
			)
		}
		return scheme + "://" + listenAddress, nil
	}
	parsed, err := url.Parse(advertised)
	if err != nil {
		return "", fmt.Errorf("parse registry advertise URI: %w", err)
	}
	if parsed.Scheme != scheme {
		return "", fmt.Errorf(
			"registry advertise URI scheme %q does not match transport %q",
			parsed.Scheme,
			scheme,
		)
	}
	if parsed.Host == "" || parsed.Path != "" {
		return "", errors.New(
			"registry advertise URI must contain host:port and no path",
		)
	}
	return parsed.String(), nil
}

func stopGRPCServer(server *grpc.Server) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		server.Stop()
		<-done
	}
}

func shutdownTelemetry(runtime *telemetry.Runtime) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return runtime.Shutdown(ctx)
}
