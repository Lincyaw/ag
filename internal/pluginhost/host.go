package pluginhost

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	Plugin          sdk.Plugin
	Listen          string
	AdvertiseURI    string
	RegistryURI     string
	LeaseTTL        time.Duration
	StateDirectory  string
	StorageURI      string
	TLSCertFile     string
	TLSKeyFile      string
	MaxMessageBytes int
	Logger          *slog.Logger
	ReadyWriter     io.Writer
}

type Ready struct {
	Name           string `json:"name"`
	URI            string `json:"uri"`
	StateDirectory string `json:"state_directory"`
	Storage        string `json:"storage"`
	PID            int    `json:"pid"`
}

func Serve(ctx context.Context, config Config) (returnErr error) {
	if config.Plugin == nil {
		return errors.New("plugin is nil")
	}
	runContext, runCancel := context.WithCancel(ctx)
	var (
		storage       sdk.StateBackend
		adapter       pluginrpc.Server
		listener      net.Listener
		server        *grpc.Server
		registry      *pluginrpc.RegistryClient
		lease         sdk.PluginLease
		serveDone     chan error
		serverStarted bool
		serveObserved bool
	)
	defer func() {
		runCancel()
		var cleanupErr error
		if registry != nil {
			if lease.ID != "" {
				cleanupCtx, cancel := context.WithTimeout(
					context.Background(),
					2*time.Second,
				)
				cleanupErr = errors.Join(
					cleanupErr,
					registry.Unregister(cleanupCtx, lease.ID),
				)
				cancel()
			}
			cleanupErr = errors.Join(cleanupErr, registry.Close())
		}
		if serverStarted {
			stopServer(server)
			if !serveObserved {
				serveErr := <-serveDone
				if serveErr != nil &&
					!errors.Is(serveErr, grpc.ErrServerStopped) &&
					!errors.Is(serveErr, net.ErrClosed) {
					cleanupErr = errors.Join(cleanupErr, serveErr)
				}
			}
		} else if listener != nil {
			if err := listener.Close(); err != nil &&
				!errors.Is(err, net.ErrClosed) {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		if adapter != nil {
			cleanupErr = errors.Join(cleanupErr, adapter.Close(closeCtx))
		} else if closer, ok := config.Plugin.(interface {
			Close(context.Context) error
		}); ok {
			cleanupErr = errors.Join(cleanupErr, closer.Close(closeCtx))
		}
		cancel()
		if storage != nil {
			closeCtx, cancel = context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			cleanupErr = errors.Join(cleanupErr, storage.Close(closeCtx))
			cancel()
		}
		returnErr = errors.Join(returnErr, cleanupErr)
	}()
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.ReadyWriter == nil {
		config.ReadyWriter = io.Discard
	}
	if strings.TrimSpace(config.Listen) == "" {
		config.Listen = "127.0.0.1:0"
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = 30 * time.Second
	}
	if config.LeaseTTL <= 0 {
		return errors.New("plugin lease TTL must be positive")
	}
	if (config.TLSCertFile == "") != (config.TLSKeyFile == "") {
		return errors.New("TLS certificate and key must be configured together")
	}
	manifest := config.Plugin.Manifest()
	if err := manifest.Validate(); err != nil {
		return err
	}
	stateDirectory := ""
	var err error
	if strings.TrimSpace(config.StorageURI) != "" {
		storage, err = sdkstorage.NewDefaultStorageRegistry().Open(
			ctx,
			config.StorageURI,
		)
	} else {
		stateDirectory, err = resolveStateDirectory(config.StateDirectory, manifest.Name)
		if err == nil {
			storage, err = sdkstorage.NewFileStateBackend(stateDirectory)
		}
	}
	if err != nil {
		return fmt.Errorf("configure plugin state backend: %w", err)
	}
	inbox, err := storage.Deliveries(sdk.PluginInboxQueue)
	if err != nil {
		return err
	}
	adapter, err = pluginrpc.NewServer(ctx, pluginrpc.ServerConfig{
		Plugin: config.Plugin, Operations: storage.Operations(), Inbox: inbox, Logger: config.Logger,
	})
	if err != nil {
		return err
	}
	listener, err = net.Listen("tcp", config.Listen)
	if err != nil {
		return fmt.Errorf("listen for plugin RPC: %w", err)
	}

	serverOptions, scheme, err := tlsServerOptions(config)
	if err != nil {
		return err
	}
	server, err = pluginrpc.NewGRPCServer(adapter, config.MaxMessageBytes, serverOptions...)
	if err != nil {
		return err
	}
	uri := strings.TrimSpace(config.AdvertiseURI)
	if uri == "" {
		if isWildcardAddress(listener.Addr().String()) {
			return errors.New("--advertise-uri is required for a wildcard listen address")
		}
		uri = scheme + "://" + listener.Addr().String()
	}

	serveDone = make(chan error, 1)
	serverStarted = true
	go func() { serveDone <- server.Serve(listener) }()

	leaseDone := make(chan error, 1)
	if config.RegistryURI != "" {
		registry, err = pluginrpc.NewRegistryClient(runContext, config.RegistryURI, pluginrpc.ClientConfig{})
		if err != nil {
			return fmt.Errorf("connect plugin registry: %w", err)
		}
		lease, err = registry.Register(runContext, sdk.PluginRegistration{
			Name: manifest.Name, URI: uri, Manifest: manifest,
		}, config.LeaseTTL)
		if err != nil {
			return fmt.Errorf("register plugin lease: %w", err)
		}
		go renewLease(runContext, registry, lease, config.LeaseTTL, leaseDone)
	}
	if err := json.NewEncoder(config.ReadyWriter).Encode(Ready{
		Name: manifest.Name, URI: uri, StateDirectory: stateDirectory,
		Storage: storage.String(), PID: os.Getpid(),
	}); err != nil {
		return fmt.Errorf("write plugin ready record: %w", err)
	}
	config.Logger.InfoContext(ctx, "plugin RPC server ready", "plugin", manifest.Name, "uri", uri)

	var runErr error
	select {
	case <-ctx.Done():
	case serveErr := <-serveDone:
		serveObserved = true
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			runErr = fmt.Errorf("serve plugin RPC: %w", serveErr)
		}
	case leaseErr := <-leaseDone:
		if leaseErr != nil && !errors.Is(leaseErr, context.Canceled) {
			runErr = leaseErr
		}
	}
	return runErr
}

func renewLease(
	ctx context.Context,
	client *pluginrpc.RegistryClient,
	lease sdk.PluginLease,
	ttl time.Duration,
	done chan<- error,
) {
	ticker := time.NewTicker(max(ttl/3, time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ticker.C:
			var err error
			lease, err = client.Renew(ctx, lease.ID, ttl)
			if err != nil {
				done <- fmt.Errorf("renew plugin lease: %w", err)
				return
			}
		}
	}
}

func tlsServerOptions(config Config) ([]grpc.ServerOption, string, error) {
	if config.TLSCertFile == "" {
		return nil, "grpc", nil
	}
	certificate, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
	if err != nil {
		return nil, "", fmt.Errorf("load plugin TLS identity: %w", err)
	}
	credentials := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	})
	return []grpc.ServerOption{grpc.Creds(credentials)}, "grpcs", nil
}

func resolveStateDirectory(configured, name string) (string, error) {
	directory := strings.TrimSpace(configured)
	if directory == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("resolve plugin state directory: %w", err)
		}
		directory = filepath.Join(base, "ag", "plugins", name)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", err
	}
	return absolute, nil
}

func isWildcardAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	return err != nil || host == "" || host == "0.0.0.0" || host == "::"
}

func stopServer(server *grpc.Server) {
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
