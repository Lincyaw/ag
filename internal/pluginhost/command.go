package pluginhost

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/internal/telemetry"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

type CommandConfig struct {
	Name        string
	Description string
	Version     string
	Stdout      io.Writer
	Stderr      io.Writer
	AddFlags    func(*cobra.Command)
	Plugin      func() (sdk.Plugin, error)
}

func Execute(args []string, config CommandConfig) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := NewCommand(config)
	command.SetArgs(args)
	if err := command.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(config.Stderr, "%s: %v\n", config.Name, err)
		return 1
	}
	return 0
}

func NewCommand(config CommandConfig) *cobra.Command {
	var host Config
	var logLevel, logFormat string
	command := &cobra.Command{
		Use:           config.Name,
		Short:         config.Description,
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			plugin, err := config.Plugin()
			if err != nil {
				return err
			}
			logger, err := logging.New(logging.Config{
				Level: logLevel, Format: logFormat, Writer: config.Stderr,
			})
			if err != nil {
				return err
			}
			observability, err := telemetry.Setup(command.Context(), telemetry.Config{
				ServiceName: config.Name, ServiceVersion: config.Version, Logger: logger,
			})
			if err != nil {
				return err
			}
			logger = logging.WithHandler(logger, observability.LogHandler)
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := observability.Shutdown(ctx); err != nil {
					logger.Error("shutdown OpenTelemetry", "error", err)
				}
			}()
			host.Plugin = plugin
			host.Logger = logger
			host.ReadyWriter = config.Stdout
			return Serve(command.Context(), host)
		},
	}
	command.SetOut(config.Stdout)
	command.SetErr(config.Stderr)
	command.Flags().StringVar(&host.Listen, "listen", "127.0.0.1:0", "TCP listen address.")
	command.Flags().StringVar(&host.AdvertiseURI, "advertise-uri", "", "Registry-facing grpc[s] URI.")
	command.Flags().StringVar(&host.RegistryURI, "registry-uri", "", "Optional lease registry URI.")
	command.Flags().StringVar(
		&host.RegistryNamespace,
		"registry-namespace",
		"default",
		"Registry namespace.",
	)
	command.Flags().StringVar(
		&host.InstanceID,
		"instance-id",
		"",
		"Registry instance identity (default: generated per process).",
	)
	command.Flags().StringToStringVar(
		&host.RegistryLabels,
		"label",
		nil,
		"Registry label in key=value form (repeatable).",
	)
	command.Flags().DurationVar(&host.LeaseTTL, "lease-ttl", 30*time.Second, "Discovery lease TTL.")
	command.Flags().StringVar(&host.StateDirectory, "state-dir", "", "Durable operation and inbox state.")
	command.Flags().StringVar(
		&host.StorageURI,
		"storage",
		"",
		"State backend URI (memory://, file://, or an application-registered scheme).",
	)
	command.Flags().StringVar(&host.TLSCertFile, "tls-cert", "", "TLS certificate PEM file.")
	command.Flags().StringVar(&host.TLSKeyFile, "tls-key", "", "TLS private key PEM file.")
	command.Flags().IntVar(&host.MaxMessageBytes, "max-message-bytes", 0, "Maximum gRPC message size.")
	command.Flags().StringVar(&logLevel, "log-level", "info", "debug, info, warn, or error.")
	command.Flags().StringVar(&logFormat, "log-format", "json", "json or text.")
	if config.AddFlags != nil {
		config.AddFlags(command)
	}
	command.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(config.Stdout, config.Version)
			return err
		},
	})
	return command
}
