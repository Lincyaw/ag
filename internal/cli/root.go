package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	gatewaymanager "github.com/lincyaw/ag/gateway/manager"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	exitOK       = 0
	exitRuntime  = 1
	exitUsage    = 2
	exitCanceled = 130
)

type app struct {
	version       string
	stdout        io.Writer
	stderr        io.Writer
	configFile    string
	output        string
	progress      string
	color         string
	showVersion   bool
	dumpSchema    bool
	launchGateway gatewaymanager.Launcher
	probeGateway  gatewaymanager.Probe
}

type usageError struct{ error }

func Run(args []string, stdout, stderr io.Writer, version string) int {
	signalContext, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	childConfig, child, childErr := gatewaymanager.ChildRequestFromEnvironment()
	if child {
		if childErr != nil {
			writeTextError(stderr, childErr)
			return exitRuntime
		}
		return runManagedGatewayChild(
			signalContext, args, childConfig, stdout, stderr, version,
		)
	}
	command := New(stdout, stderr, version)
	command.SetArgs(args)
	if err := command.ExecuteContext(signalContext); err != nil {
		if errors.Is(err, errEarlyExit) {
			return exitOK
		}
		var usage usageError
		exitCode := exitRuntime
		switch {
		case errors.As(err, &usage):
			exitCode = exitUsage
		case errors.Is(err, errUserCanceled):
			exitCode = exitCanceled
		case errors.Is(err, context.Canceled):
			exitCode = exitCanceled
		}
		if requestedOutput(args, selectedOutput(command), command) == outputJSON {
			_ = writeCLIError(stderr, command, err, exitCode)
		} else {
			writeTextError(stderr, err)
		}
		return exitCode
	}
	return exitOK
}

func New(stdout, stderr io.Writer, version string) *cobra.Command {
	application := &app{version: version, stdout: stdout, stderr: stderr}
	root := &cobra.Command{
		Use:   "ag",
		Short: "Run and inspect pluggable agent trajectories",
		Example: `  ag run -p "Summarize this repository"
  ag trajectory list
  ag trajectory list -o json | jq '.[].id'`,
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          noArgs,
		PersistentPreRunE: func(command *cobra.Command, _ []string) error {
			if err := application.validateGlobalFlags(); err != nil {
				return err
			}
			return application.earlyExit(command.Root())
		},
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageError{err}
	})
	root.PersistentFlags().StringVar(
		&application.configFile,
		"config",
		"",
		"Config file (TOML, YAML, or JSON).",
	)
	root.PersistentFlags().String(
		"state-dir",
		"",
		"Durable state directory for automatic local backend selection.",
	)
	root.PersistentFlags().String(
		"storage",
		"",
		"State backend URI (memory://, file://, duckdb://, postgres://, postgresql://, or an application-registered scheme).",
	)
	root.PersistentFlags().String(
		"state-namespace",
		"",
		"Isolate state in a named backend namespace.",
	)
	root.PersistentFlags().StringVarP(
		&application.output,
		"output",
		"o",
		outputText,
		"Output format: text or json.",
	)
	root.PersistentFlags().StringVar(
		&application.progress,
		"progress",
		progressAuto,
		"Progress display: auto, tui, plain, always, or never.",
	)
	root.PersistentFlags().StringVar(
		&application.color,
		"color",
		colorAuto,
		"Color display: auto, always, or never.",
	)
	root.PersistentFlags().BoolVar(
		&application.showVersion,
		"version",
		false,
		"Print version and exit.",
	)
	root.PersistentFlags().BoolVar(
		&application.dumpSchema,
		"dump-schema",
		false,
		"Print the command schema as JSON and exit.",
	)
	root.PersistentFlags().String("log-level", "", "debug, info, warn, or error.")
	root.PersistentFlags().String("log-format", "", "json or text.")
	root.PersistentFlags().String("log-file", "", "Append logs to this file.")
	root.PersistentFlags().Bool(
		"log-console",
		false,
		"Also write logs to stderr.",
	)
	root.PersistentFlags().Bool("otel", true, "Project lifecycle events into OpenTelemetry.")

	root.AddCommand(
		application.runCommand(),
		application.configCommand(),
		application.pluginCommand(),
		application.registryCommand(),
		application.trajectoryCommand(),
		application.invocationCommand(),
		application.stateCommand(),
		application.versionCommand(),
	)
	return root
}

func (application *app) earlyExit(root *cobra.Command) error {
	if application.showVersion {
		if err := application.writeVersion(); err != nil {
			return err
		}
		return errEarlyExit
	}
	if application.dumpSchema {
		if err := application.writeSchema(root); err != nil {
			return err
		}
		return errEarlyExit
	}
	return nil
}

func (application *app) load(command *cobra.Command) (appconfig.Loaded, error) {
	return appconfig.Load(appconfig.LoadOptions{
		ConfigFile: application.configFile,
		Flags:      command.Flags(),
	})
}

func addRunConfigFlags(flags *pflag.FlagSet) {
	flags.String("system", "", "System prompt.")
	flags.String("provider", "", "Provider resource name.")
	flags.Int("max-turns", 0, "Maximum model turns.")
	flags.Duration("timeout", 0, "Whole-run timeout.")
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
	flags.Int64("bash-max-output-bytes", 0, "Maximum retained bytes per bash output stream.")
	flags.Bool("compact", true, "Mount automatic prompt compaction.")
	addPluginConfigFlags(flags)
}

func addPluginConfigFlags(flags *pflag.FlagSet) {
	flags.StringSlice(
		"plugin",
		nil,
		"Remote plugin as name=grpc[s]://host:port or name[@instance-id] (repeatable).",
	)
	flags.String("registry-uri", "", "Remote lease registry grpc[s] URI.")
	flags.String(
		"registry-namespace",
		"",
		"Registry namespace used for discovery.",
	)
}

func commandContext(command *cobra.Command, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(command.Context())
	}
	return context.WithTimeout(command.Context(), timeout)
}
