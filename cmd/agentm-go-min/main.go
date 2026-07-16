package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/lincyaw/ag/agent"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/internal/telemetry"
	openaiplugin "github.com/lincyaw/ag/plugins/openai"
	"github.com/lincyaw/ag/plugins/workspace"
)

const (
	version       = "0.1.0"
	defaultSystem = "You are a concise command-line agent. Use the read-only workspace tools when they are useful."
)

type options struct {
	prompt      string
	system      string
	model       string
	baseURL     string
	cwd         string
	maxTurns    int
	timeout     time.Duration
	logLevel    string
	logFormat   string
	showVersion bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseOptions(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "argument error:", err)
		return 2
	}
	if opts.showVersion {
		fmt.Fprintln(stdout, version)
		return 0
	}

	logger, err := logging.New(logging.Config{
		Level:  opts.logLevel,
		Format: opts.logFormat,
		Writer: stderr,
	})
	if err != nil {
		fmt.Fprintln(stderr, "configuration error:", err)
		return 2
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, opts.timeout)
	defer cancel()

	observability, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "ag",
		ServiceVersion: version,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("telemetry setup failed", "error", err)
		return 7
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer shutdownCancel()
		if shutdownErr := observability.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("telemetry shutdown failed", "error", shutdownErr)
		}
	}()

	app, err := agent.New(
		agent.Config{
			MaxTurns: opts.maxTurns,
			Logger:   logger,
			Tracer:   observability.Tracer,
			Meter:    observability.Meter,
		},
		openaiplugin.New(openaiplugin.Config{
			Model:      opts.model,
			BaseURL:    opts.baseURL,
			MaxRetries: 2,
		}),
		workspace.New(workspace.Config{Root: opts.cwd}),
	)
	if err != nil {
		logger.Error("agent setup failed", "error", err)
		return 7
	}

	result, err := app.Run(ctx, opts.prompt, opts.system)
	if err != nil {
		logger.Error("agent run failed", "error", err)
		if errors.Is(ctx.Err(), context.Canceled) ||
			errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return 6
		}
		return 1
	}
	fmt.Fprintln(stdout, result.Output)
	return 0
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	opts := options{
		system:    defaultSystem,
		model:     envOr("OPENAI_MODEL", "gpt-5-mini"),
		baseURL:   os.Getenv("OPENAI_BASE_URL"),
		cwd:       ".",
		maxTurns:  8,
		timeout:   5 * time.Minute,
		logLevel:  envOr("LOG_LEVEL", "info"),
		logFormat: envOr("LOG_FORMAT", "json"),
	}

	flags := flag.NewFlagSet("ag", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.prompt, "p", "", "Prompt to run.")
	flags.StringVar(&opts.prompt, "prompt", "", "Prompt to run.")
	flags.StringVar(&opts.system, "system", opts.system, "System prompt.")
	flags.StringVar(&opts.model, "model", opts.model, "OpenAI model ID.")
	flags.StringVar(
		&opts.baseURL,
		"base-url",
		opts.baseURL,
		"Trusted OpenAI-compatible base URL.",
	)
	flags.StringVar(&opts.cwd, "cwd", opts.cwd, "Read-only workspace root.")
	flags.IntVar(&opts.maxTurns, "max-turns", opts.maxTurns, "Maximum model turns.")
	flags.DurationVar(&opts.timeout, "timeout", opts.timeout, "Whole-run timeout.")
	flags.StringVar(
		&opts.logLevel,
		"log-level",
		opts.logLevel,
		"debug, info, warn, or error.",
	)
	flags.StringVar(
		&opts.logFormat,
		"log-format",
		opts.logFormat,
		"json or text.",
	)
	flags.BoolVar(&opts.showVersion, "version", false, "Print version and exit.")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: ag -p <prompt> [options]")
		fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf(
			"unexpected positional arguments: %s",
			strings.Join(flags.Args(), " "),
		)
	}
	if opts.showVersion {
		return opts, nil
	}
	if strings.TrimSpace(opts.prompt) == "" {
		flags.Usage()
		return options{}, errors.New("prompt is required")
	}
	if strings.TrimSpace(opts.model) == "" {
		return options{}, errors.New("model is required")
	}
	if opts.maxTurns < 1 {
		return options{}, errors.New("max-turns must be positive")
	}
	if opts.timeout <= 0 {
		return options{}, errors.New("timeout must be positive")
	}
	return opts, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
