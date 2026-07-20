package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/lincyaw/ag/internal/bootstrap"
	appconfig "github.com/lincyaw/ag/internal/config"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	"github.com/spf13/cobra"
)

func (application *app) runCommand() *cobra.Command {
	var prompt string
	var sessionID string
	var resumeID string
	var interactive bool
	command := &cobra.Command{
		Use:   "run",
		Short: "Run a prompt and durably record its trajectory",
		Example: `  ag run                           # interactive session
  ag run -p "Summarize this repo"  # interactive with initial prompt
  ag run -p "one shot" -i=false    # non-interactive
  ag run --resume <id>             # resume interactively`,
		Args: noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if sessionID != "" && resumeID != "" {
				return usageError{errors.New("--session and --resume are mutually exclusive")}
			}
			loaded, err := application.load(command)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			useInteractive := interactive && isReaderTerminal(os.Stdin)
			if !useInteractive && strings.TrimSpace(prompt) == "" {
				return usageError{errors.New("--prompt is required in non-interactive mode")}
			}

			if useInteractive {
				return application.runInteractive(
					command, loaded, prompt, sessionID, resumeID,
				)
			}
			return application.runOnce(
				command, loaded, prompt, sessionID, resumeID,
			)
		},
	}
	command.Flags().StringVarP(&prompt, "prompt", "p", "", "Prompt to run.")
	command.Flags().StringVar(&sessionID, "session", "", "ID for a new trajectory.")
	command.Flags().StringVar(&resumeID, "resume", "", "Resume an existing trajectory ID.")
	command.Flags().BoolVarP(&interactive, "interactive", "i", true,
		"Interactive loop (use -i=false to disable).")
	addRunConfigFlags(command.Flags())
	return command
}

func (application *app) runOnce(
	command *cobra.Command,
	loaded appconfig.Loaded,
	prompt string,
	sessionID string,
	resumeID string,
) error {
	ctx, cancel := commandContext(command, loaded.Config.Agent.Timeout)
	defer cancel()
	progress := application.progressReporter()
	if progress != nil {
		if err := progress.start(cancel); err != nil {
			return fmt.Errorf("start progress display: %w", err)
		}
		defer func() { _ = progress.stop() }()
	}
	running, err := bootstrap.StartRuntime(
		ctx, loaded.Config, application.stderr, application.version, progress,
	)
	if err != nil {
		return err
	}
	defer running.Close()

	session, err := openSession(ctx, running.Runtime, loaded.Config, sessionID, resumeID)
	if err != nil {
		return err
	}
	result, err := session.Prompt(ctx, prompt)
	if err != nil {
		return fmt.Errorf("run session %s: %w", session.ID(), err)
	}
	if progress != nil {
		_ = progress.stop()
	}
	return application.writeRun(session.ID(), result)
}

func (application *app) runInteractive(
	command *cobra.Command,
	loaded appconfig.Loaded,
	initialPrompt string,
	sessionID string,
	resumeID string,
) error {
	ctx, cancel := commandContext(command, 0)
	defer cancel()

	styles := newProgressStyles(
		application.colorEnabled(application.stderr) || application.colorForced(),
	)

	eventSink := &interactiveEventSink{}

	running, err := bootstrap.StartRuntime(
		ctx, loaded.Config, application.stderr, application.version, eventSink,
	)
	if err != nil {
		return err
	}
	defer running.Close()

	session, err := openSession(ctx, running.Runtime, loaded.Config, sessionID, resumeID)
	if err != nil {
		return err
	}

	model := newInteractiveModel(session, session.ID(), styles)
	if strings.TrimSpace(initialPrompt) != "" {
		model.input.SetValue(initialPrompt)
	}
	program := tea.NewProgram(
		model,
		tea.WithOutput(os.Stderr),
	)
	eventSink.program = program

	if _, err := program.Run(); err != nil {
		return fmt.Errorf("interactive session: %w", err)
	}
	return nil
}

func openSession(
	ctx context.Context,
	rt *agentruntime.Runtime,
	cfg appconfig.Config,
	sessionID string,
	resumeID string,
) (*agentruntime.Session, error) {
	sessionConfig := agentruntime.SessionConfig{
		ID:       sessionID,
		Provider: cfg.Agent.Provider,
		System:   cfg.Agent.System,
		MaxTurns: cfg.Agent.MaxTurns,
	}
	if resumeID != "" {
		resolved, err := resolveTrajectoryPrefix(ctx, rt, resumeID)
		if err != nil {
			return nil, err
		}
		sessionConfig.ResumePolicy = agentruntime.ResumeCurrent
		session, err := rt.ResumeSession(ctx, resolved, sessionConfig)
		if err != nil {
			return nil, fmt.Errorf("create session: %w", err)
		}
		return session, nil
	}
	session, err := rt.NewSession(ctx, sessionConfig)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	return session, nil
}

func resolveTrajectoryPrefix(
	ctx context.Context,
	rt *agentruntime.Runtime,
	id string,
) (string, error) {
	summaries, err := rt.ListTrajectories(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve trajectory %q: %w", id, err)
	}
	var matches []string
	for _, s := range summaries {
		if strings.HasPrefix(s.ID, id) {
			matches = append(matches, s.ID)
		}
	}
	switch len(matches) {
	case 0:
		return id, nil
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("ambiguous trajectory prefix %q: matches %d trajectories", id, len(matches))
	}
}
