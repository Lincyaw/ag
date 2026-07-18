package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/internal/bootstrap"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	"github.com/spf13/cobra"
)

func (application *app) runCommand() *cobra.Command {
	var prompt string
	var sessionID string
	var resumeID string
	command := &cobra.Command{
		Use:   "run",
		Short: "Run a prompt and durably record its trajectory",
		Example: `  ag run -p "Summarize this repository"
  ag run --resume <session-id> -p "Continue"
  ag run -p "Inspect the repository" -o json`,
		Args: noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(prompt) == "" {
				return usageError{errors.New("--prompt is required")}
			}
			if sessionID != "" && resumeID != "" {
				return usageError{errors.New("--session and --resume are mutually exclusive")}
			}
			loaded, err := application.load(command)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
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
				ctx,
				loaded.Config,
				application.stderr,
				application.version,
				progress,
			)
			if err != nil {
				return err
			}
			defer running.Close()

			sessionConfig := agentruntime.SessionConfig{
				ID:       sessionID,
				Provider: loaded.Config.Agent.Provider,
				System:   loaded.Config.Agent.System,
				MaxTurns: loaded.Config.Agent.MaxTurns,
			}
			var session *agentruntime.Session
			if resumeID != "" {
				session, err = running.Runtime.ResumeSession(ctx, resumeID, sessionConfig)
			} else {
				session, err = running.Runtime.NewSession(ctx, sessionConfig)
			}
			if err != nil {
				return fmt.Errorf("create session: %w", err)
			}
			result, err := session.Prompt(ctx, prompt)
			if err != nil {
				return fmt.Errorf("run session %s: %w", session.ID(), err)
			}
			if progress != nil {
				_ = progress.stop()
			}
			return application.writeRun(session.ID(), result)
		},
	}
	command.Flags().StringVarP(&prompt, "prompt", "p", "", "Prompt to run.")
	command.Flags().StringVar(&sessionID, "session", "", "ID for a new trajectory.")
	command.Flags().StringVar(&resumeID, "resume", "", "Resume an existing trajectory ID.")
	addRunConfigFlags(command.Flags())
	return command
}
