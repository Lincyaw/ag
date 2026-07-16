package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

func (application *app) runCommand() *cobra.Command {
	var prompt string
	var sessionID string
	var resumeID string
	var outputFormat string
	command := &cobra.Command{
		Use:   "run",
		Short: "Run a prompt and durably record its trajectory",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(prompt) == "" {
				return usageError{errors.New("--prompt is required")}
			}
			if sessionID != "" && resumeID != "" {
				return usageError{errors.New("--session and --resume are mutually exclusive")}
			}
			if outputFormat != "text" && outputFormat != "json" {
				return usageError{errors.New(`--output must be "text" or "json"`)}
			}
			loaded, err := application.load(command)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			ctx, cancel := commandContext(command, loaded.Config.Agent.Timeout)
			defer cancel()
			running, err := startRuntime(ctx, loaded.Config, application.stderr, application.version)
			if err != nil {
				return err
			}
			defer running.close(application.stderr)

			sessionConfig := sdk.SessionConfig{
				ID:       sessionID,
				Provider: loaded.Config.Agent.Provider,
				System:   loaded.Config.Agent.System,
				MaxTurns: loaded.Config.Agent.MaxTurns,
			}
			var session *sdk.Session
			if resumeID != "" {
				session, err = running.runtime.ResumeSession(ctx, resumeID, sessionConfig)
			} else {
				session, err = running.runtime.NewSession(ctx, sessionConfig)
			}
			if err != nil {
				return fmt.Errorf("create session: %w", err)
			}
			result, err := session.Prompt(ctx, prompt)
			if err != nil {
				return fmt.Errorf("run session %s: %w", session.ID(), err)
			}
			if outputFormat == "json" {
				return writeJSON(application.stdout, struct {
					SessionID string     `json:"session_id"`
					Result    sdk.Result `json:"result"`
				}{SessionID: session.ID(), Result: result})
			}
			_, err = fmt.Fprintln(application.stdout, result.Output)
			return err
		},
	}
	command.Flags().StringVarP(&prompt, "prompt", "p", "", "Prompt to run.")
	command.Flags().StringVar(&sessionID, "session", "", "ID for a new trajectory.")
	command.Flags().StringVar(&resumeID, "resume", "", "Resume an existing trajectory ID.")
	command.Flags().StringVar(&outputFormat, "output", "text", "Output format: text or json.")
	addRunConfigFlags(command.Flags())
	return command
}

func (application *app) configCommand() *cobra.Command {
	command := &cobra.Command{Use: "config", Short: "Inspect effective configuration"}
	show := &cobra.Command{
		Use:   "show",
		Short: "Print effective non-secret configuration as JSON",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := application.load(command)
			if err != nil {
				return err
			}
			return writeJSON(application.stdout, struct {
				File   string           `json:"file"`
				Config appconfig.Config `json:"config"`
			}{File: loaded.File, Config: loaded.Config})
		},
	}
	path := &cobra.Command{
		Use:   "path",
		Short: "Print the active or default config path",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := application.load(command)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(application.stdout, loaded.Path())
			return err
		},
	}
	command.AddCommand(show, path)
	return command
}

func (application *app) pluginCommand() *cobra.Command {
	command := &cobra.Command{Use: "plugin", Short: "Inspect configured and discovered plugins"}
	addPluginConfigFlags(command.PersistentFlags())
	list := &cobra.Command{
		Use:   "list",
		Short: "List explicitly configured plugins",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			registry, _, err := application.configuredRegistry(command)
			if err != nil {
				return err
			}
			descriptors, err := registry.Discover(command.Context(), sdk.DiscoveryQuery{})
			if err != nil {
				return err
			}
			return writeJSON(application.stdout, descriptors)
		},
	}
	discover := &cobra.Command{
		Use:   "discover",
		Short: "List configured plugins plus active registry leases",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			registry, _, err := application.configuredRegistry(command)
			if err != nil {
				return err
			}
			descriptors, err := registry.Discover(command.Context(), sdk.DiscoveryQuery{IncludeDrivers: true})
			if err != nil {
				return err
			}
			return writeJSON(application.stdout, descriptors)
		},
	}
	inspect := &cobra.Command{
		Use:   "inspect <name-or-uri>",
		Short: "Describe one local or remote plugin",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			registry, _, err := application.configuredRegistry(command)
			if err != nil {
				return err
			}
			source, err := resolvePlugin(command.Context(), registry, args[0])
			if err != nil {
				return err
			}
			connection, err := source.Open(command.Context())
			if err != nil {
				return err
			}
			defer connection.Close(context.Background())
			return writeJSON(application.stdout, connection.Manifest())
		},
	}
	command.AddCommand(list, discover, inspect)
	return command
}

func (application *app) configuredRegistry(
	command *cobra.Command,
) (*sdk.PluginRegistry, []string, error) {
	loaded, err := application.load(command)
	if err != nil {
		return nil, nil, err
	}
	logger, err := logging.New(logging.Config{
		Level: loaded.Config.Logging.Level, Format: loaded.Config.Logging.Format,
		Writer: application.stderr,
	})
	if err != nil {
		return nil, nil, err
	}
	return buildRegistry(loaded.Config, logger, nil, nil)
}

func resolvePlugin(
	ctx context.Context,
	registry *sdk.PluginRegistry,
	nameOrURI string,
) (sdk.Source, error) {
	source, err := registry.Resolve(ctx, nameOrURI)
	if err == nil || strings.Contains(nameOrURI, "://") {
		return source, err
	}
	descriptors, discoverErr := registry.Discover(ctx, sdk.DiscoveryQuery{
		Name: nameOrURI, IncludeDrivers: true,
	})
	if discoverErr != nil {
		return nil, discoverErr
	}
	if len(descriptors) != 1 || descriptors[0].URI == "" {
		return nil, err
	}
	if registerErr := registry.Register(sdk.PluginReference{
		Name: descriptors[0].Name, URI: descriptors[0].URI,
		Description: descriptors[0].Description,
	}); registerErr != nil {
		return nil, registerErr
	}
	return registry.Resolve(ctx, nameOrURI)
}

func (application *app) trajectoryCommand() *cobra.Command {
	command := &cobra.Command{Use: "trajectory", Short: "Inspect and roll back durable trajectories"}
	list := &cobra.Command{
		Use:   "list",
		Short: "List trajectory summaries",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			store, _, err := application.trajectoryStore(command)
			if err != nil {
				return err
			}
			trajectories, err := store.List(command.Context())
			if err != nil {
				return err
			}
			return writeJSON(application.stdout, trajectories)
		},
	}
	var branchHead string
	show := &cobra.Command{
		Use:   "show <trajectory-id>",
		Short: "Show a trajectory or one of its branches",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			store, _, err := application.trajectoryStore(command)
			if err != nil {
				return err
			}
			trajectory, err := store.Load(command.Context(), args[0])
			if err != nil {
				return err
			}
			if branchHead != "" {
				branch, branchErr := trajectory.Branch(branchHead)
				if branchErr != nil {
					return branchErr
				}
				trajectory.Head = branchHead
				trajectory.Entries = branch
			}
			return writeJSON(application.stdout, trajectory)
		},
	}
	show.Flags().StringVar(&branchHead, "head", "", "Show only the branch ending at this entry.")
	rollback := &cobra.Command{
		Use:   "rollback <trajectory-id> <checkpoint-id>",
		Short: "Move the active branch to a prior checkpoint",
		Args:  exactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			store, loaded, err := application.trajectoryStore(command)
			if err != nil {
				return err
			}
			logger, err := logging.New(logging.Config{
				Level: loaded.Config.Logging.Level, Format: loaded.Config.Logging.Format,
				Writer: application.stderr,
			})
			if err != nil {
				return err
			}
			runtime, err := sdk.NewRuntime(sdk.RuntimeConfig{Logger: logger, Trajectories: store})
			if err != nil {
				return err
			}
			defer runtime.Close(context.Background())
			if err := runtime.RollbackTrajectory(command.Context(), args[0], args[1]); err != nil {
				return err
			}
			trajectory, err := store.Load(command.Context(), args[0])
			if err != nil {
				return err
			}
			return writeJSON(application.stdout, map[string]string{
				"trajectory_id": trajectory.ID,
				"head":          trajectory.Head,
				"checkpoint_id": args[1],
			})
		},
	}
	command.AddCommand(list, show, rollback)
	return command
}

func (application *app) trajectoryStore(
	command *cobra.Command,
) (*sdk.FileTrajectoryStore, appconfig.Loaded, error) {
	loaded, err := application.load(command)
	if err != nil {
		return nil, appconfig.Loaded{}, err
	}
	store, err := sdk.NewFileTrajectoryStore(filepath.Join(
		loaded.Config.State.Directory,
		"trajectories",
	))
	return store, loaded, err
}

func (application *app) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(application.stdout, application.version)
			return err
		},
	}
}

func noArgs(_ *cobra.Command, args []string) error {
	if len(args) != 0 {
		return usageError{fmt.Errorf("unexpected arguments: %v", args)}
	}
	return nil
}

func exactArgs(count int) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != count {
			return usageError{fmt.Errorf("expected %d argument(s), got %d", count, len(args))}
		}
		return nil
	}
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
