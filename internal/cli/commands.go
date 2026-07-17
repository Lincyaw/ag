package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
	"github.com/lincyaw/ag/sdk"
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
			running, err := startRuntime(ctx, loaded.Config, application.stderr, application.version)
			if err != nil {
				return err
			}
			defer running.close(application.stderr)

			sessionConfig := agentruntime.SessionConfig{
				ID:       sessionID,
				Provider: loaded.Config.Agent.Provider,
				System:   loaded.Config.Agent.System,
				MaxTurns: loaded.Config.Agent.MaxTurns,
			}
			var session *agentruntime.Session
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
			return application.writeRun(session.ID(), result)
		},
	}
	command.Flags().StringVarP(&prompt, "prompt", "p", "", "Prompt to run.")
	command.Flags().StringVar(&sessionID, "session", "", "ID for a new trajectory.")
	command.Flags().StringVar(&resumeID, "resume", "", "Resume an existing trajectory ID.")
	addRunConfigFlags(command.Flags())
	return command
}

func (application *app) configCommand() *cobra.Command {
	command := &cobra.Command{Use: "config", Short: "Inspect effective configuration"}
	show := &cobra.Command{
		Use:   "show",
		Short: "Print effective non-secret configuration",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := application.load(command)
			if err != nil {
				return err
			}
			return application.writeConfig(loaded)
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
			return application.writePath(loaded.Path())
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
			return application.writePlugins(descriptors)
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
			return application.writePlugins(descriptors)
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
			writeErr := application.writeManifest(connection.Manifest())
			return errors.Join(writeErr, connection.Close(context.Background()))
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
	if len(descriptors) == 0 {
		return nil, err
	}
	if len(descriptors) > 1 {
		matches := make([]string, 0, len(descriptors))
		for _, descriptor := range descriptors {
			matches = append(matches, descriptor.URI)
		}
		return nil, fmt.Errorf(
			"plugin %q is ambiguous; matches: %s",
			nameOrURI,
			strings.Join(matches, ", "),
		)
	}
	if descriptors[0].URI == "" {
		return nil, fmt.Errorf(
			"discovered plugin %q has no resolvable URI",
			nameOrURI,
		)
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
			backend, _, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			store := backend.Trajectories()
			trajectories, err := store.List(command.Context())
			if err != nil {
				return err
			}
			return application.writeTrajectoryList(trajectories)
		},
	}
	var branchHead string
	show := &cobra.Command{
		Use:   "show <trajectory-id>",
		Short: "Show a trajectory or one of its branches",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			backend, _, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			store := backend.Trajectories()
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
			return application.writeTrajectory(trajectory)
		},
	}
	show.Flags().StringVar(&branchHead, "head", "", "Show only the branch ending at this entry.")
	rollback := &cobra.Command{
		Use:   "rollback <trajectory-id> <checkpoint-id>",
		Short: "Move the active branch to a prior checkpoint",
		Args:  exactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			backend, loaded, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			store := backend.Trajectories()
			logger, err := logging.New(logging.Config{
				Level: loaded.Config.Logging.Level, Format: loaded.Config.Logging.Format,
				Writer: application.stderr,
			})
			if err != nil {
				return err
			}
			runtime, err := agentruntime.NewRuntime(
				agentruntime.RuntimeConfig{
					Logger:           logger,
					Storage:          backend,
					StorageOwnership: agentruntime.StorageBorrowed,
				},
			)
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
			return application.writeRollback(rollbackOutput{
				TrajectoryID: trajectory.ID,
				Head:         trajectory.Head,
				CheckpointID: args[1],
			})
		},
	}
	command.AddCommand(list, show, rollback)
	return command
}

func (application *app) stateBackend(
	command *cobra.Command,
) (sdk.StateBackend, appconfig.Loaded, error) {
	loaded, err := application.load(command)
	if err != nil {
		return nil, appconfig.Loaded{}, err
	}
	backend, err := openStateBackend(command.Context(), loaded.Config)
	return backend, loaded, err
}

func (application *app) stateCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "state",
		Short: "Inspect and maintain the configured state backend",
	}
	inspect := &cobra.Command{
		Use:   "inspect",
		Short: "Show backend identity and correctness capabilities",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			backend, _, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			return application.writeState(stateOutput{
				Backend:      backend.String(),
				Namespace:    backend.Namespace(),
				Capabilities: backend.Capabilities(),
			})
		},
	}
	var before string
	prune := &cobra.Command{
		Use:   "prune",
		Short: "Delete terminal state older than a cutoff",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			cutoff, err := parseRetentionCutoff(before, time.Now().UTC())
			if err != nil {
				return usageError{err}
			}
			backend, _, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			result, err := backend.Prune(command.Context(), sdk.RetentionPolicy{
				OperationsBefore:   cutoff,
				DeliveriesBefore:   cutoff,
				TrajectoriesBefore: cutoff,
			})
			if err != nil {
				return err
			}
			return application.writePrune(result)
		},
	}
	prune.Flags().StringVar(
		&before,
		"before",
		"",
		"RFC3339 timestamp or age duration such as 720h (required).",
	)
	command.AddCommand(inspect, prune)
	return command
}

func parseRetentionCutoff(value string, now time.Time) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("--before is required")
	}
	if timestamp, err := time.Parse(time.RFC3339, value); err == nil {
		return timestamp.UTC(), nil
	}
	age, err := time.ParseDuration(value)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"--before must be RFC3339 or a duration: %w",
			err,
		)
	}
	if age <= 0 {
		return time.Time{}, errors.New("--before duration must be positive")
	}
	return now.Add(-age), nil
}

func (application *app) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Args:  noArgs,
		RunE:  func(_ *cobra.Command, _ []string) error { return application.writeVersion() },
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
