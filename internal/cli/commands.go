package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	appconfig "github.com/lincyaw/ag/internal/config"
	pluginregistry "github.com/lincyaw/ag/registry"
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
			var observe func(context.Context, sdk.Event)
			progress := application.progressReporter()
			if progress != nil {
				if err := progress.start(cancel); err != nil {
					return fmt.Errorf("start progress display: %w", err)
				}
				defer func() { _ = progress.stop() }()
				observe = progress.observe
			}
			running, err := startRuntime(
				ctx,
				loaded.Config,
				application.stderr,
				application.version,
				observe,
			)
			if err != nil {
				return err
			}
			defer running.close()

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
			catalog, _, logFile, err := application.configuredRegistry(command)
			if err != nil {
				return err
			}
			defer logFile.Close()
			descriptors, err := catalog.Discover(
				command.Context(),
				sdk.DiscoveryQuery{},
			)
			if err != nil {
				return err
			}
			return application.writePlugins(descriptors)
		},
	}
	var (
		discoverName       string
		discoverInstanceID string
		discoverVersion    string
		discoverResource   string
		discoverLabels     map[string]string
	)
	discover := &cobra.Command{
		Use:   "discover",
		Short: "Discover active plugin instances from the registry",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if discoverInstanceID != "" && discoverName == "" {
				return usageError{errors.New(
					"--instance-id requires --name",
				)}
			}
			loaded, err := application.load(command)
			if err != nil {
				return err
			}
			directory, err := openPluginDirectory(
				command.Context(),
				loaded.Config.Plugins,
			)
			if err != nil {
				return err
			}
			instances, listErr := listPluginInstances(
				command.Context(),
				directory,
				pluginregistry.DiscoveryQuery{
					Namespace: loaded.Config.Plugins.RegistryNamespace,
					Name:      discoverName,
					Version:   discoverVersion,
					Resource:  discoverResource,
					Labels:    discoverLabels,
				},
			)
			if discoverInstanceID != "" {
				filtered := instances[:0]
				for _, instance := range instances {
					if instance.InstanceID == discoverInstanceID {
						filtered = append(filtered, instance)
					}
				}
				instances = filtered
			}
			writeErr := error(nil)
			if listErr == nil {
				writeErr = application.writePluginInstances(instances)
			}
			return errors.Join(
				listErr,
				writeErr,
				directory.Close(context.Background()),
			)
		},
	}
	discover.Flags().StringVar(
		&discoverName,
		"name",
		"",
		"Filter by plugin name.",
	)
	discover.Flags().StringVar(
		&discoverInstanceID,
		"instance-id",
		"",
		"Filter by instance ID (requires --name).",
	)
	discover.Flags().StringVar(
		&discoverVersion,
		"version",
		"",
		"Filter by exact plugin version.",
	)
	discover.Flags().StringVar(
		&discoverResource,
		"resource",
		"",
		"Filter by registered resource.",
	)
	discover.Flags().StringToStringVar(
		&discoverLabels,
		"label",
		nil,
		"Filter by label key=value (repeatable).",
	)
	inspect := &cobra.Command{
		Use:   "inspect <name[@instance-id]|uri>",
		Short: "Describe one local or remote plugin",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			catalog, plugins, logFile, err := application.configuredRegistry(command)
			if err != nil {
				return err
			}
			defer logFile.Close()
			source, err := resolvePluginSelection(
				command.Context(),
				catalog,
				plugins,
				args[0],
			)
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
) (*sdk.PluginRegistry, appconfig.Plugins, io.Closer, error) {
	loaded, err := application.load(command)
	if err != nil {
		return nil, appconfig.Plugins{}, nil, err
	}
	logger, logFile, err := openConfiguredLogger(
		loaded.Config.Logging,
		application.stderr,
	)
	if err != nil {
		return nil, appconfig.Plugins{}, nil, err
	}
	catalog, _, err := buildRegistry(
		command.Context(),
		loaded.Config,
		logger,
		nil,
		nil,
	)
	if err != nil {
		return nil, appconfig.Plugins{}, nil, errors.Join(
			err,
			logFile.Close(),
		)
	}
	return catalog, loaded.Config.Plugins, logFile, nil
}

func resolvePluginSelection(
	ctx context.Context,
	catalog *sdk.PluginRegistry,
	config appconfig.Plugins,
	nameOrURI string,
) (sdk.Source, error) {
	source, err := catalog.Resolve(ctx, nameOrURI)
	if err == nil || strings.Contains(nameOrURI, "://") {
		return source, err
	}
	if strings.TrimSpace(config.RegistryURI) == "" {
		return nil, err
	}
	directory, openErr := openPluginDirectory(
		ctx,
		config,
	)
	if openErr != nil {
		return nil, openErr
	}
	instance, selectErr := selectPluginInstance(
		ctx,
		directory,
		config.RegistryNamespace,
		nameOrURI,
	)
	closeErr := directory.Close(context.Background())
	if selectErr != nil || closeErr != nil {
		return nil, errors.Join(selectErr, closeErr)
	}
	return catalog.Resolve(ctx, instance.URI)
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
	var rollbackDryRun bool
	var rollbackYes bool
	var rollbackForce bool
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
			trajectory, err := store.Load(command.Context(), args[0])
			if err != nil {
				return err
			}
			if !trajectoryHasCheckpoint(trajectory, args[1]) {
				return fmt.Errorf("checkpoint not found: %s", args[1])
			}
			if rollbackDryRun {
				return application.writeRollbackPreview(rollbackPreviewOutput{
					TrajectoryID: trajectory.ID,
					CurrentHead:  trajectory.Head,
					CheckpointID: args[1],
					DryRun:       true,
				})
			}
			ok, err := application.confirm(
				fmt.Sprintf(
					"Roll back trajectory %s to checkpoint %s?",
					tableCell(args[0]),
					tableCell(args[1]),
				),
				rollbackYes || rollbackForce,
			)
			if err != nil {
				return err
			}
			if !ok {
				return errUserCanceled
			}
			logger, logFile, err := openConfiguredLogger(
				loaded.Config.Logging,
				application.stderr,
			)
			if err != nil {
				return err
			}
			defer logFile.Close()
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
			trajectory, err = store.Load(command.Context(), args[0])
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
	rollback.Flags().BoolVar(
		&rollbackDryRun,
		"dry-run",
		false,
		"Show the rollback target without changing the trajectory.",
	)
	rollback.Flags().BoolVar(
		&rollbackYes,
		"yes",
		false,
		"Skip interactive confirmation.",
	)
	rollback.Flags().BoolVar(
		&rollbackForce,
		"force",
		false,
		"Alias for --yes.",
	)
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

func (application *app) invocationCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "invocation",
		Short: "Inspect durable provider, tool, agent, and workflow calls",
	}
	show := &cobra.Command{
		Use:   "show <root-invocation-id>",
		Short: "Show one causal invocation graph",
		Args:  exactArgs(1),
		RunE: func(
			command *cobra.Command,
			args []string,
		) error {
			backend, _, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			graph, err := sdk.LoadInvocationGraph(
				command.Context(),
				backend.Operations(),
				args[0],
			)
			if err != nil {
				return err
			}
			if len(graph.Operations) == 0 {
				return fmt.Errorf("invocation root not found: %s", args[0])
			}
			return application.writeInvocationGraph(graph)
		},
	}
	command.AddCommand(show)
	return command
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
	var pruneDryRun bool
	var pruneYes bool
	var pruneForce bool
	prune := &cobra.Command{
		Use:   "prune",
		Short: "Delete terminal state older than a cutoff",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			cutoff, err := parseRetentionCutoff(before, time.Now().UTC())
			if err != nil {
				return usageError{err}
			}
			if pruneDryRun {
				return application.writePrunePreview(prunePreviewOutput{
					Cutoff: cutoff.Format(time.RFC3339),
					DryRun: true,
				})
			}
			ok, err := application.confirm(
				fmt.Sprintf(
					"Delete terminal state older than %s?",
					cutoff.Format(time.RFC3339),
				),
				pruneYes || pruneForce,
			)
			if err != nil {
				return err
			}
			if !ok {
				return errUserCanceled
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
	prune.Flags().BoolVar(
		&pruneDryRun,
		"dry-run",
		false,
		"Show the pruning cutoff without deleting state.",
	)
	prune.Flags().BoolVar(
		&pruneYes,
		"yes",
		false,
		"Skip interactive confirmation.",
	)
	prune.Flags().BoolVar(
		&pruneForce,
		"force",
		false,
		"Alias for --yes.",
	)
	command.AddCommand(inspect, prune)
	return command
}

func trajectoryHasCheckpoint(trajectory sdk.Trajectory, checkpointID string) bool {
	for _, entry := range trajectory.Entries {
		if entry.ID == checkpointID && entry.Kind == sdk.TrajectoryKindCheckpoint {
			return true
		}
	}
	return false
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
