package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/lincyaw/ag/gateway"
	gatewayclient "github.com/lincyaw/ag/gateway/client"
	cagentapp "github.com/lincyaw/ag/internal/cagent/app"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

func (application *app) runCommand() *cobra.Command {
	var prompt string
	var sessionID string
	var resumeID string
	var interactive bool
	command := &cobra.Command{
		Use:   "run [trajectory-id]",
		Short: "Open a trajectory view or run one prompt non-interactively",
		Example: `  ag run                           # create and open a trajectory view
  ag run <trajectory-id>           # attach to a background trajectory
  ag run -p "Summarize this repo"  # prefill a new trajectory view
  ag run -p "one shot" -i=false    # run through the background manager
  ag trajectory list               # list attachable trajectories`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) > 1 {
				return usageError{errors.New("ag run accepts at most one trajectory ID")}
			}
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			trajectoryID := ""
			if len(args) == 1 {
				trajectoryID = strings.TrimSpace(args[0])
			}
			if resumeID != "" {
				if trajectoryID != "" {
					return usageError{errors.New(
						"trajectory argument and compatibility --resume are mutually exclusive",
					)}
				}
				trajectoryID = resumeID
			}
			if sessionID != "" && trajectoryID != "" {
				return usageError{errors.New(
					"new trajectory ID and attached trajectory are mutually exclusive",
				)}
			}

			loaded, err := application.load(command)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			loaded.Config, err = normalizeAgentViewConfig(loaded.Config)
			if err != nil {
				return err
			}
			useInteractive := interactive && isReaderTerminal(os.Stdin)
			if !useInteractive && strings.TrimSpace(prompt) == "" {
				return usageError{errors.New("--prompt is required in non-interactive mode")}
			}
			target, err := application.ensureManagedGateway(
				command.Context(), loaded.Config,
			)
			if err != nil {
				return err
			}
			if useInteractive {
				return application.runGatewayTUI(
					command.Context(), loaded.Config, target,
					prompt, sessionID, trajectoryID,
				)
			}
			return application.runGatewayOnce(
				command, loaded.Config, target, prompt, sessionID, trajectoryID,
			)
		},
	}
	command.Flags().StringVarP(&prompt, "prompt", "p", "", "Prompt to run.")
	command.Flags().BoolVarP(
		&interactive, "interactive", "i", true,
		"Open the trajectory view (use -i=false for a direct one-shot run).",
	)
	command.Flags().StringVar(
		&sessionID, "session", "", "Compatibility ID for a new trajectory.",
	)
	command.Flags().StringVar(
		&resumeID, "resume", "", "Compatibility alias for the trajectory argument.",
	)
	_ = command.Flags().MarkHidden("session")
	_ = command.Flags().MarkHidden("resume")
	addRunConfigFlags(command.Flags())
	return command
}

func (application *app) runGatewayOnce(
	command *cobra.Command,
	config appconfig.Config,
	target string,
	prompt string,
	newID string,
	trajectoryID string,
) error {
	ctx, cancel := commandContext(command, config.Agent.Timeout)
	defer cancel()
	progress := application.progressReporter()
	if progress != nil {
		if err := progress.start(cancel); err != nil {
			return fmt.Errorf("start progress display: %w", err)
		}
		defer func() { _ = progress.stop() }()
	}
	client, err := gatewayclient.New(gatewayclient.Config{
		Target: target, UserID: localGatewayUserID,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	trajectoryID, err = openGatewayTrajectory(
		ctx, client, config, newID, trajectoryID,
	)
	if err != nil {
		return err
	}
	session := &gatewayInteractiveSession{
		frontend: gatewayRPCFrontend{client: client}, sessionID: trajectoryID,
	}
	if progress != nil {
		session.observe = progress.Observe
	}
	cursor, err := session.latestEventCursor(ctx)
	if err != nil {
		return fmt.Errorf("read trajectory event cursor: %w", err)
	}
	view, err := client.OpenView(ctx, trajectoryID, cursor)
	if err != nil {
		return fmt.Errorf("open trajectory view: %w", err)
	}
	session.frontend = gatewayRPCFrontend{client: client, view: view}
	observerContext, stopObserver := context.WithCancel(ctx)
	go session.observeEvents(observerContext, view)
	result, err := session.Prompt(ctx, prompt)
	stopObserver()
	_ = view.Close()
	if err != nil {
		return fmt.Errorf("run trajectory %s: %w", trajectoryID, err)
	}
	if progress != nil {
		_ = progress.stop()
	}
	return application.writeRun(trajectoryID, result)
}

func openGatewayTrajectory(
	ctx context.Context,
	client *gatewayclient.Client,
	config appconfig.Config,
	newID string,
	trajectoryID string,
) (string, error) {
	if trajectoryID != "" {
		resolved, err := resolveGatewaySessionPrefix(ctx, client, trajectoryID)
		if err != nil {
			return "", err
		}
		if _, err := client.GetSession(ctx, resolved); err != nil {
			return "", fmt.Errorf("open trajectory %s: %w", resolved, err)
		}
		return resolved, nil
	}
	if newID == "" {
		newID = sdk.NewID()
	}
	request, err := gatewayCreateSessionRequest(config, newID)
	if err != nil {
		return "", err
	}
	created, err := client.CreateSession(ctx, request)
	if err != nil {
		return "", fmt.Errorf("create trajectory %s: %w", newID, err)
	}
	return created.ID, nil
}

func gatewayCreateSessionRequest(
	config appconfig.Config,
	id string,
) (gatewayclient.CreateSessionRequest, error) {
	runtimeConfig, err := json.Marshal(
		appconfig.NewTrajectoryRuntimeProfile(config),
	)
	if err != nil {
		return gatewayclient.CreateSessionRequest{}, fmt.Errorf(
			"encode trajectory runtime profile: %w",
			err,
		)
	}
	autoCompact := config.Compact.Enabled
	permissions := cagentapp.LoadPermissionSettings(config.Workspace.Root)
	permissionRules := gateway.PermissionRules{}
	if permissions != nil {
		permissionRules = gateway.PermissionRules{
			Allow: slices.Clone(permissions.Allow),
			Ask:   slices.Clone(permissions.Ask),
			Deny:  slices.Clone(permissions.Deny),
		}
	}
	models := make([]string, 0, len(config.Models)+1)
	if model := strings.TrimSpace(config.OpenAI.Model); model != "" {
		models = append(models, model)
	}
	for name := range config.Models {
		if name = strings.TrimSpace(name); name != "" && !slices.Contains(models, name) {
			models = append(models, name)
		}
	}
	slices.Sort(models)
	tools := gatewayConfiguredTools(config)
	return gatewayclient.CreateSessionRequest{
		ID: id, Provider: config.Agent.Provider,
		System: config.Agent.System, MaxTurns: config.Agent.MaxTurns,
		WorkspaceRoot: config.Workspace.Root,
		RuntimeConfig: runtimeConfig,
		Settings: gateway.SessionSettings{
			Model:         config.OpenAI.Model,
			Models:        models,
			Tools:         tools,
			AutoCompact:   &autoCompact,
			ThinkingLevel: "off",
			Permissions:   permissionRules,
		},
	}, nil
}

func gatewayConfiguredTools(config appconfig.Config) []string {
	tools := []string{gateway.GatewayAskUserTool}
	if config.Workspace.Enabled {
		tools = append(tools, "read_file", "search_files")
		if config.Workspace.EnableWrite {
			tools = append(tools, "write_file", "edit_file")
		}
	}
	if config.Bash.Enabled {
		tools = append(tools, "bash")
	}
	slices.Sort(tools)
	return slices.Compact(tools)
}
