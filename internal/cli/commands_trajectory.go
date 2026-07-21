package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/gateway"
	gatewayclient "github.com/lincyaw/ag/gateway/client"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

func (application *app) trajectoryCommand() *cobra.Command {
	command := &cobra.Command{
		Use: "trajectory", Short: "Inspect and control durable trajectories",
	}
	list := &cobra.Command{
		Use:   "list",
		Short: "List foreground and background trajectories",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			return application.listManagedTrajectories(
				command.Context(), client,
			)
		},
	}
	var branchHead string
	show := &cobra.Command{
		Use:   "show <trajectory-id>",
		Short: "Show a trajectory or one of its branches",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			trajectoryID, err := resolveGatewaySessionPrefix(
				command.Context(), client, args[0],
			)
			if err != nil {
				return err
			}
			trajectory, err := loadTrajectoryInspection(
				command.Context(), client, trajectoryID, branchHead,
			)
			if err != nil {
				return err
			}
			return application.writeTrajectoryInspection(trajectory)
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
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			trajectoryID, err := resolveGatewaySessionPrefix(
				command.Context(), client, args[0],
			)
			if err != nil {
				return err
			}
			trajectory, err := loadTrajectoryInspection(
				command.Context(), client, trajectoryID, "",
			)
			if err != nil {
				return err
			}
			if err := requireTrajectoryCheckpoint(trajectory, args[1]); err != nil {
				return err
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
					tableCell(trajectoryID), tableCell(args[1]),
				),
				rollbackYes || rollbackForce,
			)
			if err != nil {
				return err
			}
			if !ok {
				return errUserCanceled
			}
			rolledBack, err := client.RollbackTrajectory(
				command.Context(), trajectoryID, args[1],
			)
			if err != nil {
				return err
			}
			return application.writeRollback(rollbackOutput{
				TrajectoryID: rolledBack.ID,
				Head:         rolledBack.Head,
				CheckpointID: args[1],
			})
		},
	}
	rollback.Flags().BoolVar(
		&rollbackDryRun, "dry-run", false,
		"Show the rollback target without changing the trajectory.",
	)
	rollback.Flags().BoolVar(
		&rollbackYes, "yes", false, "Skip interactive confirmation.",
	)
	rollback.Flags().BoolVar(
		&rollbackForce, "force", false, "Alias for --yes.",
	)
	command.AddCommand(
		list,
		show,
		rollback,
		application.trajectorySubmitCommand(),
		application.trajectoryPauseCommand(true),
		application.trajectoryPauseCommand(false),
		application.trajectoryCancelCommand(),
		application.trajectoryWaitCommand(),
	)
	return command
}

func (application *app) trajectorySubmitCommand() *cobra.Command {
	var prompt string
	command := &cobra.Command{
		Use:   "submit <trajectory-id>",
		Short: "Queue a prompt for a trajectory",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if strings.TrimSpace(prompt) == "" {
				return usageError{errors.New("--prompt is required")}
			}
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			trajectoryID, err := resolveGatewaySessionPrefix(
				command.Context(), client, args[0],
			)
			if err != nil {
				return err
			}
			input, err := client.EnqueueInput(
				command.Context(), trajectoryID, sdk.NewID(), prompt,
			)
			if err != nil {
				return err
			}
			return application.writeTrajectoryControl(trajectoryControlOutput{
				TrajectoryID: trajectoryID, Action: "submit",
				Status: string(input.State), InputID: input.ID,
			})
		},
	}
	command.Flags().StringVarP(&prompt, "prompt", "p", "", "Prompt to queue.")
	return command
}

func (application *app) trajectoryPauseCommand(paused bool) *cobra.Command {
	name := "pause"
	short := "Pause queued prompts after the active execution"
	action := "pause"
	statusName := agentStatusPaused
	if !paused {
		name = "resume"
		short = "Resume dispatching queued prompts"
		action = "resume"
		statusName = agentStatusIdle
	}
	command := &cobra.Command{
		Use: name + " <trajectory-id>", Short: short,
		Args: exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			trajectoryID, err := resolveGatewaySessionPrefix(
				command.Context(), client, args[0],
			)
			if err != nil {
				return err
			}
			trajectory, err := client.GetSession(command.Context(), trajectoryID)
			if err != nil {
				return err
			}
			if trajectory.Paused != paused {
				if paused {
					trajectory, err = client.PauseSession(
						command.Context(), trajectoryID, trajectory.Revision,
					)
				} else {
					trajectory, err = client.ResumeSession(
						command.Context(), trajectoryID, trajectory.Revision,
					)
				}
				if err != nil {
					return err
				}
			}
			if trajectory.Paused {
				statusName = agentStatusPaused
			}
			return application.writeTrajectoryControl(trajectoryControlOutput{
				TrajectoryID: trajectoryID, Action: action, Status: statusName,
			})
		},
	}
	if !paused {
		command.Aliases = []string{"unpause"}
	}
	return command
}

func (application *app) trajectoryCancelCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <trajectory-id>",
		Short: "Cancel queued prompts and the active execution",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			trajectoryID, err := resolveGatewaySessionPrefix(
				command.Context(), client, args[0],
			)
			if err != nil {
				return err
			}
			inputs, err := listAllGatewayInputs(
				command.Context(), client, trajectoryID,
			)
			if err != nil {
				return err
			}
			affected := 0
			for _, input := range inputs {
				if input.State.Terminal() {
					continue
				}
				if _, err := client.CancelInput(
					command.Context(), trajectoryID, input.ID, input.Revision,
				); err != nil {
					return err
				}
				affected++
			}
			return application.writeTrajectoryControl(trajectoryControlOutput{
				TrajectoryID: trajectoryID, Action: "cancel",
				Status: "cancellation_requested", Affected: affected,
			})
		},
	}
}

func (application *app) trajectoryWaitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "wait <trajectory-id>",
		Short: "Wait until queued and active work is terminal",
		Args:  exactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			client, _, err := application.managedGatewayClient(command)
			if err != nil {
				return err
			}
			defer client.Close()
			trajectoryID, err := resolveGatewaySessionPrefix(
				command.Context(), client, args[0],
			)
			if err != nil {
				return err
			}
			session := &gatewayInteractiveSession{
				frontend: gatewayRPCFrontend{client: client}, sessionID: trajectoryID,
			}
			cursor, err := session.latestEventCursor(command.Context())
			if err != nil {
				return err
			}
			view, err := client.OpenView(command.Context(), trajectoryID, cursor)
			if err != nil {
				return err
			}
			defer view.Close()
			for {
				inputs, err := listAllGatewayInputs(
					command.Context(), client, trajectoryID,
				)
				if err != nil {
					return err
				}
				pending := 0
				for _, input := range inputs {
					if !input.State.Terminal() {
						pending++
					}
				}
				if pending == 0 {
					statusName := agentStatusIdle
					trajectory, err := client.GetSession(
						command.Context(), trajectoryID,
					)
					if err != nil {
						return err
					}
					if trajectory.Paused {
						statusName = agentStatusPaused
					}
					return application.writeTrajectoryControl(trajectoryControlOutput{
						TrajectoryID: trajectoryID, Action: "wait", Status: statusName,
					})
				}
				_, err = view.Next()
				if err != nil {
					return err
				}
			}
		},
	}
}

func (application *app) managedGatewayClient(
	command *cobra.Command,
) (*gatewayclient.Client, string, error) {
	loaded, err := application.load(command)
	if err != nil {
		return nil, "", fmt.Errorf("load config: %w", err)
	}
	loaded.Config, err = normalizeAgentViewConfig(loaded.Config)
	if err != nil {
		return nil, "", err
	}
	target, err := application.ensureManagedGateway(
		command.Context(), loaded.Config,
	)
	if err != nil {
		return nil, "", err
	}
	client, err := gatewayclient.New(gatewayclient.Config{
		Target: target, UserID: localGatewayUserID,
	})
	if err != nil {
		return nil, "", err
	}
	return client, target, nil
}

func requireTrajectoryCheckpoint(
	trajectory gateway.TrajectoryInspection,
	checkpointID string,
) error {
	for _, entry := range trajectory.Entries {
		if entry.ID == checkpointID && entry.Kind == sdk.TrajectoryKindCheckpoint {
			return nil
		}
	}
	return fmt.Errorf("checkpoint not found: %s", checkpointID)
}

func loadTrajectoryInspection(
	ctx context.Context,
	client *gatewayclient.Client,
	trajectoryID string,
	head string,
) (gateway.TrajectoryInspection, error) {
	query := gateway.TrajectoryEntryQuery{Limit: gatewayEventPageSize}
	requestedHead := strings.TrimSpace(head)
	var inspection gateway.TrajectoryInspection
	first := true
	for {
		page, err := client.ListTrajectoryEntries(
			ctx,
			trajectoryID,
			requestedHead,
			query,
		)
		if err != nil {
			return gateway.TrajectoryInspection{}, err
		}
		if first {
			inspection = page.Trajectory
			inspection.Entries = nil
			requestedHead = inspection.Head
			first = false
		} else if page.Trajectory.ID != inspection.ID ||
			page.Trajectory.Head != inspection.Head ||
			page.Trajectory.EntryCount != inspection.EntryCount {
			return gateway.TrajectoryInspection{}, errors.New(
				"trajectory inspection changed while paging",
			)
		}
		inspection.Entries = append(inspection.Entries, page.Items...)
		if page.Next == 0 {
			return inspection, nil
		}
		query.After = page.Next
	}
}
