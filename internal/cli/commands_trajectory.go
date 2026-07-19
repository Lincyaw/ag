package cli

import (
	"context"
	"fmt"

	"github.com/lincyaw/ag/internal/bootstrap"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

func (application *app) trajectoryCommand() *cobra.Command {
	command := &cobra.Command{Use: "trajectory", Short: "Inspect and roll back durable trajectories"}
	list := &cobra.Command{
		Use:   "list",
		Short: "List trajectory summaries",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			backend, _, _, err := application.stateBackend(command)
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
			backend, _, _, err := application.stateBackend(command)
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
			backend, loaded, _, err := application.stateBackend(command)
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
			if err := bootstrap.RollbackTrajectory(
				command.Context(),
				loaded.Config,
				application.stderr,
				backend,
				args[0],
				args[1],
			); err != nil {
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

func trajectoryHasCheckpoint(trajectory sdk.Trajectory, checkpointID string) bool {
	for _, entry := range trajectory.Entries {
		if entry.ID == checkpointID && entry.Kind == sdk.TrajectoryKindCheckpoint {
			return true
		}
	}
	return false
}
