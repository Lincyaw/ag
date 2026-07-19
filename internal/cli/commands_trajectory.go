package cli

import (
	"context"
	"errors"
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
			if branchHead != "" {
				metadata, metadataErr := store.LoadMetadata(
					command.Context(),
					args[0],
				)
				if metadataErr != nil {
					return metadataErr
				}
				branch, branchErr := store.LoadBranch(
					command.Context(),
					args[0],
					branchHead,
				)
				if branchErr != nil {
					return branchErr
				}
				checkpoint, found, checkpointErr := store.FindLatest(
					command.Context(),
					args[0],
					branchHead,
					sdk.TrajectoryKindCheckpoint,
				)
				if checkpointErr != nil {
					return checkpointErr
				}
				checkpointID := ""
				if found {
					checkpointID = checkpoint.ID
				}
				return application.writeTrajectory(
					trajectoryFromMetadata(
						metadata,
						branchHead,
						checkpointID,
						branch,
					),
				)
			}
			trajectory, err := store.Load(command.Context(), args[0])
			if err != nil {
				return err
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
			metadata, err := store.LoadMetadata(command.Context(), args[0])
			if err != nil {
				return err
			}
			if err := requireTrajectoryCheckpoint(
				command.Context(),
				store,
				args[0],
				args[1],
			); err != nil {
				return err
			}
			if rollbackDryRun {
				return application.writeRollbackPreview(rollbackPreviewOutput{
					TrajectoryID: metadata.ID,
					CurrentHead:  metadata.Head,
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
			metadata, err = store.LoadMetadata(command.Context(), args[0])
			if err != nil {
				return err
			}
			return application.writeRollback(rollbackOutput{
				TrajectoryID: metadata.ID,
				Head:         metadata.Head,
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

func requireTrajectoryCheckpoint(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	checkpointID string,
) error {
	entry, err := store.LoadEntry(ctx, trajectoryID, checkpointID)
	if errors.Is(err, sdk.ErrTrajectoryEntryNotFound) {
		return fmt.Errorf("checkpoint not found: %s", checkpointID)
	}
	if err != nil {
		return err
	}
	if entry.Kind != sdk.TrajectoryKindCheckpoint {
		return fmt.Errorf("checkpoint not found: %s", checkpointID)
	}
	return nil
}

func trajectoryFromMetadata(
	metadata sdk.TrajectoryMetadata,
	head string,
	checkpoint string,
	entries []sdk.TrajectoryEntry,
) sdk.Trajectory {
	return sdk.Trajectory{
		SchemaVersion: metadata.SchemaVersion,
		ID:            metadata.ID,
		ParentID:      metadata.ParentID,
		ParentEntryID: metadata.ParentEntryID,
		CreatedAt:     metadata.CreatedAt,
		UpdatedAt:     metadata.UpdatedAt,
		Head:          head,
		Checkpoint:    checkpoint,
		Execution:     sdk.CloneTrajectoryExecution(metadata.Execution),
		Environment:   sdk.CloneTrajectoryEnvironment(metadata.Environment),
		Entries:       sdk.CloneTrajectoryEntries(entries),
	}
}
