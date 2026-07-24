package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lincyaw/ag/internal/bootstrap"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

func (application *app) stateBackend(
	command *cobra.Command,
) (sdk.StateBackend, appconfig.Loaded, bootstrap.StateBackendResolution, error) {
	loaded, err := application.load(command)
	if err != nil {
		return nil, appconfig.Loaded{}, bootstrap.StateBackendResolution{}, err
	}
	resolution, err := bootstrap.ResolveStateBackend(loaded.Config)
	if err != nil {
		return nil, appconfig.Loaded{}, bootstrap.StateBackendResolution{}, err
	}
	backend, err := bootstrap.OpenResolvedStateBackend(
		command.Context(),
		resolution,
	)
	return backend, loaded, resolution, err
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
			backend, _, resolution, err := application.stateBackend(command)
			if err != nil {
				return err
			}
			defer backend.Close(context.Background())
			return application.writeState(stateOutput{
				Backend:            backend.String(),
				Namespace:          backend.Namespace(),
				Selection:          string(resolution.Source),
				LegacyFileFallback: resolution.LegacyFileFallback(),
				Capabilities:       sdk.InspectStorageCapabilities(backend),
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
			backend, _, _, err := application.stateBackend(command)
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
