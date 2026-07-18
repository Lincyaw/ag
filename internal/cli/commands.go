package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

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
