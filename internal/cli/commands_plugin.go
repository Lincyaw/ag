package cli

import (
	"errors"

	"github.com/lincyaw/ag/internal/bootstrap"
	"github.com/spf13/cobra"
)

func (application *app) pluginCommand() *cobra.Command {
	command := &cobra.Command{Use: "plugin", Short: "Inspect configured and discovered plugins"}
	addPluginConfigFlags(command.PersistentFlags())
	list := &cobra.Command{
		Use:   "list",
		Short: "List explicitly configured plugins",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) (returnErr error) {
			loaded, err := application.load(command)
			if err != nil {
				return err
			}
			catalog, err := bootstrap.OpenPluginCatalog(
				command.Context(),
				loaded.Config,
				application.stderr,
			)
			if err != nil {
				return err
			}
			defer func() {
				returnErr = errors.Join(returnErr, catalog.Close())
			}()
			descriptors, err := catalog.DiscoverConfigured(command.Context())
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
			instances, err := bootstrap.DiscoverPluginInstances(
				command.Context(),
				loaded.Config.Plugins,
				bootstrap.PluginInstanceQuery{
					Name:       discoverName,
					InstanceID: discoverInstanceID,
					Version:    discoverVersion,
					Resource:   discoverResource,
					Labels:     discoverLabels,
				},
			)
			if err != nil {
				return err
			}
			return application.writePluginInstances(instances)
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
		RunE: func(command *cobra.Command, args []string) (returnErr error) {
			loaded, err := application.load(command)
			if err != nil {
				return err
			}
			catalog, err := bootstrap.OpenPluginCatalog(
				command.Context(),
				loaded.Config,
				application.stderr,
			)
			if err != nil {
				return err
			}
			defer func() {
				returnErr = errors.Join(returnErr, catalog.Close())
			}()
			manifest, err := catalog.Manifest(command.Context(), args[0])
			if err != nil {
				return err
			}
			return application.writeManifest(manifest)
		},
	}
	command.AddCommand(list, discover, inspect)
	return command
}
