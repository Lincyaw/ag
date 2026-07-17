package main

import (
	"os"

	"github.com/lincyaw/ag/internal/pluginhost"
	fileplugin "github.com/lincyaw/ag/plugins/file"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

const version = "0.2.0"

func main() {
	var config fileplugin.Config
	os.Exit(pluginhost.Execute(os.Args[1:], pluginhost.CommandConfig{
		Name:        "agentm-plugin-file",
		Description: "Serve the root-confined file plugin over gRPC",
		Version:     version,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		AddFlags: func(command *cobra.Command) {
			command.Flags().StringVar(&config.Root, "root", ".", "Confined filesystem root.")
			command.Flags().BoolVar(&config.EnableWrite, "write", false, "Enable atomic file writes.")
			command.Flags().Int64Var(&config.MaxReadBytes, "max-read-bytes", 0, "Maximum bytes per read.")
			command.Flags().Int64Var(&config.MaxWriteBytes, "max-write-bytes", 0, "Maximum bytes per write.")
			command.Flags().IntVar(
				&config.MaxEntries,
				"max-entries",
				0,
				"Maximum list entries, read lines, searched files, or search matches.",
			)
		},
		Plugin: func() (sdk.Plugin, error) { return fileplugin.New(config), nil },
	}))
}
