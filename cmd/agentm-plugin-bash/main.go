package main

import (
	"os"

	"github.com/lincyaw/ag/internal/pluginhost"
	"github.com/lincyaw/ag/plugins/bash"
	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

const version = "0.2.0"

func main() {
	var config bash.Config
	os.Exit(pluginhost.Execute(os.Args[1:], pluginhost.CommandConfig{
		Name:        "agentm-plugin-bash",
		Description: "Serve bounded shell execution over gRPC",
		Version:     version,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		AddFlags: func(command *cobra.Command) {
			command.Flags().StringVar(&config.Root, "root", ".", "Working directory root.")
			command.Flags().StringVar(&config.Shell, "shell", "/bin/sh", "Absolute executable shell path.")
			command.Flags().DurationVar(&config.DefaultTimeout, "timeout", 0, "Default command timeout.")
			command.Flags().DurationVar(&config.MaxTimeout, "max-timeout", 0, "Maximum command timeout.")
			command.Flags().Int64Var(&config.MaxOutputBytes, "max-output-bytes", 0, "Retained bytes per output stream.")
			command.Flags().StringArrayVar(&config.Environment, "env", nil, "Explicit KEY=VALUE environment entry (repeatable).")
		},
		Plugin: func() (sdk.Plugin, error) { return bash.New(config), nil },
	}))
}
