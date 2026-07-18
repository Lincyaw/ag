package cli

import (
	"context"
	"fmt"

	"github.com/lincyaw/ag/sdk"
	"github.com/spf13/cobra"
)

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
