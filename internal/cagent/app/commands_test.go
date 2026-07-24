package app

import (
	"testing"

	"github.com/lincyaw/ag/internal/cagent/config/types"
	"github.com/lincyaw/ag/internal/cagent/session"
)

func TestPluginCommandResolution(t *testing.T) {
	application := New(t.Context(), session.New(session.WithID("commands")))
	application.SetAgentCommands(types.Commands{
		"review": {
			Description: "Review a target",
			Instruction: "Review $ARGUMENTS carefully",
		},
		"explain": {
			Description: "Explain a target",
			Instruction: "Explain this",
		},
	})

	if got := application.ResolveCommand(
		t.Context(),
		"/review sdk/plugin.go",
	); got != "Review sdk/plugin.go carefully" {
		t.Fatalf("placeholder command resolved to %q", got)
	}
	if got := application.ResolveCommand(
		t.Context(),
		"/explain behavior",
	); got != "Explain this\n\nbehavior" {
		t.Fatalf("append command resolved to %q", got)
	}
	if got := application.ResolveCommand(
		t.Context(),
		"/unknown value",
	); got != "/unknown value" {
		t.Fatalf("unknown command resolved to %q", got)
	}
}
