package commands

import (
	"testing"

	"github.com/lincyaw/ag/internal/cagent/app"
	"github.com/lincyaw/ag/internal/cagent/config/types"
	"github.com/lincyaw/ag/internal/cagent/session"
)

func TestBuildCommandCategoriesIncludesPluginCommands(t *testing.T) {
	application := app.New(
		t.Context(),
		session.New(session.WithID("plugin-commands")),
	)
	application.SetAgentCommands(types.Commands{
		"review": {
			Description: "Review a target",
			Instruction: "Review $ARGUMENTS",
		},
	})

	categories := BuildCommandCategories(t.Context(), application)
	for _, category := range categories {
		for _, command := range category.Commands {
			if command.SlashCommand == "/review" {
				if command.Description != "Review a target" ||
					category.Name != "Agent Commands" {
					t.Fatalf(
						"plugin command = %#v in category %q",
						command,
						category.Name,
					)
				}
				return
			}
		}
	}
	t.Fatal("/review not found in TUI command categories")
}
