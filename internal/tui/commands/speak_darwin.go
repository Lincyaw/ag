//go:build darwin

package commands

import (
	tea "charm.land/bubbletea/v2"

	"github.com/lincyaw/ag/internal/tui/core"
	"github.com/lincyaw/ag/internal/tui/messages"
)

func speakCommand() *Item {
	return &Item{
		ID:           "session.speak",
		Label:        "Speak",
		SlashCommand: "/speak",
		Description:  "Start speech-to-text transcription (press Enter or Escape to stop)",
		Category:     "Session",
		Immediate:    true,
		Execute: func(string) tea.Cmd {
			return core.CmdHandler(messages.StartSpeakMsg{})
		},
	}
}
