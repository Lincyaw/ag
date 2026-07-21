package handoff

import (
	"encoding/json"

	"github.com/lincyaw/ag/internal/tui/components/spinner"
	"github.com/lincyaw/ag/internal/tui/components/toolcommon"
	"github.com/lincyaw/ag/internal/tui/core/layout"
	"github.com/lincyaw/ag/internal/tui/service"
	"github.com/lincyaw/ag/internal/tui/styles"
	"github.com/lincyaw/ag/internal/tui/toolschema"
	"github.com/lincyaw/ag/internal/tui/types"
)

func New(msg *types.Message, sessionState service.SessionStateReader) layout.Model {
	return toolcommon.NewBase(msg, sessionState, render)
}

func render(msg *types.Message, _ spinner.Spinner, _ service.SessionStateReader, _, _ int) string {
	var params toolschema.HandoffArgs
	if err := json.Unmarshal([]byte(msg.ToolCall.Function.Arguments), &params); err != nil {
		return ""
	}

	return styles.AgentBadgeStyleFor(msg.Sender).MarginLeft(2).Render(msg.Sender) + " ─► " + styles.AgentBadgeStyleFor(params.Agent).Render(params.Agent)
}
