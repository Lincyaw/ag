package userprompt

import (
	"github.com/lincyaw/ag/internal/tui/components/toolcommon"
	"github.com/lincyaw/ag/internal/tui/core/layout"
	"github.com/lincyaw/ag/internal/tui/service"
	"github.com/lincyaw/ag/internal/tui/types"
)

func New(msg *types.Message, sessionState service.SessionStateReader) layout.Model {
	return toolcommon.NewBase(msg, sessionState, toolcommon.NoArgsRenderer)
}
