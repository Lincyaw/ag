package todotool

import (
	"github.com/lincyaw/ag/internal/tui/components/toolcommon"
	"github.com/lincyaw/ag/internal/tui/core/layout"
	"github.com/lincyaw/ag/internal/tui/service"
	"github.com/lincyaw/ag/internal/tui/types"
)

// New creates a new unified todo component.
// This component handles create, create_multiple, list, and update operations.
// The TODOs themselves are displayed in the sidebar; here we only show the
// tool call header (icon + name).
func New(msg *types.Message, sessionState service.SessionStateReader) layout.Model {
	return toolcommon.NewBase(msg, sessionState, toolcommon.NoArgsRenderer)
}
