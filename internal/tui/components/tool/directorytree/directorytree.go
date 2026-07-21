package directorytree

import (
	"strings"

	pathx "github.com/lincyaw/ag/internal/cagent/path"
	"github.com/lincyaw/ag/internal/tui/components/toolcommon"
	"github.com/lincyaw/ag/internal/tui/core/layout"
	"github.com/lincyaw/ag/internal/tui/service"
	"github.com/lincyaw/ag/internal/tui/toolschema"
	"github.com/lincyaw/ag/internal/tui/types"
)

func New(msg *types.Message, sessionState service.SessionStateReader) layout.Model {
	return toolcommon.NewBase(msg, sessionState, toolcommon.SimpleRendererWithResult(
		toolcommon.ExtractField(func(a toolschema.DirectoryTreeArgs) string { return pathx.ShortenHome(a.Path) }),
		extractResult,
	))
}

func extractResult(msg *types.Message) string {
	if msg.ToolResult == nil || msg.ToolResult.Meta == nil {
		return ""
	}
	meta, ok := msg.ToolResult.Meta.(toolschema.DirectoryTreeMeta)
	if !ok {
		return ""
	}

	if meta.FileCount+meta.DirCount == 0 {
		return "empty"
	}

	var parts []string
	if meta.FileCount > 0 {
		parts = append(parts, toolcommon.Pluralize(meta.FileCount, "file", "files"))
	}
	if meta.DirCount > 0 {
		parts = append(parts, toolcommon.Pluralize(meta.DirCount, "dir", "dirs"))
	}

	result := strings.Join(parts, ", ")
	if meta.Truncated {
		result += " (truncated)"
	}
	return result
}
