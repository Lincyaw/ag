package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

type editTool struct{ filesystem *rootedFS }

type editArguments struct {
	Path           string  `json:"path"`
	ExpectedSHA256 string  `json:"expected_sha256"`
	OldText        *string `json:"old_text"`
	NewText        *string `json:"new_text"`
	ReplaceAll     bool    `json:"replace_all"`
	StartLine      *int    `json:"start_line"`
	EndLine        *int    `json:"end_line"`
}

func (editTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "edit_file",
		Description: "Atomically edit a UTF-8 file at an exact string or inclusive 1-based line range. " +
			"Requires the SHA-256 revision returned by read_file and returns a new revision plus local context.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type": "string", "description": "Relative path to the file.",
				},
				"expected_sha256": map[string]any{
					"type":        "string",
					"description": "Exact SHA-256 revision returned by read_file.",
				},
				"old_text": map[string]any{
					"type":        "string",
					"description": "Exact text to replace. It must occur once unless replace_all is true.",
				},
				"new_text": map[string]any{
					"type": "string", "description": "Replacement text; may be empty.",
				},
				"replace_all": map[string]any{
					"type":        "boolean",
					"description": "Replace every exact old_text occurrence instead of requiring one.",
				},
				"start_line": map[string]any{
					"type": "integer", "minimum": 1,
					"description": "First line of an inclusive replacement range.",
				},
				"end_line": map[string]any{
					"type": "integer", "minimum": 1,
					"description": "Last line of an inclusive replacement range.",
				},
			},
			"required":             []string{"path", "expected_sha256", "new_text"},
			"additionalProperties": false,
		},
	}
}

func (tool editTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments editArguments
	if err := decodeArguments(raw, &arguments); err != nil {
		return toolFailure(err), nil
	}
	if err := validateEditArguments(arguments); err != nil {
		return toolFailure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	target, err := tool.filesystem.writable(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	tool.filesystem.writeMu.Lock()
	defer tool.filesystem.writeMu.Unlock()

	source, info, err := tool.filesystem.readText(target)
	if err != nil {
		return toolFailure(err), nil
	}
	actualRevision := fileRevision(source)
	if !strings.EqualFold(strings.TrimSpace(arguments.ExpectedSHA256), actualRevision) {
		return toolFailure(errors.New(
			"stale file revision; call read_file again before editing",
		)), nil
	}

	updated, changedLine, replacements, err := applyEdit(string(source), arguments)
	if err != nil {
		return toolFailure(err), nil
	}
	if int64(len(updated)) > tool.filesystem.maxWriteBytes {
		return toolFailure(fmt.Errorf(
			"edited content exceeds %d byte write limit", tool.filesystem.maxWriteBytes,
		)), nil
	}
	if !utf8.ValidString(updated) {
		return toolFailure(errors.New("edited content is not valid UTF-8 text")), nil
	}
	if updated == string(source) {
		return toolFailure(errors.New("edit would not change the file")), nil
	}
	if err := tool.filesystem.atomicWrite(
		ctx,
		target,
		[]byte(updated),
		info.Mode().Perm(),
	); err != nil {
		return sdk.ToolResult{}, err
	}

	lines := splitTextLines(updated)
	start := changedLine - 4
	if start < 0 {
		start = 0
	}
	end := changedLine + 3
	if end > len(lines) {
		end = len(lines)
	}
	var output strings.Builder
	fmt.Fprintf(&output, "edited: %q\n", cleanDisplayPath(arguments.Path))
	fmt.Fprintf(&output, "replacements: %d\n", replacements)
	fmt.Fprintf(&output, "sha256: %s\n", fileRevision([]byte(updated)))
	if len(lines) == 0 {
		output.WriteString("context: 0-0 of 0\n---")
	} else {
		fmt.Fprintf(&output, "context: %d-%d of %d\n---", start+1, end, len(lines))
		for index := start; index < end; index++ {
			fmt.Fprintf(&output, "\n%d\t%s", index+1, lines[index])
		}
	}
	return sdk.ToolResult{Content: output.String()}, nil
}

func validateEditArguments(arguments editArguments) error {
	if strings.TrimSpace(arguments.ExpectedSHA256) == "" {
		return errors.New("expected_sha256 is required; call read_file first")
	}
	if arguments.NewText == nil {
		return errors.New("new_text is required; use an empty string to delete content")
	}
	if !isSHA256Revision(strings.TrimSpace(arguments.ExpectedSHA256)) {
		return errors.New(
			"expected_sha256 must be a 64-character hexadecimal SHA-256 revision",
		)
	}
	usesExactText := arguments.OldText != nil
	usesLineRange := arguments.StartLine != nil || arguments.EndLine != nil
	if usesExactText == usesLineRange {
		return errors.New(
			"provide exactly one edit selector: old_text, or both start_line and end_line",
		)
	}
	if usesExactText {
		if *arguments.OldText == "" {
			return errors.New("old_text must not be empty")
		}
		return nil
	}
	if arguments.StartLine == nil || arguments.EndLine == nil {
		return errors.New("start_line and end_line must be provided together")
	}
	if *arguments.StartLine < 1 || *arguments.EndLine < *arguments.StartLine {
		return errors.New("line range must satisfy 1 <= start_line <= end_line")
	}
	if arguments.ReplaceAll {
		return errors.New("replace_all is only valid with old_text")
	}
	return nil
}

func applyEdit(
	source string,
	arguments editArguments,
) (updated string, changedLine int, replacements int, err error) {
	if arguments.OldText != nil {
		count := strings.Count(source, *arguments.OldText)
		switch {
		case count == 0:
			return "", 0, 0, errors.New("old_text was not found exactly; re-read the file with read_file and use start_line/end_line instead")
		case count > 1 && !arguments.ReplaceAll:
			return "", 0, 0, fmt.Errorf(
				"old_text matched %d locations; provide more context or set replace_all",
				count,
			)
		}
		first := strings.Index(source, *arguments.OldText)
		changedLine = strings.Count(source[:first], "\n") + 1
		replaceCount := 1
		if arguments.ReplaceAll {
			replaceCount = -1
			replacements = count
		} else {
			replacements = 1
		}
		return strings.Replace(
			source,
			*arguments.OldText,
			*arguments.NewText,
			replaceCount,
		), changedLine, replacements, nil
	}

	lines := splitTextLines(source)
	start := *arguments.StartLine
	end := *arguments.EndLine
	if start > len(lines) || end > len(lines) {
		return "", 0, 0, fmt.Errorf(
			"line range %d-%d is outside file with %d lines",
			start,
			end,
			len(lines),
		)
	}
	replacement := splitTextLines(
		strings.ReplaceAll(*arguments.NewText, "\r\n", "\n"),
	)
	combined := make([]string, 0, len(lines)-(end-start+1)+len(replacement))
	combined = append(combined, lines[:start-1]...)
	combined = append(combined, replacement...)
	combined = append(combined, lines[end:]...)

	lineEnding := "\n"
	if strings.Contains(source, "\r\n") {
		lineEnding = "\r\n"
	}
	updated = strings.Join(combined, lineEnding)
	if strings.HasSuffix(source, "\n") && updated != "" {
		updated += lineEnding
	}
	return updated, start, end - start + 1, nil
}
