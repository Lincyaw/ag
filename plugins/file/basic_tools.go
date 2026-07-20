package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

type readTool struct{ filesystem *rootedFS }

func (readTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "read_file",
		Description: "Read a numbered range from one UTF-8 text file by workspace-relative or absolute path. The result includes a SHA-256 revision for conflict-safe edits.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type": "string", "description": "Workspace-relative or absolute path to the file.",
				},
				"offset": map[string]any{
					"type": "integer", "minimum": 1,
					"description": "First 1-based line to return; defaults to 1.",
				},
				"limit": map[string]any{
					"type": "integer", "minimum": 1,
					"description": fmt.Sprintf(
						"Maximum lines to return; defaults to %d.",
						defaultReadLineLimit,
					),
				},
			},
			"required":             []string{"path"},
			"additionalProperties": false,
		},
	}
}

func (tool readTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path   string `json:"path"`
		Offset *int   `json:"offset"`
		Limit  *int   `json:"limit"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return toolFailure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	offset := 1
	if arguments.Offset != nil {
		offset = *arguments.Offset
	}
	limit := defaultReadLineLimit
	if arguments.Limit != nil {
		limit = *arguments.Limit
	} else if limit > tool.filesystem.maxEntries {
		limit = tool.filesystem.maxEntries
	}
	if offset < 1 {
		return toolFailure(errors.New("offset must be at least 1")), nil
	}
	if limit < 1 || limit > tool.filesystem.maxEntries {
		return toolFailure(fmt.Errorf(
			"limit must be between 1 and %d", tool.filesystem.maxEntries,
		)), nil
	}
	path, err := tool.filesystem.existing(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	data, _, err := tool.filesystem.readText(path)
	if err != nil {
		return toolFailure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	lines := splitTextLines(string(data))
	if len(lines) > 0 && offset > len(lines) {
		return toolFailure(fmt.Errorf(
			"offset %d is past end of file (%d lines)", offset, len(lines),
		)), nil
	}
	if len(lines) == 0 && offset != 1 {
		return toolFailure(errors.New("offset is past end of empty file")), nil
	}
	end := len(lines)
	if candidate := offset - 1 + limit; candidate < end {
		end = candidate
	}
	startIndex := offset - 1
	if len(lines) == 0 {
		startIndex = 0
		end = 0
	}
	return sdk.ToolResult{Content: formatFileRange(
		arguments.Path,
		data,
		lines,
		startIndex,
		end,
	)}, nil
}

type writeTool struct{ filesystem *rootedFS }

func (writeTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "write_file",
		Description: "Atomically create or replace a UTF-8 file. Replacing an existing file requires the SHA-256 revision returned by read_file.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":    map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
				"expected_sha256": map[string]any{
					"type":        "string",
					"description": "Required when replacing an existing file; omit when creating a new file.",
				},
			},
			"required":             []string{"path", "content"},
			"additionalProperties": false,
		},
	}
}

func (tool writeTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path           string  `json:"path"`
		Content        *string `json:"content"`
		ExpectedSHA256 string  `json:"expected_sha256"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return toolFailure(err), nil
	}
	if arguments.Content == nil {
		return toolFailure(errors.New("content is required")), nil
	}
	if int64(len(*arguments.Content)) > tool.filesystem.maxWriteBytes {
		return toolFailure(fmt.Errorf(
			"content exceeds %d byte write limit", tool.filesystem.maxWriteBytes,
		)), nil
	}
	if !utf8.ValidString(*arguments.Content) {
		return toolFailure(errors.New("content is not valid UTF-8 text")), nil
	}
	target, err := tool.filesystem.writable(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	expectedRevision := strings.TrimSpace(arguments.ExpectedSHA256)
	if expectedRevision != "" && !isSHA256Revision(expectedRevision) {
		return toolFailure(errors.New(
			"expected_sha256 must be a 64-character hexadecimal SHA-256 revision",
		)), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	tool.filesystem.writeMu.Lock()
	defer tool.filesystem.writeMu.Unlock()

	mode := os.FileMode(0o600)
	existing, info, readErr := tool.filesystem.readText(target)
	switch {
	case readErr == nil:
		mode = info.Mode().Perm()
		actualRevision := fileRevision(existing)
		if expectedRevision == "" {
			return toolFailure(errors.New(
				"expected_sha256 is required when replacing an existing file; call read_file first",
			)), nil
		}
		if !strings.EqualFold(expectedRevision, actualRevision) {
			return toolFailure(errors.New(
				"stale file revision; call read_file again before overwriting",
			)), nil
		}
	case errors.Is(readErr, os.ErrNotExist):
		if expectedRevision != "" {
			return toolFailure(errors.New(
				"file no longer exists; omit expected_sha256 to create it",
			)), nil
		}
	default:
		return toolFailure(readErr), nil
	}
	if err := tool.filesystem.atomicWrite(ctx, target, []byte(*arguments.Content), mode); err != nil {
		return sdk.ToolResult{}, err
	}
	revision := fileRevision([]byte(*arguments.Content))
	return sdk.ToolResult{Content: fmt.Sprintf(
		"wrote: %q\nbytes: %d\nsha256: %s",
		cleanDisplayPath(arguments.Path),
		len(*arguments.Content),
		revision,
	)}, nil
}

func formatFileRange(
	path string,
	data []byte,
	lines []string,
	start int,
	end int,
) string {
	var output strings.Builder
	fmt.Fprintf(&output, "file: %q\n", cleanDisplayPath(path))
	fmt.Fprintf(&output, "bytes: %d\n", len(data))
	if len(lines) == 0 {
		output.WriteString("lines: 0-0 of 0\n")
	} else {
		fmt.Fprintf(&output, "lines: %d-%d of %d\n", start+1, end, len(lines))
	}
	fmt.Fprintf(&output, "sha256: %s\n---", fileRevision(data))
	for index := start; index < end; index++ {
		fmt.Fprintf(&output, "\n%d\t%s", index+1, lines[index])
	}
	return output.String()
}
