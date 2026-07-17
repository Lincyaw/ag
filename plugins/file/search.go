package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultSearchResults = 100
	maxSearchLineRunes   = 300
)

var errSearchTruncated = errors.New("search limit reached")

type searchTool struct{ filesystem *rootedFS }

type searchArguments struct {
	Path          string `json:"path"`
	Query         string `json:"query"`
	Glob          string `json:"glob"`
	Regex         bool   `json:"regex"`
	CaseSensitive *bool  `json:"case_sensitive"`
	IncludeHidden bool   `json:"include_hidden"`
	MaxResults    *int   `json:"max_results"`
}

type searchMatch struct {
	path   string
	line   int
	column int
	text   string
}

func (searchTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "search_files",
		Description: "Search UTF-8 files under a root-confined path and return deterministic path:line:column matches. " +
			"Literal search is the default; regular expressions and recursive globs are optional.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Relative file or directory path; defaults to .",
				},
				"query": map[string]any{
					"type": "string", "description": "Text or regular expression to find.",
				},
				"glob": map[string]any{
					"type":        "string",
					"description": "Optional file glob using *, **, and ?, for example **/*.go.",
				},
				"regex": map[string]any{
					"type": "boolean", "description": "Interpret query as a Go regular expression.",
				},
				"case_sensitive": map[string]any{
					"type": "boolean", "description": "Defaults to true.",
				},
				"include_hidden": map[string]any{
					"type": "boolean", "description": "Include dotfiles and hidden directories.",
				},
				"max_results": map[string]any{
					"type": "integer", "minimum": 1,
					"description": fmt.Sprintf(
						"Maximum matching lines; defaults to %d.",
						defaultSearchResults,
					),
				},
			},
			"required":             []string{"query"},
			"additionalProperties": false,
		},
	}
}

func (tool searchTool) Call(
	ctx context.Context,
	raw json.RawMessage,
) (sdk.ToolResult, error) {
	var arguments searchArguments
	if err := decodeArguments(raw, &arguments); err != nil {
		return toolFailure(err), nil
	}
	if arguments.Path == "" {
		arguments.Path = "."
	}
	if arguments.Query == "" {
		return toolFailure(errors.New("query must not be empty")), nil
	}
	maxResults := defaultSearchResults
	if arguments.MaxResults != nil {
		maxResults = *arguments.MaxResults
	} else if maxResults > tool.filesystem.maxEntries {
		maxResults = tool.filesystem.maxEntries
	}
	if maxResults < 1 || maxResults > tool.filesystem.maxEntries {
		return toolFailure(fmt.Errorf(
			"max_results must be between 1 and %d", tool.filesystem.maxEntries,
		)), nil
	}
	caseSensitive := true
	if arguments.CaseSensitive != nil {
		caseSensitive = *arguments.CaseSensitive
	}
	matcher, err := newTextMatcher(arguments.Query, arguments.Regex, caseSensitive)
	if err != nil {
		return toolFailure(fmt.Errorf("compile query: %w", err)), nil
	}
	glob, err := compileFileGlob(arguments.Glob)
	if err != nil {
		return toolFailure(fmt.Errorf("compile glob: %w", err)), nil
	}
	root, err := tool.filesystem.existing(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}

	matches := make([]searchMatch, 0, maxResults)
	filesScanned := 0
	filesSkipped := 0
	truncated := false
	searchFile := func(path string) error {
		if filesScanned >= tool.filesystem.maxEntries {
			truncated = true
			return errSearchTruncated
		}
		relative, err := filepath.Rel(tool.filesystem.root, path)
		if err != nil {
			return err
		}
		display := filepath.ToSlash(relative)
		if glob != nil && !glob.matches(display) {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			filesSkipped++
			return nil
		}
		if info.Size() > tool.filesystem.maxReadBytes {
			filesSkipped++
			return nil
		}
		data, _, err := tool.filesystem.readText(path)
		if err != nil {
			filesSkipped++
			return nil
		}
		filesScanned++
		for lineIndex, line := range splitTextLines(string(data)) {
			if err := ctx.Err(); err != nil {
				return err
			}
			byteColumn := matcher.find(line)
			if byteColumn < 0 {
				continue
			}
			matches = append(matches, searchMatch{
				path:   display,
				line:   lineIndex + 1,
				column: utf8.RuneCountInString(line[:byteColumn]) + 1,
				text:   truncateRunes(strings.TrimSpace(line), maxSearchLineRunes),
			})
			if len(matches) >= maxResults {
				truncated = true
				return errSearchTruncated
			}
		}
		return nil
	}

	info, err := os.Stat(root)
	if err != nil {
		return toolFailure(err), nil
	}
	if info.Mode().IsRegular() {
		err = searchFile(root)
	} else if !info.IsDir() {
		return toolFailure(errors.New("search path is not a regular file or directory")), nil
	} else {
		err = filepath.WalkDir(root, func(
			path string,
			entry fs.DirEntry,
			walkErr error,
		) error {
			if walkErr != nil {
				filesSkipped++
				if entry != nil && entry.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if path != root && !arguments.IncludeHidden &&
				strings.HasPrefix(entry.Name(), ".") {
				if entry.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				if entry.IsDir() {
					return fs.SkipDir
				}
				filesSkipped++
				return nil
			}
			if entry.IsDir() {
				return nil
			}
			return searchFile(path)
		})
	}
	if err != nil && !errors.Is(err, errSearchTruncated) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return sdk.ToolResult{}, err
		}
		return toolFailure(err), nil
	}

	var output strings.Builder
	fmt.Fprintf(&output, "query: %q\n", arguments.Query)
	fmt.Fprintf(&output, "path: %q\n", cleanDisplayPath(arguments.Path))
	fmt.Fprintf(&output, "matches: %d\n", len(matches))
	fmt.Fprintf(&output, "files_scanned: %d\n", filesScanned)
	fmt.Fprintf(&output, "files_skipped: %d\n", filesSkipped)
	fmt.Fprintf(&output, "truncated: %t\n---", truncated)
	for _, match := range matches {
		fmt.Fprintf(
			&output,
			"\n%s:%d:%d: %s",
			match.path,
			match.line,
			match.column,
			match.text,
		)
	}
	return sdk.ToolResult{Content: output.String()}, nil
}

type textMatcher struct {
	literal    string
	expression *regexp.Regexp
}

func newTextMatcher(
	query string,
	regexMode bool,
	caseSensitive bool,
) (textMatcher, error) {
	if regexMode {
		if !caseSensitive {
			query = "(?i:" + query + ")"
		}
		expression, err := regexp.Compile(query)
		if err != nil {
			return textMatcher{}, err
		}
		return textMatcher{expression: expression}, nil
	}
	if !caseSensitive {
		expression, err := regexp.Compile("(?i:" + regexp.QuoteMeta(query) + ")")
		if err != nil {
			return textMatcher{}, err
		}
		return textMatcher{expression: expression}, nil
	}
	return textMatcher{literal: query}, nil
}

func (matcher textMatcher) find(line string) int {
	if matcher.expression != nil {
		location := matcher.expression.FindStringIndex(line)
		if location == nil {
			return -1
		}
		return location[0]
	}
	return strings.Index(line, matcher.literal)
}

type fileGlob struct {
	expression *regexp.Regexp
	matchBase  bool
}

func compileFileGlob(pattern string) (*fileGlob, error) {
	pattern = strings.TrimSpace(filepath.ToSlash(pattern))
	if pattern == "" {
		return nil, nil
	}
	var expression strings.Builder
	expression.WriteString("^")
	for index := 0; index < len(pattern); {
		switch pattern[index] {
		case '*':
			if index+1 < len(pattern) && pattern[index+1] == '*' {
				index += 2
				if index < len(pattern) && pattern[index] == '/' {
					expression.WriteString("(?:.*/)?")
					index++
				} else {
					expression.WriteString(".*")
				}
			} else {
				expression.WriteString("[^/]*")
				index++
			}
		case '?':
			expression.WriteString("[^/]")
			index++
		case '[', ']':
			return nil, errors.New("character classes are not supported; use *, **, or ?")
		default:
			character, size := utf8.DecodeRuneInString(pattern[index:])
			expression.WriteString(regexp.QuoteMeta(string(character)))
			index += size
		}
	}
	expression.WriteString("$")
	compiled, err := regexp.Compile(expression.String())
	if err != nil {
		return nil, err
	}
	return &fileGlob{
		expression: compiled,
		matchBase:  !strings.Contains(pattern, "/"),
	}, nil
}

func (glob *fileGlob) matches(relative string) bool {
	if glob.expression.MatchString(relative) {
		return true
	}
	return glob.matchBase && glob.expression.MatchString(filepath.Base(relative))
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "…"
}
