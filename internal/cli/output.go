package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"charm.land/glamour/v2"
	"charm.land/glamour/v2/styles"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	"github.com/spf13/cobra"
)

const (
	outputText = "text"
	outputJSON = "json"

	defaultMarkdownWidth = 80
)

type runOutput struct {
	SessionID string              `json:"session_id"`
	Result    agentruntime.Result `json:"result"`
}

type cliErrorOutput struct {
	Error cliErrorDetail `json:"error"`
}

type cliErrorDetail struct {
	Type       string `json:"type"`
	Message    string `json:"message"`
	ExitCode   int    `json:"exit_code"`
	Retryable  bool   `json:"retryable"`
	Suggestion string `json:"suggestion,omitempty"`
}

func (application *app) validateGlobalFlags() error {
	switch application.output {
	case outputText, outputJSON:
	default:
		return usageError{fmt.Errorf(
			`--output must be %q or %q`, outputText, outputJSON,
		)}
	}
	switch application.progress {
	case progressAuto, progressAlways, progressPlain, progressTUI, progressNever:
	default:
		return usageError{fmt.Errorf(
			`--progress must be %q, %q, %q, %q, or %q`,
			progressAuto,
			progressTUI,
			progressPlain,
			progressAlways,
			progressNever,
		)}
	}
	switch application.color {
	case colorAuto, colorAlways, colorNever:
		return nil
	default:
		return usageError{fmt.Errorf(
			`--color must be %q, %q, or %q`,
			colorAuto,
			colorAlways,
			colorNever,
		)}
	}
}

func selectedOutput(command *cobra.Command) string {
	flag := command.Root().PersistentFlags().Lookup("output")
	if flag == nil {
		return outputText
	}
	return flag.Value.String()
}

func requestedOutput(
	args []string,
	fallback string,
	command *cobra.Command,
) string {
	selected := fallback
	target := command
	if command != nil {
		target, _, _ = command.Root().Find(args)
	}
	for index := 0; index < len(args); index++ {
		argument := args[index]
		switch {
		case argument == "--":
			return selected
		case argument == "-o" || argument == "--output":
			if index+1 < len(args) {
				selected = args[index+1]
				index++
			}
		case strings.HasPrefix(argument, "--output="):
			selected = strings.TrimPrefix(argument, "--output=")
		case strings.HasPrefix(argument, "-o="):
			selected = strings.TrimPrefix(argument, "-o=")
		case strings.HasPrefix(argument, "-o") && len(argument) > len("-o"):
			selected = strings.TrimPrefix(argument, "-o")
		case flagConsumesNext(target, argument):
			index++
		}
	}
	return selected
}

func flagConsumesNext(command *cobra.Command, argument string) bool {
	if command == nil || strings.Contains(argument, "=") {
		return false
	}
	var name, shorthand string
	switch {
	case strings.HasPrefix(argument, "--") && len(argument) > len("--"):
		name = strings.TrimPrefix(argument, "--")
	case strings.HasPrefix(argument, "-") && len(argument) == len("-x"):
		shorthand = strings.TrimPrefix(argument, "-")
	default:
		return false
	}
	if name != "" {
		flag := command.Flag(name)
		return flag != nil && flag.NoOptDefVal == ""
	}
	command.InheritedFlags()
	flag := command.Flags().ShorthandLookup(shorthand)
	return flag != nil && flag.NoOptDefVal == ""
}

func writeCLIError(
	writer io.Writer,
	_ *cobra.Command,
	err error,
	exitCode int,
) error {
	detail := cliErrorDetail{
		Type:      "runtime",
		Message:   err.Error(),
		ExitCode:  exitCode,
		Retryable: false,
	}
	var usage usageError
	switch {
	case errors.As(err, &usage):
		detail.Type = "usage"
		detail.Suggestion = suggestionForError(err)
		if detail.Suggestion == "" {
			detail.Suggestion = "Run 'ag --help' or 'ag <command> --help' for valid arguments."
		}
	case errors.Is(err, errUserCanceled), errors.Is(err, context.Canceled):
		detail.Type = "cancelled"
		detail.Suggestion = suggestionForError(err)
		if detail.Suggestion == "" {
			detail.Suggestion = "Run the command again when ready."
		}
	default:
		detail.Suggestion = suggestionForError(err)
	}
	return writeJSON(writer, cliErrorOutput{Error: detail})
}

func (application *app) render(machine any, human func(io.Writer) error) error {
	if application.output == outputJSON {
		return writeJSON(application.stdout, machine)
	}
	return human(application.stdout)
}

func (application *app) writeRun(sessionID string, result agentruntime.Result) error {
	value := runOutput{SessionID: sessionID, Result: result}
	return application.render(value, func(writer io.Writer) error {
		if result.Output != "" {
			if err := application.writeHumanContent(writer, result.Output); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(writer); err != nil {
				return err
			}
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Session:\t%s\n", tableCell(sessionID))
		fmt.Fprintf(table, "Turns:\t%d\n", result.Turns)
		fmt.Fprintf(table, "Tool calls:\t%d\n", result.ToolCalls)
		fmt.Fprintf(table, "Tokens:\t%d in / %d out\n", result.InputTokens, result.OutputTokens)
		fmt.Fprintf(
			table,
			"Cause:\t%s\n",
			tableCell(emptyAs(result.Cause.Code, "unknown")),
		)
		return table.Flush()
	})
}

func (application *app) writeHumanContent(writer io.Writer, value string) error {
	content := humanContent(value)
	if looksLikeMarkdown(content) {
		content = renderMarkdownContent(writer, content)
	}
	if err := application.writeStyledContent(writer, content); err != nil {
		return err
	}
	if !strings.HasSuffix(content, "\n") {
		_, err := fmt.Fprintln(writer)
		return err
	}
	return nil
}

func (application *app) writeStyledContent(writer io.Writer, content string) error {
	switch application.color {
	case colorAlways:
		_, err := (&colorprofile.Writer{
			Forward: writer,
			Profile: colorprofile.TrueColor,
		}).WriteString(content)
		return err
	case colorNever:
		_, err := (&colorprofile.Writer{
			Forward: writer,
			Profile: colorprofile.NoTTY,
		}).WriteString(content)
		return err
	default:
		_, err := lipgloss.Fprint(writer, content)
		return err
	}
}

func renderMarkdownContent(writer io.Writer, value string) string {
	width := terminalWidth(writer)
	if width <= 0 {
		width = defaultMarkdownWidth
	}
	style := styles.DarkStyleConfig
	style.H2.Prefix = ""
	style.H3.Prefix = ""
	style.H4.Prefix = ""
	style.H5.Prefix = ""
	style.H6.Prefix = ""
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStyles(style),
		glamour.WithWordWrap(width),
	)
	if err != nil {
		return value
	}
	rendered, err := renderer.Render(value)
	if err != nil {
		return value
	}
	return strings.Trim(rendered, "\n")
}

func looksLikeMarkdown(value string) bool {
	scanner := bufio.NewScanner(strings.NewReader(value))
	for scanner.Scan() {
		if markdownLine(scanner.Text()) {
			return true
		}
	}
	return strings.Contains(value, "**") ||
		strings.Contains(value, "__") ||
		strings.Contains(value, "`") ||
		(strings.Contains(value, "[") && strings.Contains(value, "]("))
}

func markdownLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}
	switch {
	case strings.HasPrefix(trimmed, "```"),
		strings.HasPrefix(trimmed, "~~~"),
		strings.HasPrefix(trimmed, "> "):
		return true
	case strings.HasPrefix(trimmed, "- "),
		strings.HasPrefix(trimmed, "* "),
		strings.HasPrefix(trimmed, "+ "):
		return true
	case strings.Contains(trimmed, "|") && strings.Contains(trimmed, "---"):
		return true
	}
	if strings.HasPrefix(trimmed, "#") {
		trimmed = strings.TrimLeft(trimmed, "#")
		return strings.HasPrefix(trimmed, " ")
	}
	return orderedMarkdownLine(trimmed)
}

func orderedMarkdownLine(line string) bool {
	index := 0
	for index < len(line) && line[index] >= '0' && line[index] <= '9' {
		index++
	}
	return index > 0 &&
		index+1 < len(line) &&
		(line[index] == '.' || line[index] == ')') &&
		line[index+1] == ' '
}

func (application *app) writePath(path string) error {
	return application.render(map[string]string{"path": path}, func(writer io.Writer) error {
		_, err := fmt.Fprintln(writer, tableCell(path))
		return err
	})
}

func (application *app) writeVersion() error {
	return application.render(
		map[string]string{"version": application.version},
		func(writer io.Writer) error {
			_, err := fmt.Fprintln(writer, tableCell(application.version))
			return err
		},
	)
}

func writeSection(writer io.Writer, title string, rows ...[2]string) error {
	if _, err := fmt.Fprintf(writer, "%s\n", title); err != nil {
		return err
	}
	table := newTable(writer)
	for _, row := range rows {
		fmt.Fprintf(table, "  %s:\t%s\n", tableCell(row[0]), tableCell(row[1]))
	}
	if err := table.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintln(writer)
	return err
}

func newTable(writer io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func emptyAs(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	cells := make([]string, len(values))
	for index, value := range values {
		cells[index] = tableCell(value)
	}
	return strings.Join(cells, ", ")
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "-"
	}
	keys := slices.Sorted(maps.Keys(labels))
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+labels[key])
	}
	return strings.Join(values, ",")
}

func tableCell(value string) string {
	var output strings.Builder
	for _, character := range value {
		switch character {
		case '\t', '\r', '\n':
			output.WriteByte(' ')
		case unicode.ReplacementChar:
			output.WriteRune(character)
		default:
			if unicode.IsControl(character) {
				fmt.Fprintf(&output, "\\u%04x", character)
			} else {
				output.WriteRune(character)
			}
		}
	}
	return output.String()
}

func humanContent(value string) string {
	var output strings.Builder
	for _, character := range value {
		switch character {
		case '\n', '\t':
			output.WriteRune(character)
		case '\r':
			output.WriteString("\\r")
		default:
			if unicode.IsControl(character) {
				fmt.Fprintf(&output, "\\u%04x", character)
			} else {
				output.WriteRune(character)
			}
		}
	}
	return output.String()
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.Format(time.RFC3339)
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
