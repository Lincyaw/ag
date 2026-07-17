package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	"github.com/spf13/cobra"
)

const (
	outputText = "text"
	outputJSON = "json"
)

type runOutput struct {
	SessionID string              `json:"session_id"`
	Result    agentruntime.Result `json:"result"`
}

type configOutput struct {
	File   string           `json:"file"`
	Config appconfig.Config `json:"config"`
}

type rollbackOutput struct {
	TrajectoryID string `json:"trajectory_id"`
	Head         string `json:"head"`
	CheckpointID string `json:"checkpoint_id"`
}

type stateOutput struct {
	Backend      string                  `json:"backend"`
	Namespace    string                  `json:"namespace"`
	Capabilities sdk.StorageCapabilities `json:"capabilities"`
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

func (application *app) validateOutput() error {
	switch application.output {
	case outputText, outputJSON:
		return nil
	default:
		return usageError{fmt.Errorf(
			`--output must be %q or %q`, outputText, outputJSON,
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

func requestedOutput(args []string, fallback string) string {
	selected := fallback
	for index := 0; index < len(args); index++ {
		argument := args[index]
		switch {
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
		}
	}
	return selected
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
		detail.Suggestion = "Run 'ag --help' or 'ag <command> --help' for valid arguments."
	case errors.Is(err, context.Canceled):
		detail.Type = "cancelled"
		detail.Suggestion = "Run the command again when ready."
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
			if _, err := fmt.Fprintln(writer, humanContent(result.Output)); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(writer); err != nil {
				return err
			}
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Session:\t%s\n", sessionID)
		fmt.Fprintf(table, "Turns:\t%d\n", result.Turns)
		fmt.Fprintf(table, "Tool calls:\t%d\n", result.ToolCalls)
		fmt.Fprintf(table, "Cause:\t%s\n", emptyAs(result.Cause.Code, "unknown"))
		return table.Flush()
	})
}

func (application *app) writeConfig(loaded appconfig.Loaded) error {
	value := configOutput{File: loaded.File, Config: loaded.Config}
	return application.render(value, func(writer io.Writer) error {
		config := loaded.Config
		source := loaded.Path()
		if loaded.File == "" {
			source += " (defaults; file not present)"
		}
		if _, err := fmt.Fprintf(writer, "Effective configuration\n\n"); err != nil {
			return err
		}
		if err := writeSection(writer, "Source",
			[2]string{"File", source},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Agent",
			[2]string{"Provider", config.Agent.Provider},
			[2]string{"Max turns", fmt.Sprint(config.Agent.MaxTurns)},
			[2]string{"Timeout", config.Agent.Timeout.String()},
			[2]string{"System", fmt.Sprintf("%q", config.Agent.System)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "OpenAI",
			[2]string{"Enabled", yesNo(config.OpenAI.Enabled)},
			[2]string{"Model", config.OpenAI.Model},
			[2]string{"Base URL", emptyAs(config.OpenAI.BaseURL, "provider default")},
			[2]string{"Max retries", fmt.Sprint(config.OpenAI.MaxRetries)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Workspace",
			[2]string{"Enabled", yesNo(config.Workspace.Enabled)},
			[2]string{"Root", config.Workspace.Root},
			[2]string{"Writes", yesNo(config.Workspace.EnableWrite)},
			[2]string{"Max read bytes", fmt.Sprint(config.Workspace.MaxReadBytes)},
			[2]string{"Max write bytes", fmt.Sprint(config.Workspace.MaxWriteBytes)},
			[2]string{"Max entries", fmt.Sprint(config.Workspace.MaxEntries)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Bash",
			[2]string{"Enabled", yesNo(config.Bash.Enabled)},
			[2]string{"Shell", config.Bash.Shell},
			[2]string{"Default timeout", config.Bash.DefaultTimeout.String()},
			[2]string{"Max timeout", config.Bash.MaxTimeout.String()},
			[2]string{"Max output bytes", fmt.Sprint(config.Bash.MaxOutputBytes)},
			[2]string{"Environment", listOrNone(config.Bash.Environment)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Plugins",
			[2]string{"Remote", listOrNone(config.Plugins.Remote)},
			[2]string{"Registry", emptyAs(config.Plugins.RegistryURI, "none")},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "State",
			[2]string{"Backend URI", emptyAs(config.State.BackendURI, "file")},
			[2]string{"Directory", config.State.Directory},
			[2]string{"Namespace", emptyAs(config.State.Namespace, "default")},
		); err != nil {
			return err
		}
		return writeSection(writer, "Diagnostics",
			[2]string{"OpenTelemetry", yesNo(config.Observability.Enabled)},
			[2]string{"Log level", config.Logging.Level},
			[2]string{"Log format", config.Logging.Format},
		)
	})
}

func (application *app) writePath(path string) error {
	return application.render(map[string]string{"path": path}, func(writer io.Writer) error {
		_, err := fmt.Fprintln(writer, tableCell(path))
		return err
	})
}

func (application *app) writePlugins(descriptors []sdk.PluginDescriptor) error {
	return application.render(descriptors, func(writer io.Writer) error {
		if len(descriptors) == 0 {
			_, err := fmt.Fprintln(writer, "No plugins found.")
			return err
		}
		table := newTable(writer)
		fmt.Fprintln(table, "NAME\tSCHEME\tURI\tDESCRIPTION")
		for _, descriptor := range descriptors {
			fmt.Fprintf(
				table,
				"%s\t%s\t%s\t%s\n",
				tableCell(descriptor.Name),
				tableCell(descriptor.Scheme),
				tableCell(emptyAs(descriptor.URI, "-")),
				tableCell(emptyAs(descriptor.Description, "-")),
			)
		}
		return table.Flush()
	})
}

func (application *app) writeManifest(manifest sdk.Manifest) error {
	return application.render(manifest, func(writer io.Writer) error {
		minimumAPI, maximumAPI := manifest.APIRange()
		apiVersion := fmt.Sprint(minimumAPI)
		if minimumAPI != maximumAPI {
			apiVersion = fmt.Sprintf("%d-%d", minimumAPI, maximumAPI)
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Name:\t%s\n", tableCell(manifest.Name))
		fmt.Fprintf(table, "Version:\t%s\n", tableCell(manifest.Version))
		fmt.Fprintf(table, "Description:\t%s\n", tableCell(manifest.Description))
		fmt.Fprintf(table, "API version:\t%s\n", apiVersion)
		fmt.Fprintf(table, "Requires:\t%s\n", listOrNone(manifest.Requires))
		fmt.Fprintf(table, "Conflicts:\t%s\n", listOrNone(manifest.Conflicts))
		if err := table.Flush(); err != nil {
			return err
		}
		if len(manifest.Registers) == 0 {
			_, err := fmt.Fprintln(writer, "\nRegisters: none")
			return err
		}
		if _, err := fmt.Fprintln(writer, "\nRegisters:"); err != nil {
			return err
		}
		for _, resource := range manifest.Registers {
			if _, err := fmt.Fprintf(writer, "  - %s\n", tableCell(resource)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (application *app) writeTrajectoryList(
	trajectories []sdk.TrajectorySummary,
) error {
	return application.render(trajectories, func(writer io.Writer) error {
		if len(trajectories) == 0 {
			_, err := fmt.Fprintln(writer, "No trajectories found.")
			return err
		}
		table := newTable(writer)
		fmt.Fprintln(table, "ID\tENTRIES\tUPDATED\tHEAD")
		for _, trajectory := range trajectories {
			fmt.Fprintf(
				table,
				"%s\t%d\t%s\t%s\n",
				tableCell(trajectory.ID),
				trajectory.EntryCount,
				formatTime(trajectory.UpdatedAt),
				tableCell(emptyAs(trajectory.Head, "-")),
			)
		}
		return table.Flush()
	})
}

func (application *app) writeTrajectory(trajectory sdk.Trajectory) error {
	return application.render(trajectory, func(writer io.Writer) error {
		table := newTable(writer)
		fmt.Fprintf(table, "Trajectory:\t%s\n", tableCell(trajectory.ID))
		fmt.Fprintf(table, "Head:\t%s\n", tableCell(emptyAs(trajectory.Head, "-")))
		fmt.Fprintf(table, "Created:\t%s\n", formatTime(trajectory.CreatedAt))
		fmt.Fprintf(table, "Updated:\t%s\n", formatTime(trajectory.UpdatedAt))
		fmt.Fprintf(table, "Entries:\t%d\n", len(trajectory.Entries))
		if err := table.Flush(); err != nil {
			return err
		}
		if len(trajectory.Entries) == 0 {
			return nil
		}
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		entries := newTable(writer)
		fmt.Fprintln(entries, "TIME\tKIND\tID\tDETAIL")
		for _, entry := range trajectory.Entries {
			fmt.Fprintf(
				entries,
				"%s\t%s\t%s\t%s\n",
				formatTime(entry.Timestamp),
				tableCell(entry.Kind),
				tableCell(entry.ID),
				tableCell(trajectoryEntryDetail(entry)),
			)
		}
		return entries.Flush()
	})
}

func (application *app) writeRollback(value rollbackOutput) error {
	return application.render(value, func(writer io.Writer) error {
		if _, err := fmt.Fprintf(
			writer,
			"Rolled back trajectory %s\n\n",
			tableCell(value.TrajectoryID),
		); err != nil {
			return err
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Checkpoint:\t%s\n", tableCell(value.CheckpointID))
		fmt.Fprintf(table, "New head:\t%s\n", tableCell(value.Head))
		return table.Flush()
	})
}

func (application *app) writeState(value stateOutput) error {
	return application.render(value, func(writer io.Writer) error {
		table := newTable(writer)
		fmt.Fprintf(table, "Backend:\t%s\n", tableCell(value.Backend))
		fmt.Fprintf(table, "Namespace:\t%s\n", tableCell(emptyAs(value.Namespace, "default")))
		if err := table.Flush(); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer, "\nCapabilities:"); err != nil {
			return err
		}
		capabilities := newTable(writer)
		fmt.Fprintf(capabilities, "  Durable:\t%s\n", yesNo(value.Capabilities.Durable))
		fmt.Fprintf(capabilities, "  Multi-process safe:\t%s\n", yesNo(value.Capabilities.MultiProcessSafe))
		fmt.Fprintf(capabilities, "  Atomic state:\t%s\n", yesNo(value.Capabilities.AtomicState))
		fmt.Fprintf(capabilities, "  Pagination:\t%s\n", yesNo(value.Capabilities.Pagination))
		fmt.Fprintf(capabilities, "  Maintenance:\t%s\n", yesNo(value.Capabilities.Maintenance))
		fmt.Fprintf(capabilities, "  Operation fencing:\t%s\n", yesNo(value.Capabilities.OperationFencing))
		fmt.Fprintf(capabilities, "  Named queues:\t%s\n", yesNo(value.Capabilities.NamedQueues))
		fmt.Fprintf(capabilities, "  Namespace isolation:\t%s\n", yesNo(value.Capabilities.NamespaceIsolation))
		fmt.Fprintf(capabilities, "  Encrypted at rest:\t%s\n", yesNo(value.Capabilities.EncryptedAtRest))
		return capabilities.Flush()
	})
}

func (application *app) writePrune(result sdk.PruneResult) error {
	return application.render(result, func(writer io.Writer) error {
		if _, err := fmt.Fprintln(writer, "State pruning complete."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Operations deleted:\t%d\n", result.Operations)
		fmt.Fprintf(table, "Deliveries deleted:\t%d\n", result.Deliveries)
		fmt.Fprintf(table, "Trajectories deleted:\t%d\n", result.Trajectories)
		return table.Flush()
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

func trajectoryEntryDetail(entry sdk.TrajectoryEntry) string {
	var payload map[string]any
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return truncateText(string(entry.Payload), 100)
	}
	turn := numberField(payload, "turn")
	if call, ok := payload["call"].(map[string]any); ok {
		name, _ := call["name"].(string)
		detail := "tool=" + emptyAs(name, "unknown")
		if turn != "" {
			detail = "turn=" + turn + " " + detail
		}
		if result, ok := payload["result"].(map[string]any); ok {
			if failed, _ := result["is_error"].(bool); failed {
				detail += " status=error"
			} else {
				detail += " status=ok"
			}
		}
		return detail
	}
	if role, ok := payload["role"].(string); ok {
		content, _ := payload["content"].(string)
		return role + "=" + fmt.Sprintf("%q", truncateText(content, 80))
	}
	if provider, ok := payload["provider"].(string); ok {
		detail := "provider=" + provider
		if turn != "" {
			detail = "turn=" + turn + " " + detail
		}
		return detail
	}
	if cause, ok := payload["cause"].(map[string]any); ok {
		code, _ := cause["code"].(string)
		return "cause=" + emptyAs(code, "unknown")
	}
	if turns := numberField(payload, "turns"); turns != "" {
		return "turns=" + turns + " tools=" + emptyAs(numberField(payload, "tool_calls"), "0")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, entry.Payload); err != nil {
		return truncateText(string(entry.Payload), 100)
	}
	return truncateText(compact.String(), 100)
}

func numberField(value map[string]any, name string) string {
	number, ok := value[name].(float64)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%.0f", number)
}

func truncateText(value string, limit int) string {
	value = strings.ReplaceAll(value, "\n", " ")
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "…"
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
