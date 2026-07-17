package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type rollbackOutput struct {
	TrajectoryID string `json:"trajectory_id"`
	Head         string `json:"head"`
	CheckpointID string `json:"checkpoint_id"`
}

type rollbackPreviewOutput struct {
	TrajectoryID string `json:"trajectory_id"`
	CurrentHead  string `json:"current_head"`
	CheckpointID string `json:"checkpoint_id"`
	DryRun       bool   `json:"dry_run"`
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
				tableCell(string(entry.Kind)),
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

func (application *app) writeRollbackPreview(value rollbackPreviewOutput) error {
	return application.render(value, func(writer io.Writer) error {
		if _, err := fmt.Fprintf(
			writer,
			"Would roll back trajectory %s\n\n",
			tableCell(value.TrajectoryID),
		); err != nil {
			return err
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Checkpoint:\t%s\n", tableCell(value.CheckpointID))
		fmt.Fprintf(table, "Current head:\t%s\n", tableCell(emptyAs(value.CurrentHead, "-")))
		fmt.Fprintf(table, "Dry run:\tyes\n")
		return table.Flush()
	})
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
