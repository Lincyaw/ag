package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	progressStatusRun    = "run"
	progressStatusModel  = "model"
	progressStatusPlan   = "plan"
	progressStatusTool   = "tool"
	progressStatusOK     = "ok"
	progressStatusError  = "error"
	progressStatusAnswer = "answer"
	progressStatusDone   = "done"
)

type progressRecord struct {
	EventName string
	At        time.Time
	Status    string
	Turn      int
	SessionID string
	Provider  string
	ToolName  string
	Task      string
	Label     string
	Detail    string
	Technical string
	Overview  bool
	Recent    bool
}

func progressDroppedRecord(count uint64) progressRecord {
	return progressRecord{
		At:        time.Now(),
		Label:     "Progress limited",
		Detail:    fmt.Sprintf("dropped %d update(s) while renderer was busy", count),
		Technical: fmt.Sprintf("progress_queue_dropped=%d", count),
		Overview:  true,
		Recent:    true,
	}
}

func progressRecordFromEvent(event sdk.Event) progressRecord {
	switch event.Name {
	case sdk.EventAgentStart:
		var payload sdk.AgentStartPayload
		_ = decodeProgressPayload(event, &payload)
		task := summarizeTask(payload.Messages)
		return progressRecord{
			Status:    progressStatusRun,
			SessionID: event.SessionID,
			Task:      task,
			Label:     "Starting",
			Detail:    emptyAs(task, "new session"),
			Technical: "session=" + emptyAs(event.SessionID, "new"),
			Overview:  true,
		}
	case sdk.EventTurnStart:
		return progressRecord{}
	case sdk.EventBeforeProvider:
		return progressRecord{}
	case sdk.EventAfterProvider:
		var payload sdk.AfterProviderPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		if payload.Error != "" {
			return progressRecord{
				Status:    progressStatusError,
				Turn:      payload.Turn + 1,
				Provider:  payload.Provider,
				Label:     "Model request failed",
				Detail:    summarizeText(payload.Error, 180),
				Technical: "provider=" + emptyAs(payload.Provider, "unknown"),
				Overview:  true,
				Recent:    true,
			}
		}
		if payload.Response == nil {
			return progressRecord{
				Status:    progressStatusModel,
				Turn:      payload.Turn + 1,
				Provider:  payload.Provider,
				Label:     "Thinking",
				Detail:    "model returned",
				Technical: "provider=" + emptyAs(payload.Provider, "unknown"),
			}
		}
		if len(payload.Response.ToolCalls) == 0 {
			return progressRecord{
				Status:    progressStatusAnswer,
				Turn:      payload.Turn + 1,
				Provider:  payload.Provider,
				Label:     "Answer ready",
				Detail:    summarizeAnswer(payload.Response),
				Technical: summarizeModelResponse(*payload.Response),
				Overview:  true,
				Recent:    true,
			}
		}
		detail := summarizeToolPlan(payload.Response.ToolCalls)
		if thought := summarizeText(payload.Response.Content, 80); thought != "" {
			detail = thought
		}
		return progressRecord{
			Status:    progressStatusPlan,
			Turn:      payload.Turn + 1,
			Provider:  payload.Provider,
			Label:     "Planning",
			Detail:    detail,
			Technical: summarizeToolCalls(payload.Response.ToolCalls),
			Overview:  true,
			Recent:    true,
		}
	case sdk.EventBeforeTool:
		var payload sdk.BeforeToolPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		label, detail, technical := summarizeToolStart(payload.Call)
		return progressRecord{
			Status:    progressStatusTool,
			Turn:      payload.Turn + 1,
			ToolName:  payload.Call.Name,
			Label:     label,
			Detail:    detail,
			Technical: technical,
			Overview:  true,
			Recent:    true,
		}
	case sdk.EventAfterTool:
		var payload sdk.AfterToolPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		status := progressStatusOK
		if payload.Result.IsError {
			status = progressStatusError
		}
		label, detail, technical := summarizeToolFinish(payload.Call, payload.Result)
		return progressRecord{
			Status:    status,
			Turn:      payload.Turn + 1,
			ToolName:  payload.Call.Name,
			Label:     label,
			Detail:    detail,
			Technical: technical,
			Overview:  true,
			Recent:    true,
		}
	case sdk.EventToolError:
		var payload sdk.ToolErrorPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		return progressRecord{
			Status:    progressStatusError,
			Turn:      payload.Turn + 1,
			ToolName:  payload.Call.Name,
			Label:     "Tool failed",
			Detail:    summarizeText(payload.Reason, 180),
			Technical: "tool=" + emptyAs(payload.Call.Name, "unknown"),
			Overview:  true,
			Recent:    true,
		}
	case sdk.EventAgentEnd:
		var payload sdk.AgentEndPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		return progressRecord{
			Status:    progressStatusDone,
			SessionID: event.SessionID,
			Label:     "Done",
			Detail:    emptyAs(payload.Cause.Code, "unknown"),
			Technical: "session=" + emptyAs(event.SessionID, "unknown"),
		}
	default:
		return progressRecord{}
	}
}

func decodeProgressPayload(event sdk.Event, target any) error {
	return json.Unmarshal(event.Payload, target)
}

func (record progressRecord) line() string {
	var parts []string
	if record.Turn > 0 {
		parts = append(parts, fmt.Sprintf("turn=%d", record.Turn))
	}
	if display := record.display(); display != "" {
		parts = append(parts, display)
	}
	if record.Technical != "" {
		parts = append(parts, record.Technical)
	}
	return strings.Join(parts, "  ")
}

func (record progressRecord) display() string {
	switch {
	case record.Label == "":
		return record.Detail
	case record.Detail == "":
		return record.Label
	default:
		return record.Label + " - " + record.Detail
	}
}

type progressRecordMsg progressRecord
type progressDoneMsg struct{}
