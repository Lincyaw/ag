package gateway

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultTrajectoryEntryPageSize = 500
	maxTrajectoryEntryPageSize     = 1000
	trajectoryEntryPageBytes       = 4 << 20
)

type TrajectoryEntrySummary struct {
	ID             string                    `json:"id"`
	ParentID       string                    `json:"parent_id,omitempty"`
	Ordinal        uint64                    `json:"ordinal"`
	Depth          uint64                    `json:"depth"`
	Kind           sdk.TrajectoryKind        `json:"kind"`
	Timestamp      time.Time                 `json:"timestamp"`
	Generation     uint64                    `json:"generation,omitempty"`
	Fields         sdk.TrajectoryEntryFields `json:"fields"`
	PayloadVersion uint32                    `json:"payload_version"`
	PayloadBytes   int                       `json:"payload_bytes"`
	AuditCount     int                       `json:"audit_count,omitempty"`
	AttributeCount int                       `json:"attribute_count,omitempty"`
}

type TrajectoryInspection struct {
	SchemaVersion uint32                   `json:"schema_version"`
	ID            string                   `json:"id"`
	ParentID      string                   `json:"parent_id,omitempty"`
	ParentEntryID string                   `json:"parent_entry_id,omitempty"`
	CreatedAt     time.Time                `json:"created_at"`
	UpdatedAt     time.Time                `json:"updated_at"`
	Head          string                   `json:"head,omitempty"`
	Checkpoint    string                   `json:"checkpoint,omitempty"`
	Execution     *sdk.TrajectoryExecution `json:"execution,omitempty"`
	EntryCount    int                      `json:"entry_count"`
	Entries       []TrajectoryEntrySummary `json:"entries"`
}

type TrajectoryEntryQuery struct {
	After uint64 `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type TrajectoryEntryPage struct {
	Trajectory TrajectoryInspection     `json:"trajectory"`
	Items      []TrajectoryEntrySummary `json:"items"`
	Next       uint64                   `json:"next,omitempty"`
}

func projectTrajectoryEntryPage(
	trajectory sdk.Trajectory,
	query TrajectoryEntryQuery,
) (TrajectoryEntryPage, error) {
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		return TrajectoryEntryPage{}, err
	}
	entries := make([]sdk.TrajectoryEntryInspection, 0, len(branch))
	for _, entry := range branch {
		entries = append(entries, sdk.TrajectoryEntryInspection{
			ID:             entry.ID,
			TrajectoryID:   entry.TrajectoryID,
			ParentID:       entry.ParentID,
			Ordinal:        entry.Ordinal,
			Depth:          entry.Depth,
			Kind:           entry.Kind,
			Timestamp:      entry.Timestamp,
			Generation:     entry.Generation,
			Fields:         entry.Fields,
			PayloadVersion: entry.PayloadVersion,
			PayloadBytes:   len(entry.Payload),
			AuditCount:     len(entry.Audit),
			AttributeCount: len(entry.Attributes),
		})
	}
	return projectInspectedTrajectoryEntryPage(
		sdk.TrajectoryMetadata{
			SchemaVersion: trajectory.SchemaVersion,
			ID:            trajectory.ID,
			ParentID:      trajectory.ParentID,
			ParentEntryID: trajectory.ParentEntryID,
			CreatedAt:     trajectory.CreatedAt,
			UpdatedAt:     trajectory.UpdatedAt,
			Head:          trajectory.Head,
			Checkpoint:    trajectory.Checkpoint,
			Execution:     sdk.CloneTrajectoryExecution(trajectory.Execution),
			EntryCount:    len(branch),
		},
		entries,
		query,
	)
}

func projectInspectedTrajectoryEntryPage(
	metadata sdk.TrajectoryMetadata,
	entries []sdk.TrajectoryEntryInspection,
	query TrajectoryEntryQuery,
) (TrajectoryEntryPage, error) {
	query, err := normalizeTrajectoryEntryQuery(query)
	if err != nil {
		return TrajectoryEntryPage{}, err
	}
	page := TrajectoryEntryPage{
		Trajectory: TrajectoryInspection{
			SchemaVersion: metadata.SchemaVersion,
			ID:            metadata.ID,
			ParentID:      metadata.ParentID,
			ParentEntryID: metadata.ParentEntryID,
			CreatedAt:     metadata.CreatedAt,
			UpdatedAt:     metadata.UpdatedAt,
			Head:          metadata.Head,
			Checkpoint:    metadata.Checkpoint,
			Execution:     sdk.CloneTrajectoryExecution(metadata.Execution),
			EntryCount:    len(entries),
		},
		Items: make([]TrajectoryEntrySummary, 0, query.Limit),
	}
	if query.After >= uint64(len(entries)) {
		return page, nil
	}
	pageBytes := 0
	for index := int(query.After); index < len(entries); index++ {
		if len(page.Items) >= query.Limit {
			break
		}
		item := summarizeInspectedTrajectoryEntry(entries[index])
		encoded, err := json.Marshal(item)
		if err != nil {
			return TrajectoryEntryPage{}, err
		}
		if len(page.Items) > 0 && pageBytes+len(encoded) > trajectoryEntryPageBytes {
			break
		}
		page.Items = append(page.Items, item)
		pageBytes += len(encoded)
	}
	end := query.After + uint64(len(page.Items))
	if end < uint64(len(entries)) {
		page.Next = end
	}
	return page, nil
}

func summarizeInspectedTrajectoryEntry(
	entry sdk.TrajectoryEntryInspection,
) TrajectoryEntrySummary {
	return TrajectoryEntrySummary{
		ID:             entry.ID,
		ParentID:       entry.ParentID,
		Ordinal:        entry.Ordinal,
		Depth:          entry.Depth,
		Kind:           entry.Kind,
		Timestamp:      entry.Timestamp,
		Generation:     entry.Generation,
		Fields:         entry.Fields,
		PayloadVersion: entry.PayloadVersion,
		PayloadBytes:   entry.PayloadBytes,
		AuditCount:     entry.AuditCount,
		AttributeCount: entry.AttributeCount,
	}
}

func normalizeTrajectoryEntryQuery(
	query TrajectoryEntryQuery,
) (TrajectoryEntryQuery, error) {
	if query.Limit == 0 {
		query.Limit = defaultTrajectoryEntryPageSize
	}
	if query.Limit < 1 || query.Limit > maxTrajectoryEntryPageSize {
		return TrajectoryEntryQuery{}, errors.New(
			"trajectory entry page limit must be between 1 and 1000",
		)
	}
	return query, nil
}

func summarizeTrajectoryEntry(entry sdk.TrajectoryEntry) TrajectoryEntrySummary {
	return TrajectoryEntrySummary{
		ID:             entry.ID,
		ParentID:       entry.ParentID,
		Ordinal:        entry.Ordinal,
		Depth:          entry.Depth,
		Kind:           entry.Kind,
		Timestamp:      entry.Timestamp,
		Generation:     entry.Generation,
		Fields:         entry.Fields,
		PayloadVersion: entry.PayloadVersion,
		PayloadBytes:   len(entry.Payload),
		AuditCount:     len(entry.Audit),
		AttributeCount: len(entry.Attributes),
	}
}
