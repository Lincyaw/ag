package storage

import (
	"fmt"
	"slices"
	"strings"

	sdk "github.com/lincyaw/ag/sdk"
)

func validateTrajectoryAnalysisQuery(
	query sdk.TrajectoryEntryQuery,
) (int, error) {
	if query.TrajectoryID != "" {
		if err := sdk.ValidateResourceName(
			"trajectory",
			query.TrajectoryID,
		); err != nil {
			return 0, err
		}
	}
	if query.Kind != "" {
		if err := validateTrajectoryKind(query.Kind); err != nil {
			return 0, err
		}
	}
	limit := query.Limit
	if limit == 0 {
		limit = sdk.DefaultPageSize
	}
	if limit < 0 || limit > sdk.MaxPageSize {
		return 0, fmt.Errorf(
			"trajectory analysis limit %d must be between 0 and %d",
			limit,
			sdk.MaxPageSize,
		)
	}
	return limit, nil
}

func trajectoryEntryMatchesQuery(
	entry sdk.TrajectoryEntry,
	query sdk.TrajectoryEntryQuery,
) bool {
	if query.TrajectoryID != "" && entry.TrajectoryID != query.TrajectoryID {
		return false
	}
	if query.ExecutionID != "" &&
		entry.Fields.ExecutionID != query.ExecutionID {
		return false
	}
	if query.OperationKey != "" &&
		entry.Fields.OperationKey != query.OperationKey {
		return false
	}
	if query.Kind != "" && entry.Kind != query.Kind {
		return false
	}
	if query.Provider != "" && entry.Fields.Provider != query.Provider {
		return false
	}
	if query.ToolName != "" && entry.Fields.ToolName != query.ToolName {
		return false
	}
	if query.CorrelationID != "" &&
		entry.Fields.CorrelationID != query.CorrelationID {
		return false
	}
	return true
}

func limitTrajectoryAnalysisEntries(
	entries []sdk.TrajectoryEntry,
	limit int,
) []sdk.TrajectoryEntry {
	slices.SortFunc(entries, compareTrajectoryAnalysisEntries)
	if len(entries) > limit {
		entries = entries[:limit]
	}
	result := make([]sdk.TrajectoryEntry, len(entries))
	for index, entry := range entries {
		result[index] = cloneTrajectoryEntry(entry)
	}
	return result
}

func compareTrajectoryAnalysisEntries(
	left sdk.TrajectoryEntry,
	right sdk.TrajectoryEntry,
) int {
	if order := left.Timestamp.Compare(right.Timestamp); order != 0 {
		return order
	}
	if order := strings.Compare(
		left.TrajectoryID,
		right.TrajectoryID,
	); order != 0 {
		return order
	}
	if left.Ordinal < right.Ordinal {
		return -1
	}
	if left.Ordinal > right.Ordinal {
		return 1
	}
	return strings.Compare(left.ID, right.ID)
}
