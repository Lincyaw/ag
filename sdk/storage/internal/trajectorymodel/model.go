package trajectorymodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sdk "github.com/lincyaw/ag/sdk"
)

type EntryLookup func(string) (sdk.TrajectoryEntry, bool, error)

type PreparedNewTrajectory struct {
	Trajectory          sdk.Trajectory
	InheritedEntryCount uint64
}

func ValidateNewTrajectory(trajectory sdk.Trajectory) error {
	if trajectory.SchemaVersion > sdk.TrajectorySchemaVersion {
		return fmt.Errorf(
			"%w: got %d, maximum supported is %d",
			sdk.ErrTrajectoryVersion,
			trajectory.SchemaVersion,
			sdk.TrajectorySchemaVersion,
		)
	}
	if err := sdk.ValidateResourceName("trajectory", trajectory.ID); err != nil {
		return err
	}
	if trajectory.Head != "" ||
		trajectory.Checkpoint != "" ||
		trajectory.Execution != nil ||
		len(trajectory.Entries) != 0 {
		return errors.New(
			"new trajectory must not contain entries, a head, a checkpoint, or an execution",
		)
	}
	return ValidateTrajectoryParent(trajectory)
}

func PrepareNewTrajectory(
	trajectory sdk.Trajectory,
	now time.Time,
) (PreparedNewTrajectory, error) {
	NormalizeTrajectory(&trajectory)
	if err := ValidateNewTrajectory(trajectory); err != nil {
		return PreparedNewTrajectory{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if trajectory.CreatedAt.IsZero() {
		trajectory.CreatedAt = now
	} else {
		trajectory.CreatedAt = trajectory.CreatedAt.UTC()
	}
	if trajectory.UpdatedAt.IsZero() {
		trajectory.UpdatedAt = trajectory.CreatedAt
	} else {
		trajectory.UpdatedAt = trajectory.UpdatedAt.UTC()
	}
	trajectory.Entries = []sdk.TrajectoryEntry{}
	return PreparedNewTrajectory{Trajectory: trajectory}, nil
}

func PrepareNewTrajectoryFork(
	prepared PreparedNewTrajectory,
	inheritedBranch []sdk.TrajectoryEntry,
) (PreparedNewTrajectory, error) {
	trajectory := prepared.Trajectory
	if trajectory.ParentID == "" {
		return prepared, nil
	}
	if len(inheritedBranch) == 0 {
		return PreparedNewTrajectory{}, fmt.Errorf(
			"trajectory %q fork branch is empty",
			trajectory.ID,
		)
	}
	prepared.InheritedEntryCount = uint64(len(inheritedBranch))
	trajectory.Head = trajectory.ParentEntryID
	if checkpoint, found := FindLatestInBranch(
		inheritedBranch,
		sdk.TrajectoryKindCheckpoint,
	); found {
		trajectory.Checkpoint = checkpoint.ID
	}
	prepared.Trajectory = trajectory
	return prepared, nil
}

func ValidateTrajectoryParent(trajectory sdk.Trajectory) error {
	if (trajectory.ParentID == "") != (trajectory.ParentEntryID == "") {
		return errors.New(
			"trajectory parent_id and parent_entry_id must be set together",
		)
	}
	if trajectory.ParentID == "" {
		return nil
	}
	if err := sdk.ValidateResourceName(
		"trajectory parent",
		trajectory.ParentID,
	); err != nil {
		return err
	}
	if err := sdk.ValidateResourceName(
		"trajectory parent entry",
		trajectory.ParentEntryID,
	); err != nil {
		return err
	}
	return nil
}

func PrepareTrajectoryEntries(
	trajectoryID string,
	nextOrdinal uint64,
	hasHistory bool,
	input []sdk.TrajectoryEntry,
	lookup EntryLookup,
) ([]sdk.TrajectoryEntry, error) {
	if len(input) == 0 {
		return nil, errors.New("trajectory append contains no entries")
	}
	entries := make([]sdk.TrajectoryEntry, len(input))
	prepared := make(map[string]sdk.TrajectoryEntry, len(input))
	for index, source := range input {
		entry := CloneTrajectoryEntry(source)
		if err := sdk.ValidateResourceName("trajectory entry", entry.ID); err != nil {
			return nil, err
		}
		if _, duplicate := prepared[entry.ID]; duplicate {
			return nil, fmt.Errorf("trajectory entry %q already exists", entry.ID)
		}
		if _, duplicate, err := lookup(entry.ID); err != nil {
			return nil, err
		} else if duplicate {
			return nil, fmt.Errorf("trajectory entry %q already exists", entry.ID)
		}
		if !entry.Kind.Valid() {
			return nil, fmt.Errorf(
				"trajectory entry %q has unsupported kind %q",
				entry.ID,
				entry.Kind,
			)
		}
		if err := ValidateTrajectoryEntryFields(entry); err != nil {
			return nil, err
		}
		if !json.Valid(entry.Payload) {
			return nil, fmt.Errorf(
				"trajectory entry %q payload is invalid JSON",
				entry.ID,
			)
		}

		var parent sdk.TrajectoryEntry
		if entry.ParentID == "" {
			if (hasHistory || len(prepared) != 0) &&
				entry.Kind != sdk.TrajectoryKindRestore {
				return nil, fmt.Errorf(
					"trajectory entry %q has no parent in a non-empty trajectory",
					entry.ID,
				)
			}
		} else if candidate, exists := prepared[entry.ParentID]; exists {
			parent = candidate
		} else {
			var exists bool
			var err error
			parent, exists, err = lookup(entry.ParentID)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, fmt.Errorf(
					"trajectory entry %q has unknown parent %q",
					entry.ID,
					entry.ParentID,
				)
			}
		}

		if entry.TrajectoryID == "" {
			entry.TrajectoryID = trajectoryID
		} else if entry.TrajectoryID != trajectoryID {
			return nil, fmt.Errorf(
				"trajectory entry %q belongs to trajectory %q, not %q",
				entry.ID,
				entry.TrajectoryID,
				trajectoryID,
			)
		}
		nextOrdinal++
		if entry.Ordinal == 0 {
			entry.Ordinal = nextOrdinal
		} else if entry.Ordinal != nextOrdinal {
			return nil, fmt.Errorf(
				"trajectory entry %q has ordinal %d, expected %d",
				entry.ID,
				entry.Ordinal,
				nextOrdinal,
			)
		}
		expectedDepth := uint64(1)
		if entry.ParentID != "" {
			expectedDepth = parent.Depth + 1
		}
		if entry.Depth == 0 {
			entry.Depth = expectedDepth
		} else if entry.Depth != expectedDepth {
			return nil, fmt.Errorf(
				"trajectory entry %q has depth %d, expected %d",
				entry.ID,
				entry.Depth,
				expectedDepth,
			)
		}
		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now().UTC()
		}
		if entry.PayloadVersion == 0 {
			entry.PayloadVersion = sdk.TrajectoryPayloadVersion
		}
		if entry.PayloadVersion > sdk.TrajectoryPayloadVersion {
			return nil, fmt.Errorf(
				"%w: entry %q payload version %d, maximum supported is %d",
				sdk.ErrTrajectoryVersion,
				entry.ID,
				entry.PayloadVersion,
				sdk.TrajectoryPayloadVersion,
			)
		}
		prepared[entry.ID] = entry
		entries[index] = entry
	}
	return entries, nil
}

func TrajectoryMetadata(
	trajectory sdk.Trajectory,
	entryCount int,
	ownedEntryCount int,
) sdk.TrajectoryMetadata {
	return sdk.TrajectoryMetadata{
		SchemaVersion:   trajectory.SchemaVersion,
		ID:              trajectory.ID,
		ParentID:        trajectory.ParentID,
		ParentEntryID:   trajectory.ParentEntryID,
		CreatedAt:       trajectory.CreatedAt,
		UpdatedAt:       trajectory.UpdatedAt,
		Head:            trajectory.Head,
		Checkpoint:      trajectory.Checkpoint,
		Execution:       CloneTrajectoryExecution(trajectory.Execution),
		Environment:     CloneTrajectoryEnvironment(trajectory.Environment),
		EntryCount:      entryCount,
		OwnedEntryCount: ownedEntryCount,
	}
}

func SummarizeTrajectory(
	trajectory sdk.Trajectory,
	entryCount int,
	ownedEntryCount int,
) sdk.TrajectorySummary {
	summary := sdk.TrajectorySummary{
		SchemaVersion:   trajectory.SchemaVersion,
		ID:              trajectory.ID,
		ParentID:        trajectory.ParentID,
		ParentEntryID:   trajectory.ParentEntryID,
		CreatedAt:       trajectory.CreatedAt,
		UpdatedAt:       trajectory.UpdatedAt,
		Head:            trajectory.Head,
		Checkpoint:      trajectory.Checkpoint,
		EntryCount:      entryCount,
		OwnedEntryCount: ownedEntryCount,
	}
	if trajectory.Execution != nil {
		summary.ExecutionID = trajectory.Execution.ID
		summary.ExecutionState = trajectory.Execution.State
	}
	return summary
}

func FindLatestInBranch(
	branch []sdk.TrajectoryEntry,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool) {
	for index := len(branch) - 1; index >= 0; index-- {
		if branch[index].Kind == kind {
			return CloneTrajectoryEntry(branch[index]), true
		}
	}
	return sdk.TrajectoryEntry{}, false
}

func LatestEntry(
	head string,
	kind sdk.TrajectoryKind,
	lookup EntryLookup,
) (sdk.TrajectoryEntry, bool, error) {
	seen := make(map[string]struct{})
	for cursor := head; cursor != ""; {
		if _, cycle := seen[cursor]; cycle {
			return sdk.TrajectoryEntry{}, false, fmt.Errorf(
				"trajectory contains a cycle at %q",
				cursor,
			)
		}
		seen[cursor] = struct{}{}
		entry, exists, err := lookup(cursor)
		if err != nil {
			return sdk.TrajectoryEntry{}, false, err
		}
		if !exists {
			return sdk.TrajectoryEntry{}, false, fmt.Errorf(
				"trajectory branch references unknown entry %q",
				cursor,
			)
		}
		if entry.Kind == kind {
			return entry, true, nil
		}
		cursor = entry.ParentID
	}
	return sdk.TrajectoryEntry{}, false, nil
}

func LatestCheckpointAfterAppend(
	currentHead string,
	currentCheckpoint string,
	newHead string,
	prepared map[string]sdk.TrajectoryEntry,
	lookup EntryLookup,
) (string, error) {
	seen := make(map[string]struct{})
	for cursor := newHead; cursor != ""; {
		if _, cycle := seen[cursor]; cycle {
			return "", fmt.Errorf("trajectory contains a cycle at %q", cursor)
		}
		seen[cursor] = struct{}{}
		entry, isPrepared := prepared[cursor]
		if !isPrepared {
			if cursor == currentHead {
				return currentCheckpoint, nil
			}
			checkpoint, found, err := LatestEntry(
				cursor,
				sdk.TrajectoryKindCheckpoint,
				lookup,
			)
			if err != nil || !found {
				return "", err
			}
			return checkpoint.ID, nil
		}
		if entry.Kind == sdk.TrajectoryKindCheckpoint {
			return entry.ID, nil
		}
		cursor = entry.ParentID
	}
	return "", nil
}

func CloneTrajectory(source sdk.Trajectory) sdk.Trajectory {
	return sdk.CloneTrajectory(source)
}

func NormalizeTrajectory(trajectory *sdk.Trajectory) {
	if trajectory.SchemaVersion == 0 {
		trajectory.SchemaVersion = sdk.TrajectorySchemaVersion
	}
	for index := range trajectory.Entries {
		if trajectory.Entries[index].PayloadVersion == 0 {
			trajectory.Entries[index].PayloadVersion = sdk.TrajectoryPayloadVersion
		}
	}
}

func ValidateLoadedTrajectory(trajectory *sdk.Trajectory) error {
	if trajectory.SchemaVersion > sdk.TrajectorySchemaVersion {
		return fmt.Errorf(
			"%w: got %d, maximum supported is %d",
			sdk.ErrTrajectoryVersion,
			trajectory.SchemaVersion,
			sdk.TrajectorySchemaVersion,
		)
	}
	NormalizeTrajectory(trajectory)
	if err := ValidateTrajectoryParent(*trajectory); err != nil {
		return err
	}
	if trajectory.Execution != nil {
		if err := ValidateTrajectoryExecution(*trajectory.Execution); err != nil {
			return err
		}
	}
	known := make(map[string]sdk.TrajectoryEntry, len(trajectory.Entries))
	for index := range trajectory.Entries {
		entry := &trajectory.Entries[index]
		if !entry.Kind.Valid() {
			return fmt.Errorf(
				"trajectory entry %q has unsupported kind %q",
				entry.ID,
				entry.Kind,
			)
		}
		if trajectory.SchemaVersion >= 2 {
			if err := ValidateTrajectoryEntryFields(*entry); err != nil {
				return err
			}
		}
		if entry.PayloadVersion > sdk.TrajectoryPayloadVersion {
			return fmt.Errorf(
				"%w: entry %q payload version %d, maximum supported is %d",
				sdk.ErrTrajectoryVersion,
				entry.ID,
				entry.PayloadVersion,
				sdk.TrajectoryPayloadVersion,
			)
		}
		if trajectory.SchemaVersion < 2 {
			if entry.TrajectoryID == "" {
				entry.TrajectoryID = trajectory.ID
			}
			if entry.Ordinal == 0 {
				entry.Ordinal = uint64(index + 1)
			}
			if entry.Depth == 0 {
				entry.Depth = 1
				if parent, exists := known[entry.ParentID]; exists {
					entry.Depth = parent.Depth + 1
				}
			}
		} else {
			if entry.TrajectoryID != trajectory.ID {
				return fmt.Errorf(
					"trajectory entry %q belongs to trajectory %q, not %q",
					entry.ID,
					entry.TrajectoryID,
					trajectory.ID,
				)
			}
			if entry.Ordinal != uint64(index+1) {
				return fmt.Errorf(
					"trajectory entry %q has ordinal %d, expected %d",
					entry.ID,
					entry.Ordinal,
					index+1,
				)
			}
			if entry.Depth == 0 {
				return fmt.Errorf(
					"trajectory entry %q has zero depth",
					entry.ID,
				)
			}
		}
		known[entry.ID] = *entry
	}
	if trajectory.Execution != nil {
		if _, exists := known[trajectory.Execution.InputEntryID]; !exists {
			return fmt.Errorf(
				"trajectory execution %q references unknown input entry %q",
				trajectory.Execution.ID,
				trajectory.Execution.InputEntryID,
			)
		}
	}
	return nil
}

func CloneTrajectoryEnvironment(source sdk.TrajectoryEnvironment) sdk.TrajectoryEnvironment {
	return sdk.CloneTrajectoryEnvironment(source)
}

func CloneTrajectoryEntry(source sdk.TrajectoryEntry) sdk.TrajectoryEntry {
	return sdk.CloneTrajectoryEntry(source)
}

func CloneTrajectoryExecution(
	source *sdk.TrajectoryExecution,
) *sdk.TrajectoryExecution {
	return sdk.CloneTrajectoryExecution(source)
}

func BindTrajectoryExecutionEntries(
	executionID string,
	entries []sdk.TrajectoryEntry,
) ([]sdk.TrajectoryEntry, error) {
	bound := make([]sdk.TrajectoryEntry, len(entries))
	for index, source := range entries {
		entry := CloneTrajectoryEntry(source)
		if entry.Fields.ExecutionID == "" {
			entry.Fields.ExecutionID = executionID
		} else if entry.Fields.ExecutionID != executionID {
			return nil, fmt.Errorf(
				"trajectory entry %q belongs to execution %q, not %q",
				entry.ID,
				entry.Fields.ExecutionID,
				executionID,
			)
		}
		bound[index] = entry
	}
	return bound, nil
}

func ValidateTrajectoryExecution(execution sdk.TrajectoryExecution) error {
	if err := sdk.ValidateResourceName("trajectory execution", execution.ID); err != nil {
		return err
	}
	if execution.Revision == 0 {
		return fmt.Errorf(
			"trajectory execution %q revision must be positive",
			execution.ID,
		)
	}
	if err := sdk.ValidateResourceName(
		"trajectory execution input entry",
		execution.InputEntryID,
	); err != nil {
		return err
	}
	if execution.MaxTurns < 1 {
		return fmt.Errorf(
			"trajectory execution %q max_turns must be positive",
			execution.ID,
		)
	}
	if execution.CreatedAt.IsZero() || execution.UpdatedAt.IsZero() {
		return fmt.Errorf(
			"trajectory execution %q timestamps are required",
			execution.ID,
		)
	}
	switch execution.State {
	case sdk.TrajectoryExecutionPending:
		if execution.Owner != "" ||
			execution.LeaseToken != "" ||
			!execution.LeaseExpiresAt.IsZero() {
			return fmt.Errorf(
				"pending trajectory execution %q contains a lease",
				execution.ID,
			)
		}
	case sdk.TrajectoryExecutionRunning:
		if strings.TrimSpace(execution.Owner) == "" ||
			execution.LeaseToken == "" ||
			execution.LeaseExpiresAt.IsZero() {
			return fmt.Errorf(
				"running trajectory execution %q has an invalid lease",
				execution.ID,
			)
		}
	case sdk.TrajectoryExecutionSucceeded,
		sdk.TrajectoryExecutionFailed,
		sdk.TrajectoryExecutionCancelled:
		if execution.Owner != "" ||
			execution.LeaseToken != "" ||
			!execution.LeaseExpiresAt.IsZero() {
			return fmt.Errorf(
				"terminal trajectory execution %q contains a lease",
				execution.ID,
			)
		}
		if execution.State == sdk.TrajectoryExecutionSucceeded &&
			execution.LastError != "" {
			return fmt.Errorf(
				"succeeded trajectory execution %q contains an error",
				execution.ID,
			)
		}
	default:
		return fmt.Errorf(
			"trajectory execution %q has invalid state %q",
			execution.ID,
			execution.State,
		)
	}
	return nil
}

func PrepareTrajectoryExecutionStart(
	start sdk.TrajectoryExecutionStart,
	baseHead string,
	inputEntryID string,
	now time.Time,
) (sdk.TrajectoryExecution, error) {
	if err := sdk.ValidateResourceName("trajectory execution", start.ID); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if start.MaxTurns < 1 {
		return sdk.TrajectoryExecution{}, errors.New(
			"trajectory execution max_turns must be positive",
		)
	}
	execution := sdk.TrajectoryExecution{
		ID:           start.ID,
		State:        sdk.TrajectoryExecutionPending,
		Revision:     1,
		BaseHead:     baseHead,
		InputEntryID: inputEntryID,
		Provider:     start.Provider,
		System:       start.System,
		MaxTurns:     start.MaxTurns,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return execution, ValidateTrajectoryExecution(execution)
}

func ClaimTrajectoryExecution(
	execution sdk.TrajectoryExecution,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	if strings.TrimSpace(owner) == "" {
		return sdk.TrajectoryExecution{}, errors.New(
			"trajectory execution lease owner is empty",
		)
	}
	if ttl <= 0 {
		return sdk.TrajectoryExecution{}, errors.New(
			"trajectory execution lease TTL must be positive",
		)
	}
	if execution.Terminal() {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: execution %s is terminal",
			sdk.ErrTrajectoryExecution,
			execution.ID,
		)
	}
	if execution.RecoveryDelay(now) > 0 {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: execution %s is owned by %s until %s",
			sdk.ErrTrajectoryClaimed,
			execution.ID,
			execution.Owner,
			execution.LeaseExpiresAt.Format(time.RFC3339Nano),
		)
	}
	execution.State = sdk.TrajectoryExecutionRunning
	execution.Revision++
	execution.Owner = owner
	execution.LeaseToken = sdk.NewID()
	execution.LeaseExpiresAt = now.Add(ttl)
	execution.UpdatedAt = now
	if err := ValidateTrajectoryExecution(execution); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	return execution, nil
}

func RenewTrajectoryExecution(
	execution sdk.TrajectoryExecution,
	executionID string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	if ttl <= 0 {
		return sdk.TrajectoryExecution{}, errors.New(
			"trajectory execution lease TTL must be positive",
		)
	}
	if execution.ID != executionID ||
		execution.State != sdk.TrajectoryExecutionRunning ||
		execution.LeaseToken != token ||
		!execution.LeaseExpiresAt.After(now) {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: execution %s token is stale or expired",
			sdk.ErrTrajectoryFence,
			executionID,
		)
	}
	execution.Revision++
	execution.LeaseExpiresAt = now.Add(ttl)
	execution.UpdatedAt = now
	return execution, ValidateTrajectoryExecution(execution)
}

func CommitTrajectoryExecution(
	execution sdk.TrajectoryExecution,
	commit sdk.TrajectoryExecutionCommit,
	now time.Time,
) (sdk.TrajectoryExecution, error) {
	if execution.ID != commit.ExecutionID ||
		execution.State != sdk.TrajectoryExecutionRunning ||
		execution.LeaseToken != commit.LeaseToken ||
		!execution.LeaseExpiresAt.After(now) {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: execution %s token is stale or expired",
			sdk.ErrTrajectoryFence,
			commit.ExecutionID,
		)
	}
	execution.Revision++
	execution.UpdatedAt = now
	if commit.State == "" || commit.State == sdk.TrajectoryExecutionRunning {
		return execution, ValidateTrajectoryExecution(execution)
	}
	switch commit.State {
	case sdk.TrajectoryExecutionPending,
		sdk.TrajectoryExecutionSucceeded,
		sdk.TrajectoryExecutionFailed,
		sdk.TrajectoryExecutionCancelled:
	default:
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"trajectory execution cannot commit state %q",
			commit.State,
		)
	}
	execution.State = commit.State
	execution.Owner = ""
	execution.LeaseToken = ""
	execution.LeaseExpiresAt = time.Time{}
	if execution.Terminal() {
		execution.System = ""
	}
	execution.LastError = commit.Error
	if execution.State == sdk.TrajectoryExecutionSucceeded {
		execution.LastError = ""
	}
	return execution, ValidateTrajectoryExecution(execution)
}

func CancelTrajectoryExecution(
	execution sdk.TrajectoryExecution,
	executionID string,
	reason string,
	now time.Time,
) (sdk.TrajectoryExecution, bool, error) {
	if executionID == "" {
		return sdk.TrajectoryExecution{}, false, errors.New(
			"trajectory execution cancellation has no execution ID",
		)
	}
	if execution.ID != executionID {
		return sdk.TrajectoryExecution{}, false, fmt.Errorf(
			"%w: execution %s is no longer current",
			sdk.ErrTrajectoryFence,
			executionID,
		)
	}
	if execution.State == sdk.TrajectoryExecutionCancelled {
		return execution, false, nil
	}
	if execution.Terminal() {
		return sdk.TrajectoryExecution{}, false, fmt.Errorf(
			"%w: execution %s is already %s",
			sdk.ErrTrajectoryExecution,
			execution.ID,
			execution.State,
		)
	}
	execution.State = sdk.TrajectoryExecutionCancelled
	execution.Revision++
	execution.Owner = ""
	execution.LeaseToken = ""
	execution.LeaseExpiresAt = time.Time{}
	execution.LastError = reason
	execution.UpdatedAt = now
	if err := ValidateTrajectoryExecution(execution); err != nil {
		return sdk.TrajectoryExecution{}, false, err
	}
	return execution, true, nil
}

func NormalizeMutationTime(now time.Time) time.Time {
	now = now.UTC()
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now
}

func ValidateTrajectoryKind(kind sdk.TrajectoryKind) error {
	if !kind.Valid() || strings.TrimSpace(string(kind)) == "" {
		return fmt.Errorf("unsupported trajectory kind %q", kind)
	}
	return nil
}

func ValidateTrajectoryEntryFields(entry sdk.TrajectoryEntry) error {
	requireTurn := func() error {
		if entry.Fields.Turn == nil || *entry.Fields.Turn < 0 {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires a non-negative turn",
				entry.ID,
				entry.Kind,
			)
		}
		return nil
	}
	switch entry.Kind {
	case sdk.TrajectoryKindProviderRequest,
		sdk.TrajectoryKindProviderResponse:
		if err := requireTurn(); err != nil {
			return err
		}
		if strings.TrimSpace(entry.Fields.Provider) == "" {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires a provider",
				entry.ID,
				entry.Kind,
			)
		}
		if entry.Kind == sdk.TrajectoryKindProviderRequest &&
			strings.TrimSpace(entry.Fields.OperationKey) == "" {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires operation_key",
				entry.ID,
				entry.Kind,
			)
		}
		if entry.Kind == sdk.TrajectoryKindProviderResponse &&
			entry.Fields.IsError == nil {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires is_error",
				entry.ID,
				entry.Kind,
			)
		}
	case sdk.TrajectoryKindToolCall, sdk.TrajectoryKindToolResult:
		if err := requireTurn(); err != nil {
			return err
		}
		if strings.TrimSpace(entry.Fields.ToolName) == "" ||
			strings.TrimSpace(entry.Fields.ToolCallID) == "" {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires tool_name and tool_call_id",
				entry.ID,
				entry.Kind,
			)
		}
		if entry.Kind == sdk.TrajectoryKindToolCall &&
			strings.TrimSpace(entry.Fields.OperationKey) == "" {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires operation_key",
				entry.ID,
				entry.Kind,
			)
		}
		if entry.Kind == sdk.TrajectoryKindToolResult &&
			entry.Fields.IsError == nil {
			return fmt.Errorf(
				"trajectory entry %q kind %q requires is_error",
				entry.ID,
				entry.Kind,
			)
		}
	case sdk.TrajectoryKindDecision:
		return requireTurn()
	}
	return nil
}
