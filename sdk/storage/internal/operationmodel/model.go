package operationmodel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func ValidateNewRecord(record sdk.OperationRecord) error {
	if record.Operation.ID == "" {
		return errors.New("operation ID is empty")
	}
	if record.Operation.IdempotencyKey == "" {
		return errors.New("operation idempotency key is empty")
	}
	switch record.Kind {
	case sdk.OperationKindProvider,
		sdk.OperationKindTool,
		sdk.OperationKindAgent,
		sdk.OperationKindWorkflow,
		sdk.OperationKindCapability:
	default:
		return fmt.Errorf("invalid operation kind %q", record.Kind)
	}
	if err := sdk.ValidateResourceName(
		string(record.Kind),
		record.Resource,
	); err != nil {
		return err
	}
	if !json.Valid(record.Input) {
		return errors.New("operation input is invalid JSON")
	}
	if err := sdk.ValidateInvocation(record.Invocation); err != nil {
		return err
	}
	return nil
}

func PrepareNewRecord(
	record sdk.OperationRecord,
	now time.Time,
) (sdk.OperationRecord, error) {
	if record.Operation.ID == "" {
		record.Operation.ID = sdk.NewID()
	}
	if err := ValidateNewRecord(record); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = NormalizeMutationTime(now)
	submittedAt := record.Operation.SubmittedAt.UTC()
	if submittedAt.IsZero() {
		submittedAt = now
	}
	record.Operation.SubmittedAt = submittedAt
	record.Operation.UpdatedAt = submittedAt
	record.Operation.State = sdk.OperationPending
	record.Operation.Revision = 1
	record.Operation.Output = nil
	record.Operation.Error = ""
	record.Execution = nil
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return sdk.OperationRecord{}, err
	}
	return CloneRecord(record), nil
}

func ValidateTransition(current, next sdk.OperationState) error {
	return sdk.ValidateOperationTransition(current, next)
}

func ValidateClaim(owner string, ttl time.Duration) error {
	if strings.TrimSpace(owner) == "" {
		return errors.New("operation lease owner is empty")
	}
	return ValidateLeaseDuration(ttl)
}

func ValidateLeaseDuration(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("operation lease TTL must be positive")
	}
	return nil
}

func ValidateCompletionState(state sdk.OperationState) error {
	if state != sdk.OperationSucceeded && state != sdk.OperationFailed {
		return fmt.Errorf("claimed operation cannot complete as %q", state)
	}
	return nil
}

func ValidateExpectedRevision(
	record sdk.OperationRecord,
	expectedRevision uint64,
) error {
	if record.Operation.Revision != expectedRevision {
		return fmt.Errorf(
			"%w: operation %s has revision %d, expected %d",
			sdk.ErrOperationConflict,
			record.Operation.ID,
			record.Operation.Revision,
			expectedRevision,
		)
	}
	return nil
}

func NormalizeMutationTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func Cancel(
	record sdk.OperationRecord,
	expectedRevision uint64,
	now time.Time,
) (sdk.OperationRecord, error) {
	return Transition(
		record,
		expectedRevision,
		sdk.OperationCancelled,
		nil,
		"",
		now,
	)
}

func Fail(
	record sdk.OperationRecord,
	expectedRevision uint64,
	operationError string,
	now time.Time,
) (sdk.OperationRecord, error) {
	return Transition(
		record,
		expectedRevision,
		sdk.OperationFailed,
		nil,
		operationError,
		now,
	)
}

func Transition(
	record sdk.OperationRecord,
	expectedRevision uint64,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
	now time.Time,
) (sdk.OperationRecord, error) {
	if err := ValidateExpectedRevision(record, expectedRevision); err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := ValidateTransition(record.Operation.State, state); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = NormalizeMutationTime(now)
	record.Operation.State = state
	record.Operation.Revision++
	record.Operation.Output = append(json.RawMessage(nil), output...)
	record.Operation.Error = operationError
	record.Operation.UpdatedAt = now
	if state != sdk.OperationRunning {
		record.Execution = nil
	}
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return sdk.OperationRecord{}, err
	}
	return CloneRecord(record), nil
}

func Claim(
	record sdk.OperationRecord,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if err := ValidateClaim(owner, ttl); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = NormalizeMutationTime(now)
	if record.Operation.Terminal() {
		return sdk.OperationRecord{}, fmt.Errorf(
			"terminal operation %q cannot be claimed",
			record.Operation.ID,
		)
	}
	if record.Execution != nil && record.Execution.ExpiresAt.After(now) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s is owned by %s until %s",
			sdk.ErrOperationClaimed,
			record.Operation.ID,
			record.Execution.Owner,
			record.Execution.ExpiresAt.Format(time.RFC3339Nano),
		)
	}
	record.Operation.State = sdk.OperationRunning
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	record.Execution = &sdk.OperationLease{
		Owner:     owner,
		Token:     sdk.NewID(),
		ExpiresAt: now.Add(ttl),
	}
	return record, nil
}

func Renew(
	record sdk.OperationRecord,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if err := ValidateLeaseDuration(ttl); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = NormalizeMutationTime(now)
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(now) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			sdk.ErrOperationFence,
			record.Operation.ID,
		)
	}
	record.Execution.ExpiresAt = now.Add(ttl)
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	return record, nil
}

func Complete(
	record sdk.OperationRecord,
	token string,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
	now time.Time,
) (sdk.OperationRecord, error) {
	if err := ValidateCompletionState(state); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = NormalizeMutationTime(now)
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(now) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			sdk.ErrOperationFence,
			record.Operation.ID,
		)
	}
	record.Operation.State = state
	record.Operation.Revision++
	record.Operation.Output = append(json.RawMessage(nil), output...)
	record.Operation.Error = operationError
	record.Operation.UpdatedAt = now
	record.Execution = nil
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return sdk.OperationRecord{}, err
	}
	return record, nil
}

func Release(
	record sdk.OperationRecord,
	token string,
	now time.Time,
) (sdk.OperationRecord, error) {
	now = NormalizeMutationTime(now)
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale",
			sdk.ErrOperationFence,
			record.Operation.ID,
		)
	}
	record.Execution = nil
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	return record, nil
}

func RecoverableAt(record sdk.OperationRecord, now time.Time) bool {
	if record.Operation.Terminal() {
		return false
	}
	now = NormalizeMutationTime(now)
	return record.Execution == nil ||
		!record.Execution.ExpiresAt.After(now)
}

func IdempotencyIndex(record sdk.OperationRecord) string {
	return string(record.Kind) + "\x00" + record.Resource + "\x00" +
		record.ResourceRevision + "\x00" +
		record.Operation.IdempotencyKey
}

func CloneRecord(record sdk.OperationRecord) sdk.OperationRecord {
	return sdk.CloneOperationRecord(record)
}

func SameSubmission(left, right sdk.OperationRecord) bool {
	return bytes.Equal(left.Input, right.Input) &&
		left.Invocation.ID == right.Invocation.ID &&
		left.Invocation.RootID == right.Invocation.RootID &&
		left.Invocation.ParentID == right.Invocation.ParentID &&
		left.Invocation.GroupID == right.Invocation.GroupID &&
		left.Invocation.SessionID == right.Invocation.SessionID &&
		left.Invocation.TargetSessionID ==
			right.Invocation.TargetSessionID &&
		left.Invocation.ExecutionID ==
			right.Invocation.ExecutionID &&
		left.Invocation.Ordinal == right.Invocation.Ordinal &&
		slices.Equal(
			left.Invocation.Dependencies,
			right.Invocation.Dependencies,
		)
}

func ValidateLoadedRecord(record sdk.OperationRecord) error {
	if err := ValidateNewRecord(record); err != nil {
		return err
	}
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return err
	}
	if record.Execution == nil {
		return nil
	}
	if record.Operation.State != sdk.OperationRunning ||
		strings.TrimSpace(record.Execution.Owner) == "" ||
		record.Execution.Token == "" ||
		record.Execution.ExpiresAt.IsZero() {
		return errors.New("operation execution lease is invalid")
	}
	return nil
}
