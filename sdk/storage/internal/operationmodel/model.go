package operationmodel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

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

func ValidateTransition(current, next sdk.OperationState) error {
	switch current {
	case sdk.OperationPending:
		switch next {
		case sdk.OperationRunning,
			sdk.OperationFailed,
			sdk.OperationCancelled:
			return nil
		}
	case sdk.OperationRunning:
		switch next {
		case sdk.OperationRunning,
			sdk.OperationSucceeded,
			sdk.OperationFailed,
			sdk.OperationCancelled:
			return nil
		}
	case sdk.OperationSucceeded,
		sdk.OperationFailed,
		sdk.OperationCancelled:
		return fmt.Errorf(
			"terminal operation in state %q cannot transition",
			current,
		)
	}
	return fmt.Errorf("invalid operation transition %q -> %q", current, next)
}

func IdempotencyIndex(record sdk.OperationRecord) string {
	return string(record.Kind) + "\x00" + record.Resource + "\x00" +
		record.ResourceRevision + "\x00" +
		record.Operation.IdempotencyKey
}

func CloneRecord(record sdk.OperationRecord) sdk.OperationRecord {
	record.Input = append(json.RawMessage(nil), record.Input...)
	record.Invocation = sdk.CloneInvocation(record.Invocation)
	record.Operation.Output = append(
		json.RawMessage(nil),
		record.Operation.Output...,
	)
	if record.Execution != nil {
		execution := *record.Execution
		record.Execution = &execution
	}
	return record
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
