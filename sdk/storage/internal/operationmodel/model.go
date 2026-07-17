package operationmodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
		sdk.OperationKindCapability,
		sdk.OperationKind("agent"),
		sdk.OperationKind("workflow"):
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
	raw, err := json.Marshal(record)
	if err == nil {
		var cloned sdk.OperationRecord
		if err := json.Unmarshal(raw, &cloned); err == nil {
			return cloned
		}
	}
	record.Input = append(json.RawMessage(nil), record.Input...)
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
	left.Operation = sdk.Operation{}
	left.Execution = nil
	right.Operation = sdk.Operation{}
	right.Execution = nil
	return reflect.DeepEqual(left, right)
}

// MarshalOptionalInvocation preserves the newer invocation field when the SDK
// exposes it, while keeping storage implementations compatible with callers
// built against the base OperationRecord contract.
func MarshalOptionalInvocation(record sdk.OperationRecord) ([]byte, error) {
	field := reflect.ValueOf(record).FieldByName("Invocation")
	if !field.IsValid() {
		return []byte(`{}`), nil
	}
	return json.Marshal(field.Interface())
}

func UnmarshalOptionalInvocation(
	record *sdk.OperationRecord,
	raw []byte,
) error {
	if record == nil || len(raw) == 0 {
		return nil
	}
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return errors.New("operation record is nil")
	}
	field := value.Elem().FieldByName("Invocation")
	if !field.IsValid() || !field.CanAddr() {
		return nil
	}
	return json.Unmarshal(raw, field.Addr().Interface())
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
