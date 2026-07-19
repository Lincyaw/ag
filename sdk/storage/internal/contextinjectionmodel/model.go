package contextinjectionmodel

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type Record struct {
	Sequence  uint64
	Injection sdk.ContextInjection
}

func PrepareBatch(
	injections []sdk.ContextInjection,
	now time.Time,
) ([]sdk.ContextInjection, error) {
	if len(injections) == 0 {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	prepared := make([]sdk.ContextInjection, 0, len(injections))
	seen := make(map[string]sdk.ContextInjection, len(injections))
	for _, injection := range injections {
		normalized, err := sdk.NormalizeContextInjection(injection, now.UTC())
		if err != nil {
			return nil, err
		}
		if existing, ok := seen[normalized.ID]; ok {
			if SameIdentity(existing, normalized) {
				continue
			}
			return nil, fmt.Errorf(
				"context injection %q appears more than once with different identity",
				normalized.ID,
			)
		}
		seen[normalized.ID] = normalized
		prepared = append(prepared, normalized)
	}
	return prepared, nil
}

func SameIdentity(
	left sdk.ContextInjection,
	right sdk.ContextInjection,
) bool {
	return left.ID == right.ID &&
		left.Priority == right.Priority &&
		left.Mode == right.Mode &&
		left.Origin == right.Origin &&
		left.TargetSessionID == right.TargetSessionID &&
		left.TargetExecutionID == right.TargetExecutionID &&
		left.IsMeta == right.IsMeta &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		maps.Equal(left.Attributes, right.Attributes) &&
		reflect.DeepEqual(left.Messages, right.Messages)
}

func ValidateLoaded(injection sdk.ContextInjection) error {
	if injection.CreatedAt.IsZero() {
		return errors.New("context injection created_at is empty")
	}
	normalized, err := sdk.NormalizeContextInjection(
		injection,
		injection.CreatedAt,
	)
	if err != nil {
		return err
	}
	if !SameIdentity(normalized, injection) {
		return fmt.Errorf(
			"context injection %q is not normalized",
			injection.ID,
		)
	}
	return nil
}

func ValidateLoadedRecord(record Record) error {
	if record.Sequence == 0 {
		return errors.New("context injection sequence is empty")
	}
	return ValidateLoaded(record.Injection)
}

func ValidateQuery(query sdk.ContextInjectionQuery) error {
	if query.TargetSessionID != "" {
		if err := sdk.ValidateResourceName(
			"context injection target session",
			query.TargetSessionID,
		); err != nil {
			return err
		}
	}
	if query.TargetExecutionID != "" {
		if err := sdk.ValidateResourceName(
			"context injection target execution",
			query.TargetExecutionID,
		); err != nil {
			return err
		}
	}
	if query.Limit < 0 {
		return errors.New("context injection query limit cannot be negative")
	}
	return nil
}

func ValidateConsumeIDs(ids []string) error {
	for _, id := range ids {
		if err := sdk.ValidateResourceName("context injection", id); err != nil {
			return fmt.Errorf("consume context injection %q: %w", id, err)
		}
	}
	return nil
}

func MatchesQuery(
	injection sdk.ContextInjection,
	query sdk.ContextInjectionQuery,
) bool {
	if query.TargetSessionID != "" &&
		injection.TargetSessionID != "" &&
		injection.TargetSessionID != query.TargetSessionID {
		return false
	}
	if query.TargetExecutionID != "" &&
		injection.TargetExecutionID != "" &&
		injection.TargetExecutionID != query.TargetExecutionID {
		return false
	}
	return true
}

func SortRecords(records []Record) {
	slices.SortFunc(records, func(left, right Record) int {
		if left.Sequence < right.Sequence {
			return -1
		}
		if left.Sequence > right.Sequence {
			return 1
		}
		return 0
	})
}
