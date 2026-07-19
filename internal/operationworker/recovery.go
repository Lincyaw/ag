package operationworker

import (
	"context"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

// RecoveryCandidate is the worker-facing recovery schedule for one
// non-terminal operation. Delay is positive while another worker's lease is
// still valid.
type RecoveryCandidate struct {
	OperationID string
	Delay       time.Duration
}

func ListRecoveryCandidates(
	ctx context.Context,
	store sdk.OperationStore,
	now time.Time,
) ([]RecoveryCandidate, error) {
	if store == nil {
		return nil, fmt.Errorf("operation recovery store is nil")
	}
	records, err := store.ListNonTerminal(ctx)
	if err != nil {
		return nil, fmt.Errorf("list non-terminal operations: %w", err)
	}
	candidates := make([]RecoveryCandidate, 0, len(records))
	for _, record := range records {
		candidate, ok := RecoveryCandidateFromRecord(record, now)
		if !ok {
			return nil, fmt.Errorf(
				"%w: operation %s returned by non-terminal index is terminal",
				sdk.ErrOperationConflict,
				record.Operation.ID,
			)
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func RecoveryCandidateFromRecord(
	record sdk.OperationRecord,
	now time.Time,
) (RecoveryCandidate, bool) {
	if record.Operation.Terminal() {
		return RecoveryCandidate{}, false
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	delay := time.Duration(0)
	if record.Execution != nil && record.Execution.ExpiresAt.After(now) {
		delay = record.Execution.ExpiresAt.Sub(now)
	}
	return RecoveryCandidate{
		OperationID: record.Operation.ID,
		Delay:       delay,
	}, true
}

func (candidate RecoveryCandidate) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if candidate.Delay <= 0 {
		return nil
	}
	timer := time.NewTimer(candidate.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
