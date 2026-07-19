package sdk

import "testing"

func TestValidateOperationProgressRejectsImpossibleTransition(t *testing.T) {
	t.Parallel()

	current := Operation{
		ID:             "operation-1",
		IdempotencyKey: "request-1",
		State:          OperationRunning,
		Revision:       2,
	}
	next := Operation{
		ID:             current.ID,
		IdempotencyKey: current.IdempotencyKey,
		State:          OperationPending,
		Revision:       3,
	}
	if err := ValidateOperationProgress(current, next); err == nil {
		t.Fatal("invalid operation progress was accepted")
	}
}

func TestValidateOperationProgressAllowsObservedCompletion(t *testing.T) {
	t.Parallel()

	pending := Operation{
		ID:             "operation-1",
		IdempotencyKey: "request-1",
		State:          OperationPending,
		Revision:       1,
	}
	succeeded := Operation{
		ID:             pending.ID,
		IdempotencyKey: pending.IdempotencyKey,
		State:          OperationSucceeded,
		Revision:       2,
		Output:         []byte(`{"ok":true}`),
	}
	if err := ValidateOperationProgress(pending, succeeded); err != nil {
		t.Fatal(err)
	}
}

func TestEventContractAllowsEffect(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		contract EventContract
		want     bool
	}{
		"observer only": {
			contract: EventContract{Name: EventAgentEnd},
		},
		"mutable payload": {
			contract: EventContract{
				Name:          EventBeforeProvider,
				MutableFields: []string{"provider"},
			},
			want: true,
		},
		"block": {
			contract: EventContract{
				Name:       EventBeforeTool,
				AllowBlock: true,
			},
			want: true,
		},
		"action": {
			contract: EventContract{
				Name:        EventDecide,
				AllowAction: true,
			},
			want: true,
		},
	}
	for name, testCase := range tests {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := testCase.contract.AllowsEffect(); got != testCase.want {
				t.Fatalf("AllowsEffect() = %v, want %v", got, testCase.want)
			}
		})
	}
}
