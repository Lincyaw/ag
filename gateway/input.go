package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultInputPageSize = 100
	maxInputPageSize     = 1000
)

var (
	ErrInputNotFound = errors.New("gateway input not found")
	ErrInputConflict = errors.New("gateway input conflict")
)

type AgentInputKind string

const AgentInputPrompt AgentInputKind = "prompt"

type AgentInputState string

const (
	AgentInputQueued      AgentInputState = "queued"
	AgentInputDispatching AgentInputState = "dispatching"
	AgentInputSucceeded   AgentInputState = "succeeded"
	AgentInputFailed      AgentInputState = "failed"
	AgentInputCancelled   AgentInputState = "cancelled"
)

func (state AgentInputState) Terminal() bool {
	switch state {
	case AgentInputSucceeded, AgentInputFailed, AgentInputCancelled:
		return true
	default:
		return false
	}
}

type AgentInput struct {
	ID          string          `json:"id"`
	SessionID   string          `json:"session_id"`
	Sequence    uint64          `json:"sequence"`
	Kind        AgentInputKind  `json:"kind"`
	Content     string          `json:"content"`
	State       AgentInputState `json:"state"`
	ExecutionID string          `json:"execution_id,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Revision    uint64          `json:"revision"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type InputQuery struct {
	After uint64 `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type InputPage struct {
	Items []AgentInput `json:"items"`
	Next  uint64       `json:"next,omitempty"`
}

type AcquiredInput struct {
	Input   AgentInput
	Resumed bool
}

// InputStore persists the user-command state machine consumed by the gateway
// supervisor. AcquireNext returns an existing dispatching input after restart,
// or atomically advances the oldest queued input to dispatching.
type InputStore interface {
	Enqueue(context.Context, AgentInput) (AgentInput, error)
	Get(context.Context, string, string) (AgentInput, error)
	List(context.Context, string, InputQuery) (InputPage, error)
	AcquireNext(context.Context, string) (AcquiredInput, bool, error)
	BindExecution(context.Context, string, string, string) (AgentInput, error)
	CancelQueued(context.Context, string, string, uint64) (AgentInput, error)
	Complete(context.Context, string, string, AgentInputState, string) (AgentInput, error)
	Close(context.Context) error
}

type inputStream struct {
	NextSequence uint64       `json:"next_sequence"`
	Inputs       []AgentInput `json:"inputs"`
}

func normalizeAgentInput(input AgentInput) (AgentInput, error) {
	input.ID = strings.TrimSpace(input.ID)
	if input.ID == "" {
		input.ID = sdk.NewID()
	}
	if err := sdk.ValidateResourceName("gateway input", input.ID); err != nil {
		return AgentInput{}, err
	}
	var err error
	input.SessionID, err = normalizeEventSessionID(input.SessionID)
	if err != nil {
		return AgentInput{}, err
	}
	if input.Kind == "" {
		input.Kind = AgentInputPrompt
	}
	if input.Kind != AgentInputPrompt {
		return AgentInput{}, fmt.Errorf("unknown gateway input kind %q", input.Kind)
	}
	input.Content = strings.TrimSpace(input.Content)
	if input.Content == "" {
		return AgentInput{}, errors.New("gateway input content is empty")
	}
	return input, nil
}

func normalizeInputQuery(query InputQuery) (InputQuery, error) {
	if query.Limit == 0 {
		query.Limit = defaultInputPageSize
	}
	if query.Limit < 1 || query.Limit > maxInputPageSize {
		return InputQuery{}, fmt.Errorf(
			"gateway input page limit must be between 1 and %d",
			maxInputPageSize,
		)
	}
	return query, nil
}

func enqueueAgentInput(
	stream inputStream,
	input AgentInput,
	now time.Time,
) (inputStream, AgentInput, bool, error) {
	for _, current := range stream.Inputs {
		if current.ID != input.ID {
			continue
		}
		if current.SessionID == input.SessionID && current.Kind == input.Kind &&
			current.Content == input.Content {
			return stream, current, false, nil
		}
		return inputStream{}, AgentInput{}, false, fmt.Errorf(
			"%w: input ID %s was reused with different content",
			ErrInputConflict,
			input.ID,
		)
	}
	if stream.NextSequence == 0 {
		stream.NextSequence = 1
	}
	if stream.NextSequence == math.MaxUint64 {
		return inputStream{}, AgentInput{}, false, errors.New(
			"gateway input sequence is exhausted",
		)
	}
	now = now.UTC()
	input.Sequence = stream.NextSequence
	input.State = AgentInputQueued
	input.ExecutionID = ""
	input.LastError = ""
	input.Revision = 1
	input.CreatedAt = now
	input.UpdatedAt = now
	stream.NextSequence++
	stream.Inputs = append(stream.Inputs, input)
	return stream, input, true, nil
}

func listAgentInputs(stream inputStream, query InputQuery) InputPage {
	page := InputPage{Items: make([]AgentInput, 0, query.Limit)}
	for _, input := range stream.Inputs {
		if input.Sequence <= query.After {
			continue
		}
		page.Items = append(page.Items, input)
		if len(page.Items) == query.Limit {
			break
		}
	}
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	return page
}

func acquireAgentInput(
	stream inputStream,
	now time.Time,
) (inputStream, AcquiredInput, bool, bool, error) {
	dispatching := -1
	for index, input := range stream.Inputs {
		if input.State != AgentInputDispatching {
			continue
		}
		if dispatching >= 0 {
			return inputStream{}, AcquiredInput{}, false, false, errors.New(
				"gateway input stream contains multiple dispatching inputs",
			)
		}
		dispatching = index
	}
	if dispatching >= 0 {
		return stream, AcquiredInput{
			Input: stream.Inputs[dispatching], Resumed: true,
		}, true, false, nil
	}
	for index, input := range stream.Inputs {
		if input.State != AgentInputQueued {
			continue
		}
		input.State = AgentInputDispatching
		input.Revision++
		input.UpdatedAt = now.UTC()
		stream.Inputs[index] = input
		return stream, AcquiredInput{Input: input}, true, true, nil
	}
	return stream, AcquiredInput{}, false, false, nil
}

func bindAgentInputExecution(
	stream inputStream,
	inputID string,
	executionID string,
	now time.Time,
) (inputStream, AgentInput, bool, error) {
	if err := sdk.ValidateResourceName("gateway input execution", executionID); err != nil {
		return inputStream{}, AgentInput{}, false, err
	}
	for index, input := range stream.Inputs {
		if input.ID != inputID {
			continue
		}
		if input.State != AgentInputDispatching {
			return inputStream{}, AgentInput{}, false, fmt.Errorf(
				"%w: input %s is %s",
				ErrInputConflict,
				input.ID,
				input.State,
			)
		}
		if input.ExecutionID == executionID {
			return stream, input, false, nil
		}
		if input.ExecutionID != "" {
			return inputStream{}, AgentInput{}, false, fmt.Errorf(
				"%w: input %s is bound to execution %s",
				ErrInputConflict,
				input.ID,
				input.ExecutionID,
			)
		}
		input.ExecutionID = executionID
		input.Revision++
		input.UpdatedAt = now.UTC()
		stream.Inputs[index] = input
		return stream, input, true, nil
	}
	return inputStream{}, AgentInput{}, false, fmt.Errorf(
		"%w: %s",
		ErrInputNotFound,
		inputID,
	)
}

func completeAgentInput(
	stream inputStream,
	inputID string,
	state AgentInputState,
	lastError string,
	now time.Time,
) (inputStream, AgentInput, bool, error) {
	if !state.Terminal() {
		return inputStream{}, AgentInput{}, false, fmt.Errorf(
			"gateway input completion state %q is not terminal",
			state,
		)
	}
	lastError = strings.TrimSpace(lastError)
	for index, input := range stream.Inputs {
		if input.ID != inputID {
			continue
		}
		if input.State == state && input.LastError == lastError {
			return stream, input, false, nil
		}
		if input.State != AgentInputDispatching {
			return inputStream{}, AgentInput{}, false, fmt.Errorf(
				"%w: input %s is %s",
				ErrInputConflict,
				input.ID,
				input.State,
			)
		}
		input.State = state
		input.LastError = lastError
		input.Revision++
		input.UpdatedAt = now.UTC()
		stream.Inputs[index] = input
		return stream, input, true, nil
	}
	return inputStream{}, AgentInput{}, false, fmt.Errorf(
		"%w: %s",
		ErrInputNotFound,
		inputID,
	)
}

func cancelQueuedAgentInput(
	stream inputStream,
	inputID string,
	expectedRevision uint64,
	now time.Time,
) (inputStream, AgentInput, bool, error) {
	for index, input := range stream.Inputs {
		if input.ID != inputID {
			continue
		}
		if input.Revision != expectedRevision {
			return inputStream{}, AgentInput{}, false, fmt.Errorf(
				"%w: input %s has revision %d, expected %d",
				ErrInputConflict,
				input.ID,
				input.Revision,
				expectedRevision,
			)
		}
		if input.State == AgentInputCancelled {
			return stream, input, false, nil
		}
		if input.State != AgentInputQueued {
			return inputStream{}, AgentInput{}, false, fmt.Errorf(
				"%w: input %s is %s",
				ErrInputConflict,
				input.ID,
				input.State,
			)
		}
		input.State = AgentInputCancelled
		input.Revision++
		input.UpdatedAt = now.UTC()
		stream.Inputs[index] = input
		return stream, input, true, nil
	}
	return inputStream{}, AgentInput{}, false, fmt.Errorf(
		"%w: %s",
		ErrInputNotFound,
		inputID,
	)
}

func validateInputStream(
	sessionID string,
	stream inputStream,
) (inputStream, error) {
	if stream.NextSequence == 0 {
		stream.NextSequence = 1
	}
	var previous uint64
	seen := make(map[string]struct{}, len(stream.Inputs))
	dispatching := 0
	for index, input := range stream.Inputs {
		normalized, err := normalizeAgentInput(input)
		if err != nil || normalized.SessionID != sessionID ||
			input.Sequence == 0 || input.Sequence <= previous ||
			input.Revision == 0 || input.CreatedAt.IsZero() || input.UpdatedAt.IsZero() {
			return inputStream{}, fmt.Errorf(
				"gateway input stream %q contains invalid input at index %d: %w",
				sessionID,
				index,
				err,
			)
		}
		if _, exists := seen[input.ID]; exists {
			return inputStream{}, fmt.Errorf(
				"gateway input stream %q contains duplicate input %q",
				sessionID,
				input.ID,
			)
		}
		seen[input.ID] = struct{}{}
		switch input.State {
		case AgentInputQueued, AgentInputDispatching,
			AgentInputSucceeded, AgentInputFailed, AgentInputCancelled:
		default:
			return inputStream{}, fmt.Errorf(
				"gateway input %q has invalid state %q",
				input.ID,
				input.State,
			)
		}
		if input.State == AgentInputDispatching {
			dispatching++
		}
		if input.ExecutionID != "" {
			if err := sdk.ValidateResourceName(
				"gateway input execution",
				input.ExecutionID,
			); err != nil {
				return inputStream{}, err
			}
		}
		input.CreatedAt = input.CreatedAt.UTC()
		input.UpdatedAt = input.UpdatedAt.UTC()
		stream.Inputs[index] = input
		previous = input.Sequence
	}
	if dispatching > 1 || previous >= stream.NextSequence {
		return inputStream{}, fmt.Errorf(
			"gateway input stream %q has inconsistent sequencing or dispatch state",
			sessionID,
		)
	}
	return stream, nil
}
