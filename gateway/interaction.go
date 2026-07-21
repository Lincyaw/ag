package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	GatewayEventInteractionRequested = "gateway_interaction_requested"
	GatewayEventInteractionResolved  = "gateway_interaction_resolved"
	GatewayEventInteractionCancelled = "gateway_interaction_cancelled"
	defaultInteractionPageSize       = 100
	maxInteractionPageSize           = 1000
)

var (
	ErrInteractionNotFound = errors.New("gateway interaction not found")
	ErrInteractionConflict = errors.New("gateway interaction conflict")
)

type InteractionKind string

const (
	InteractionQuestion     InteractionKind = "question"
	InteractionConfirmation InteractionKind = "confirmation"
	InteractionPermission   InteractionKind = "permission"
)

type InteractionState string

const (
	InteractionPending   InteractionState = "pending"
	InteractionResolved  InteractionState = "resolved"
	InteractionCancelled InteractionState = "cancelled"
)

func (state InteractionState) Terminal() bool {
	return state == InteractionResolved || state == InteractionCancelled
}

type InteractionOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type InteractionAnswer struct {
	OptionID string `json:"option_id,omitempty"`
	Text     string `json:"text,omitempty"`
}

type Interaction struct {
	ID          string              `json:"id"`
	SessionID   string              `json:"session_id"`
	ExecutionID string              `json:"execution_id"`
	Sequence    uint64              `json:"sequence"`
	Kind        InteractionKind     `json:"kind"`
	Prompt      string              `json:"prompt"`
	Options     []InteractionOption `json:"options,omitempty"`
	State       InteractionState    `json:"state"`
	Answer      *InteractionAnswer  `json:"answer,omitempty"`
	Revision    uint64              `json:"revision"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

type InteractionQuery struct {
	After uint64           `json:"after,omitempty"`
	Limit int              `json:"limit,omitempty"`
	State InteractionState `json:"state,omitempty"`
}

type InteractionPage struct {
	Items []Interaction `json:"items"`
	Next  uint64        `json:"next,omitempty"`
}

type InteractionStore interface {
	Create(context.Context, Interaction) (Interaction, error)
	Get(context.Context, string, string) (Interaction, error)
	List(context.Context, string, InteractionQuery) (InteractionPage, error)
	Wait(context.Context, string, string) (Interaction, error)
	Resolve(context.Context, string, string, uint64, InteractionAnswer) (Interaction, error)
	Cancel(context.Context, string, string, uint64) (Interaction, error)
	Close(context.Context) error
}

type interactionStream struct {
	NextSequence uint64        `json:"next_sequence"`
	Items        []Interaction `json:"items"`
}

func normalizeInteraction(value Interaction) (Interaction, error) {
	value.ID = strings.TrimSpace(value.ID)
	if value.ID == "" {
		value.ID = sdk.NewID()
	}
	if err := sdk.ValidateResourceName("gateway interaction", value.ID); err != nil {
		return Interaction{}, err
	}
	var err error
	value.SessionID, err = normalizeEventSessionID(value.SessionID)
	if err != nil {
		return Interaction{}, err
	}
	value.ExecutionID = strings.TrimSpace(value.ExecutionID)
	if err := sdk.ValidateResourceName(
		"gateway interaction execution",
		value.ExecutionID,
	); err != nil {
		return Interaction{}, err
	}
	if value.Kind == "" {
		value.Kind = InteractionQuestion
	}
	switch value.Kind {
	case InteractionQuestion, InteractionConfirmation, InteractionPermission:
	default:
		return Interaction{}, fmt.Errorf("unknown gateway interaction kind %q", value.Kind)
	}
	value.Prompt = strings.TrimSpace(value.Prompt)
	if value.Prompt == "" {
		return Interaction{}, errors.New("gateway interaction prompt is empty")
	}
	seen := make(map[string]struct{}, len(value.Options))
	for index, option := range value.Options {
		option.ID = strings.TrimSpace(option.ID)
		option.Label = strings.TrimSpace(option.Label)
		option.Description = strings.TrimSpace(option.Description)
		if err := sdk.ValidateResourceName(
			"gateway interaction option",
			option.ID,
		); err != nil {
			return Interaction{}, err
		}
		if option.Label == "" {
			return Interaction{}, errors.New("gateway interaction option label is empty")
		}
		if _, duplicate := seen[option.ID]; duplicate {
			return Interaction{}, fmt.Errorf(
				"gateway interaction contains duplicate option %q",
				option.ID,
			)
		}
		seen[option.ID] = struct{}{}
		value.Options[index] = option
	}
	return cloneInteraction(value), nil
}

func normalizeInteractionQuery(query InteractionQuery) (InteractionQuery, error) {
	if query.Limit == 0 {
		query.Limit = defaultInteractionPageSize
	}
	if query.Limit < 1 || query.Limit > maxInteractionPageSize {
		return InteractionQuery{}, fmt.Errorf(
			"gateway interaction page limit must be between 1 and %d",
			maxInteractionPageSize,
		)
	}
	if query.State != "" {
		switch query.State {
		case InteractionPending, InteractionResolved, InteractionCancelled:
		default:
			return InteractionQuery{}, fmt.Errorf(
				"unknown gateway interaction state %q",
				query.State,
			)
		}
	}
	return query, nil
}

func cloneInteraction(value Interaction) Interaction {
	value.Options = append([]InteractionOption(nil), value.Options...)
	if value.Answer != nil {
		answer := *value.Answer
		value.Answer = &answer
	}
	return value
}

func createInteraction(
	stream interactionStream,
	value Interaction,
	now time.Time,
) (interactionStream, Interaction, bool, error) {
	for _, current := range stream.Items {
		if current.ID != value.ID {
			continue
		}
		left, _ := json.Marshal(struct {
			Kind    InteractionKind
			Prompt  string
			Options []InteractionOption
		}{current.Kind, current.Prompt, current.Options})
		right, _ := json.Marshal(struct {
			Kind    InteractionKind
			Prompt  string
			Options []InteractionOption
		}{value.Kind, value.Prompt, value.Options})
		if string(left) == string(right) && current.SessionID == value.SessionID &&
			current.ExecutionID == value.ExecutionID {
			return stream, cloneInteraction(current), false, nil
		}
		return interactionStream{}, Interaction{}, false, fmt.Errorf(
			"%w: interaction ID %s was reused",
			ErrInteractionConflict,
			value.ID,
		)
	}
	if stream.NextSequence == 0 {
		stream.NextSequence = 1
	}
	now = now.UTC()
	value.Sequence = stream.NextSequence
	stream.NextSequence++
	value.State = InteractionPending
	value.Answer = nil
	value.Revision = 1
	value.CreatedAt = now
	value.UpdatedAt = now
	stream.Items = append(stream.Items, cloneInteraction(value))
	return stream, value, true, nil
}

func listInteractions(stream interactionStream, query InteractionQuery) InteractionPage {
	page := InteractionPage{Items: make([]Interaction, 0, query.Limit)}
	for _, item := range stream.Items {
		if item.Sequence <= query.After ||
			(query.State != "" && item.State != query.State) {
			continue
		}
		page.Items = append(page.Items, cloneInteraction(item))
		if len(page.Items) == query.Limit {
			break
		}
	}
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	return page
}

func resolveInteraction(
	stream interactionStream,
	id string,
	expectedRevision uint64,
	answer InteractionAnswer,
	now time.Time,
) (interactionStream, Interaction, bool, error) {
	answer.OptionID = strings.TrimSpace(answer.OptionID)
	answer.Text = strings.TrimSpace(answer.Text)
	for index, item := range stream.Items {
		if item.ID != id {
			continue
		}
		if item.Revision != expectedRevision {
			return interactionStream{}, Interaction{}, false, fmt.Errorf(
				"%w: interaction %s has revision %d, expected %d",
				ErrInteractionConflict,
				id,
				item.Revision,
				expectedRevision,
			)
		}
		if item.State != InteractionPending {
			return interactionStream{}, Interaction{}, false, fmt.Errorf(
				"%w: interaction %s is %s",
				ErrInteractionConflict,
				id,
				item.State,
			)
		}
		if answer.OptionID == "" && answer.Text == "" {
			return interactionStream{}, Interaction{}, false, errors.New(
				"gateway interaction answer is empty",
			)
		}
		if answer.OptionID != "" {
			found := false
			for _, option := range item.Options {
				if option.ID == answer.OptionID {
					found = true
					break
				}
			}
			if !found {
				return interactionStream{}, Interaction{}, false, fmt.Errorf(
					"gateway interaction option %q is unavailable",
					answer.OptionID,
				)
			}
		}
		item.State = InteractionResolved
		item.Answer = &answer
		item.Revision++
		item.UpdatedAt = now.UTC()
		stream.Items[index] = cloneInteraction(item)
		return stream, item, true, nil
	}
	return interactionStream{}, Interaction{}, false, fmt.Errorf(
		"%w: %s",
		ErrInteractionNotFound,
		id,
	)
}

func cancelInteraction(
	stream interactionStream,
	id string,
	expectedRevision uint64,
	now time.Time,
) (interactionStream, Interaction, bool, error) {
	for index, item := range stream.Items {
		if item.ID != id {
			continue
		}
		if item.Revision != expectedRevision {
			return interactionStream{}, Interaction{}, false, fmt.Errorf(
				"%w: interaction %s has revision %d, expected %d",
				ErrInteractionConflict,
				id,
				item.Revision,
				expectedRevision,
			)
		}
		if item.State != InteractionPending {
			return interactionStream{}, Interaction{}, false, fmt.Errorf(
				"%w: interaction %s is %s",
				ErrInteractionConflict,
				id,
				item.State,
			)
		}
		item.State = InteractionCancelled
		item.Answer = nil
		item.Revision++
		item.UpdatedAt = now.UTC()
		stream.Items[index] = cloneInteraction(item)
		return stream, item, true, nil
	}
	return interactionStream{}, Interaction{}, false, fmt.Errorf(
		"%w: %s",
		ErrInteractionNotFound,
		id,
	)
}
