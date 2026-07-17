package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrOperationNotFound = errors.New("operation not found")
	ErrOperationConflict = errors.New("operation revision conflict")
	ErrOperationClaimed  = errors.New("operation is claimed by another worker")
	ErrOperationFence    = errors.New("operation execution lease is no longer valid")
)

type OperationKind string

const (
	OperationKindProvider   OperationKind = "provider"
	OperationKindTool       OperationKind = "tool"
	OperationKindCapability OperationKind = "capability"
)

type OperationRecord struct {
	Operation        Operation       `json:"operation"`
	Kind             OperationKind   `json:"kind"`
	Resource         string          `json:"resource"`
	ResourceRevision string          `json:"resource_revision,omitempty"`
	Input            json.RawMessage `json:"input"`
	Execution        *OperationLease `json:"execution,omitempty"`
}

type OperationLease struct {
	Owner     string    `json:"owner"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type OperationPage struct {
	Items []OperationRecord `json:"items"`
	Next  string            `json:"next,omitempty"`
}

type OperationStore interface {
	Submit(context.Context, OperationRecord) (OperationRecord, bool, error)
	Get(context.Context, string) (OperationRecord, error)
	Transition(
		context.Context,
		string,
		uint64,
		OperationState,
		json.RawMessage,
		string,
	) (OperationRecord, error)
	Claim(
		context.Context,
		string,
		string,
		time.Time,
		time.Duration,
	) (OperationRecord, error)
	Renew(
		context.Context,
		string,
		string,
		time.Time,
		time.Duration,
	) (OperationRecord, error)
	Complete(
		context.Context,
		string,
		string,
		OperationState,
		json.RawMessage,
		string,
	) (OperationRecord, error)
	Release(context.Context, string, string) (OperationRecord, error)
	List(context.Context) ([]OperationRecord, error)
	ListPage(context.Context, PageRequest) (OperationPage, error)
	PurgeTerminal(context.Context, time.Time) (int, error)
}
