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
	OperationKindRun        OperationKind = "run"
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

func ResourceRevision(
	manifest Manifest,
	kind OperationKind,
	name string,
	spec any,
) string {
	return PluginResourceRevision(manifest, string(kind), name, spec)
}

func PluginResourceRevision(
	manifest Manifest,
	kind string,
	name string,
	spec any,
) string {
	raw, err := json.Marshal(struct {
		Plugin  string `json:"plugin"`
		Version string `json:"version"`
		Kind    string `json:"kind"`
		Name    string `json:"name"`
		Spec    any    `json:"spec"`
	}{
		Plugin:  manifest.Name,
		Version: manifest.Version,
		Kind:    kind,
		Name:    name,
		Spec:    spec,
	})
	if err != nil {
		return digestString(
			manifest.Name + "\x00" + manifest.Version + "\x00" +
				kind + "\x00" + name,
		)
	}
	return digestBytes(raw)
}
