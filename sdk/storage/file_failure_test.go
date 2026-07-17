package storage

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestFileConstructorsRejectEmptyDirectory(t *testing.T) {
	for name, construct := range map[string]func() error{
		"state": func() error {
			_, err := NewFileStateBackend(" ")
			return err
		},
		"operation": func() error {
			_, err := NewFileOperationStore(" ")
			return err
		},
		"delivery": func() error {
			_, err := NewFileDeliveryStore(" ")
			return err
		},
		"trajectory": func() error {
			_, err := NewFileTrajectoryStore(" ")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := construct()
			if err == nil || !strings.Contains(err.Error(), "directory is empty") {
				t.Fatalf("constructor error = %v, want empty directory", err)
			}
		})
	}
}

func TestFileOperationStoreDoesNotReturnUnpublishedRecord(t *testing.T) {
	storePort, err := NewFileOperationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := storePort.(*fileOperationStore)
	store.directory = filepath.Join(t.TempDir(), "missing")

	record, created, err := store.Submit(context.Background(), sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             "unpublished-operation",
			IdempotencyKey: "unpublished-operation",
		},
		Kind:     sdk.OperationKindTool,
		Resource: "tool",
		Input:    []byte(`{}`),
	})
	if err == nil {
		t.Fatal("Submit() error = nil, want publish failure")
	}
	if created || !reflect.DeepEqual(record, sdk.OperationRecord{}) {
		t.Fatalf("Submit() = (%#v, %t, %v), want zero result", record, created, err)
	}
}

func TestFileDeliveryStoreDoesNotReturnUnpublishedLease(t *testing.T) {
	storePort, err := NewFileDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := storePort.(*fileDeliveryStore)
	now := time.Date(2026, 7, 17, 17, 0, 0, 0, time.UTC)
	if err := store.Enqueue(context.Background(), sdk.Delivery{
		ID:           "unpublished-lease",
		Plugin:       "observer",
		Subscription: "events",
		Event: sdk.Event{
			ID:      "unpublished-lease-event",
			Name:    sdk.EventAgentStart,
			Payload: []byte(`{}`),
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	store.directory = filepath.Join(t.TempDir(), "missing")

	delivery, err := store.Lease(context.Background(), now, time.Minute)
	if err == nil {
		t.Fatal("Lease() error = nil, want publish failure")
	}
	if !reflect.DeepEqual(delivery, sdk.Delivery{}) {
		t.Fatalf("Lease() = (%#v, %v), want zero delivery", delivery, err)
	}
}

func TestFileOperationStoreRejectsInvalidPersistedState(t *testing.T) {
	directory := t.TempDir()
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             "invalid-operation",
			IdempotencyKey: "invalid-operation",
			State:          sdk.OperationState("unknown"),
			Revision:       1,
		},
		Kind:     sdk.OperationKindTool,
		Resource: "tool",
		Input:    []byte(`{}`),
	}
	if err := writeJSONAtomic(
		context.Background(),
		directory,
		filepath.Join(directory, "operations.json"),
		".test-operations-*.tmp",
		"test operations",
		fileOperationState{
			SchemaVersion: operationStoreSchemaVersion,
			Operations: map[string]sdk.OperationRecord{
				record.Operation.ID: record,
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), record.Operation.ID); err == nil ||
		!strings.Contains(err.Error(), "invalid state") {
		t.Fatalf("Get() error = %v, want invalid persisted state", err)
	}
}

func TestFileDeliveryStoreRejectsInvalidPersistedState(t *testing.T) {
	directory := t.TempDir()
	delivery := sdk.Delivery{
		ID:           "invalid-delivery",
		Sequence:     1,
		Plugin:       "observer",
		Subscription: "events",
		Event: sdk.Event{
			ID:      "invalid-delivery-event",
			Name:    sdk.EventAgentStart,
			Payload: []byte(`{}`),
		},
		State: sdk.DeliveryState("unknown"),
	}
	if err := writeJSONAtomic(
		context.Background(),
		directory,
		filepath.Join(directory, "deliveries.json"),
		".test-deliveries-*.tmp",
		"test deliveries",
		fileDeliveryState{
			SchemaVersion: deliveryStoreSchemaVersion,
			NextSequence:  delivery.Sequence,
			Deliveries: map[string]sdk.Delivery{
				delivery.ID: delivery,
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileDeliveryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "invalid state") {
		t.Fatalf("List() error = %v, want invalid persisted state", err)
	}
}

func TestFileTrajectoryStoreRejectsInvalidPersistedParent(t *testing.T) {
	directory := t.TempDir()
	now := time.Now().UTC()
	trajectory := sdk.Trajectory{
		SchemaVersion: sdk.TrajectorySchemaVersion,
		ID:            "child",
		ParentID:      "../parent",
		ParentEntryID: "fork-point",
		CreatedAt:     now,
		UpdatedAt:     now,
		Entries:       []sdk.TrajectoryEntry{},
	}
	if err := writeJSONAtomic(
		t.Context(),
		directory,
		filepath.Join(directory, "child.json"),
		".test-trajectory-*.tmp",
		"test trajectory",
		trajectory,
	); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.LoadMetadata(t.Context(), trajectory.ID)
	want := sdk.ValidateResourceName("trajectory parent", trajectory.ParentID)
	if err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("LoadMetadata() error = %v, want %v", err, want)
	}
}
