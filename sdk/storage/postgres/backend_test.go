package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const postgresTestDSNEnvironment = "AG_TEST_POSTGRES_DSN"
const postgresCrashHelperEnvironment = "AG_TEST_POSTGRES_CRASH_HELPER"

func openPostgresTestBackend(
	t *testing.T,
	namespace string,
) *Backend {
	t.Helper()
	dsn := os.Getenv(postgresTestDSNEnvironment)
	if dsn == "" {
		t.Skip(
			"set AG_TEST_POSTGRES_DSN to run PostgreSQL integration tests",
		)
	}
	backend, err := Open(t.Context(), Config{
		ConnectionString: dsn,
		Namespace:        namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL test backend: %v", err)
		}
	})
	return backend
}

func testTrajectoryEntry(
	id string,
	parent string,
	kind sdk.TrajectoryKind,
) sdk.TrajectoryEntry {
	return sdk.TrajectoryEntry{
		ID:        id,
		ParentID:  parent,
		Kind:      kind,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"test":true}`),
	}
}

func testDelivery(id string, eventID string) sdk.Delivery {
	now := time.Now().UTC()
	return sdk.Delivery{
		ID:           id,
		Plugin:       "test-plugin",
		Subscription: "test-subscription",
		Partition:    "test-partition",
		Event: sdk.Event{
			ID:      eventID,
			Name:    "test.event",
			Payload: json.RawMessage(`{"ok":true}`),
		},
		CreatedAt: now,
	}
}

func TestBackendSharesDurableStateAcrossIndependentPools(t *testing.T) {
	namespace := "multipool-" + sdk.NewID()
	first := openPostgresTestBackend(t, namespace)
	second := openPostgresTestBackend(t, namespace)
	capabilities := sdk.InspectStorageCapabilities(first)
	if !capabilities.Durable ||
		!capabilities.MultiProcessSafe ||
		!capabilities.AtomicState {
		t.Fatalf("capabilities = %#v", capabilities)
	}
	trajectoryID := "shared-trajectory"
	if err := first.Trajectories().Create(
		t.Context(),
		sdk.Trajectory{ID: trajectoryID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Trajectories().BeginExecution(
		t.Context(),
		trajectoryID,
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "shared-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		testTrajectoryEntry(
			"shared-input",
			"",
			sdk.TrajectoryKindUserMessage,
		),
	); err != nil {
		t.Fatal(err)
	}
	recoverable, err := second.Trajectories().ListRecoverable(
		t.Context(),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 ||
		recoverable[0].ID != trajectoryID {
		t.Fatalf("recoverable = %#v", recoverable)
	}
	if _, err := second.Trajectories().ClaimExecution(
		t.Context(),
		trajectoryID,
		"second-process",
		time.Now().UTC(),
		time.Minute,
	); err != nil {
		t.Fatal(err)
	}

	operation, created, err := first.Operations().Submit(
		t.Context(),
		sdk.OperationRecord{
			Operation: sdk.Operation{
				IdempotencyKey: "shared-operation-key",
			},
			Kind:     sdk.OperationKindTool,
			Resource: "shared-tool",
			Input:    json.RawMessage(`{"input":1}`),
		},
	)
	if err != nil || !created {
		t.Fatalf("submit operation: created=%t err=%v", created, err)
	}
	if _, err := second.Operations().Get(
		t.Context(),
		operation.Operation.ID,
	); err != nil {
		t.Fatal(err)
	}

	firstQueue, err := first.Deliveries("shared-queue")
	if err != nil {
		t.Fatal(err)
	}
	secondQueue, err := second.Deliveries("shared-queue")
	if err != nil {
		t.Fatal(err)
	}
	if err := firstQueue.Enqueue(
		t.Context(),
		testDelivery("shared-delivery", "shared-event"),
	); err != nil {
		t.Fatal(err)
	}
	leased, err := secondQueue.Lease(
		t.Context(),
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if leased.ID != "shared-delivery" {
		t.Fatalf("leased delivery = %#v", leased)
	}
}

func TestCommitExecutionRollsBackTrajectoryAndOutboxOnConflict(
	t *testing.T,
) {
	namespace := "atomic-" + sdk.NewID()
	backend := openPostgresTestBackend(t, namespace)
	ctx := t.Context()
	trajectoryID := "atomic-trajectory"
	if err := backend.Trajectories().Create(
		ctx,
		sdk.Trajectory{ID: trajectoryID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Trajectories().BeginExecution(
		ctx,
		trajectoryID,
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "atomic-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		testTrajectoryEntry(
			"atomic-input",
			"",
			sdk.TrajectoryKindUserMessage,
		),
	); err != nil {
		t.Fatal(err)
	}
	execution, err := backend.Trajectories().ClaimExecution(
		ctx,
		trajectoryID,
		"atomic-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := backend.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(
		ctx,
		testDelivery("atomic-conflict", "original-event"),
	); err != nil {
		t.Fatal(err)
	}
	checkpoint := testTrajectoryEntry(
		"atomic-checkpoint",
		"atomic-input",
		sdk.TrajectoryKindCheckpoint,
	)
	mutation := sdk.ExecutionMutationCommit{
		Trajectory: sdk.TrajectoryExecutionCommit{
			TrajectoryID: trajectoryID,
			ExecutionID:  execution.ID,
			LeaseToken:   execution.LeaseToken,
			ExpectedHead: "atomic-input",
			Entries:      []sdk.TrajectoryEntry{checkpoint},
		},
		Outbox: []sdk.StateMutationDeliveries{{
			Queue: sdk.HostOutboxQueue,
			Deliveries: []sdk.Delivery{
				testDelivery(
					"atomic-conflict",
					"different-event",
				),
			},
		}},
	}
	if _, err := backend.CommitExecution(ctx, mutation); err == nil {
		t.Fatal("conflicting outbox delivery unexpectedly committed")
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		ctx,
		trajectoryID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Head != "atomic-input" {
		t.Fatalf("trajectory head after rollback = %q", metadata.Head)
	}
	outboxItems, err := outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(outboxItems) != 1 ||
		outboxItems[0].ID != "atomic-conflict" ||
		outboxItems[0].Event.ID != "original-event" {
		t.Fatalf("outbox after rollback = %#v", outboxItems)
	}

	mutation.Outbox[0].Deliveries[0] = testDelivery(
		"atomic-result",
		"atomic-result-event",
	)
	result, err := backend.CommitExecution(ctx, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if result.Trajectory.Head != "atomic-checkpoint" {
		t.Fatalf("atomic result = %#v", result)
	}
	outboxItems, err = outbox.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(outboxItems) != 2 {
		t.Fatalf("committed outbox = %#v", outboxItems)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	first := openPostgresTestBackend(t, "isolation-a-"+sdk.NewID())
	second := openPostgresTestBackend(t, "isolation-b-"+sdk.NewID())
	if err := first.Trajectories().Create(
		t.Context(),
		sdk.Trajectory{ID: "isolated"},
	); err != nil {
		t.Fatal(err)
	}
	_, err := second.Trajectories().LoadMetadata(
		t.Context(),
		"isolated",
	)
	if !errors.Is(err, sdk.ErrTrajectoryNotFound) {
		t.Fatalf("other namespace load error = %v", err)
	}
}

func TestConcurrentPoolsFenceClaims(t *testing.T) {
	namespace := "fencing-" + sdk.NewID()
	first := openPostgresTestBackend(t, namespace)
	second := openPostgresTestBackend(t, namespace)
	ctx := t.Context()
	operation, _, err := first.Operations().Submit(
		ctx,
		sdk.OperationRecord{
			Operation: sdk.Operation{
				IdempotencyKey: "fenced-operation",
			},
			Kind:     sdk.OperationKindTool,
			Resource: "fenced-tool",
			Input:    json.RawMessage(`{}`),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	stores := []sdk.OperationStore{
		first.Operations(),
		second.Operations(),
	}
	results := make(chan error, len(stores))
	var wait sync.WaitGroup
	now := time.Now().UTC()
	for index, store := range stores {
		wait.Add(1)
		go func(index int, store sdk.OperationStore) {
			defer wait.Done()
			_, err := store.Claim(
				context.Background(),
				operation.Operation.ID,
				"worker-"+string(rune('a'+index)),
				now,
				time.Minute,
			)
			results <- err
		}(index, store)
	}
	wait.Wait()
	close(results)
	successes := 0
	claimed := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, sdk.ErrOperationClaimed):
			claimed++
		default:
			t.Fatalf("concurrent claim error = %v", err)
		}
	}
	if successes != 1 || claimed != 1 {
		t.Fatalf(
			"concurrent claims: successes=%d claimed=%d",
			successes,
			claimed,
		)
	}
}

func TestConcurrentPoolsSerializeDeliverySequences(t *testing.T) {
	namespace := "delivery-sequence-" + sdk.NewID()
	first := openPostgresTestBackend(t, namespace)
	second := openPostgresTestBackend(t, namespace)
	firstQueue, err := first.Deliveries("concurrent-queue")
	if err != nil {
		t.Fatal(err)
	}
	secondQueue, err := second.Deliveries("concurrent-queue")
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errorsByWorker := make(chan error, 2)
	for index, queue := range []sdk.DeliveryStore{firstQueue, secondQueue} {
		go func(index int, queue sdk.DeliveryStore) {
			<-start
			delivery := testDelivery(
				fmt.Sprintf("concurrent-delivery-%d", index),
				fmt.Sprintf("concurrent-event-%d", index),
			)
			delivery.Partition = fmt.Sprintf("partition-%d", index)
			errorsByWorker <- queue.Enqueue(context.Background(), delivery)
		}(index, queue)
	}
	close(start)
	for range 2 {
		if err := <-errorsByWorker; err != nil {
			t.Fatal(err)
		}
	}

	deliveries, err := firstQueue.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 ||
		deliveries[0].Sequence != 1 ||
		deliveries[1].Sequence != 2 {
		t.Fatalf("concurrent delivery order = %#v", deliveries)
	}
}

func TestOperationAndDeliveryContracts(t *testing.T) {
	backend := openPostgresTestBackend(
		t,
		"contracts-"+sdk.NewID(),
	)
	ctx := t.Context()
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			IdempotencyKey: "contract-operation",
		},
		Kind:     sdk.OperationKindProvider,
		Resource: "contract-provider",
		Input:    json.RawMessage(`{"input":1}`),
	}
	first, created, err := backend.Operations().Submit(ctx, record)
	if err != nil || !created {
		t.Fatalf("first submit: created=%t err=%v", created, err)
	}
	second, created, err := backend.Operations().Submit(ctx, record)
	if err != nil || created || second.Operation.ID != first.Operation.ID {
		t.Fatalf(
			"idempotent submit: record=%#v created=%t err=%v",
			second,
			created,
			err,
		)
	}
	collision := record
	collision.Input = json.RawMessage(`{"input":2}`)
	if _, _, err := backend.Operations().Submit(
		ctx,
		collision,
	); err == nil {
		t.Fatal("idempotency input collision unexpectedly succeeded")
	}
	claimed, err := backend.Operations().Claim(
		ctx,
		first.Operation.ID,
		"contract-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := backend.Operations().Renew(
		ctx,
		first.Operation.ID,
		claimed.Execution.Token,
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Operations().Complete(
		ctx,
		first.Operation.ID,
		"stale-token",
		sdk.OperationSucceeded,
		json.RawMessage(`{}`),
		"",
	); !errors.Is(err, sdk.ErrOperationFence) {
		t.Fatalf("stale completion error = %v", err)
	}
	completed, err := backend.Operations().Complete(
		ctx,
		first.Operation.ID,
		renewed.Execution.Token,
		sdk.OperationSucceeded,
		json.RawMessage(`{"done":true}`),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Operation.State != sdk.OperationSucceeded {
		t.Fatalf("completed operation = %#v", completed)
	}

	queue, err := backend.Deliveries("contract-queue")
	if err != nil {
		t.Fatal(err)
	}
	firstDelivery := testDelivery("contract-first", "contract-event-1")
	secondDelivery := testDelivery("contract-second", "contract-event-2")
	secondDelivery.Partition = firstDelivery.Partition
	otherDelivery := testDelivery("contract-other", "contract-event-3")
	otherDelivery.Partition = "other-partition"
	if err := queue.Enqueue(
		ctx,
		firstDelivery,
		secondDelivery,
		otherDelivery,
	); err != nil {
		t.Fatal(err)
	}
	leaseOne, err := queue.Lease(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if leaseOne.ID != firstDelivery.ID {
		t.Fatalf("first lease = %#v", leaseOne)
	}
	leaseTwo, err := queue.Lease(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if leaseTwo.ID != otherDelivery.ID {
		t.Fatalf(
			"partition FIFO allowed second item early: %#v",
			leaseTwo,
		)
	}
	if err := queue.Retry(
		ctx,
		leaseOne.ID,
		leaseOne.LeaseToken,
		time.Now().UTC().Add(-time.Second),
		"retry",
	); err != nil {
		t.Fatal(err)
	}
	retried, err := queue.Lease(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != firstDelivery.ID || retried.Attempt != 2 {
		t.Fatalf("retried delivery = %#v", retried)
	}
	if err := queue.Ack(
		ctx,
		retried.ID,
		retried.LeaseToken,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	next, err := queue.Lease(ctx, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != secondDelivery.ID {
		t.Fatalf("next partition item = %#v", next)
	}
}

func TestPendingExecutionSurvivesAbruptProcessExit(t *testing.T) {
	if helper := os.Getenv(postgresCrashHelperEnvironment); helper != "" {
		parts := strings.SplitN(helper, "\n", 2)
		if len(parts) != 2 {
			t.Fatal("invalid PostgreSQL crash helper configuration")
		}
		backend, err := Open(context.Background(), Config{
			ConnectionString: parts[0],
			Namespace:        parts[1],
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Trajectories().Create(
			context.Background(),
			sdk.Trajectory{ID: "crash-trajectory"},
		); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Trajectories().BeginExecution(
			context.Background(),
			"crash-trajectory",
			"",
			sdk.TrajectoryExecutionStart{
				ID:       "crash-execution",
				Provider: "test-provider",
				MaxTurns: 2,
			},
			testTrajectoryEntry(
				"crash-input",
				"",
				sdk.TrajectoryKindUserMessage,
			),
		); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	dsn := os.Getenv(postgresTestDSNEnvironment)
	if dsn == "" {
		t.Skip(
			"set AG_TEST_POSTGRES_DSN to run PostgreSQL integration tests",
		)
	}
	namespace := "crash-" + sdk.NewID()
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestPendingExecutionSurvivesAbruptProcessExit$",
	)
	command.Env = append(
		os.Environ(),
		postgresCrashHelperEnvironment+"="+dsn+"\n"+namespace,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("PostgreSQL crash helper: %v\n%s", err, output)
	}
	backend := openPostgresTestBackend(t, namespace)
	recoverable, err := backend.Trajectories().ListRecoverable(
		t.Context(),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 ||
		recoverable[0].ID != "crash-trajectory" ||
		recoverable[0].Execution == nil ||
		recoverable[0].Execution.State !=
			sdk.TrajectoryExecutionPending {
		t.Fatalf("recoverable after crash = %#v", recoverable)
	}
}
