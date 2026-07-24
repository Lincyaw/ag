package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestEventStoreContract(t *testing.T) {
	factories := map[string]func(*testing.T) EventStore{
		"memory": func(*testing.T) EventStore {
			return NewMemoryEventStore()
		},
		"file": func(t *testing.T) EventStore {
			store, err := NewFileEventStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"gorm-sqlite": func(t *testing.T) EventStore {
			store, err := NewGORMEventStore(
				t.Context(),
				gormEventStoreTestURI(t, "contract"),
			)
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			testEventStoreContract(t, factory(t))
		})
	}
}

func testEventStoreContract(t *testing.T, store EventStore) {
	t.Helper()
	ctx := t.Context()
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	first, err := store.Append(ctx, "session-a", testRuntimeEvent("event-a", "one"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || first.SessionID != "session-a" ||
		first.CreatedAt.IsZero() {
		t.Fatalf("first event = %#v", first)
	}
	first.Payload[0] = '['
	loaded, err := store.List(ctx, "session-a", EventQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Items) != 1 || string(loaded.Items[0].Payload) != `{"value":"one"}` {
		t.Fatalf("loaded events = %#v", loaded)
	}

	duplicate, err := store.Append(ctx, "session-a", testRuntimeEvent("event-a", "changed"))
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Sequence != 1 || string(duplicate.Payload) != `{"value":"one"}` {
		t.Fatalf("duplicate event = %#v", duplicate)
	}
	second, err := store.Append(ctx, "session-a", testRuntimeEvent("event-b", "two"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 2 {
		t.Fatalf("second event = %#v", second)
	}
	latest, err := store.Latest(ctx, "session-a")
	if err != nil || latest != 2 {
		t.Fatalf("latest sequence = %d, %v", latest, err)
	}

	page, err := store.List(ctx, "session-a", EventQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Next != 1 {
		t.Fatalf("first page = %#v", page)
	}
	page, err = store.List(ctx, "session-a", EventQuery{After: page.Next, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Sequence != 2 || page.Next != 2 {
		t.Fatalf("second page = %#v", page)
	}

	waited := make(chan EventPage, 1)
	waitErr := make(chan error, 1)
	go func() {
		page, err := store.Wait(context.Background(), "session-a", EventQuery{After: 2})
		if err != nil {
			waitErr <- err
			return
		}
		waited <- page
	}()
	if _, err := store.Append(ctx, "session-a", testRuntimeEvent("event-c", "three")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitErr:
		t.Fatal(err)
	case page := <-waited:
		if len(page.Items) != 1 || page.Items[0].Sequence != 3 {
			t.Fatalf("waited page = %#v", page)
		}
	case <-time.After(time.Second):
		t.Fatal("event wait did not wake after append")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Wait(cancelled, "session-a", EventQuery{After: 3}); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("cancelled wait error = %v", err)
	}
}

func TestFileEventStorePersistsPrivateState(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	store, err := NewFileEventStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "persistent", testRuntimeEvent("event-a", "one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileEventStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	page, err := reopened.List(ctx, "persistent", EventQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Sequence != 1 {
		t.Fatalf("persisted events = %#v", page)
	}
	latest, err := reopened.Latest(ctx, "persistent")
	if err != nil || latest != 1 {
		t.Fatalf("persisted latest sequence = %d, %v", latest, err)
	}
	info, err := os.Stat(filepath.Join(directory, "events.journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("event state mode = %o", info.Mode().Perm())
	}
}

func TestFileEventStoreCompactsJournalIntoSnapshot(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	opened, err := NewFileEventStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	store := opened.(*fileEventStore)
	for _, id := range []string{"event-a", "event-b"} {
		if _, err := store.Append(
			ctx,
			"persistent",
			testRuntimeEvent(id, id),
		); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	err = store.compactJournalLocked(ctx, true)
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, "events.journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("compacted journal size = %d, want 0", info.Size())
	}
	if _, err := os.Stat(filepath.Join(directory, "events.snapshot.json")); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileEventStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	page, err := reopened.List(ctx, "persistent", EventQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 ||
		page.Items[0].Sequence != 1 ||
		page.Items[1].Sequence != 2 {
		t.Fatalf("events after compact/reopen = %#v", page)
	}
}

func TestGORMEventStorePersistsPrivateState(t *testing.T) {
	ctx := t.Context()
	rawURI := gormEventStoreTestURI(t, "persistent")
	store, err := NewGORMEventStore(ctx, rawURI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(
		ctx,
		"persistent",
		testRuntimeEvent("event-a", "one"),
	); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewGORMEventStore(ctx, rawURI)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	page, err := reopened.List(ctx, "persistent", EventQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Sequence != 1 {
		t.Fatalf("persisted events = %#v", page)
	}
	latest, err := reopened.Latest(ctx, "persistent")
	if err != nil || latest != 1 {
		t.Fatalf("persisted latest sequence = %d, %v", latest, err)
	}
}

func TestGORMEventStoreSharesSQLiteDatabaseWithSessionState(t *testing.T) {
	ctx := t.Context()
	rawURI := gormEventStoreTestURI(t, "manager")
	events, err := NewGORMEventStore(ctx, rawURI)
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close(context.Background())
	factory, err := NewStorageSessionStateFactory(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	state, err := factory.Open(ctx, Session{ID: "trajectory-one"})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close(context.Background())
	if err := state.Trajectories().Create(
		ctx,
		sdk.Trajectory{ID: "trajectory-one"},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(
		ctx,
		"trajectory-one",
		testRuntimeEvent("event-a", "one"),
	); err != nil {
		t.Fatal(err)
	}

	for index := range 16 {
		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			errs <- state.Trajectories().Create(ctx, sdk.Trajectory{
				ID: fmt.Sprintf("trajectory-%d", index),
			})
		}()
		go func() {
			<-start
			_, err := events.Append(
				ctx,
				"trajectory-one",
				testRuntimeEvent(
					fmt.Sprintf("event-%d", index),
					"concurrent",
				),
			)
			errs <- err
		}()
		close(start)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("concurrent SQLite write %d: %v", index, err)
			}
		}
	}
}

func TestEventStorePagesLargePayloadsByEncodedBytes(t *testing.T) {
	store := NewMemoryEventStore()
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	ctx := t.Context()
	value := strings.Repeat("x", 2<<20)
	for index := range 3 {
		if _, err := store.Append(
			ctx,
			"large-events",
			testRuntimeEvent(fmt.Sprintf("large-%d", index), value),
		); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.List(ctx, "large-events", EventQuery{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || first.Next != 1 {
		t.Fatalf("first byte-bounded page = %#v", first)
	}
	second, err := store.List(
		ctx,
		"large-events",
		EventQuery{After: first.Next, Limit: 1000},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Next != 2 {
		t.Fatalf("second byte-bounded page = %#v", second)
	}
}

func TestRuntimeEventProjectionDoesNotDuplicateTurnConversation(t *testing.T) {
	payload, err := json.Marshal(sdk.TurnEndPayload{
		Turn: 42,
		Messages: []sdk.Message{
			{Role: sdk.RoleUser, Content: strings.Repeat("question", 1000)},
			{Role: sdk.RoleAssistant, Content: strings.Repeat("answer", 1000)},
		},
		Action: sdk.Action{Kind: sdk.ActionStep},
	})
	if err != nil {
		t.Fatal(err)
	}
	event, err := normalizeRuntimeEvent(sdk.Event{
		ID: "turn-end", Name: sdk.EventTurnEnd, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var projected sdk.TurnEndPayload
	if err := json.Unmarshal(event.Payload, &projected); err != nil {
		t.Fatal(err)
	}
	if projected.Turn != 42 || projected.Action.Kind != sdk.ActionStep {
		t.Fatalf("projected turn end = %#v", projected)
	}
	if len(projected.Messages) != 0 {
		t.Fatalf("projected messages = %#v", projected.Messages)
	}
	if len(event.Payload) >= len(payload)/10 {
		t.Fatalf(
			"projected payload remained too large: got %d bytes from %d",
			len(event.Payload),
			len(payload),
		)
	}
}

func gormEventStoreTestURI(t *testing.T, namespace string) string {
	t.Helper()
	return (&url.URL{
		Scheme: "sqlite",
		Path:   filepath.Join(t.TempDir(), "events.db"),
		RawQuery: url.Values{
			"namespace": {namespace},
		}.Encode(),
	}).String()
}

func testRuntimeEvent(id string, value string) sdk.Event {
	payload, _ := json.Marshal(map[string]string{"value": value})
	return sdk.Event{
		ID: id, Name: sdk.EventTurnStart, SessionID: "session-a",
		Generation: 1, Payload: payload,
	}
}
