package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInteractionStoreContract(t *testing.T) {
	factories := map[string]func(*testing.T) InteractionStore{
		"memory": func(*testing.T) InteractionStore {
			return NewMemoryInteractionStore()
		},
		"file": func(t *testing.T) InteractionStore {
			store, err := NewFileInteractionStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			defer store.Close(context.Background())
			created, err := store.Create(t.Context(), testInteraction())
			if err != nil {
				t.Fatal(err)
			}
			if created.State != InteractionPending || created.Revision != 1 {
				t.Fatalf("created = %#v", created)
			}
			waited := make(chan Interaction, 1)
			go func() {
				item, _ := store.Wait(
					context.Background(), created.SessionID, created.ID,
				)
				waited <- item
			}()
			select {
			case <-waited:
				t.Fatal("interaction wait returned before resolution")
			case <-time.After(25 * time.Millisecond):
			}
			resolved, err := store.Resolve(
				t.Context(), created.SessionID, created.ID, created.Revision,
				InteractionAnswer{OptionID: "yes"},
			)
			if err != nil || resolved.State != InteractionResolved {
				t.Fatalf("resolved = %#v, %v", resolved, err)
			}
			select {
			case item := <-waited:
				if item.Answer == nil || item.Answer.OptionID != "yes" {
					t.Fatalf("waited = %#v", item)
				}
			case <-time.After(time.Second):
				t.Fatal("interaction wait did not wake")
			}
		})
	}
}

func TestFileInteractionStorePersistsPrivateState(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileInteractionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(t.Context(), testInteraction()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileInteractionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if _, err := reopened.Get(t.Context(), "session-a", "interaction-a"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, "interactions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("interaction state mode = %o", info.Mode().Perm())
	}
}

func TestInteractionStoreCancellationWakesWaiter(t *testing.T) {
	for name, factory := range map[string]func(*testing.T) InteractionStore{
		"memory": func(*testing.T) InteractionStore { return NewMemoryInteractionStore() },
		"file": func(t *testing.T) InteractionStore {
			store, err := NewFileInteractionStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			defer store.Close(context.Background())
			created, err := store.Create(t.Context(), testInteraction())
			if err != nil {
				t.Fatal(err)
			}
			waited := make(chan Interaction, 1)
			go func() {
				item, _ := store.Wait(context.Background(), created.SessionID, created.ID)
				waited <- item
			}()
			cancelled, err := store.Cancel(
				t.Context(), created.SessionID, created.ID, created.Revision,
			)
			if err != nil || cancelled.State != InteractionCancelled ||
				cancelled.Answer != nil {
				t.Fatalf("cancelled = %#v, %v", cancelled, err)
			}
			select {
			case item := <-waited:
				if item.State != InteractionCancelled {
					t.Fatalf("waited = %#v", item)
				}
			case <-time.After(time.Second):
				t.Fatal("interaction wait did not wake after cancellation")
			}
		})
	}
}

func testInteraction() Interaction {
	return Interaction{
		ID: "interaction-a", SessionID: "session-a",
		ExecutionID: "execution-a", Kind: InteractionConfirmation,
		Prompt: "Continue?",
		Options: []InteractionOption{
			{ID: "yes", Label: "Yes"},
			{ID: "no", Label: "No"},
		},
	}
}
