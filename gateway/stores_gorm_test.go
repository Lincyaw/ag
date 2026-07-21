package gateway

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestGORMGatewayStoresMigrateLegacyControlState(t *testing.T) {
	ctx := t.Context()
	legacyRoot := t.TempDir()
	legacy := LegacyGatewayStoreDirectories{
		Sessions:     filepath.Join(legacyRoot, "control"),
		Inputs:       filepath.Join(legacyRoot, "inputs"),
		Interactions: filepath.Join(legacyRoot, "interactions"),
	}

	sessions, err := NewFileSessionStore(legacy.Sessions)
	if err != nil {
		t.Fatal(err)
	}
	session := testSession("migrated-session")
	session.RuntimeConfig = []byte(`{"provider":{"token":"private"}}`)
	createdSession, err := sessions.Create(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.Close(ctx); err != nil {
		t.Fatal(err)
	}

	inputs, err := NewFileInputStore(legacy.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := inputs.Enqueue(ctx, AgentInput{
		ID: "migrated-input", SessionID: session.ID, Content: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	acquired, ok, err := inputs.AcquireNext(ctx, session.ID)
	if err != nil || !ok {
		t.Fatalf("acquire legacy input = %#v, %v, %v", acquired, ok, err)
	}
	if err := inputs.Close(ctx); err != nil {
		t.Fatal(err)
	}

	interactions, err := NewFileInteractionStore(legacy.Interactions)
	if err != nil {
		t.Fatal(err)
	}
	interaction := testInteraction()
	interaction.SessionID = session.ID
	createdInteraction, err := interactions.Create(ctx, interaction)
	if err != nil {
		t.Fatal(err)
	}
	if err := interactions.Close(ctx); err != nil {
		t.Fatal(err)
	}

	rawURI := gormEventStoreTestURI(t, "unified-control")
	stores, err := NewGORMGatewayStores(ctx, rawURI, legacy)
	if err != nil {
		t.Fatal(err)
	}
	loadedSession, err := stores.Sessions.Get(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedSession.Revision != createdSession.Revision ||
		string(loadedSession.RuntimeConfig) != string(session.RuntimeConfig) {
		t.Fatalf("migrated session = %#v", loadedSession)
	}
	loadedInput, err := stores.Inputs.Get(ctx, session.ID, queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedInput.State != AgentInputDispatching ||
		loadedInput.Revision != acquired.Input.Revision ||
		loadedInput.Sequence != queued.Sequence {
		t.Fatalf("migrated input = %#v", loadedInput)
	}
	loadedInteraction, err := stores.Interactions.Get(
		ctx, session.ID, createdInteraction.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if loadedInteraction.State != InteractionPending ||
		loadedInteraction.Revision != createdInteraction.Revision {
		t.Fatalf("migrated interaction = %#v", loadedInteraction)
	}
	if _, err := stores.Events.Append(
		ctx, session.ID, testRuntimeEvent("unified-event", "same database"),
	); err != nil {
		t.Fatal(err)
	}

	sessionStore := stores.Sessions.(*gormSessionStore)
	for _, table := range []any{
		&gatewaySessionRow{}, &gatewayInputRow{}, &gatewayInteractionRow{},
		&gatewayEventRow{},
	} {
		if !sessionStore.db.Migrator().HasTable(table) {
			t.Fatalf("unified database is missing table for %T", table)
		}
	}
	if err := stores.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Migration is idempotent: legacy files remain a rollback aid but never
	// duplicate rows or reset newer SQL state on subsequent starts.
	reopened, err := NewGORMGatewayStores(ctx, rawURI, legacy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	page, err := reopened.Sessions.List(ctx, sdk.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != session.ID {
		t.Fatalf("sessions after repeated migration = %#v", page)
	}
	inputPage, err := reopened.Inputs.List(ctx, session.ID, InputQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputPage.Items) != 1 || inputPage.Items[0].ID != queued.ID {
		t.Fatalf("inputs after repeated migration = %#v", inputPage)
	}
}
