package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type InteractionManager struct {
	store  InteractionStore
	events EventStore
}

func NewInteractionManager(
	store InteractionStore,
	events EventStore,
) (*InteractionManager, error) {
	if store == nil {
		return nil, errors.New("gateway interaction store is nil")
	}
	if events == nil {
		return nil, errors.New("gateway interaction event store is nil")
	}
	return &InteractionManager{store: store, events: events}, nil
}

func (manager *InteractionManager) Request(
	ctx context.Context,
	request Interaction,
) (InteractionAnswer, error) {
	created, err := manager.store.Create(ctx, request)
	if err != nil {
		return InteractionAnswer{}, err
	}
	if created.State == InteractionPending {
		manager.emit(ctx, created.SessionID, GatewayEventInteractionRequested, created)
	}
	resolved, err := manager.store.Wait(ctx, created.SessionID, created.ID)
	if err != nil {
		manager.cancelPending(created.SessionID, created.ID)
		return InteractionAnswer{}, err
	}
	if resolved.State != InteractionResolved || resolved.Answer == nil {
		return InteractionAnswer{}, errors.New("gateway interaction was cancelled")
	}
	return *resolved.Answer, nil
}

func (manager *InteractionManager) cancelPending(sessionID, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	current, err := manager.store.Get(ctx, sessionID, id)
	if err != nil || current.State != InteractionPending {
		return
	}
	cancelled, err := manager.store.Cancel(
		ctx,
		sessionID,
		id,
		current.Revision,
	)
	if err == nil {
		manager.emit(
			ctx,
			sessionID,
			GatewayEventInteractionCancelled,
			cancelled,
		)
	}
}

func (manager *InteractionManager) Get(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	return manager.store.Get(ctx, sessionID, id)
}

func (manager *InteractionManager) List(
	ctx context.Context,
	sessionID string,
	query InteractionQuery,
) (InteractionPage, error) {
	return manager.store.List(ctx, sessionID, query)
}

func (manager *InteractionManager) Resolve(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
	answer InteractionAnswer,
) (Interaction, error) {
	resolved, err := manager.store.Resolve(
		ctx,
		sessionID,
		id,
		expectedRevision,
		answer,
	)
	if err == nil {
		manager.emit(ctx, sessionID, GatewayEventInteractionResolved, resolved)
	}
	return resolved, err
}

func (manager *InteractionManager) emit(
	ctx context.Context,
	sessionID string,
	name string,
	payload any,
) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = manager.events.Append(ctx, sessionID, sdk.Event{
		ID: managerEventID(name, raw), Name: name,
		SessionID: sessionID, Payload: raw,
	})
}

func (manager *InteractionManager) Close(ctx context.Context) error {
	if manager == nil {
		return nil
	}
	return manager.store.Close(ctx)
}
