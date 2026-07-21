package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/gateway"
	gatewayclient "github.com/lincyaw/ag/gateway/client"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	gatewayEventPageSize = 1000
	gatewayPollInterval  = 250 * time.Millisecond
	gatewayCancelTimeout = 5 * time.Second
)

type gatewayEventSubscription interface {
	Next() (gateway.AgentEvent, error)
	Close() error
}

type gatewayFrontend interface {
	EnqueueInput(context.Context, string, string, string) (gateway.AgentInput, error)
	GetInput(context.Context, string, string) (gateway.AgentInput, error)
	CancelInput(context.Context, string, string, uint64) (gateway.AgentInput, error)
	ResolveInteraction(
		context.Context,
		string,
		string,
		uint64,
		gateway.InteractionAnswer,
	) (gateway.Interaction, error)
	GetExecution(context.Context, string, string) (gateway.Execution, error)
	ListEvents(context.Context, string, gateway.EventQuery) (gateway.EventPage, error)
	GetEventCursor(context.Context, string) (gateway.EventCursor, error)
	SubscribeEvents(context.Context, string, uint64) (gatewayEventSubscription, error)
}

type gatewayRPCFrontend struct {
	client *gatewayclient.Client
	view   *gatewayclient.View
}

func (frontend gatewayRPCFrontend) EnqueueInput(
	ctx context.Context,
	sessionID string,
	inputID string,
	content string,
) (gateway.AgentInput, error) {
	if frontend.view != nil {
		return frontend.view.EnqueueInput(ctx, inputID, content)
	}
	return frontend.client.EnqueueInput(ctx, sessionID, inputID, content)
}

func (frontend gatewayRPCFrontend) GetInput(
	ctx context.Context,
	sessionID string,
	inputID string,
) (gateway.AgentInput, error) {
	return frontend.client.GetInput(ctx, sessionID, inputID)
}

func (frontend gatewayRPCFrontend) CancelInput(
	ctx context.Context,
	sessionID string,
	inputID string,
	expectedRevision uint64,
) (gateway.AgentInput, error) {
	if frontend.view != nil {
		return frontend.view.CancelInput(ctx, inputID, expectedRevision)
	}
	return frontend.client.CancelInput(
		ctx,
		sessionID,
		inputID,
		expectedRevision,
	)
}

func (frontend gatewayRPCFrontend) ResolveInteraction(
	ctx context.Context,
	sessionID string,
	interactionID string,
	expectedRevision uint64,
	answer gateway.InteractionAnswer,
) (gateway.Interaction, error) {
	if frontend.view != nil {
		return frontend.view.ResolveInteraction(
			ctx, interactionID, expectedRevision, answer,
		)
	}
	return frontend.client.ResolveInteraction(
		ctx,
		sessionID,
		interactionID,
		expectedRevision,
		answer,
	)
}

func (frontend gatewayRPCFrontend) GetExecution(
	ctx context.Context,
	sessionID string,
	executionID string,
) (gateway.Execution, error) {
	return frontend.client.GetExecution(ctx, sessionID, executionID)
}

func (frontend gatewayRPCFrontend) ListEvents(
	ctx context.Context,
	sessionID string,
	query gateway.EventQuery,
) (gateway.EventPage, error) {
	return frontend.client.ListEvents(ctx, sessionID, query)
}

func (frontend gatewayRPCFrontend) GetEventCursor(
	ctx context.Context,
	sessionID string,
) (gateway.EventCursor, error) {
	return frontend.client.GetEventCursor(ctx, sessionID)
}

func (frontend gatewayRPCFrontend) SubscribeEvents(
	ctx context.Context,
	sessionID string,
	after uint64,
) (gatewayEventSubscription, error) {
	if frontend.view == nil {
		return nil, errors.New("trajectory RPC view is not open")
	}
	if frontend.view.Trajectory().ID != sessionID || frontend.view.Cursor() < after {
		return nil, fmt.Errorf(
			"trajectory RPC view cursor is %d for %s, expected %d for %s",
			frontend.view.Cursor(), frontend.view.Trajectory().ID, after, sessionID,
		)
	}
	return frontend.view, nil
}

type gatewayInteractiveSession struct {
	frontend  gatewayFrontend
	sessionID string
	observe   func(context.Context, sdk.Event)
}

func (session *gatewayInteractiveSession) ID() string {
	return session.sessionID
}

func (session *gatewayInteractiveSession) RespondInteraction(
	ctx context.Context,
	interaction gateway.Interaction,
	answer gateway.InteractionAnswer,
) error {
	_, err := session.frontend.ResolveInteraction(
		ctx,
		session.sessionID,
		interaction.ID,
		interaction.Revision,
		answer,
	)
	return err
}

func (session *gatewayInteractiveSession) Prompt(
	ctx context.Context,
	prompt string,
) (agentruntime.Result, error) {
	input, err := session.frontend.EnqueueInput(
		ctx,
		session.sessionID,
		sdk.NewID(),
		prompt,
	)
	if err != nil {
		return agentruntime.Result{}, fmt.Errorf("enqueue gateway input: %w", err)
	}
	return session.waitInput(ctx, input)
}

func (session *gatewayInteractiveSession) ResumeInput(
	ctx context.Context,
	inputID string,
) (agentruntime.Result, error) {
	input, err := session.frontend.GetInput(ctx, session.sessionID, inputID)
	if err != nil {
		return agentruntime.Result{}, fmt.Errorf("read gateway input %s: %w", inputID, err)
	}
	return session.waitInput(ctx, input)
}

func (session *gatewayInteractiveSession) waitInput(
	ctx context.Context,
	input gateway.AgentInput,
) (agentruntime.Result, error) {
	ticker := time.NewTicker(gatewayPollInterval)
	defer ticker.Stop()

	for {
		result, terminal, err := session.resultIfInputTerminal(ctx, input)
		if err != nil {
			return agentruntime.Result{}, err
		}
		if terminal {
			return result, nil
		}
		select {
		case <-ctx.Done():
			if errors.Is(context.Cause(ctx), errInteractiveDetached) {
				return agentruntime.Result{}, errInteractiveDetached
			}
			return agentruntime.Result{}, session.cancelInput(
				input.ID,
				ctx.Err(),
			)
		case <-ticker.C:
			current, err := session.frontend.GetInput(
				ctx,
				session.sessionID,
				input.ID,
			)
			if err != nil {
				continue
			}
			input = current
		}
	}
}

func (session *gatewayInteractiveSession) observeEvents(
	ctx context.Context,
	subscription gatewayEventSubscription,
) {
	defer subscription.Close()
	for {
		observed, err := subscription.Next()
		if err != nil {
			return
		}
		if session.observe != nil {
			session.observe(ctx, sdk.Event{
				ID: observed.ID, Name: observed.Name,
				SessionID: observed.SessionID, Generation: observed.Generation,
				Payload: append(json.RawMessage(nil), observed.Payload...),
			})
		}
	}
}

func (session *gatewayInteractiveSession) latestEventCursor(
	ctx context.Context,
) (uint64, error) {
	cursor, err := session.frontend.GetEventCursor(ctx, session.sessionID)
	return cursor.Sequence, err
}

func (session *gatewayInteractiveSession) cancelInput(
	inputID string,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), gatewayCancelTimeout)
	defer cancel()
	for {
		input, err := session.frontend.GetInput(ctx, session.sessionID, inputID)
		if err != nil {
			return errors.Join(cause, err)
		}
		if input.State.Terminal() {
			return cause
		}
		_, err = session.frontend.CancelInput(
			ctx,
			session.sessionID,
			input.ID,
			input.Revision,
		)
		if err == nil {
			return cause
		}
		if status.Code(err) != codes.Aborted {
			return errors.Join(cause, err)
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(cause, ctx.Err())
		case <-timer.C:
		}
	}
}

var _ gatewayFrontend = gatewayRPCFrontend{}

func (session *gatewayInteractiveSession) resultIfInputTerminal(
	ctx context.Context,
	input gateway.AgentInput,
) (agentruntime.Result, bool, error) {
	if input.ExecutionID == "" {
		if input.State == gateway.AgentInputCancelled {
			return agentruntime.Result{}, true, context.Canceled
		}
		if input.State == gateway.AgentInputFailed {
			return agentruntime.Result{}, true, errors.New(input.LastError)
		}
		return agentruntime.Result{}, false, nil
	}
	execution, err := session.frontend.GetExecution(
		ctx,
		session.sessionID,
		input.ExecutionID,
	)
	if err != nil {
		if input.State.Terminal() {
			return agentruntime.Result{}, true, fmt.Errorf(
				"read terminal gateway execution %s: %w",
				input.ExecutionID,
				err,
			)
		}
		return agentruntime.Result{}, false, nil
	}
	if !execution.Execution.Terminal() {
		if input.State.Terminal() {
			return agentruntime.Result{}, true, errors.New(
				"gateway input is terminal before its execution",
			)
		}
		return agentruntime.Result{}, false, nil
	}
	result, err := gatewayExecutionResult(execution)
	return result, true, err
}

func gatewayExecutionResult(execution gateway.Execution) (agentruntime.Result, error) {
	if execution.Result != nil {
		return *execution.Result, nil
	}
	switch execution.Execution.State {
	case sdk.TrajectoryExecutionCancelled:
		return agentruntime.Result{}, context.Canceled
	case sdk.TrajectoryExecutionFailed:
		message := execution.Execution.LastError
		if message == "" {
			message = "gateway execution failed"
		}
		return agentruntime.Result{}, errors.New(message)
	default:
		return agentruntime.Result{}, errors.New(
			"gateway execution completed without a result",
		)
	}
}
