package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
)

// pollOperation is the narrow read boundary used while awaiting async work.
type pollOperation func(context.Context, string, uint64) (sdk.Operation, error)
type cancelOperation func(context.Context, string) (sdk.Operation, error)

// operationAwait groups one accepted operation snapshot with the resource
// lifecycle functions needed to wait, validate, and cancel it.
type operationAwait struct {
	expectedIdempotencyKey string
	initial                sdk.Operation
	poll                   pollOperation
	cancel                 cancelOperation
}

func operationAwaitForRequest(
	request sdk.OperationRequest,
	initial sdk.Operation,
	poll pollOperation,
	cancel cancelOperation,
) operationAwait {
	return operationAwait{
		expectedIdempotencyKey: request.IdempotencyKey,
		initial:                initial,
		poll:                   poll,
		cancel:                 cancel,
	}
}

func (runtime *Runtime) awaitOperation(
	ctx context.Context,
	await operationAwait,
) (sdk.Operation, error) {
	current := await.initial
	if err := sdk.ValidateOperation(current); err != nil {
		return sdk.Operation{}, err
	}
	if await.expectedIdempotencyKey == "" {
		return sdk.Operation{}, errors.New(
			"operation idempotency key is empty",
		)
	}
	if current.IdempotencyKey != await.expectedIdempotencyKey {
		return sdk.Operation{}, fmt.Errorf(
			"operation %q returned idempotency key %q, expected %q",
			current.ID,
			current.IdempotencyKey,
			await.expectedIdempotencyKey,
		)
	}
	if await.poll == nil {
		return sdk.Operation{}, errors.New("operation poll function is nil")
	}
	if await.cancel == nil {
		return sdk.Operation{}, errors.New("operation cancel function is nil")
	}
	for !current.Terminal() {
		if !waitContext(ctx, runtime.operation.poll) {
			runtime.mu.Lock()
			closed := runtime.closed
			runtime.mu.Unlock()
			if closed {
				return sdk.Operation{}, ctx.Err()
			}
			cancelCtx, cancelFunc := lifecycle.WithDetachedTimeout(
				ctx,
				2*time.Second,
			)
			defer cancelFunc()
			_, cancelErr := await.cancel(cancelCtx, current.ID)
			return sdk.Operation{}, errors.Join(ctx.Err(), cancelErr)
		}
		next, err := await.poll(ctx, current.ID, current.Revision)
		if err != nil {
			return sdk.Operation{}, err
		}
		if err := sdk.ValidateOperationProgress(current, next); err != nil {
			return sdk.Operation{}, err
		}
		current = next
	}
	switch current.State {
	case sdk.OperationSucceeded:
		return current, nil
	case sdk.OperationFailed:
		return sdk.Operation{}, fmt.Errorf(
			"operation %q failed: %s",
			current.ID,
			current.Error,
		)
	case sdk.OperationCancelled:
		return sdk.Operation{}, fmt.Errorf("operation %q was cancelled", current.ID)
	default:
		return sdk.Operation{}, fmt.Errorf(
			"operation %q reached unsupported terminal state %q",
			current.ID,
			current.State,
		)
	}
}

func awaitOperationJSON[T any](
	runtime *Runtime,
	ctx context.Context,
	await operationAwait,
	awaitLabel string,
	decodeLabel string,
) (T, error) {
	var result T
	operation, err := runtime.awaitOperation(ctx, await)
	if err != nil {
		return result, wrapOperationLabel(awaitLabel, err)
	}
	if err := json.Unmarshal(operation.Output, &result); err != nil {
		return result, fmt.Errorf("decode %s: %w", decodeLabel, err)
	}
	return result, nil
}

func awaitOperationRequestJSON[T any](
	runtime *Runtime,
	ctx context.Context,
	request sdk.OperationRequest,
	initial sdk.Operation,
	poll pollOperation,
	cancel cancelOperation,
	awaitLabel string,
	decodeLabel string,
) (T, error) {
	return awaitOperationJSON[T](
		runtime,
		ctx,
		operationAwaitForRequest(request, initial, poll, cancel),
		awaitLabel,
		decodeLabel,
	)
}

func awaitOperationRawJSON(
	runtime *Runtime,
	ctx context.Context,
	await operationAwait,
	awaitLabel string,
	outputLabel string,
) (json.RawMessage, error) {
	operation, err := runtime.awaitOperation(ctx, await)
	if err != nil {
		return nil, wrapOperationLabel(awaitLabel, err)
	}
	output := operation.Output
	if !json.Valid(output) {
		return nil, fmt.Errorf("%s returned invalid JSON", outputLabel)
	}
	return append(json.RawMessage(nil), output...), nil
}

func awaitOperationRequestRawJSON(
	runtime *Runtime,
	ctx context.Context,
	request sdk.OperationRequest,
	initial sdk.Operation,
	poll pollOperation,
	cancel cancelOperation,
	awaitLabel string,
	outputLabel string,
) (json.RawMessage, error) {
	return awaitOperationRawJSON(
		runtime,
		ctx,
		operationAwaitForRequest(request, initial, poll, cancel),
		awaitLabel,
		outputLabel,
	)
}

func wrapOperationLabel(label string, err error) error {
	if label == "" || err == nil {
		return err
	}
	return fmt.Errorf("%s: %w", label, err)
}
