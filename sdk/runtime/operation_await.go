package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
)

// pollOperation is the narrow read boundary used while awaiting async work.
type pollOperation func(context.Context, string, uint64) (sdk.Operation, error)
type watchOperation func(context.Context, string, uint64) (sdk.Operation, error)
type cancelOperation func(context.Context, string) (sdk.Operation, error)

// operationAwait groups one accepted operation snapshot with the resource
// lifecycle functions needed to wait, validate, and cancel it.
type operationAwait struct {
	expectedIdempotencyKey string
	initial                sdk.Operation
	poll                   pollOperation
	watch                  watchOperation
	cancel                 cancelOperation
}

func operationAwaitForRequest(
	request sdk.OperationRequest,
	initial sdk.Operation,
	poll pollOperation,
	cancel cancelOperation,
	watch ...watchOperation,
) operationAwait {
	var watcher watchOperation
	if len(watch) > 0 {
		watcher = watch[0]
	}
	return operationAwait{
		expectedIdempotencyKey: request.IdempotencyKey,
		initial:                initial,
		poll:                   poll,
		watch:                  watcher,
		cancel:                 cancel,
	}
}

func operationWatcher(resource any) watchOperation {
	watcher, ok := resource.(sdk.OperationWatcher)
	if !ok {
		return nil
	}
	return watcher.WatchOperation
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
		next, err := runtime.observeOperation(ctx, await, current)
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
		return sdk.Operation{}, &sdk.OperationTerminalError{
			Operation: sdk.CloneOperation(current),
		}
	case sdk.OperationCancelled:
		return sdk.Operation{}, &sdk.OperationTerminalError{
			Operation: sdk.CloneOperation(current),
		}
	default:
		return sdk.Operation{}, fmt.Errorf(
			"operation %q reached unsupported terminal state %q",
			current.ID,
			current.State,
		)
	}
}

func (runtime *Runtime) observeOperation(
	ctx context.Context,
	await operationAwait,
	current sdk.Operation,
) (sdk.Operation, error) {
	if await.watch != nil {
		next, err := await.watch(ctx, current.ID, current.Revision)
		if err == nil {
			if sameOperationObservation(current, next) && !next.Terminal() {
				if err := ctx.Err(); err != nil {
					return sdk.Operation{}, runtime.cancelAwaitedOperation(
						ctx,
						await,
						current,
					)
				}
				return runtime.pollAfterUnchangedWatch(ctx, await, current)
			}
			return next, nil
		}
		if ctx.Err() != nil {
			return sdk.Operation{}, runtime.cancelAwaitedOperation(
				ctx,
				await,
				current,
			)
		}
		return sdk.Operation{}, err
	}
	return runtime.waitAndPollAwaitedOperation(ctx, await, current)
}

func sameOperationObservation(left sdk.Operation, right sdk.Operation) bool {
	return left.ID == right.ID &&
		left.IdempotencyKey == right.IdempotencyKey &&
		left.State == right.State &&
		left.Revision == right.Revision &&
		left.Error == right.Error &&
		bytes.Equal(left.Output, right.Output)
}

func (runtime *Runtime) waitAndPollAwaitedOperation(
	ctx context.Context,
	await operationAwait,
	current sdk.Operation,
) (sdk.Operation, error) {
	if err := runtime.waitOrCancelAwaitedOperation(
		ctx,
		await,
		current,
	); err != nil {
		return sdk.Operation{}, err
	}
	return runtime.pollAwaitedOperation(ctx, await, current)
}

func (runtime *Runtime) pollAwaitedOperation(
	ctx context.Context,
	await operationAwait,
	current sdk.Operation,
) (sdk.Operation, error) {
	next, err := await.poll(ctx, current.ID, current.Revision)
	if err != nil && ctx.Err() != nil {
		return sdk.Operation{}, runtime.cancelAwaitedOperation(
			ctx,
			await,
			current,
		)
	}
	return next, err
}

func (runtime *Runtime) pollAfterUnchangedWatch(
	ctx context.Context,
	await operationAwait,
	current sdk.Operation,
) (sdk.Operation, error) {
	next, err := runtime.pollAwaitedOperation(ctx, await, current)
	if err != nil {
		return sdk.Operation{}, err
	}
	if sameOperationObservation(current, next) && !next.Terminal() {
		if err := runtime.waitOrCancelAwaitedOperation(
			ctx,
			await,
			current,
		); err != nil {
			return sdk.Operation{}, err
		}
	}
	return next, nil
}

func (runtime *Runtime) waitOrCancelAwaitedOperation(
	ctx context.Context,
	await operationAwait,
	current sdk.Operation,
) error {
	if waitContext(ctx, runtime.operation.poll) {
		return nil
	}
	return runtime.cancelAwaitedOperation(ctx, await, current)
}

func (runtime *Runtime) cancelAwaitedOperation(
	ctx context.Context,
	await operationAwait,
	current sdk.Operation,
) error {
	if runtime.operationRecoveryHandoffActive() {
		return ctx.Err()
	}
	cancelCtx, cancelFunc := lifecycle.WithDetachedTimeout(
		ctx,
		runtime.operation.effectiveCancelTimeout(),
	)
	defer cancelFunc()
	_, cancelErr := await.cancel(cancelCtx, current.ID)
	return errors.Join(ctx.Err(), cancelErr)
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
	watch ...watchOperation,
) (T, error) {
	return awaitOperationJSON[T](
		runtime,
		ctx,
		operationAwaitForRequest(request, initial, poll, cancel, watch...),
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
	watch ...watchOperation,
) (json.RawMessage, error) {
	return awaitOperationRawJSON(
		runtime,
		ctx,
		operationAwaitForRequest(request, initial, poll, cancel, watch...),
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
