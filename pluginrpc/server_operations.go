package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
)

func (server *server) submitStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	if !server.reserveOperation() {
		return sdk.Operation{}, errors.New("RPC server is closed")
	}
	target := server.operationTarget(kind, resource)
	operation, err := server.operationHost().SubmitReserved(
		ctx,
		target,
		request,
		server.executeLocal,
		server.wait.Done,
	)
	if err != nil {
		return sdk.Operation{}, err
	}
	return operation, nil
}

func (server *server) getStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	record, err := server.operations.Get(ctx, id)
	if err != nil {
		return sdk.Operation{}, err
	}
	if err := server.operationTarget(kind, resource).Validate(record); err != nil {
		return sdk.Operation{}, err
	}
	return record.Operation, nil
}

func (server *server) cancelStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	cancelled, err := server.operationHost().Cancel(
		ctx,
		id,
		server.operationTarget(kind, resource).ValidateTarget,
	)
	if err != nil {
		return sdk.Operation{}, err
	}
	return cancelled.Operation, nil
}

func (server *server) recoverOperations(ctx context.Context) error {
	candidates, err := operationworker.ListRecoveryCandidates(
		ctx,
		server.operations,
		time.Now().UTC(),
	)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.Delay > 0 {
			if server.reserveOperation() {
				server.startReservedRecovery(ctx, candidate)
			}
			continue
		}
		if err := server.recoverOperation(ctx, candidate.OperationID); err != nil {
			return err
		}
	}
	return nil
}

func (server *server) recoverOperation(
	ctx context.Context,
	operationID string,
) error {
	current, err := server.operations.Get(ctx, operationID)
	if err != nil {
		return fmt.Errorf(
			"recover operation %q: %w",
			operationID,
			err,
		)
	}
	candidate, ok := operationworker.RecoveryCandidateFromRecord(
		current,
		time.Now().UTC(),
	)
	if !ok {
		return nil
	}
	if candidate.Delay > 0 {
		if server.reserveOperation() {
			server.startReservedRecovery(ctx, candidate)
		}
		return nil
	}
	record, failed, err := operationworker.FailInvalid(
		ctx,
		server.operations,
		operationID,
		server.validateOperationRevision,
	)
	if err != nil {
		return fmt.Errorf(
			"recover operation %q: %w",
			operationID,
			err,
		)
	}
	if record.Operation.Terminal() || failed {
		return nil
	}
	if server.reserveOperation() {
		server.startReservedOperation(ctx, operationID)
	}
	return nil
}

func (server *server) startReservedRecovery(
	parent context.Context,
	candidate operationworker.RecoveryCandidate,
) {
	go func() {
		defer server.wait.Done()
		defer server.recoverReservedRecoveryPanic(
			lifecycle.Detached(parent),
			candidate.OperationID,
		)
		if err := candidate.Wait(server.context); err != nil {
			return
		}
		recoveryCtx, cancelRecovery := context.WithCancel(
			lifecycle.Detached(parent),
		)
		stopServerCancel := context.AfterFunc(server.context, cancelRecovery)
		defer func() {
			stopServerCancel()
			cancelRecovery()
		}()
		if err := server.recoverOperation(
			recoveryCtx,
			candidate.OperationID,
		); err != nil {
			server.logger.WarnContext(
				recoveryCtx,
				"recover operation",
				"operation_id",
				candidate.OperationID,
				"error",
				err,
			)
		}
	}()
}

func (server *server) recoverReservedRecoveryPanic(
	ctx context.Context,
	operationID string,
) {
	if recovered := recover(); recovered != nil {
		server.logger.ErrorContext(
			ctx,
			"recover operation panic",
			"operation_id",
			operationID,
			"panic",
			recovered,
			"stack",
			string(debug.Stack()),
		)
	}
}

func (server *server) reserveOperation() bool {
	server.lifecycleMu.Lock()
	defer server.lifecycleMu.Unlock()
	if server.closed {
		return false
	}
	server.wait.Add(1)
	return true
}

func (server *server) startReservedOperation(parent context.Context, id string) {
	if server.operationHost().StartAsync(
		parent,
		id,
		server.validateOperationRevision,
		server.executeLocal,
		server.wait.Done,
	) {
		return
	}
	server.wait.Done()
}

func (server *server) operationHost() operationworker.Host {
	return operationworker.Host{
		Inflight: &server.operationInflight,
		Runner: operationworker.Runner{
			Store:  server.operations,
			Logger: server.logger,
			Owner:  server.operationWorkerID,
			Lease:  server.operationLease,
		},
	}
}

func (server *server) operationTarget(
	kind sdk.OperationKind,
	resource string,
) operationworker.Target {
	return operationworker.Target{
		Kind:     kind,
		Resource: resource,
		ResourceRevision: server.registrar.ResourceRevision(
			server.manifest,
			sdk.ResourceKind(kind),
			resource,
		),
	}
}

func (server *server) validateOperationRevision(
	record sdk.OperationRecord,
) error {
	return server.operationTarget(record.Kind, record.Resource).Validate(record)
}

func (server *server) executeLocal(
	ctx context.Context,
	record sdk.OperationRecord,
) (json.RawMessage, error) {
	switch record.Kind {
	case sdk.OperationKindProvider:
		provider, ok := server.registrar.Providers[record.Resource].Value.(sdk.SyncProvider)
		if !ok {
			return nil, fmt.Errorf("provider %q is not synchronous", record.Resource)
		}
		return pluginpolicy.InvokeProviderOperation(ctx, provider, record.Input)
	case sdk.OperationKindTool:
		tool, ok := server.registrar.Tools[record.Resource].Value.(sdk.SyncTool)
		if !ok {
			return nil, fmt.Errorf("tool %q is not synchronous", record.Resource)
		}
		return pluginpolicy.InvokeToolOperation(ctx, tool, record.Input)
	case sdk.OperationKindCapability:
		capability, ok := server.registrar.Capabilities[record.Resource].Value.(sdk.SyncCapability)
		if !ok {
			return nil, fmt.Errorf("capability %q is not synchronous", record.Resource)
		}
		return pluginpolicy.InvokeCapabilityOperation(ctx, capability, record.Input)
	default:
		return nil, fmt.Errorf("unsupported stored operation kind %q", record.Kind)
	}
}
