package pluginrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/protobuf/types/known/structpb"
)

func toProtoManifest(manifest sdk.Manifest) *pluginv1.Manifest {
	return &pluginv1.Manifest{
		Name:          manifest.Name,
		Version:       manifest.Version,
		Description:   manifest.Description,
		ApiVersion:    uint32(manifest.APIVersion),
		MinApiVersion: uint32(manifest.MinAPIVersion),
		MaxApiVersion: uint32(manifest.MaxAPIVersion),
		Requires:      append([]string(nil), manifest.Requires...),
		Conflicts:     append([]string(nil), manifest.Conflicts...),
		Registers:     append([]string(nil), manifest.Registers...),
	}
}

func fromProtoManifest(manifest *pluginv1.Manifest) (sdk.Manifest, error) {
	if manifest == nil {
		return sdk.Manifest{}, errors.New("plugin description has no manifest")
	}
	result := sdk.Manifest{
		Name:          manifest.GetName(),
		Version:       manifest.GetVersion(),
		Description:   manifest.GetDescription(),
		APIVersion:    int(manifest.GetApiVersion()),
		MinAPIVersion: int(manifest.GetMinApiVersion()),
		MaxAPIVersion: int(manifest.GetMaxApiVersion()),
		Requires:      append([]string(nil), manifest.GetRequires()...),
		Conflicts:     append([]string(nil), manifest.GetConflicts()...),
		Registers:     append([]string(nil), manifest.GetRegisters()...),
	}
	if err := result.Validate(); err != nil {
		return sdk.Manifest{}, err
	}
	return result, nil
}

func toProtoProviderSpec(spec sdk.ProviderSpec) *pluginv1.ProviderSpec {
	return &pluginv1.ProviderSpec{Name: spec.Name, Model: spec.Model}
}

func fromProtoProviderSpec(spec *pluginv1.ProviderSpec) sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: spec.GetName(), Model: spec.GetModel()}
}

func toProtoToolSpec(spec sdk.ToolSpec) (*pluginv1.ToolSpec, error) {
	parameters, err := mapToProtoStruct(spec.Parameters)
	if err != nil {
		return nil, fmt.Errorf("encode tool %q parameters: %w", spec.Name, err)
	}
	return &pluginv1.ToolSpec{
		Name:        spec.Name,
		Description: spec.Description,
		Parameters:  parameters,
	}, nil
}

func fromProtoToolSpec(spec *pluginv1.ToolSpec) sdk.ToolSpec {
	parameters := map[string]any(nil)
	if spec.GetParameters() != nil {
		parameters = spec.GetParameters().AsMap()
	}
	return sdk.ToolSpec{
		Name:        spec.GetName(),
		Description: spec.GetDescription(),
		Parameters:  parameters,
	}
}

func toProtoHookSpec(spec sdk.HookSpec) *pluginv1.HookSpec {
	return &pluginv1.HookSpec{
		Name:          spec.Name,
		Event:         spec.Event,
		Priority:      int32(spec.Priority),
		FailurePolicy: toProtoFailurePolicy(spec.FailurePolicy),
		TimeoutMillis: spec.Timeout.Milliseconds(),
	}
}

func fromProtoHookSpec(spec *pluginv1.HookSpec) sdk.HookSpec {
	return sdk.HookSpec{
		Name:          spec.GetName(),
		Event:         spec.GetEvent(),
		Priority:      sdk.Priority(spec.GetPriority()),
		FailurePolicy: fromProtoFailurePolicy(spec.GetFailurePolicy()),
		Timeout:       time.Duration(spec.GetTimeoutMillis()) * time.Millisecond,
	}
}

func toProtoSubscriberSpec(spec sdk.SubscriberSpec) *pluginv1.SubscriberSpec {
	return &pluginv1.SubscriberSpec{
		Name:          spec.Name,
		Events:        append([]string(nil), spec.Events...),
		TimeoutMillis: spec.Timeout.Milliseconds(),
	}
}

func fromProtoSubscriberSpec(spec *pluginv1.SubscriberSpec) sdk.SubscriberSpec {
	return sdk.SubscriberSpec{
		Name:    spec.GetName(),
		Events:  append([]string(nil), spec.GetEvents()...),
		Timeout: time.Duration(spec.GetTimeoutMillis()) * time.Millisecond,
	}
}

func toProtoCapabilitySpec(spec sdk.CapabilitySpec) (*pluginv1.CapabilitySpec, error) {
	input, err := mapToProtoStruct(spec.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("encode capability %q input schema: %w", spec.Name, err)
	}
	output, err := mapToProtoStruct(spec.OutputSchema)
	if err != nil {
		return nil, fmt.Errorf("encode capability %q output schema: %w", spec.Name, err)
	}
	return &pluginv1.CapabilitySpec{
		Name:         spec.Name,
		Description:  spec.Description,
		InputSchema:  input,
		OutputSchema: output,
	}, nil
}

func fromProtoCapabilitySpec(spec *pluginv1.CapabilitySpec) sdk.CapabilitySpec {
	var input, output map[string]any
	if spec.GetInputSchema() != nil {
		input = spec.GetInputSchema().AsMap()
	}
	if spec.GetOutputSchema() != nil {
		output = spec.GetOutputSchema().AsMap()
	}
	return sdk.CapabilitySpec{
		Name:         spec.GetName(),
		Description:  spec.GetDescription(),
		InputSchema:  input,
		OutputSchema: output,
	}
}

func toProtoEventContract(contract sdk.EventContract) *pluginv1.EventContract {
	return &pluginv1.EventContract{
		Name:          contract.Name,
		MutableFields: append([]string(nil), contract.MutableFields...),
		AllowBlock:    contract.AllowBlock,
		AllowAction:   contract.AllowAction,
	}
}

func fromProtoEventContract(contract *pluginv1.EventContract) sdk.EventContract {
	return sdk.EventContract{
		Name:          contract.GetName(),
		MutableFields: append([]string(nil), contract.GetMutableFields()...),
		AllowBlock:    contract.GetAllowBlock(),
		AllowAction:   contract.GetAllowAction(),
	}
}

func toProtoEvent(event sdk.Event) (*pluginv1.EventEnvelope, error) {
	payload, err := rawToStruct(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("encode event %q payload: %w", event.Name, err)
	}
	return &pluginv1.EventEnvelope{
		Id:         event.ID,
		Name:       event.Name,
		SessionId:  event.SessionID,
		Generation: event.Generation,
		Payload:    payload,
	}, nil
}

func fromProtoEvent(event *pluginv1.EventEnvelope) (sdk.Event, error) {
	if event == nil {
		return sdk.Event{}, errors.New("event envelope is nil")
	}
	payload, err := structToRaw(event.GetPayload())
	if err != nil {
		return sdk.Event{}, err
	}
	return sdk.Event{
		ID:         event.GetId(),
		Name:       event.GetName(),
		SessionID:  event.GetSessionId(),
		Generation: event.GetGeneration(),
		Payload:    payload,
	}, nil
}

func toProtoEffect(effect sdk.Effect) (*pluginv1.Effect, error) {
	patch := make(map[string]any, len(effect.Patch))
	for name, raw := range effect.Patch {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("decode patch field %q: %w", name, err)
		}
		patch[name] = value
	}
	patchStruct, err := structpb.NewStruct(patch)
	if err != nil {
		return nil, fmt.Errorf("encode effect patch: %w", err)
	}
	result := &pluginv1.Effect{Patch: patchStruct}
	if effect.Block != nil {
		result.Block = &pluginv1.Block{Reason: effect.Block.Reason, Kind: effect.Block.Kind}
	}
	if effect.Action != nil {
		action, err := toProtoAction(*effect.Action)
		if err != nil {
			return nil, err
		}
		result.Action = action
	}
	return result, nil
}

func fromProtoEffect(effect *pluginv1.Effect) (sdk.Effect, error) {
	if effect == nil {
		return sdk.Effect{}, nil
	}
	result := sdk.Effect{Patch: make(map[string]json.RawMessage)}
	if effect.GetPatch() != nil {
		for name, value := range effect.GetPatch().AsMap() {
			raw, err := json.Marshal(value)
			if err != nil {
				return sdk.Effect{}, fmt.Errorf("decode effect patch field %q: %w", name, err)
			}
			result.Patch[name] = raw
		}
	}
	if effect.GetBlock() != nil {
		result.Block = &sdk.Block{
			Reason: effect.GetBlock().GetReason(),
			Kind:   effect.GetBlock().GetKind(),
		}
	}
	if effect.GetAction() != nil {
		action, err := fromProtoAction(effect.GetAction())
		if err != nil {
			return sdk.Effect{}, err
		}
		result.Action = &action
	}
	return result, nil
}

func toProtoAction(action sdk.Action) (*pluginv1.Action, error) {
	result := &pluginv1.Action{Kind: toProtoActionKind(action.Kind)}
	if action.Cause != nil {
		result.Cause = &pluginv1.Cause{
			Code: action.Cause.Code, Detail: action.Cause.Detail, Final: action.Cause.Final,
		}
	}
	for _, message := range action.Messages {
		converted, err := toProtoMessage(message)
		if err != nil {
			return nil, err
		}
		result.Messages = append(result.Messages, converted)
	}
	return result, nil
}

func fromProtoAction(action *pluginv1.Action) (sdk.Action, error) {
	result := sdk.Action{Kind: fromProtoActionKind(action.GetKind())}
	if action.GetCause() != nil {
		result.Cause = &sdk.Cause{
			Code:   action.GetCause().GetCode(),
			Detail: action.GetCause().GetDetail(),
			Final:  action.GetCause().GetFinal(),
		}
	}
	for _, message := range action.GetMessages() {
		converted, err := fromProtoMessage(message)
		if err != nil {
			return sdk.Action{}, err
		}
		result.Messages = append(result.Messages, converted)
	}
	return result, nil
}

func toProtoMessage(message sdk.Message) (*pluginv1.Message, error) {
	result := &pluginv1.Message{
		Role:       string(message.Role),
		Content:    message.Content,
		ToolCallId: message.ToolCallID,
	}
	for _, call := range message.ToolCalls {
		arguments, err := rawToStruct(call.Arguments)
		if err != nil {
			return nil, fmt.Errorf("encode tool call %q arguments: %w", call.ID, err)
		}
		result.ToolCalls = append(result.ToolCalls, &pluginv1.ToolCall{
			Id: call.ID, Name: call.Name, Arguments: arguments,
		})
	}
	return result, nil
}

func fromProtoMessage(message *pluginv1.Message) (sdk.Message, error) {
	result := sdk.Message{
		Role:       sdk.Role(message.GetRole()),
		Content:    message.GetContent(),
		ToolCallID: message.GetToolCallId(),
	}
	for _, call := range message.GetToolCalls() {
		arguments, err := structToRaw(call.GetArguments())
		if err != nil {
			return sdk.Message{}, err
		}
		result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{
			ID: call.GetId(), Name: call.GetName(), Arguments: arguments,
		})
	}
	return result, nil
}

func toProtoInvocation(
	invocation sdk.Invocation,
) *pluginv1.Invocation {
	if invocation.Empty() {
		return nil
	}
	return &pluginv1.Invocation{
		Id:              invocation.ID,
		RootId:          invocation.RootID,
		ParentId:        invocation.ParentID,
		GroupId:         invocation.GroupID,
		SessionId:       invocation.SessionID,
		TargetSessionId: invocation.TargetSessionID,
		ExecutionId:     invocation.ExecutionID,
		Dependencies: append(
			[]string(nil),
			invocation.Dependencies...,
		),
		Ordinal: invocation.Ordinal,
	}
}

func fromProtoInvocation(
	invocation *pluginv1.Invocation,
) sdk.Invocation {
	if invocation == nil {
		return sdk.Invocation{}
	}
	return sdk.Invocation{
		ID:              invocation.GetId(),
		RootID:          invocation.GetRootId(),
		ParentID:        invocation.GetParentId(),
		GroupID:         invocation.GetGroupId(),
		SessionID:       invocation.GetSessionId(),
		TargetSessionID: invocation.GetTargetSessionId(),
		ExecutionID:     invocation.GetExecutionId(),
		Dependencies: append(
			[]string(nil),
			invocation.GetDependencies()...,
		),
		Ordinal: invocation.GetOrdinal(),
	}
}

func toProtoOperation(operation sdk.Operation) (*pluginv1.Operation, error) {
	var output *structpb.Struct
	var err error
	if len(operation.Output) > 0 {
		output, err = rawToStruct(operation.Output)
		if err != nil {
			return nil, fmt.Errorf("encode operation %q output: %w", operation.ID, err)
		}
	}
	return &pluginv1.Operation{
		Id:                 operation.ID,
		IdempotencyKey:     operation.IdempotencyKey,
		State:              toProtoOperationState(operation.State),
		Revision:           operation.Revision,
		Output:             output,
		Error:              operation.Error,
		SubmittedUnixMilli: unixMilli(operation.SubmittedAt),
		UpdatedUnixMilli:   unixMilli(operation.UpdatedAt),
	}, nil
}

func fromProtoOperation(operation *pluginv1.Operation) (sdk.Operation, error) {
	if operation == nil {
		return sdk.Operation{}, errors.New("operation is nil")
	}
	var output json.RawMessage
	var err error
	if operation.GetOutput() != nil {
		output, err = structToRaw(operation.GetOutput())
		if err != nil {
			return sdk.Operation{}, err
		}
	}
	return sdk.Operation{
		ID:             operation.GetId(),
		IdempotencyKey: operation.GetIdempotencyKey(),
		State:          fromProtoOperationState(operation.GetState()),
		Revision:       operation.GetRevision(),
		Output:         output,
		Error:          operation.GetError(),
		SubmittedAt:    fromUnixMilli(operation.GetSubmittedUnixMilli()),
		UpdatedAt:      fromUnixMilli(operation.GetUpdatedUnixMilli()),
	}, nil
}

func toProtoDelivery(delivery sdk.Delivery) (*pluginv1.Delivery, error) {
	event, err := toProtoEvent(delivery.Event)
	if err != nil {
		return nil, err
	}
	return &pluginv1.Delivery{
		Id:               delivery.ID,
		Sequence:         delivery.Sequence,
		Plugin:           delivery.Plugin,
		PluginVersion:    delivery.PluginVersion,
		Subscription:     delivery.Subscription,
		ResourceRevision: delivery.ResourceRevision,
		Partition:        delivery.Partition,
		Event:            event,
		Attempt:          int32(delivery.Attempt),
	}, nil
}

func fromProtoDelivery(delivery *pluginv1.Delivery) (sdk.Delivery, error) {
	if delivery == nil {
		return sdk.Delivery{}, errors.New("delivery is nil")
	}
	event, err := fromProtoEvent(delivery.GetEvent())
	if err != nil {
		return sdk.Delivery{}, err
	}
	return sdk.Delivery{
		ID: delivery.GetId(), Sequence: delivery.GetSequence(),
		Plugin:           delivery.GetPlugin(),
		PluginVersion:    delivery.GetPluginVersion(),
		Subscription:     delivery.GetSubscription(),
		ResourceRevision: delivery.GetResourceRevision(),
		Partition:        delivery.GetPartition(),
		Event:            event,
		Attempt:          int(delivery.GetAttempt()),
	}, nil
}

func rawToStruct(raw json.RawMessage) (*structpb.Struct, error) {
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return structpb.NewStruct(value)
}

func mapToProtoStruct(value map[string]any) (*structpb.Struct, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized map[string]any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, err
	}
	return structpb.NewStruct(normalized)
}

func structToRaw(value *structpb.Struct) (json.RawMessage, error) {
	if value == nil {
		return []byte(`{}`), nil
	}
	return json.Marshal(value.AsMap())
}

func toProtoFailurePolicy(value sdk.FailurePolicy) pluginv1.FailurePolicy {
	switch value {
	case sdk.FailurePolicyFailClosed:
		return pluginv1.FailurePolicy_FAILURE_POLICY_FAIL_CLOSED
	case sdk.FailurePolicyContinue:
		return pluginv1.FailurePolicy_FAILURE_POLICY_CONTINUE
	default:
		return pluginv1.FailurePolicy_FAILURE_POLICY_UNSPECIFIED
	}
}

func fromProtoFailurePolicy(value pluginv1.FailurePolicy) sdk.FailurePolicy {
	switch value {
	case pluginv1.FailurePolicy_FAILURE_POLICY_FAIL_CLOSED:
		return sdk.FailurePolicyFailClosed
	case pluginv1.FailurePolicy_FAILURE_POLICY_CONTINUE:
		return sdk.FailurePolicyContinue
	default:
		return ""
	}
}

func toProtoActionKind(value sdk.ActionKind) pluginv1.ActionKind {
	switch value {
	case sdk.ActionStep:
		return pluginv1.ActionKind_ACTION_KIND_STEP
	case sdk.ActionStop:
		return pluginv1.ActionKind_ACTION_KIND_STOP
	case sdk.ActionInject:
		return pluginv1.ActionKind_ACTION_KIND_INJECT
	default:
		return pluginv1.ActionKind_ACTION_KIND_UNSPECIFIED
	}
}

func fromProtoActionKind(value pluginv1.ActionKind) sdk.ActionKind {
	switch value {
	case pluginv1.ActionKind_ACTION_KIND_STEP:
		return sdk.ActionStep
	case pluginv1.ActionKind_ACTION_KIND_STOP:
		return sdk.ActionStop
	case pluginv1.ActionKind_ACTION_KIND_INJECT:
		return sdk.ActionInject
	default:
		return ""
	}
}

func toProtoOperationState(value sdk.OperationState) pluginv1.OperationState {
	switch value {
	case sdk.OperationPending:
		return pluginv1.OperationState_OPERATION_STATE_PENDING
	case sdk.OperationRunning:
		return pluginv1.OperationState_OPERATION_STATE_RUNNING
	case sdk.OperationSucceeded:
		return pluginv1.OperationState_OPERATION_STATE_SUCCEEDED
	case sdk.OperationFailed:
		return pluginv1.OperationState_OPERATION_STATE_FAILED
	case sdk.OperationCancelled:
		return pluginv1.OperationState_OPERATION_STATE_CANCELLED
	default:
		return pluginv1.OperationState_OPERATION_STATE_UNSPECIFIED
	}
}

func fromProtoOperationState(value pluginv1.OperationState) sdk.OperationState {
	switch value {
	case pluginv1.OperationState_OPERATION_STATE_PENDING:
		return sdk.OperationPending
	case pluginv1.OperationState_OPERATION_STATE_RUNNING:
		return sdk.OperationRunning
	case pluginv1.OperationState_OPERATION_STATE_SUCCEEDED:
		return sdk.OperationSucceeded
	case pluginv1.OperationState_OPERATION_STATE_FAILED:
		return sdk.OperationFailed
	case pluginv1.OperationState_OPERATION_STATE_CANCELLED:
		return sdk.OperationCancelled
	default:
		return ""
	}
}

func toProtoOperationKind(value sdk.OperationKind) pluginv1.OperationKind {
	switch value {
	case sdk.OperationKindProvider:
		return pluginv1.OperationKind_OPERATION_KIND_PROVIDER
	case sdk.OperationKindTool:
		return pluginv1.OperationKind_OPERATION_KIND_TOOL
	case sdk.OperationKindCapability:
		return pluginv1.OperationKind_OPERATION_KIND_CAPABILITY
	default:
		return pluginv1.OperationKind_OPERATION_KIND_UNSPECIFIED
	}
}

func fromProtoOperationKind(value pluginv1.OperationKind) sdk.OperationKind {
	switch value {
	case pluginv1.OperationKind_OPERATION_KIND_PROVIDER:
		return sdk.OperationKindProvider
	case pluginv1.OperationKind_OPERATION_KIND_TOOL:
		return sdk.OperationKindTool
	case pluginv1.OperationKind_OPERATION_KIND_CAPABILITY:
		return sdk.OperationKindCapability
	default:
		return ""
	}
}

func unixMilli(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func fromUnixMilli(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(value).UTC()
}
