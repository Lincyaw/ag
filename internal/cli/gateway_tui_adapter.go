package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/lincyaw/ag/gateway"
	gatewayclient "github.com/lincyaw/ag/gateway/client"
	cagentapp "github.com/lincyaw/ag/internal/cagent/app"
	cagentchat "github.com/lincyaw/ag/internal/cagent/chat"
	cagentruntime "github.com/lincyaw/ag/internal/cagent/runtime"
	cagentsession "github.com/lincyaw/ag/internal/cagent/session"
	cagenttools "github.com/lincyaw/ag/internal/cagent/tools"
	tuimessages "github.com/lincyaw/ag/internal/tui/messages"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const gatewayTUIAgentName = "ag"

// gatewayTUIBinding is the only backend-specific object the migrated terminal
// frontend needs. The TUI owns an in-memory cagent session as a disposable view
// model; the gateway trajectory remains the durable source of truth.
type gatewayTUIBinding struct {
	App     *cagentapp.App
	Session *cagentsession.Session

	client   *gatewayclient.Client
	view     *gatewayclient.View
	metadata gateway.Session
	control  *gatewayTUIController

	createTemplate *gatewayclient.CreateSessionRequest
	execution      *sdk.TrajectoryExecution
	pending        *gateway.Interaction

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	mu             sync.Mutex
	updateMu       sync.Mutex
	streaming      bool
	pendingEchoes  []string
	totalInput     int64
	totalOutput    int64
	pendingToolIDs map[string]struct{}
}

func newGatewayTUIBinding(
	ctx context.Context,
	client *gatewayclient.Client,
	trajectory gateway.Session,
	firstMessage string,
	createTemplate *gatewayclient.CreateSessionRequest,
) (*gatewayTUIBinding, error) {
	if client == nil {
		return nil, errors.New("gateway TUI client is nil")
	}
	cursor, err := client.GetEventCursor(ctx, trajectory.ID)
	if err != nil {
		return nil, fmt.Errorf("read trajectory event cursor: %w", err)
	}
	// Open the reconnectable stream before hydrating the conversation. Events
	// produced during hydration are buffered by the view instead of falling into
	// the cursor/conversation race window.
	view, err := client.OpenView(ctx, trajectory.ID, cursor.Sequence)
	if err != nil {
		return nil, fmt.Errorf("open trajectory view: %w", err)
	}
	messages, execution, err := loadGatewayConversation(ctx, client, trajectory.ID)
	if err != nil {
		_ = view.Close()
		return nil, err
	}
	pending, err := latestPendingGatewayInteraction(ctx, client, trajectory.ID)
	if err != nil {
		_ = view.Close()
		return nil, err
	}

	mirror := gatewayConversationSession(trajectory, messages)
	bindingCtx, cancel := context.WithCancel(ctx)
	binding := &gatewayTUIBinding{
		Session:        mirror,
		client:         client,
		view:           view,
		metadata:       trajectory,
		createTemplate: cloneGatewayCreateTemplate(createTemplate),
		execution:      execution,
		pending:        pending,
		ctx:            bindingCtx,
		cancel:         cancel,
		pendingToolIDs: make(map[string]struct{}),
	}
	control := &gatewayTUIController{
		binding:      binding,
		firstMessage: strings.TrimSpace(firstMessage),
	}
	binding.control = control
	binding.App = cagentapp.New(
		bindingCtx,
		mirror,
		cagentapp.WithController(control),
	)
	binding.App.SetAgentInfo(nil, nil, trajectory.Models, trajectory.Model)
	binding.App.TrackThinkingLevel(trajectory.ThinkingLevel)
	return binding, nil
}

func cloneGatewayCreateTemplate(
	template *gatewayclient.CreateSessionRequest,
) *gatewayclient.CreateSessionRequest {
	if template == nil {
		return nil
	}
	clone := *template
	clone.ID = ""
	clone.RuntimeConfig = append([]byte(nil), template.RuntimeConfig...)
	clone.Settings.Models = slices.Clone(template.Settings.Models)
	clone.Settings.Permissions = gateway.PermissionRules{
		Allow: slices.Clone(template.Settings.Permissions.Allow),
		Ask:   slices.Clone(template.Settings.Permissions.Ask),
		Deny:  slices.Clone(template.Settings.Permissions.Deny),
	}
	if template.Settings.AutoCompact != nil {
		enabled := *template.Settings.AutoCompact
		clone.Settings.AutoCompact = &enabled
	}
	return &clone
}

func latestPendingGatewayInteraction(
	ctx context.Context,
	client *gatewayclient.Client,
	trajectoryID string,
) (*gateway.Interaction, error) {
	var (
		after  uint64
		latest *gateway.Interaction
	)
	for {
		page, err := client.ListInteractions(ctx, trajectoryID, gateway.InteractionQuery{
			After: after,
			Limit: gatewayEventPageSize,
			State: gateway.InteractionPending,
		})
		if err != nil {
			return nil, fmt.Errorf("list pending trajectory interactions: %w", err)
		}
		for _, item := range page.Items {
			item := item
			latest = &item
		}
		if page.Next == 0 || len(page.Items) < gatewayEventPageSize {
			return latest, nil
		}
		after = page.Next
	}
}

func (binding *gatewayTUIBinding) Start() {
	if binding == nil || binding.App == nil {
		return
	}
	if binding.execution != nil && !binding.execution.Terminal() {
		binding.setStreaming(true)
		binding.App.EmitEvent(cagentruntime.StreamStarted(
			binding.Session.ID,
			gatewayTUIAgentName,
		))
	}
	if binding.pending != nil {
		binding.emitInteraction(*binding.pending)
	}
	if binding.metadata.AutoCompact != nil {
		binding.App.EmitEvent(cagentruntime.ConfigUpdate(
			binding.Session.ID,
			"auto_compact",
			*binding.metadata.AutoCompact,
			gatewayTUIAgentName,
		))
	}
	if binding.metadata.ThinkingLevel != "" {
		binding.App.EmitEvent(cagentruntime.ConfigValueUpdate(
			binding.Session.ID,
			"thinking_level",
			binding.metadata.ThinkingLevel,
			gatewayTUIAgentName,
		))
	}
	go binding.pump()
}

func (binding *gatewayTUIBinding) Close() {
	if binding == nil {
		return
	}
	binding.closeOnce.Do(func() {
		binding.cancel()
		if binding.view != nil {
			_ = binding.view.Close()
		}
	})
}

func (binding *gatewayTUIBinding) Spawner() func(
	context.Context,
	string,
) (*cagentapp.App, *cagentsession.Session, func(), error) {
	return func(
		ctx context.Context,
		workingDir string,
	) (*cagentapp.App, *cagentsession.Session, func(), error) {
		binding.mu.Lock()
		metadata := binding.metadata
		binding.mu.Unlock()
		request := gatewayclient.CreateSessionRequest{
			ID:            sdk.NewID(),
			Provider:      metadata.Provider,
			System:        metadata.System,
			MaxTurns:      metadata.MaxTurns,
			WorkspaceRoot: workingDir,
			Settings: gateway.SessionSettings{
				Model: metadata.Model, Models: slices.Clone(metadata.Models),
				AutoCompact:   metadata.AutoCompact,
				ThinkingLevel: metadata.ThinkingLevel,
				Permissions: gateway.PermissionRules{
					Allow: slices.Clone(metadata.Permissions.Allow),
					Ask:   slices.Clone(metadata.Permissions.Ask),
					Deny:  slices.Clone(metadata.Permissions.Deny),
				},
			},
		}
		if binding.createTemplate != nil {
			request = *cloneGatewayCreateTemplate(binding.createTemplate)
			request.ID = sdk.NewID()
			if strings.TrimSpace(workingDir) != "" {
				request.WorkspaceRoot = workingDir
			}
		}
		if strings.TrimSpace(request.WorkspaceRoot) == "" {
			request.WorkspaceRoot = metadata.WorkspaceRoot
		}
		created, err := binding.client.CreateSession(ctx, request)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create trajectory tab: %w", err)
		}
		child, err := newGatewayTUIBinding(
			ctx,
			binding.client,
			created,
			"",
			binding.createTemplate,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		child.Start()
		return child.App, child.Session, child.Close, nil
	}
}

func (binding *gatewayTUIBinding) SessionLister() func(
	context.Context,
) ([]cagentsession.Summary, error) {
	return func(ctx context.Context) ([]cagentsession.Summary, error) {
		var (
			after     string
			summaries []cagentsession.Summary
		)
		for {
			page, err := binding.client.ListSessions(ctx, sdk.PageRequest{
				After: after,
				Limit: gatewayEventPageSize,
			})
			if err != nil {
				return nil, err
			}
			for _, item := range page.Items {
				title := strings.TrimSpace(item.Title)
				if title == "" {
					title = gatewayConversationTitle(item, nil)
				}
				summaries = append(summaries, cagentsession.Summary{
					ID: item.ID, Title: title, CreatedAt: item.UpdatedAt,
				})
			}
			if page.Next == "" || len(page.Items) < gatewayEventPageSize {
				break
			}
			after = page.Next
		}
		return summaries, nil
	}
}

func (binding *gatewayTUIBinding) SessionAttacher() func(
	context.Context,
	string,
) (*cagentapp.App, *cagentsession.Session, func(), error) {
	return func(
		ctx context.Context,
		trajectoryID string,
	) (*cagentapp.App, *cagentsession.Session, func(), error) {
		resolved, err := resolveGatewaySessionPrefix(ctx, binding.client, trajectoryID)
		if err != nil {
			return nil, nil, nil, err
		}
		trajectory, err := binding.client.GetSession(ctx, resolved)
		if err != nil {
			return nil, nil, nil, err
		}
		attached, err := newGatewayTUIBinding(
			ctx,
			binding.client,
			trajectory,
			"",
			binding.createTemplate,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		attached.Start()
		return attached.App, attached.Session, attached.Close, nil
	}
}

func (binding *gatewayTUIBinding) pump() {
	for {
		event, err := binding.view.Next()
		if err != nil {
			if binding.ctx.Err() == nil {
				binding.App.EmitEvent(cagentruntime.Error(
					"gateway view closed: " + err.Error(),
				))
				binding.stopStream("gateway_error")
			}
			return
		}
		binding.translate(event)
	}
}

func (binding *gatewayTUIBinding) translate(event gateway.AgentEvent) {
	switch event.Name {
	case sdk.EventAgentStart:
		var payload sdk.AgentStartPayload
		if json.Unmarshal(event.Payload, &payload) == nil {
			if user := latestSDKUserMessage(payload.Messages); user != "" &&
				!binding.consumePendingEcho(user) {
				binding.App.EmitEvent(cagentruntime.UserMessage(
					user,
					binding.Session.ID,
					nil,
				))
			}
		}
		if !binding.isStreaming() {
			binding.setStreaming(true)
			binding.App.EmitEvent(cagentruntime.StreamStarted(
				binding.Session.ID,
				gatewayTUIAgentName,
			))
		}

	case sdk.EventAfterProvider:
		var payload sdk.AfterProviderPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		if payload.Error != "" {
			binding.App.EmitEvent(cagentruntime.Error(payload.Error))
			return
		}
		if payload.Response == nil {
			return
		}
		response := payload.Response
		if response.Content != "" {
			binding.App.EmitEvent(cagentruntime.AgentChoice(
				gatewayTUIAgentName,
				binding.Session.ID,
				response.Content,
			))
		}
		for _, call := range response.ToolCalls {
			converted, definition := gatewayTUITool(call)
			binding.rememberPendingTool(call.ID)
			binding.App.EmitEvent(cagentruntime.PartialToolCall(
				converted,
				definition,
				gatewayTUIAgentName,
			))
		}
		binding.addUsage(response.Usage)

	case sdk.EventBeforeTool:
		var payload sdk.BeforeToolPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		call, definition := gatewayTUITool(payload.Call)
		binding.rememberPendingTool(payload.Call.ID)
		binding.App.EmitEvent(cagentruntime.ToolCall(
			call,
			definition,
			gatewayTUIAgentName,
		))

	case sdk.EventAfterTool:
		var payload sdk.AfterToolPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		binding.emitToolResult(payload.Call, payload.Result)

	case sdk.EventToolError:
		var payload sdk.ToolErrorPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		result := payload.Result
		result.IsError = true
		if result.Content == "" {
			result.Content = payload.Reason
		}
		binding.emitToolResult(payload.Call, result)

	case sdk.EventAgentEnd:
		var payload sdk.AgentEndPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			binding.emitDecodeError(event, err)
			binding.stopStream("decode_error")
			return
		}
		if payload.InputTokens != 0 || payload.OutputTokens != 0 {
			binding.setUsage(payload.InputTokens, payload.OutputTokens)
		}
		switch payload.Cause.Code {
		case sdk.CauseProviderError,
			sdk.CauseHookError,
			sdk.CauseExecutionError,
			sdk.CausePromptBlocked:
			message := payload.Cause.Detail
			if message == "" {
				message = strings.ReplaceAll(payload.Cause.Code, "_", " ")
			}
			binding.App.EmitEvent(cagentruntime.Error(message))
		}
		binding.stopStream(payload.Cause.Code)

	case gateway.GatewayEventInteractionRequested:
		var interaction gateway.Interaction
		if err := json.Unmarshal(event.Payload, &interaction); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		binding.emitInteraction(interaction)

	case gateway.GatewayEventInteractionResolved,
		gateway.GatewayEventInteractionCancelled:
		binding.mu.Lock()
		binding.pending = nil
		binding.mu.Unlock()

	case gateway.GatewayEventSessionUpdated:
		var metadata gateway.Session
		if err := json.Unmarshal(event.Payload, &metadata); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		binding.applyMetadata(metadata)

	case gateway.GatewayEventSessionPaused,
		gateway.GatewayEventSessionResumed:
		var metadata gateway.Session
		if err := json.Unmarshal(event.Payload, &metadata); err != nil {
			binding.emitDecodeError(event, err)
			return
		}
		binding.applyMetadata(metadata)
	}
}

func (binding *gatewayTUIBinding) applyMetadata(metadata gateway.Session) {
	binding.mu.Lock()
	binding.metadata = metadata
	binding.mu.Unlock()
	binding.App.SetAgentInfo(nil, nil, metadata.Models, metadata.Model)
	binding.App.TrackThinkingLevel(metadata.ThinkingLevel)
	binding.Session.Permissions = &cagentsession.PermissionsConfig{
		Allow: slices.Clone(metadata.Permissions.Allow),
		Ask:   slices.Clone(metadata.Permissions.Ask),
		Deny:  slices.Clone(metadata.Permissions.Deny),
	}
	if binding.Session.Title != metadata.Title {
		binding.Session.Title = metadata.Title
		binding.App.EmitEvent(cagentruntime.SessionTitle(
			binding.Session.ID,
			metadata.Title,
		))
	}
	if metadata.AutoCompact != nil {
		binding.App.EmitEvent(cagentruntime.ConfigUpdate(
			binding.Session.ID,
			"auto_compact",
			*metadata.AutoCompact,
			gatewayTUIAgentName,
		))
	}
	if metadata.ThinkingLevel != "" {
		binding.App.EmitEvent(cagentruntime.ConfigValueUpdate(
			binding.Session.ID,
			"thinking_level",
			metadata.ThinkingLevel,
			gatewayTUIAgentName,
		))
	}
}

func (binding *gatewayTUIBinding) updateMetadata(
	ctx context.Context,
	patch gateway.SessionPatch,
) (gateway.Session, error) {
	binding.updateMu.Lock()
	defer binding.updateMu.Unlock()
	for attempt := 0; attempt < 3; attempt++ {
		binding.mu.Lock()
		current := binding.metadata
		binding.mu.Unlock()
		updated, err := binding.client.UpdateSession(
			ctx,
			current.ID,
			current.Revision,
			patch,
		)
		if err == nil {
			binding.applyMetadata(updated)
			return updated, nil
		}
		if status.Code(err) != codes.Aborted {
			return gateway.Session{}, err
		}
		refreshed, refreshErr := binding.client.GetSession(ctx, current.ID)
		if refreshErr != nil {
			return gateway.Session{}, errors.Join(err, refreshErr)
		}
		binding.applyMetadata(refreshed)
	}
	return gateway.Session{}, errors.New("trajectory settings changed concurrently")
}

func (binding *gatewayTUIBinding) emitDecodeError(
	event gateway.AgentEvent,
	err error,
) {
	binding.App.EmitEvent(cagentruntime.Error(fmt.Sprintf(
		"decode gateway event %s: %v",
		event.Name,
		err,
	)))
}

func (binding *gatewayTUIBinding) emitToolResult(
	call sdk.ToolCall,
	result sdk.ToolResult,
) {
	converted, definition := gatewayTUITool(call)
	binding.App.EmitEvent(cagentruntime.ToolCallResponse(
		converted.ID,
		definition,
		&cagenttools.ToolCallResult{
			Output:  result.Content,
			IsError: result.IsError,
		},
		result.Content,
		gatewayTUIAgentName,
	))
	binding.mu.Lock()
	delete(binding.pendingToolIDs, call.ID)
	binding.mu.Unlock()
}

func (binding *gatewayTUIBinding) emitInteraction(interaction gateway.Interaction) {
	binding.mu.Lock()
	copy := interaction
	binding.pending = &copy
	binding.mu.Unlock()
	args, _ := json.Marshal(map[string]any{
		"message": interaction.Prompt,
		"title":   string(interaction.Kind),
		"options": interaction.Options,
	})
	name := "user_prompt"
	if interaction.Kind == gateway.InteractionPermission {
		name = "permission"
	}
	call := cagenttools.ToolCall{
		ID:   interaction.ID,
		Type: cagenttools.ToolType("function"),
		Function: cagenttools.FunctionCall{
			Name:      name,
			Arguments: string(args),
		},
	}
	binding.App.EmitEvent(cagentruntime.ToolCallConfirmation(
		call,
		cagenttools.Tool{Name: name},
		gatewayTUIAgentName,
	))
}

func gatewayTUITool(call sdk.ToolCall) (cagenttools.ToolCall, cagenttools.Tool) {
	arguments := strings.TrimSpace(string(call.Arguments))
	if arguments == "" {
		arguments = "{}"
	}
	category := ""
	switch strings.ToLower(call.Name) {
	case "bash", "shell", "exec_command":
		category = "shell"
	case "fetch", "http", "web":
		category = "api"
	}
	return cagenttools.ToolCall{
		ID:   call.ID,
		Type: cagenttools.ToolType("function"),
		Function: cagenttools.FunctionCall{
			Name:      call.Name,
			Arguments: arguments,
		},
	}, cagenttools.Tool{Name: call.Name, Category: category}
}

func (binding *gatewayTUIBinding) rememberPendingTool(id string) {
	if id == "" {
		return
	}
	binding.mu.Lock()
	binding.pendingToolIDs[id] = struct{}{}
	binding.mu.Unlock()
}

func latestSDKUserMessage(messages []sdk.Message) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == sdk.RoleUser {
			return messages[index].Content
		}
	}
	return ""
}

func (binding *gatewayTUIBinding) addPendingEcho(content string) {
	binding.mu.Lock()
	binding.pendingEchoes = append(binding.pendingEchoes, content)
	binding.mu.Unlock()
}

func (binding *gatewayTUIBinding) consumePendingEcho(content string) bool {
	binding.mu.Lock()
	defer binding.mu.Unlock()
	for index, pending := range binding.pendingEchoes {
		if pending == content {
			binding.pendingEchoes = append(
				binding.pendingEchoes[:index],
				binding.pendingEchoes[index+1:]...,
			)
			return true
		}
	}
	return false
}

func (binding *gatewayTUIBinding) isStreaming() bool {
	binding.mu.Lock()
	defer binding.mu.Unlock()
	return binding.streaming
}

func (binding *gatewayTUIBinding) setStreaming(streaming bool) {
	binding.mu.Lock()
	binding.streaming = streaming
	binding.mu.Unlock()
}

func (binding *gatewayTUIBinding) stopStream(reason string) {
	binding.setStreaming(false)
	binding.App.EmitEvent(cagentruntime.StreamStopped(
		binding.Session.ID,
		gatewayTUIAgentName,
		reason,
	))
}

func (binding *gatewayTUIBinding) addUsage(usage sdk.Usage) {
	binding.mu.Lock()
	binding.totalInput += usage.InputTokens
	binding.totalOutput += usage.OutputTokens
	input, output := binding.totalInput, binding.totalOutput
	binding.mu.Unlock()
	binding.emitUsage(input, output)
}

func (binding *gatewayTUIBinding) setUsage(input, output int64) {
	binding.mu.Lock()
	binding.totalInput = input
	binding.totalOutput = output
	binding.mu.Unlock()
	binding.emitUsage(input, output)
}

func (binding *gatewayTUIBinding) emitUsage(input, output int64) {
	binding.Session.SetUsage(input, output)
	binding.App.EmitEvent(cagentruntime.NewTokenUsageEvent(
		binding.Session.ID,
		gatewayTUIAgentName,
		&cagentruntime.Usage{
			InputTokens:   input,
			OutputTokens:  output,
			ContextLength: input + output,
		},
	))
}

func gatewayConversationSession(
	trajectory gateway.Session,
	messages []sdk.Message,
) *cagentsession.Session {
	title := gatewayConversationTitle(trajectory, messages)
	mirror := cagentsession.New(
		cagentsession.WithID(trajectory.ID),
		cagentsession.WithTitle(title),
		cagentsession.WithWorkingDir(trajectory.WorkspaceRoot),
		cagentsession.WithMaxIterations(trajectory.MaxTurns),
		cagentsession.WithPermissions(&cagentsession.PermissionsConfig{
			Allow: slices.Clone(trajectory.Permissions.Allow),
			Ask:   slices.Clone(trajectory.Permissions.Ask),
			Deny:  slices.Clone(trajectory.Permissions.Deny),
		}),
	)
	mirror.CreatedAt = trajectory.CreatedAt
	for _, message := range messages {
		converted := gatewayConversationMessage(message, trajectory.Model)
		if converted != nil {
			mirror.AddMessage(converted)
		}
	}
	return mirror
}

func gatewayConversationTitle(
	trajectory gateway.Session,
	messages []sdk.Message,
) string {
	if title := strings.TrimSpace(trajectory.Title); title != "" {
		return title
	}
	for _, message := range messages {
		if message.Role != sdk.RoleUser {
			continue
		}
		line := strings.TrimSpace(strings.SplitN(message.Content, "\n", 2)[0])
		if line == "" {
			continue
		}
		runes := []rune(line)
		if len(runes) > 48 {
			line = string(runes[:47]) + "…"
		}
		return line
	}
	if base := filepath.Base(trajectory.WorkspaceRoot); base != "." && base != "" {
		return base
	}
	if len(trajectory.ID) > 8 {
		return trajectory.ID[:8]
	}
	return trajectory.ID
}

func gatewayConversationMessage(
	message sdk.Message,
	model string,
) *cagentsession.Message {
	role := cagentchat.MessageRole(message.Role)
	switch role {
	case cagentchat.MessageRoleUser,
		cagentchat.MessageRoleAssistant,
		cagentchat.MessageRoleTool:
	default:
		return nil
	}
	converted := cagentchat.Message{
		Role:       role,
		Content:    message.Content,
		ToolCallID: message.ToolCallID,
		IsError:    message.IsError,
		Model:      model,
	}
	if len(message.ToolCalls) > 0 {
		converted.ToolCalls = make([]cagenttools.ToolCall, 0, len(message.ToolCalls))
		converted.ToolDefinitions = make([]cagenttools.Tool, 0, len(message.ToolCalls))
		for _, call := range message.ToolCalls {
			toolCall, definition := gatewayTUITool(call)
			converted.ToolCalls = append(converted.ToolCalls, toolCall)
			converted.ToolDefinitions = append(converted.ToolDefinitions, definition)
		}
	}
	agentName := ""
	if role == cagentchat.MessageRoleAssistant || role == cagentchat.MessageRoleTool {
		agentName = gatewayTUIAgentName
	}
	return cagentsession.NewAgentMessage(agentName, &converted)
}

// gatewayTUIController implements the copied frontend's backend seam. All
// durable operations go through the long-lived gRPC view/client; the methods
// that are intentionally local (bang commands and title chrome) never mutate
// trajectory history directly.
type gatewayTUIController struct {
	binding *gatewayTUIBinding

	mu           sync.Mutex
	firstMessage string
	currentInput string
	thinking     string
}

var _ cagentapp.Controller = (*gatewayTUIController)(nil)

func (controller *gatewayTUIController) FirstMessage() (string, bool) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.firstMessage == "" {
		return "", false
	}
	message := controller.firstMessage
	controller.firstMessage = ""
	return message, true
}

func (controller *gatewayTUIController) Run(
	ctx context.Context,
	cancel context.CancelFunc,
	message string,
	attachments []tuimessages.Attachment,
) {
	_ = cancel
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	backendContent := gatewayInputContent(message, attachments)
	controller.binding.addPendingEcho(backendContent)
	controller.binding.App.EmitEvent(cagentruntime.UserMessage(
		gatewayUserTranscript(message, attachments),
		controller.binding.Session.ID,
		nil,
	))
	controller.submit(ctx, backendContent)
}

func (controller *gatewayTUIController) RunCooperative(
	ctx context.Context,
	cancel context.CancelFunc,
	message string,
	attachments []tuimessages.Attachment,
) {
	_ = cancel
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	controller.submit(ctx, gatewayInputContent(message, attachments))
}

func (controller *gatewayTUIController) RunWithMessage(
	ctx context.Context,
	cancel context.CancelFunc,
	message *cagentsession.Message,
) {
	if message == nil {
		return
	}
	controller.Run(ctx, cancel, message.Message.Content, nil)
}

func (controller *gatewayTUIController) CompactSession(
	ctx context.Context,
	cancel context.CancelFunc,
	additionalPrompt string,
) {
	command := "/compact"
	if prompt := strings.TrimSpace(additionalPrompt); prompt != "" {
		command += " " + prompt
	}
	controller.Run(ctx, cancel, command, nil)
}

func (controller *gatewayTUIController) submit(
	ctx context.Context,
	content string,
) {
	input, err := controller.binding.view.EnqueueInput(ctx, sdk.NewID(), content)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			controller.binding.App.EmitEvent(cagentruntime.Error(
				"enqueue gateway input: " + err.Error(),
			))
		}
		controller.binding.stopStream("enqueue_error")
		return
	}
	controller.mu.Lock()
	controller.currentInput = input.ID
	controller.mu.Unlock()
	go controller.cancelWhenRequested(ctx, input.ID)
}

func (controller *gatewayTUIController) cancelWhenRequested(
	ctx context.Context,
	inputID string,
) {
	<-ctx.Done()
	controller.mu.Lock()
	current := controller.currentInput == inputID
	controller.mu.Unlock()
	if current {
		controller.cancelInput(inputID)
	}
}

func (controller *gatewayTUIController) cancelInput(inputID string) {
	ctx, cancel := context.WithTimeout(context.Background(), gatewayCancelTimeout)
	defer cancel()
	input, err := controller.binding.client.GetInput(
		ctx,
		controller.binding.Session.ID,
		inputID,
	)
	if err != nil || input.State.Terminal() {
		return
	}
	if input.ExecutionID != "" {
		_, _ = controller.binding.view.CancelExecution(ctx, input.ExecutionID)
		return
	}
	_, _ = controller.binding.view.CancelInput(ctx, input.ID, input.Revision)
}

func (controller *gatewayTUIController) Interrupt() {
	controller.mu.Lock()
	inputID := controller.currentInput
	controller.mu.Unlock()
	if inputID != "" {
		go controller.cancelInput(inputID)
	}
}

func (controller *gatewayTUIController) TogglePause() (bool, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), gatewayCancelTimeout)
	defer cancel()
	controller.binding.updateMu.Lock()
	defer controller.binding.updateMu.Unlock()
	for attempt := 0; attempt < 3; attempt++ {
		controller.binding.mu.Lock()
		current := controller.binding.metadata
		controller.binding.mu.Unlock()
		var (
			updated gateway.Session
			err     error
		)
		if current.Paused {
			updated, err = controller.binding.client.ResumeSession(
				ctx, current.ID, current.Revision,
			)
		} else {
			updated, err = controller.binding.client.PauseSession(
				ctx, current.ID, current.Revision,
			)
		}
		if err == nil {
			controller.binding.applyMetadata(updated)
			return updated.Paused, true
		}
		if status.Code(err) != codes.Aborted {
			controller.binding.App.EmitEvent(cagentruntime.Error(
				"toggle trajectory pause: " + err.Error(),
			))
			return current.Paused, true
		}
		refreshed, refreshErr := controller.binding.client.GetSession(ctx, current.ID)
		if refreshErr != nil {
			controller.binding.App.EmitEvent(cagentruntime.Error(
				"refresh trajectory pause state: " + refreshErr.Error(),
			))
			return current.Paused, true
		}
		controller.binding.applyMetadata(refreshed)
	}
	return false, true
}

func (controller *gatewayTUIController) CancelBackground(taskID string) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), gatewayCancelTimeout)
		defer cancel()
		_, _ = controller.binding.client.CancelExecution(
			ctx,
			controller.binding.Session.ID,
			taskID,
		)
	}()
}

func (controller *gatewayTUIController) Resume(request cagentruntime.ResumeRequest) {
	controller.binding.mu.Lock()
	pending := controller.binding.pending
	controller.binding.mu.Unlock()
	if pending == nil {
		return
	}
	answer := gatewayInteractionAnswer(*pending, request)
	go func(interaction gateway.Interaction) {
		ctx, cancel := context.WithTimeout(context.Background(), gatewayCancelTimeout)
		defer cancel()
		resolved, err := controller.binding.view.ResolveInteraction(
			ctx,
			interaction.ID,
			interaction.Revision,
			answer,
		)
		if err != nil {
			controller.binding.App.EmitEvent(cagentruntime.Error(
				"resolve gateway interaction: " + err.Error(),
			))
			return
		}
		controller.binding.mu.Lock()
		controller.binding.pending = nil
		controller.binding.mu.Unlock()
		_ = resolved
	}(*pending)
}

func gatewayInteractionAnswer(
	interaction gateway.Interaction,
	request cagentruntime.ResumeRequest,
) gateway.InteractionAnswer {
	reject := request.Type == cagentruntime.ResumeTypeReject
	wanted := []string{"approve", "allow", "yes", "continue", "ok"}
	if reject {
		wanted = []string{"reject", "deny", "no", "cancel", "stop"}
	}
	for _, candidate := range wanted {
		for _, option := range interaction.Options {
			id := strings.ToLower(option.ID)
			label := strings.ToLower(option.Label)
			if strings.Contains(id, candidate) || strings.Contains(label, candidate) {
				return gateway.InteractionAnswer{
					OptionID: option.ID,
					Text:     request.Reason,
				}
			}
		}
	}
	if len(interaction.Options) > 0 {
		index := 0
		if reject && len(interaction.Options) > 1 {
			index = 1
		}
		return gateway.InteractionAnswer{
			OptionID: interaction.Options[index].ID,
			Text:     request.Reason,
		}
	}
	return gateway.InteractionAnswer{Text: request.Reason}
}

func (controller *gatewayTUIController) UpdateSessionTitle(
	ctx context.Context,
	title string,
) error {
	title = strings.TrimSpace(title)
	if title == "" {
		return errors.New("session title is empty")
	}
	_, err := controller.binding.updateMetadata(
		ctx,
		gateway.SessionPatch{Title: &title},
	)
	if err != nil {
		return fmt.Errorf("update trajectory title: %w", err)
	}
	return nil
}

func (controller *gatewayTUIController) RunBangCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	command string,
) {
	_ = cancel
	command = strings.TrimSpace(command)
	if command == "" {
		return
	}
	binding := controller.binding
	binding.App.EmitEvent(cagentruntime.UserMessage(
		"! "+command,
		binding.Session.ID,
		nil,
	))
	binding.App.EmitEvent(cagentruntime.StreamStarted(
		binding.Session.ID,
		gatewayTUIAgentName,
	))
	arguments, _ := json.Marshal(map[string]string{"cmd": command})
	call := sdk.ToolCall{ID: sdk.NewID(), Name: "bash", Arguments: arguments}
	converted, definition := gatewayTUITool(call)
	binding.App.EmitEvent(cagentruntime.ToolCall(
		converted,
		definition,
		gatewayTUIAgentName,
	))
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	cmd := exec.CommandContext(ctx, shell, "-lc", command)
	cmd.Dir = binding.Session.WorkingDir
	outputBytes, runErr := cmd.CombinedOutput()
	output := string(outputBytes)
	binding.App.EmitEvent(cagentruntime.ToolCallResponse(
		converted.ID,
		definition,
		&cagenttools.ToolCallResult{Output: output, IsError: runErr != nil},
		output,
		gatewayTUIAgentName,
	))
	reason := "normal"
	if runErr != nil {
		reason = "error"
	}
	if ctx.Err() != nil {
		reason = "cancelled"
	}
	binding.App.EmitEvent(cagentruntime.StreamStopped(
		binding.Session.ID,
		gatewayTUIAgentName,
		reason,
	))
	if ctx.Err() != nil {
		return
	}
	contextMessage := fmt.Sprintf(
		"The user ran a local shell command via `!` shell mode. Use this result as context and respond normally.\n\nCommand:\n%s\n\nOutput:\n```text\n%s\n```",
		command,
		strings.TrimRight(output, "\r\n"),
	)
	controller.submit(ctx, contextMessage)
}

func (controller *gatewayTUIController) NewSession() {
	controller.binding.App.EmitEvent(cagentruntime.Warning(
		"Open a new trajectory with Ctrl+T.",
		gatewayTUIAgentName,
	))
}

func (controller *gatewayTUIController) ClearSession() {
	controller.NewSession()
}

func (controller *gatewayTUIController) SwitchModel(name string) {
	name = strings.TrimSpace(name)
	controller.binding.mu.Lock()
	current := controller.binding.metadata.Model
	controller.binding.mu.Unlock()
	if name == "" || name == current {
		return
	}
	controller.updateAsync(
		"switch trajectory model",
		gateway.SessionPatch{Model: &name},
	)
}

func (controller *gatewayTUIController) SetAutoCompact(enabled bool) {
	controller.updateAsync(
		"set trajectory auto-compact",
		gateway.SessionPatch{AutoCompact: &enabled},
	)
}

func (controller *gatewayTUIController) SetPermissionRule(kind, pattern string) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return
	}
	controller.updateAsync(
		"set trajectory permission",
		gateway.SessionPatch{PermissionRule: &gateway.PermissionRule{
			Kind: kind, Pattern: pattern,
		}},
	)
}

func (controller *gatewayTUIController) SetThinkingMode(enabled bool) {
	level := "off"
	if enabled {
		level = "high"
	}
	controller.SetThinkingLevel(level)
}

func (controller *gatewayTUIController) SetThinkingLevel(level string) {
	level = strings.ToLower(strings.TrimSpace(level))
	controller.mu.Lock()
	controller.thinking = level
	controller.mu.Unlock()
	controller.updateAsync(
		"set trajectory thinking level",
		gateway.SessionPatch{ThinkingLevel: &level},
	)
}

func (controller *gatewayTUIController) updateAsync(
	action string,
	patch gateway.SessionPatch,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), gatewayCancelTimeout)
		defer cancel()
		if _, err := controller.binding.updateMetadata(ctx, patch); err != nil {
			controller.binding.App.EmitEvent(cagentruntime.Error(
				action + ": " + err.Error(),
			))
		}
	}()
}

func gatewayUserTranscript(
	content string,
	attachments []tuimessages.Attachment,
) string {
	if len(attachments) == 0 {
		return content
	}
	lines := []string{strings.TrimRight(content, "\n")}
	for _, attachment := range attachments {
		name := strings.TrimSpace(attachment.Name)
		if name == "" {
			name = filepath.Base(attachment.FilePath)
		}
		if name == "" {
			name = "attachment"
		}
		lines = append(lines, "⎿  Read "+name)
	}
	return strings.Join(lines, "\n")
}

func gatewayInputContent(
	content string,
	attachments []tuimessages.Attachment,
) string {
	if len(attachments) == 0 {
		return content
	}
	var builder strings.Builder
	builder.WriteString(content)
	for _, attachment := range attachments {
		builder.WriteString("\n\n<attachment")
		if attachment.Name != "" {
			builder.WriteString(" name=")
			encoded, _ := json.Marshal(attachment.Name)
			builder.Write(encoded)
		}
		if attachment.FilePath != "" {
			builder.WriteString(" path=")
			encoded, _ := json.Marshal(attachment.FilePath)
			builder.Write(encoded)
		}
		builder.WriteString(">")
		if attachment.Content != "" {
			builder.WriteString("\n")
			builder.WriteString(attachment.Content)
		}
		builder.WriteString("\n</attachment>")
	}
	return builder.String()
}
