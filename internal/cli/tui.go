package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/lincyaw/ag/gateway"
	gatewayclient "github.com/lincyaw/ag/gateway/client"
	cagentapp "github.com/lincyaw/ag/internal/cagent/app"
	cagentsession "github.com/lincyaw/ag/internal/cagent/session"
	appconfig "github.com/lincyaw/ag/internal/config"
	terminaltui "github.com/lincyaw/ag/internal/tui"
	tuiinput "github.com/lincyaw/ag/internal/tui/input"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

const localGatewayUserID = "local"

func (application *app) runGatewayTUI(
	ctx context.Context,
	config appconfig.Config,
	gatewayTarget string,
	initialPrompt string,
	sessionID string,
	resumeID string,
) error {
	client, err := gatewayclient.New(gatewayclient.Config{
		Target: gatewayTarget,
		UserID: localGatewayUserID,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	sessionID, err = openGatewayTrajectory(
		ctx, client, config, sessionID, resumeID,
	)
	if err != nil {
		return err
	}
	createTemplate, err := gatewayCreateSessionRequest(config, "")
	if err != nil {
		return err
	}
	return application.runAgentView(
		ctx,
		client,
		sessionID,
		agentViewOptions{
			InitialPrompt:  initialPrompt,
			CreateTemplate: &createTemplate,
		},
	)
}

type agentViewOptions struct {
	InitialPrompt  string
	CreateTemplate *gatewayclient.CreateSessionRequest
}

// runAgentView is the shared TUI route for a durable trajectory. CLI commands
// are only deep links into this view; the gateway remains the source of truth
// for conversation history, work state, and live events.
func (application *app) runAgentView(
	ctx context.Context,
	client *gatewayclient.Client,
	sessionID string,
	options agentViewOptions,
) error {
	trajectory, err := client.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("load trajectory %s: %w", sessionID, err)
	}
	binding, err := newGatewayTUIBinding(
		ctx,
		client,
		trajectory,
		options.InitialPrompt,
		options.CreateTemplate,
	)
	if err != nil {
		return err
	}
	defer binding.Close()

	workingDir := trajectory.WorkspaceRoot
	if strings.TrimSpace(workingDir) == "" {
		workingDir = "."
	}
	model := terminaltui.New(
		ctx,
		binding.Spawner(),
		binding.App,
		workingDir,
		binding.Close,
		terminaltui.WithAppName("ag"),
		terminaltui.WithVersion(application.version),
		terminaltui.WithHideSidebar(),
		terminaltui.WithSessionNavigator(
			binding.SessionLister(),
			binding.SessionAttacher(),
		),
	)
	return application.runTerminalTUI(ctx, model, binding.Start)
}

func (application *app) runHistoricalAgentView(
	ctx context.Context,
	client *gatewayclient.Client,
	sessionID string,
	head string,
) error {
	trajectory, err := client.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("load trajectory %s: %w", sessionID, err)
	}
	messages, _, err := loadGatewayConversationAtHead(
		ctx, client, trajectory.ID, strings.TrimSpace(head),
	)
	if err != nil {
		return err
	}
	mirror := gatewayConversationSession(trajectory, messages)
	readonly := cagentapp.New(ctx, mirror, cagentapp.WithReadOnly())
	readonly.SetAgentInfo(
		gatewayTUIToolNames(trajectory), nil, trajectory.Models, trajectory.Model,
	)
	readonly.TrackThinkingLevel(trajectory.ThinkingLevel)
	workingDir := trajectory.WorkspaceRoot
	if strings.TrimSpace(workingDir) == "" {
		workingDir = "."
	}
	model := terminaltui.New(
		ctx,
		func(context.Context, string) (
			*cagentapp.App, *cagentsession.Session, func(), error,
		) {
			return nil, nil, nil, errors.New("historical branch is read-only")
		},
		readonly,
		workingDir,
		nil,
		terminaltui.WithAppName("ag"),
		terminaltui.WithVersion(application.version),
		terminaltui.WithHideSidebar(),
		terminaltui.WithIsolatedView(),
	)
	return application.runTerminalTUI(ctx, model, nil)
}

func (application *app) runTerminalTUI(
	ctx context.Context,
	model tea.Model,
	start func(),
) error {
	coalescer := tuiinput.NewWheelCoalescer()
	filter := func(_ tea.Model, message tea.Msg) tea.Msg {
		if wheel, ok := message.(tea.MouseWheelMsg); ok && coalescer.Handle(wheel) {
			return nil
		}
		return message
	}
	program := tea.NewProgram(
		model,
		tea.WithContext(ctx),
		tea.WithOutput(application.stderr),
		tea.WithFilter(filter),
	)
	coalescer.SetSender(program.Send)
	if settable, ok := model.(interface{ SetProgram(*tea.Program) }); ok {
		settable.SetProgram(program)
	}
	if start != nil {
		start()
	}
	_, err := program.Run()
	if err != nil {
		return fmt.Errorf("trajectory TUI view: %w", err)
	}
	return nil
}

func hydrateGatewayModel(
	ctx context.Context,
	client gatewayHydrationClient,
	session *gatewayInteractiveSession,
	model *interactiveModel,
) error {
	var pendingInputs []gateway.AgentInput
	var after uint64
	for {
		page, err := client.ListInputs(ctx, session.sessionID, gateway.InputQuery{
			After: after, Limit: gatewayEventPageSize,
		})
		if err != nil {
			return fmt.Errorf("list background gateway inputs: %w", err)
		}
		for _, input := range page.Items {
			if input.State.Terminal() {
				continue
			}
			pendingInputs = append(pendingInputs, input)
		}
		if page.Next == 0 || len(page.Items) < gatewayEventPageSize {
			break
		}
		after = page.Next
	}

	after = 0
	var pending *gateway.Interaction
	for {
		page, err := client.ListInteractions(
			ctx,
			session.sessionID,
			gateway.InteractionQuery{
				After: after, Limit: gatewayEventPageSize,
				State: gateway.InteractionPending,
			},
		)
		if err != nil {
			return fmt.Errorf("list pending gateway interactions: %w", err)
		}
		for _, interaction := range page.Items {
			interaction := interaction
			pending = &interaction
		}
		if page.Next == 0 || len(page.Items) < gatewayEventPageSize {
			break
		}
		after = page.Next
	}

	messages, execution, err := loadGatewayConversation(
		ctx,
		client,
		session.sessionID,
	)
	if err != nil {
		return err
	}
	model.hydrateConversation(messages)
	for _, input := range pendingInputs {
		input := input
		if conversationExecutionFinished(execution, input.ExecutionID) {
			continue
		}
		showInput := !conversationContainsExecution(execution, input.ExecutionID)
		model.resumeExecution(input, showInput, func(ctx context.Context) (agentruntime.Result, error) {
			return session.ResumeInput(ctx, input.ID)
		})
	}
	if pending != nil {
		model.resumeInteraction(*pending)
	}
	model.rebuildViewport()
	return nil
}

func loadGatewayConversation(
	ctx context.Context,
	client gatewayConversationClient,
	trajectoryID string,
) ([]sdk.Message, *sdk.TrajectoryExecution, error) {
	return loadGatewayConversationAtHead(ctx, client, trajectoryID, "")
}

func loadGatewayConversationAtHead(
	ctx context.Context,
	client gatewayConversationClient,
	trajectoryID string,
	requestedHead string,
) ([]sdk.Message, *sdk.TrajectoryExecution, error) {
	query := gateway.ConversationQuery{Limit: gatewayEventPageSize}
	head := strings.TrimSpace(requestedHead)
	var (
		messages  []sdk.Message
		execution *sdk.TrajectoryExecution
	)
	for {
		page, err := client.ListConversation(ctx, trajectoryID, head, query)
		if err != nil {
			return nil, nil, fmt.Errorf("load background conversation: %w", err)
		}
		if head == "" {
			head = page.Head
			execution = sdk.CloneTrajectoryExecution(page.Execution)
		} else if page.Head != head {
			return nil, nil, fmt.Errorf(
				"background conversation head changed from %q to %q",
				head,
				page.Head,
			)
		} else if execution == nil {
			execution = sdk.CloneTrajectoryExecution(page.Execution)
		}
		for _, item := range page.Items {
			if item.Continuation {
				if len(messages) == 0 || messages[len(messages)-1].Role != item.Role {
					return nil, nil, errors.New(
						"background conversation contains an invalid continuation",
					)
				}
				messages[len(messages)-1].Content += item.Content
				continue
			}
			messages = append(messages, sdk.Message{
				Role:       item.Role,
				Content:    item.Content,
				ToolCalls:  sdkToolCalls(item.ToolCalls),
				ToolCallID: item.ToolCallID,
				IsError:    item.IsError,
			})
		}
		if page.Next == 0 {
			return messages, execution, nil
		}
		query.After = page.Next
	}
}

func sdkToolCalls(calls []gateway.ConversationToolCall) []sdk.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	result := make([]sdk.ToolCall, len(calls))
	for index, call := range calls {
		result[index] = sdk.ToolCall{ID: call.ID, Name: call.Name}
	}
	return result
}

func conversationContainsExecution(
	execution *sdk.TrajectoryExecution,
	executionID string,
) bool {
	return executionID != "" && execution != nil && execution.ID == executionID
}

func conversationExecutionFinished(
	execution *sdk.TrajectoryExecution,
	executionID string,
) bool {
	return conversationContainsExecution(execution, executionID) &&
		execution.Terminal()
}

type gatewayHydrationClient interface {
	gatewayConversationClient
	ListInputs(context.Context, string, gateway.InputQuery) (gateway.InputPage, error)
	ListInteractions(
		context.Context,
		string,
		gateway.InteractionQuery,
	) (gateway.InteractionPage, error)
}

type gatewayConversationClient interface {
	ListConversation(
		context.Context,
		string,
		string,
		gateway.ConversationQuery,
	) (gateway.ConversationPage, error)
}

func resolveGatewaySessionPrefix(
	ctx context.Context,
	client *gatewayclient.Client,
	id string,
) (string, error) {
	var (
		after   string
		matches []string
	)
	for {
		page, err := client.ListSessions(
			ctx,
			sdk.PageRequest{After: after, Limit: sdk.MaxPageSize},
		)
		if err != nil {
			return "", fmt.Errorf("list trajectories: %w", err)
		}
		for _, session := range page.Items {
			if session.ID == id {
				return id, nil
			}
			if strings.HasPrefix(session.ID, id) {
				matches = append(matches, session.ID)
			}
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	switch len(matches) {
	case 0:
		return id, nil
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf(
			"ambiguous trajectory prefix %q: matches %d trajectories",
			id,
			len(matches),
		)
	}
}

var _ interactiveSession = (*gatewayInteractiveSession)(nil)
