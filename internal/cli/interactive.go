package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/textarea"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/lincyaw/ag/gateway"
	"github.com/lincyaw/ag/internal/tui/animation"
	tuiPath "github.com/lincyaw/ag/internal/tui/path"
	"github.com/lincyaw/ag/internal/tui/spinner"
	"github.com/lincyaw/ag/internal/tui/statusbar"
	"github.com/lincyaw/ag/internal/tui/transcript"
	"github.com/lincyaw/ag/internal/tui/types"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type interactiveState int

const (
	stateInput interactiveState = iota
	stateExecuting
)

var errInteractiveDetached = errors.New("agent view detached")

type executionDoneMsg struct {
	requestID string
	result    agentruntime.Result
	err       error
}

type interactionRequestedMsg gateway.Interaction

type interactionResolvedMsg gateway.Interaction

type interactionResponseDoneMsg struct {
	interactionID string
	err           error
}

// interactiveSession is the only execution boundary the TUI model owns. The
// concrete implementation may be an in-process runtime session or a remote
// gateway-backed session.
type interactiveSession interface {
	ID() string
	Prompt(context.Context, string) (agentruntime.Result, error)
}

type interactiveInteractionResponder interface {
	RespondInteraction(
		context.Context,
		gateway.Interaction,
		gateway.InteractionAnswer,
	) error
}

type interactiveModel struct {
	state      interactiveState
	session    interactiveSession
	styles     progressStyles
	input      textarea.Model
	transcript *transcript.Model
	spinner    spinner.Spinner
	statusBar  *statusbar.StatusBar
	statusLine string

	history      []string
	historyIndex int

	width  int
	height int

	execCancels map[string]context.CancelCauseFunc
	interaction *gateway.Interaction
	initialCmds []tea.Cmd

	sessionID   string
	provider    string
	workspace   string
	paused      bool
	showContext bool
	turn        int
	toolsDone   int
	toolsTotal  int

	quitting bool
	detached bool
}

type interactiveEventSink struct {
	program *tea.Program
}

func (s *interactiveEventSink) Observe(_ context.Context, event sdk.Event) {
	if s.program == nil {
		return
	}
	switch event.Name {
	case gateway.GatewayEventInteractionRequested:
		var interaction gateway.Interaction
		if json.Unmarshal(event.Payload, &interaction) == nil {
			s.program.Send(interactionRequestedMsg(interaction))
		}
	case gateway.GatewayEventInteractionResolved:
		fallthrough
	case gateway.GatewayEventInteractionCancelled:
		var interaction gateway.Interaction
		if json.Unmarshal(event.Payload, &interaction) == nil {
			s.program.Send(interactionResolvedMsg(interaction))
		}
	}
	record := progressRecordFromEvent(event)
	if record.Label != "" || record.Detail != "" {
		s.program.Send(progressRecordMsg(record))
	}
}

func newInteractiveModel(
	session interactiveSession,
	sessionID string,
	styles progressStyles,
) interactiveModel {
	ta := textarea.New()
	ta.Placeholder = "Send a message..."
	ta.Prompt = "❯ "
	ta.CharLimit = 0
	ta.SetWidth(80)
	ta.SetHeight(1)
	ta.ShowLineNumbers = false
	ta.KeyMap.InsertNewline.SetEnabled(false)
	ta.Focus()

	sp := spinner.New(spinner.ModeBoth, spinner.DotsStyle)
	sb := statusbar.New(statusbar.WithTitle("ag"))

	return interactiveModel{
		state:        stateInput,
		session:      session,
		styles:       styles,
		input:        ta,
		transcript:   transcript.New(),
		spinner:      sp,
		statusBar:    sb,
		sessionID:    sessionID,
		historyIndex: -1,
		execCancels:  make(map[string]context.CancelCauseFunc),
	}
}

func (m interactiveModel) Init() tea.Cmd {
	commands := append([]tea.Cmd(nil), m.initialCmds...)
	if command := m.transcript.Init(); command != nil {
		commands = append(commands, command)
	}
	if m.state == stateExecuting {
		commands = append(commands, m.spinner.Init())
	}
	return tea.Batch(commands...)
}

func (m interactiveModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		return m.handleKey(msg)

	case tea.MouseWheelMsg,
		tea.MouseClickMsg,
		tea.MouseMotionMsg,
		tea.MouseReleaseMsg:
		handled, transcriptCmd := m.transcript.Update(msg)
		if handled {
			return m, transcriptCmd
		}
		var inputCmd tea.Cmd
		m.input, inputCmd = m.input.Update(msg)
		return m, tea.Batch(transcriptCmd, inputCmd)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.recalculateLayout()
		return m, nil

	case animation.TickMsg:
		if m.state == stateExecuting {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			m.rebuildViewport()
			cmds = append(cmds, cmd, animation.StartTick())
		}
		return m, tea.Batch(cmds...)

	case progressRecordMsg:
		record := progressRecord(msg)
		m.applyProgressRecord(record)
		m.rebuildViewport()
		return m, nil

	case executionDoneMsg:
		delete(m.execCancels, msg.requestID)
		if len(m.execCancels) == 0 {
			m.state = stateInput
			m.spinner.Stop()
			m.interaction = nil
		} else {
			m.state = stateExecuting
		}
		if msg.err != nil && !errors.Is(msg.err, context.Canceled) {
			m.transcript.Append(types.Error(msg.err.Error()))
		} else if msg.err != nil {
			m.transcript.Append(types.Notice("interrupted"))
		} else if msg.result.Output != "" {
			m.transcript.Append(types.Agent(
				types.MessageTypeAssistant,
				"ag",
				msg.result.Output,
			))
		}
		if len(m.execCancels) == 0 {
			m.statusLine = ""
			m.turn = 0
			m.toolsDone = 0
			m.toolsTotal = 0
		} else {
			m.statusLine = fmt.Sprintf(
				"%d queued input(s) remaining",
				len(m.execCancels),
			)
		}
		m.rebuildViewport()
		m.input.Focus()
		return m, nil

	case interactionRequestedMsg:
		interaction := gateway.Interaction(msg)
		if m.interaction != nil && m.interaction.ID == interaction.ID {
			return m, nil
		}
		m.interaction = &interaction
		m.transcript.Append(types.Notice(interactionDisplay(interaction)))
		m.statusLine = "waiting for your answer"
		m.rebuildViewport()
		m.input.Focus()
		return m, nil

	case interactionResolvedMsg:
		interaction := gateway.Interaction(msg)
		if m.interaction != nil && m.interaction.ID == interaction.ID {
			m.interaction = nil
		}
		return m, nil

	case interactionResponseDoneMsg:
		if msg.err != nil {
			m.transcript.Append(types.Error(msg.err.Error()))
		} else if m.interaction != nil && m.interaction.ID == msg.interactionID {
			m.transcript.Append(types.Notice("answer sent"))
			m.interaction = nil
		}
		m.rebuildViewport()
		return m, nil

	default:
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		cmds = append(cmds, cmd)
		return m, tea.Batch(cmds...)
	}
}

func (m interactiveModel) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+b":
		if m.hasBackgroundWork() {
			m.detach()
			return m, tea.Quit
		}

	case "ctrl+d":
		if m.hasBackgroundWork() {
			m.detach()
		} else {
			m.quitting = true
		}
		return m, tea.Quit

	case "ctrl+c":
		if m.state == stateExecuting && len(m.execCancels) > 0 {
			for _, cancel := range m.execCancels {
				cancel(context.Canceled)
			}
			return m, nil
		}
		m.input.Reset()
		return m, nil

	case "enter":
		text := strings.TrimSpace(m.input.Value())
		if text == "" {
			return m, nil
		}
		if m.interaction != nil {
			interaction := *m.interaction
			responder, ok := m.session.(interactiveInteractionResponder)
			if !ok {
				m.transcript.Append(types.Error(
					"this session cannot answer interactions",
				))
				m.rebuildViewport()
				return m, nil
			}
			answer := interactionAnswer(interaction, text)
			m.transcript.Append(types.User(text))
			m.input.Reset()
			return m, func() tea.Msg {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					gatewayCancelTimeout,
				)
				defer cancel()
				err := responder.RespondInteraction(
					ctx, interaction, answer,
				)
				return interactionResponseDoneMsg{
					interactionID: interaction.ID, err: err,
				}
			}
		}
		wasExecuting := m.state == stateExecuting
		m.history = append(m.history, text)
		m.historyIndex = len(m.history)
		m.transcript.Append(types.User(text))
		m.input.Reset()
		m.state = stateExecuting
		if wasExecuting {
			m.statusLine = fmt.Sprintf(
				"queued — %d input(s) pending",
				len(m.execCancels)+1,
			)
		} else {
			m.statusLine = "starting..."
		}
		m.spinner = m.spinner.Reset()
		m.rebuildViewport()
		m.transcript.GotoBottom()
		command := m.startExecution(text)
		if wasExecuting {
			return m, command
		}
		return m, tea.Batch(command, m.spinner.Init())

	case "pgup":
		m.transcript.PageUp()
		return m, nil

	case "pgdown":
		m.transcript.PageDown()
		return m, nil

	case "up":
		if m.state == stateInput && m.input.Value() == "" && len(m.history) > 0 {
			if m.historyIndex > 0 {
				m.historyIndex--
				m.input.SetValue(m.history[m.historyIndex])
			}
			return m, nil
		}

	case "down":
		if m.state == stateInput && m.input.Value() == "" && len(m.history) > 0 {
			if m.historyIndex < len(m.history)-1 {
				m.historyIndex++
				m.input.SetValue(m.history[m.historyIndex])
			} else {
				m.historyIndex = len(m.history)
				m.input.SetValue("")
			}
			return m, nil
		}
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m *interactiveModel) startExecution(prompt string) tea.Cmd {
	return m.trackExecution(func(ctx context.Context) (agentruntime.Result, error) {
		return m.session.Prompt(ctx, prompt)
	})
}

func (m *interactiveModel) trackExecution(
	run func(context.Context) (agentruntime.Result, error),
) tea.Cmd {
	ctx, cancel := context.WithCancelCause(context.Background())
	requestID := sdk.NewID()
	m.execCancels[requestID] = cancel
	return func() tea.Msg {
		result, err := run(ctx)
		return executionDoneMsg{requestID: requestID, result: result, err: err}
	}
}

func (m *interactiveModel) hasBackgroundWork() bool {
	return len(m.execCancels) > 0 || m.interaction != nil
}

func (m *interactiveModel) detach() {
	m.detached = true
	m.quitting = true
	for _, cancel := range m.execCancels {
		cancel(errInteractiveDetached)
	}
}

func (m *interactiveModel) resumeExecution(
	input gateway.AgentInput,
	showInput bool,
	run func(context.Context) (agentruntime.Result, error),
) {
	if showInput {
		m.transcript.Append(types.User(input.Content))
		m.history = append(m.history, input.Content)
		m.historyIndex = len(m.history)
	}
	m.state = stateExecuting
	m.statusLine = "reconnected to background input"
	m.initialCmds = append(m.initialCmds, m.trackExecution(run))
}

func (m *interactiveModel) hydrateConversation(messages []sdk.Message) {
	transcriptMessages := make([]*types.Message, 0, len(messages))
	pendingTools := make(map[string]string)
	var pendingToolIDs []string
	for _, message := range messages {
		switch message.Role {
		case sdk.RoleUser:
			if strings.TrimSpace(message.Content) == "" {
				continue
			}
			transcriptMessages = append(
				transcriptMessages,
				types.User(message.Content),
			)
			m.history = append(m.history, message.Content)
		case sdk.RoleAssistant:
			if strings.TrimSpace(message.Content) != "" {
				transcriptMessages = append(
					transcriptMessages,
					types.Agent(types.MessageTypeAssistant, "ag", message.Content),
				)
			}
			for _, call := range message.ToolCalls {
				if call.ID == "" {
					continue
				}
				if _, exists := pendingTools[call.ID]; !exists {
					pendingToolIDs = append(pendingToolIDs, call.ID)
				}
				pendingTools[call.ID] = emptyAs(call.Name, "tool")
			}
		case sdk.RoleTool:
			name := emptyAs(pendingTools[message.ToolCallID], "tool")
			content := historicalToolResult(name, message.Content, false)
			if message.IsError {
				transcriptMessages = append(transcriptMessages, types.Error(content))
			} else {
				transcriptMessages = append(transcriptMessages, types.Notice(content))
			}
			delete(pendingTools, message.ToolCallID)
		}
	}
	for _, id := range pendingToolIDs {
		name, exists := pendingTools[id]
		if !exists {
			continue
		}
		transcriptMessages = append(
			transcriptMessages,
			types.Notice(historicalToolResult(name, "", true)),
		)
	}
	_ = m.transcript.Load(transcriptMessages)
	m.historyIndex = len(m.history)
}

func historicalToolResult(name, content string, pending bool) string {
	if pending {
		return name + " — pending"
	}
	summary := summarizeText(content, 160)
	if summary == "" {
		summary = "completed"
	}
	return name + " — " + summary
}

func (m *interactiveModel) hydrateSession(session gateway.Session) {
	m.sessionID = session.ID
	m.provider = session.Provider
	m.workspace = session.WorkspaceRoot
	m.paused = session.Paused
	m.showContext = true
}

func (m *interactiveModel) resumeInteraction(interaction gateway.Interaction) {
	m.interaction = &interaction
	m.transcript.Append(types.Notice(interactionDisplay(interaction)))
	m.statusLine = "waiting for your answer"
}

func (m *interactiveModel) applyProgressRecord(record progressRecord) {
	if record.SessionID != "" {
		m.sessionID = record.SessionID
	}
	if record.Turn > 0 {
		m.turn = record.Turn
	}
	switch record.Status {
	case progressStatusTool:
		m.toolsTotal++
	case progressStatusOK, progressStatusError:
		if record.ToolName != "" {
			m.toolsDone++
		}
	}

	if record.Status == progressStatusError {
		m.transcript.Append(types.Error(record.display()))
	}

	label := record.Label
	if record.Detail != "" {
		label += " — " + record.Detail
	}
	m.statusLine = label
}

func (m *interactiveModel) recalculateLayout() {
	const editorHeight = 3
	const separatorHeight = 1
	const bottomBorderHeight = 1
	const statusHeight = 1
	chrome := editorHeight + separatorHeight + bottomBorderHeight + statusHeight
	vpHeight := m.height - chrome
	if vpHeight < 3 {
		vpHeight = 3
	}
	vpWidth := m.width
	if vpWidth < 20 {
		vpWidth = 80
	}
	m.transcript.SetSize(vpWidth, vpHeight)
	m.input.SetWidth(vpWidth - 4)
}

func (m *interactiveModel) rebuildViewport() {
	tail := ""
	if m.state == stateExecuting && m.statusLine != "" {
		progress := m.spinner.View() + " " + m.styles.muted.Render(m.statusLine)
		tail = "\n" + progress
	}
	m.transcript.SetTail(tail)
}

func (m interactiveModel) View() tea.View {
	if m.quitting {
		return tea.NewView("")
	}
	if m.width == 0 {
		return tea.NewView("Loading...")
	}

	transcriptView := m.transcript.View()
	separator := m.styles.muted.Render(strings.Repeat("─", m.width))
	inputView := m.input.View()
	bottomBorder := m.styles.muted.Render(strings.Repeat("─", m.width))

	m.statusBar.SetWidth(m.width)
	m.updateStatusBarBindings()
	statusView := m.statusBar.View()

	content := lipgloss.JoinVertical(
		lipgloss.Left,
		transcriptView,
		separator,
		inputView,
		bottomBorder,
		statusView,
	)

	view := tea.NewView(content)
	view.MouseMode = tea.MouseModeAllMotion
	return view
}

func (m *interactiveModel) updateStatusBarBindings() {
	var bindings []statusbar.HelpBinding
	if m.interaction != nil {
		bindings = append(bindings, statusbar.HelpBinding{Key: "enter", Desc: "answer"})
		bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+b", Desc: "background"})
		bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+c", Desc: "cancel"})
	} else if m.state == stateExecuting {
		bindings = append(bindings, statusbar.HelpBinding{Key: "enter", Desc: "queue"})
		bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+b", Desc: "background"})
		bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+c", Desc: "cancel"})
	} else {
		bindings = append(bindings, statusbar.HelpBinding{Key: "enter", Desc: "send"})
	}
	if m.transcript.LineCount() > m.transcript.Height() {
		bindings = append(bindings, statusbar.HelpBinding{Key: "pgup/pgdn", Desc: "scroll"})
	}
	bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+d", Desc: "exit"})
	m.statusBar.SetBindings(bindings)

	var rightParts []string
	if m.showContext {
		rightParts = append(rightParts, m.agentStatus())
	}
	if m.turn > 0 {
		rightParts = append(rightParts, fmt.Sprintf("turn %d", m.turn))
	}
	if m.toolsTotal > 0 {
		rightParts = append(rightParts, fmt.Sprintf("tools %d/%d", m.toolsDone, m.toolsTotal))
	}
	if m.provider != "" {
		rightParts = append(rightParts, m.provider)
	}
	if m.workspace != "" {
		rightParts = append(rightParts, compactWorkspace(m.workspace))
	}
	if m.sessionID != "" {
		rightParts = append(rightParts, shortIdentifier(m.sessionID))
	}
	m.statusBar.SetActivity(strings.Join(rightParts, "  "))
}

func (m interactiveModel) agentStatus() string {
	switch {
	case m.interaction != nil:
		return agentStatusWaiting
	case m.state == stateExecuting:
		return agentStatusRunning
	case m.paused:
		return agentStatusPaused
	default:
		return agentStatusIdle
	}
}

func compactWorkspace(workspace string) string {
	const maxRunes = 24
	workspace = tuiPath.ShortenHome(workspace)
	runes := []rune(workspace)
	if len(runes) <= maxRunes {
		return workspace
	}
	return "…" + string(runes[len(runes)-maxRunes+1:])
}

func interactionDisplay(interaction gateway.Interaction) string {
	lines := []string{"Question: " + interaction.Prompt}
	for index, option := range interaction.Options {
		line := fmt.Sprintf("%d. %s", index+1, option.Label)
		if option.Description != "" {
			line += " — " + option.Description
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func interactionAnswer(
	interaction gateway.Interaction,
	value string,
) gateway.InteractionAnswer {
	value = strings.TrimSpace(value)
	for index, option := range interaction.Options {
		if value == fmt.Sprint(index+1) ||
			strings.EqualFold(value, option.ID) ||
			strings.EqualFold(value, option.Label) {
			return gateway.InteractionAnswer{OptionID: option.ID}
		}
	}
	return gateway.InteractionAnswer{Text: value}
}
