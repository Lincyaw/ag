package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/lincyaw/ag/gateway"
	"github.com/lincyaw/ag/internal/tui/animation"
	"github.com/lincyaw/ag/internal/tui/spinner"
	"github.com/lincyaw/ag/internal/tui/statusbar"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type interactiveState int

const (
	stateInput interactiveState = iota
	stateExecuting
)

var errInteractiveDetached = errors.New("agent view detached")

type chatMessage struct {
	role    string // "user", "assistant", "status", "error"
	content string
}

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
	chat       []chatMessage
	input      textarea.Model
	viewport   viewport.Model
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

	sessionID  string
	turn       int
	toolsDone  int
	toolsTotal int

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

	vp := viewport.New(viewport.WithWidth(80), viewport.WithHeight(20))
	vp.SetContent("")

	sp := spinner.New(spinner.ModeBoth, spinner.DotsStyle)
	sb := statusbar.New(statusbar.WithTitle("ag"))

	return interactiveModel{
		state:        stateInput,
		session:      session,
		styles:       styles,
		input:        ta,
		viewport:     vp,
		spinner:      sp,
		statusBar:    sb,
		sessionID:    sessionID,
		historyIndex: -1,
		execCancels:  make(map[string]context.CancelCauseFunc),
	}
}

func (m interactiveModel) Init() tea.Cmd {
	commands := append([]tea.Cmd(nil), m.initialCmds...)
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

	case tea.MouseWheelMsg:
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(msg)
		return m, cmd

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
			m.chat = append(m.chat, chatMessage{
				role:    "error",
				content: msg.err.Error(),
			})
		} else if msg.err != nil {
			m.chat = append(m.chat, chatMessage{
				role:    "status",
				content: "interrupted",
			})
		} else if msg.result.Output != "" {
			rendered := renderMarkdownContent(os.Stderr, msg.result.Output)
			m.chat = append(m.chat, chatMessage{
				role:    "assistant",
				content: rendered,
			})
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
		m.chat = append(m.chat, chatMessage{
			role: "status", content: interactionDisplay(interaction),
		})
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
			m.chat = append(m.chat, chatMessage{
				role: "error", content: msg.err.Error(),
			})
		} else if m.interaction != nil && m.interaction.ID == msg.interactionID {
			m.chat = append(m.chat, chatMessage{
				role: "status", content: "answer sent",
			})
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
				m.chat = append(m.chat, chatMessage{
					role: "error", content: "this session cannot answer interactions",
				})
				m.rebuildViewport()
				return m, nil
			}
			answer := interactionAnswer(interaction, text)
			m.chat = append(m.chat, chatMessage{role: "user", content: text})
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
		m.chat = append(m.chat, chatMessage{role: "user", content: text})
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
		m.viewport.GotoBottom()
		command := m.startExecution(text)
		if wasExecuting {
			return m, command
		}
		return m, tea.Batch(command, m.spinner.Init())

	case "pgup":
		m.viewport.PageUp()
		return m, nil

	case "pgdown":
		m.viewport.PageDown()
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
		m.chat = append(m.chat, chatMessage{role: "user", content: input.Content})
		m.history = append(m.history, input.Content)
		m.historyIndex = len(m.history)
	}
	m.state = stateExecuting
	m.statusLine = "reconnected to background input"
	m.initialCmds = append(m.initialCmds, m.trackExecution(run))
}

func (m *interactiveModel) hydrateConversation(messages []sdk.Message) {
	for _, message := range messages {
		if strings.TrimSpace(message.Content) == "" {
			continue
		}
		switch message.Role {
		case sdk.RoleUser:
			m.chat = append(m.chat, chatMessage{
				role: "user", content: message.Content,
			})
			m.history = append(m.history, message.Content)
		case sdk.RoleAssistant:
			m.chat = append(m.chat, chatMessage{
				role:    "assistant",
				content: renderMarkdownContent(os.Stderr, message.Content),
			})
		}
	}
	m.historyIndex = len(m.history)
}

func (m *interactiveModel) resumeInteraction(interaction gateway.Interaction) {
	m.interaction = &interaction
	m.chat = append(m.chat, chatMessage{
		role: "status", content: interactionDisplay(interaction),
	})
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
		m.chat = append(m.chat, chatMessage{
			role:    "error",
			content: record.display(),
		})
	}

	label := record.Label
	if record.Detail != "" {
		label += " — " + record.Detail
	}
	m.statusLine = label
}

func (m *interactiveModel) recalculateLayout() {
	wasAtBottom := m.viewport.AtBottom()
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
	m.viewport.SetWidth(vpWidth)
	m.viewport.SetHeight(vpHeight)
	if wasAtBottom {
		m.viewport.GotoBottom()
	}
	m.input.SetWidth(vpWidth - 4)
}

func (m *interactiveModel) rebuildViewport() {
	wasAtBottom := m.viewport.AtBottom()
	var lines []string
	for _, msg := range m.chat {
		switch msg.role {
		case "user":
			lines = append(lines, "")
			lines = append(lines, m.styles.strong.Render("❯")+" "+msg.content)
		case "assistant":
			lines = append(lines, "")
			lines = append(lines, m.styles.brand.Render("⏺")+" "+m.styles.brand.Render("ag"))
			lines = append(lines, msg.content)
		case "error":
			lines = append(lines, m.styles.err.Render("  ⎿ ")+m.styles.muted.Render(msg.content))
		case "status":
			lines = append(lines, m.styles.muted.Render("  ⎿ "+msg.content))
		}
	}

	if m.state == stateExecuting && m.statusLine != "" {
		lines = append(lines, "")
		progress := m.spinner.View() + " " + m.styles.muted.Render(m.statusLine)
		lines = append(lines, progress)
	}

	content := strings.Join(lines, "\n")
	m.viewport.SetContent(content)
	if wasAtBottom {
		m.viewport.GotoBottom()
	}
}

func (m interactiveModel) View() tea.View {
	if m.quitting {
		return tea.NewView("")
	}
	if m.width == 0 {
		return tea.NewView("Loading...")
	}

	vpView := m.viewport.View()
	separator := m.styles.muted.Render(strings.Repeat("─", m.width))
	inputView := m.input.View()
	bottomBorder := m.styles.muted.Render(strings.Repeat("─", m.width))

	m.statusBar.SetWidth(m.width)
	m.updateStatusBarBindings()
	statusView := m.statusBar.View()

	content := lipgloss.JoinVertical(
		lipgloss.Left,
		vpView,
		separator,
		inputView,
		bottomBorder,
		statusView,
	)

	view := tea.NewView(content)
	view.AltScreen = true
	view.MouseMode = tea.MouseModeCellMotion
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
	if m.viewport.TotalLineCount() > m.viewport.Height() {
		bindings = append(bindings, statusbar.HelpBinding{Key: "pgup/pgdn", Desc: "scroll"})
	}
	bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+d", Desc: "exit"})
	m.statusBar.SetBindings(bindings)

	var rightParts []string
	if m.turn > 0 {
		rightParts = append(rightParts, fmt.Sprintf("turn %d", m.turn))
	}
	if m.toolsTotal > 0 {
		rightParts = append(rightParts, fmt.Sprintf("tools %d/%d", m.toolsDone, m.toolsTotal))
	}
	if m.sessionID != "" {
		rightParts = append(rightParts, shortIdentifier(m.sessionID))
	}
	m.statusBar.SetActivity(strings.Join(rightParts, "  "))
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
