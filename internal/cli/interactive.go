package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/lincyaw/ag/internal/tui/animation"
	"github.com/lincyaw/ag/internal/tui/spinner"
	"github.com/lincyaw/ag/internal/tui/statusbar"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type interactiveState int

const (
	stateInput     interactiveState = iota
	stateExecuting
)

type chatMessage struct {
	role    string // "user", "assistant", "status", "error"
	content string
}

type executionDoneMsg struct {
	result agentruntime.Result
	err    error
}

type interactiveModel struct {
	state      interactiveState
	session    *agentruntime.Session
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

	execCancel context.CancelFunc

	sessionID  string
	turn       int
	toolsDone  int
	toolsTotal int

	quitting bool
}

type interactiveEventSink struct {
	program *tea.Program
}

func (s *interactiveEventSink) Observe(_ context.Context, event sdk.Event) {
	if s.program == nil {
		return
	}
	record := progressRecordFromEvent(event)
	if record.Label != "" || record.Detail != "" {
		s.program.Send(progressRecordMsg(record))
	}
}

func newInteractiveModel(
	session *agentruntime.Session,
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
	}
}

func (m interactiveModel) Init() tea.Cmd {
	return nil
}

func (m interactiveModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		return m.handleKey(msg)

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
		m.state = stateInput
		m.execCancel = nil
		m.spinner.Stop()
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
		m.statusLine = ""
		m.turn = 0
		m.toolsDone = 0
		m.toolsTotal = 0
		m.rebuildViewport()
		m.input.Focus()
		return m, nil

	default:
		if m.state == stateInput {
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			cmds = append(cmds, cmd)
		}
		return m, tea.Batch(cmds...)
	}
}

func (m interactiveModel) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+d":
		m.quitting = true
		return m, tea.Quit

	case "ctrl+c":
		if m.state == stateExecuting && m.execCancel != nil {
			m.execCancel()
			return m, nil
		}
		m.input.Reset()
		return m, nil

	case "enter":
		if m.state == stateInput {
			text := strings.TrimSpace(m.input.Value())
			if text == "" {
				return m, nil
			}
			m.history = append(m.history, text)
			m.historyIndex = len(m.history)
			m.chat = append(m.chat, chatMessage{role: "user", content: text})
			m.input.Reset()
			m.state = stateExecuting
			m.statusLine = "starting..."
			m.spinner = m.spinner.Reset()
			m.rebuildViewport()
			m.input.Blur()
			return m, tea.Batch(m.startExecution(text), m.spinner.Init())
		}
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

	if m.state == stateInput {
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m *interactiveModel) startExecution(prompt string) tea.Cmd {
	ctx, cancel := context.WithCancel(context.Background())
	m.execCancel = cancel
	session := m.session
	return func() tea.Msg {
		result, err := session.Prompt(ctx, prompt)
		return executionDoneMsg{result: result, err: err}
	}
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
	m.input.SetWidth(vpWidth - 4)
}

func (m *interactiveModel) rebuildViewport() {
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
	m.viewport.GotoBottom()
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
	return view
}

func (m *interactiveModel) updateStatusBarBindings() {
	var bindings []statusbar.HelpBinding
	if m.state == stateExecuting {
		bindings = append(bindings, statusbar.HelpBinding{Key: "ctrl+c", Desc: "cancel"})
	} else {
		bindings = append(bindings, statusbar.HelpBinding{Key: "enter", Desc: "send"})
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
