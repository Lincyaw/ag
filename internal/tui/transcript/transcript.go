// Package transcript owns the durable, scrollable message view used by the
// agent TUI. It follows terminal-go's separation between the complete message
// list and the currently visible terminal window: loading or appending a
// message never replaces older transcript entries.
package transcript

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/lincyaw/ag/internal/tui/message"
	"github.com/lincyaw/ag/internal/tui/scrollview"
	"github.com/lincyaw/ag/internal/tui/types"
)

type entry struct {
	message *types.Message
	view    message.Model
	width   int
	lines   []string
}

// Model stores the complete transcript independently from its scroll offset.
// Message views are retained so their markdown renderers and Claude-style
// presentation can be reused while the visible window moves over the list.
type Model struct {
	entries   []entry
	tailLines []string
	lines     []string
	scroll    *scrollview.Model
	width     int
	height    int
}

// New creates an empty transcript using the same message and scroll-view
// components as terminal-go.
func New() *Model {
	scroll := scrollview.New(
		scrollview.WithReserveScrollbarSpace(true),
		scrollview.WithShowScrollbar(true),
	)
	model := &Model{scroll: scroll, width: 80, height: 20}
	model.scroll.SetSize(model.width, model.height)
	model.scroll.SetContent(nil, 0)
	return model
}

// Init initializes retained message views.
func (m *Model) Init() tea.Cmd {
	commands := make([]tea.Cmd, 0, len(m.entries))
	for _, item := range m.entries {
		if command := item.view.Init(); command != nil {
			commands = append(commands, command)
		}
	}
	return tea.Batch(commands...)
}

// Load replaces the transcript with a complete, ordered conversation. The
// operation is intentionally distinct from Append: session hydration should
// establish one authoritative history before live events are attached.
func (m *Model) Load(messages []*types.Message) tea.Cmd {
	m.entries = nil
	previous := (*types.Message)(nil)
	commands := make([]tea.Cmd, 0, len(messages))
	for _, item := range messages {
		if item == nil {
			continue
		}
		view := message.New(item, previous)
		m.entries = append(m.entries, entry{message: item, view: view})
		if command := view.Init(); command != nil {
			commands = append(commands, command)
		}
		previous = item
	}
	for index := 0; index+1 < len(m.entries); index++ {
		m.entries[index].view.Finalize()
	}
	m.rebuildEntries()
	m.rebuildContent()
	m.scroll.ScrollToBottom()
	return tea.Batch(commands...)
}

// Append adds one message without disturbing prior transcript entries. It
// follows new output only when the user was already at the bottom.
func (m *Model) Append(item *types.Message) tea.Cmd {
	if item == nil {
		return nil
	}
	follow := m.AtBottom()
	var previous *types.Message
	if len(m.entries) > 0 {
		last := &m.entries[len(m.entries)-1]
		last.view.Finalize()
		previous = last.message
	}
	view := message.New(item, previous)
	m.entries = append(m.entries, entry{message: item, view: view})
	m.renderEntry(len(m.entries) - 1)
	m.rebuildContent()
	if follow {
		m.scroll.ScrollToBottom()
	}
	return view.Init()
}

// SetTail sets transient lines, such as the current agent activity, below the
// durable transcript without turning them into history messages.
func (m *Model) SetTail(value string) {
	follow := m.AtBottom()
	value = strings.TrimRight(value, "\n")
	if value == "" {
		m.tailLines = nil
	} else {
		m.tailLines = strings.Split(value, "\n")
	}
	m.rebuildContent()
	if follow {
		m.scroll.ScrollToBottom()
	}
}

// SetSize updates the visible window and re-renders messages at the new
// content width while preserving the user's scroll intent.
func (m *Model) SetSize(width, height int) tea.Cmd {
	follow := m.AtBottom()
	m.width = max(1, width)
	m.height = max(1, height)
	m.scroll.SetSize(m.width, m.height)
	for index := range m.entries {
		m.entries[index].width = 0
	}
	m.rebuildEntries()
	m.rebuildContent()
	if follow {
		m.scroll.ScrollToBottom()
	}
	return nil
}

// Update handles transcript scrolling. The caller keeps editor key handling
// separate, exactly as the surrounding agent view does for other components.
func (m *Model) Update(msg tea.Msg) (bool, tea.Cmd) {
	return m.scroll.Update(msg)
}

// View renders only the visible window while the complete transcript remains
// retained in entries and lines.
func (m *Model) View() string {
	return m.scroll.View()
}

func (m *Model) PageUp()        { m.scroll.PageUp() }
func (m *Model) PageDown()      { m.scroll.PageDown() }
func (m *Model) GotoBottom()    { m.scroll.ScrollToBottom() }
func (m *Model) YOffset() int   { return m.scroll.ScrollOffset() }
func (m *Model) Height() int    { return m.height }
func (m *Model) Len() int       { return len(m.entries) }
func (m *Model) LineCount() int { return len(m.lines) }

// Messages returns a stable snapshot of the transcript's logical messages.
func (m *Model) Messages() []*types.Message {
	result := make([]*types.Message, len(m.entries))
	for index, item := range m.entries {
		result[index] = item.message
	}
	return result
}

// AtBottom reports whether new output should continue following the agent.
func (m *Model) AtBottom() bool {
	return m.scroll.ScrollOffset() >= max(0, len(m.lines)-m.height)
}

// Content returns the complete rendered transcript, primarily for export and
// deterministic verification. It is not limited to the visible window.
func (m *Model) Content() string {
	return strings.Join(m.lines, "\n")
}

func (m *Model) rebuildEntries() {
	for index := range m.entries {
		m.renderEntry(index)
	}
}

func (m *Model) renderEntry(index int) {
	item := &m.entries[index]
	width := m.scroll.ContentWidth()
	if item.width == width && item.lines != nil {
		return
	}
	_ = item.view.SetSize(width, 0)
	rendered := item.view.View()
	item.width = width
	item.lines = strings.Split(rendered, "\n")
}

func (m *Model) rebuildContent() {
	count := len(m.tailLines)
	for _, item := range m.entries {
		count += len(item.lines)
	}
	lines := make([]string, 0, count)
	for _, item := range m.entries {
		lines = append(lines, item.lines...)
	}
	lines = append(lines, m.tailLines...)
	m.lines = lines
	m.scroll.SetContent(m.lines, len(m.lines))
}
