package messages

import (
	"fmt"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	cagentsession "github.com/lincyaw/ag/internal/cagent/session"
	"github.com/lincyaw/ag/internal/tui/service"
)

func TestLongUserMessageExpandsWithKeyboard(t *testing.T) {
	t.Parallel()
	m := longUserMessageModel(t)

	m.Focus()
	if m.selectedMessageIndex != 0 {
		t.Fatalf("selected message = %d, want 0", m.selectedMessageIndex)
	}
	if !strings.Contains(m.View(), "[+] expand") {
		t.Fatal("long user message did not start collapsed")
	}

	m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	view := m.View()
	if strings.Contains(view, "[+] expand") || !strings.Contains(view, "line 31") {
		t.Fatalf("Enter did not expand the selected user message:\n%s", view)
	}

	m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if view := m.View(); !strings.Contains(view, "[+] expand") {
		t.Fatalf("second Enter did not collapse the selected user message:\n%s", view)
	}
}

func TestArrowKeysScrollWithinOnlySelectedMessage(t *testing.T) {
	t.Parallel()
	m := longUserMessageModel(t)
	m.SetSize(80, 8)
	m.Focus()
	m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})

	before := m.scrollOffset
	m.Update(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.scrollOffset <= before {
		t.Fatalf("Down offset = %d, want greater than %d", m.scrollOffset, before)
	}

	m.Update(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.scrollOffset != before {
		t.Fatalf("Up offset = %d, want %d", m.scrollOffset, before)
	}
}

func TestLongUserMessageExpandsWithMouse(t *testing.T) {
	t.Parallel()
	m := longUserMessageModel(t)
	m.SetPosition(1, 0)
	m.ensureAllItemsRendered()

	toggleable := m.views[0].(toggleableView)
	item := m.renderItem(0, m.views[0])
	toggleLine := -1
	for line := range item.height {
		if toggleable.IsToggleLine(line) {
			toggleLine = line
			break
		}
	}
	if toggleLine < 0 {
		t.Fatal("long user message has no clickable toggle line")
	}

	m.Update(tea.MouseClickMsg{
		X:      m.xPos + 2,
		Y:      m.yPos + toggleLine - m.scrollOffset,
		Button: tea.MouseLeft,
	})
	if view := m.View(); strings.Contains(view, "[+] expand") || !strings.Contains(view, "line 31") {
		t.Fatalf("mouse click did not expand the long user message:\n%s", view)
	}
}

func longUserMessageModel(t *testing.T) *model {
	t.Helper()
	sess := cagentsession.New()
	state := service.NewSessionState(sess)
	m := newModel(80, 40, state, false)
	lines := make([]string, 32)
	for i := range lines {
		lines[i] = fmt.Sprintf("line %02d", i)
	}
	m.AddUserMessage(strings.Join(lines, "\n"))
	return m
}
