// Package statusbar provides a single-line status bar component.
// Copied from AgentM terminal-go with simplified help interface.
package statusbar

import (
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/lincyaw/ag/internal/tui/styles"
)

// HelpBinding represents a single key binding for display.
type HelpBinding struct {
	Key  string
	Desc string
}

// StatusBar displays key-binding help on the left and status info on the right.
type StatusBar struct {
	width int
	title string

	bindings      []HelpBinding
	activity      string
	modeLine      string
	modeLineRight string

	cached     string
	cacheDirty bool
}

// Option is a functional option for configuring a StatusBar.
type Option func(*StatusBar)

// WithTitle sets a custom title for the status bar.
func WithTitle(title string) Option {
	return func(s *StatusBar) {
		s.title = title
	}
}

// New creates a new StatusBar instance.
func New(opts ...Option) *StatusBar {
	s := &StatusBar{
		title:      "ag",
		cacheDirty: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SetWidth sets the width of the status bar.
func (s *StatusBar) SetWidth(width int) {
	if s.width != width {
		s.width = width
		s.cacheDirty = true
	}
}

// SetBindings sets the key binding help to display.
func (s *StatusBar) SetBindings(bindings []HelpBinding) {
	s.bindings = bindings
	s.cacheDirty = true
}

// SetActivity sets the compact background activity label on the right side.
func (s *StatusBar) SetActivity(activity string) {
	if s.activity != activity {
		s.activity = activity
		s.cacheDirty = true
	}
}

// SetModeLine overrides the left help area with a context-specific status phrase.
func (s *StatusBar) SetModeLine(modeLine string) {
	if s.modeLine != modeLine {
		s.modeLine = modeLine
		s.cacheDirty = true
	}
}

// SetModeLineRight sets a right-aligned mode line.
func (s *StatusBar) SetModeLineRight(modeLineRight string) {
	if s.modeLineRight != modeLineRight {
		s.modeLineRight = modeLineRight
		s.cacheDirty = true
	}
}

// Height returns the rendered height (always 1).
func (s *StatusBar) Height() int {
	return 1
}

// InvalidateCache clears cached rendering.
func (s *StatusBar) InvalidateCache() {
	s.cacheDirty = true
}

func (s *StatusBar) rebuild() {
	s.cacheDirty = false

	const pad = 1
	var rightW int
	var rightParts []string

	if s.modeLineRight != "" {
		if strings.Contains(s.modeLineRight, "\x1b[") {
			rightParts = append(rightParts, s.modeLineRight)
		} else {
			rightParts = append(rightParts, styles.SecondaryStyle.Render(s.modeLineRight))
		}
	}
	if s.activity != "" {
		rightParts = append(rightParts, styles.MutedStyle.Render(s.activity))
	}
	if s.title != "" {
		rightParts = append(rightParts, styles.MutedStyle.Render(s.title))
	}
	right := strings.Join(rightParts, "  ")
	rightW = lipgloss.Width(right)

	maxRightW := max(0, s.width-pad)
	if rightW > maxRightW {
		right = ansi.Truncate(right, maxRightW, "...")
		rightW = lipgloss.Width(right)
	}

	maxHelpW := s.width - rightW - 2*pad - 1

	var left string
	var leftW int
	if s.modeLine != "" {
		if strings.Contains(s.modeLine, "\x1b[") {
			left = "  " + s.modeLine
		} else {
			left = "  " + styles.SecondaryStyle.Render(s.modeLine)
		}
		leftW = lipgloss.Width(left)
	} else if len(s.bindings) > 0 && maxHelpW > 0 {
		var parts []string
		for _, b := range s.bindings {
			if b.Key != "" && b.Desc != "" {
				parts = append(parts,
					styles.HighlightWhiteStyle.Render(b.Key)+
						" "+
						styles.SecondaryStyle.Render(b.Desc))
			}
		}
		if len(parts) > 0 {
			helpStr := strings.Join(parts, "  ")
			helpW := lipgloss.Width(helpStr)
			if helpW > maxHelpW {
				helpStr = ansi.Truncate(helpStr, maxHelpW, "...")
			}
			left = "  " + helpStr
			leftW = lipgloss.Width(left)
		}
	}

	gap := max(1, s.width-leftW-rightW-2*pad)
	s.cached = left + strings.Repeat(" ", gap) + right + " "
}

// View renders the status bar.
func (s *StatusBar) View() string {
	if s.cacheDirty {
		s.rebuild()
	}
	return s.cached
}
