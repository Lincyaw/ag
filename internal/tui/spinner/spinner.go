package spinner

import (
	"math/rand/v2"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/lincyaw/ag/internal/tui/animation"
	"github.com/lincyaw/ag/internal/tui/styles"
)

type Mode int

const (
	ModeBoth Mode = iota
	ModeSpinnerOnly
)

type Spinner interface {
	Init() tea.Cmd
	Update(msg tea.Msg) (Spinner, tea.Cmd)
	View() string
	Reset() Spinner
	Stop()
	RawFrame() string
	FrameIndex() int
	CurrentMessage() string
}

type spinner struct {
	animSub             *animation.Subscription
	dotsStyle           lipgloss.Style
	styledSpinnerFrames []string
	mode                Mode
	currentMessage      string
	lightPosition       int
	frame               int
	direction           int
	pauseFrames         int
}

var defaultMessages = []string{
	"Accomplishing",
	"Cascading",
	"Channeling",
	"Computing",
	"Determining",
	"Envisioning",
	"Gusting",
	"Infusing",
	"Manifesting",
	"Nucleating",
	"Tempering",
	"Tinkering",
	"Whisking",
}

// DotsStyle is the default style for spinner dots (exported for use in New).
var DotsStyle = styles.SpinnerDotsAccentStyle

func New(mode Mode, dotsStyle lipgloss.Style) Spinner {
	styledFrames := make([]string, len(Frames))
	for i, char := range Frames {
		styledFrames[i] = dotsStyle.Render(char)
	}

	sub := &animation.Subscription{}
	return &spinner{
		animSub:             sub,
		dotsStyle:           dotsStyle,
		styledSpinnerFrames: styledFrames,
		mode:                mode,
		currentMessage:      defaultMessages[rand.IntN(len(defaultMessages))],
		lightPosition:       -3,
		direction:           1,
	}
}

func (s *spinner) Reset() Spinner {
	return New(s.mode, s.dotsStyle)
}

func (s *spinner) Update(message tea.Msg) (Spinner, tea.Cmd) {
	if msg, ok := message.(animation.TickMsg); ok {
		s.frame = msg.Frame
		if s.mode == ModeBoth {
			if s.pauseFrames > 0 {
				s.pauseFrames--
				if s.pauseFrames == 0 {
					s.direction = -1
				}
			} else {
				s.lightPosition += s.direction
				if s.direction == 1 && s.lightPosition > len([]rune(s.currentMessage))+2 {
					s.pauseFrames = 6
				} else if s.direction == -1 && s.lightPosition < -3 {
					s.direction = 1
				}
			}
		}
	}
	return s, nil
}

func (s *spinner) View() string {
	frame := s.styledSpinnerFrames[s.frame%len(s.styledSpinnerFrames)]
	if s.mode == ModeSpinnerOnly {
		return frame
	}
	return frame + " " + s.renderMessage()
}

func (s *spinner) Init() tea.Cmd {
	return s.animSub.Start()
}

func (s *spinner) Stop() {
	s.animSub.Stop()
}

func (s *spinner) RawFrame() string {
	return Frames[s.frame%len(Frames)]
}

func (s *spinner) FrameIndex() int {
	return s.frame
}

func (s *spinner) CurrentMessage() string {
	return s.currentMessage
}

// Frames holds the braille animation frames.
var Frames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// Frame returns the spinner character for the given animation frame.
func Frame(index int) string {
	return Frames[index%len(Frames)]
}

// lightStyles maps distance from light position to style.
var lightStyles = []lipgloss.Style{
	styles.SpinnerTextBrightestStyle,
	styles.SpinnerTextBrightStyle,
	styles.SpinnerTextDimStyle,
	styles.SpinnerTextDimmestStyle,
}

func (s *spinner) renderMessage() string {
	var out strings.Builder
	for i, char := range s.currentMessage {
		dist := abs(i - s.lightPosition)
		if dist >= len(lightStyles) {
			dist = len(lightStyles) - 1
		}
		out.WriteString(lightStyles[dist].Render(string(char)))
	}
	return out.String()
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
