package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/lincyaw/ag/sdk"
)

const (
	progressAuto   = "auto"
	progressAlways = "always"
	progressPlain  = "plain"
	progressTUI    = "tui"
	progressNever  = "never"

	colorAuto   = "auto"
	colorAlways = "always"
	colorNever  = "never"
)

type progressReporter struct {
	writer   io.Writer
	input    io.Reader
	styles   progressStyles
	useTUI   bool
	program  *tea.Program
	done     chan error
	lineMu   sync.Mutex
	queue    *progressRecordQueue
	stopOnce sync.Once
	stopErr  error
}

func (application *app) progressReporter() *progressReporter {
	if application.output != outputText {
		return nil
	}
	terminal := isTerminal(application.stderr)
	var input io.Reader
	if terminal && isReaderTerminal(os.Stdin) {
		input = os.Stdin
	}
	switch application.progress {
	case progressAlways:
		return newProgressReporter(
			application.stderr,
			input,
			application.colorEnabled(application.stderr),
			application.colorForced(),
			terminal,
		)
	case progressPlain:
		return newProgressReporter(
			application.stderr,
			nil,
			application.colorEnabled(application.stderr),
			application.colorForced(),
			false,
		)
	case progressTUI:
		return newProgressReporter(
			application.stderr,
			input,
			application.colorEnabled(application.stderr),
			application.colorForced(),
			terminal,
		)
	case progressAuto:
		if !terminal {
			return nil
		}
		return newProgressReporter(
			application.stderr,
			input,
			application.colorEnabled(application.stderr),
			application.colorForced(),
			true,
		)
	default:
		return nil
	}
}

func newProgressReporter(
	writer io.Writer,
	input io.Reader,
	useColor bool,
	forceColor bool,
	useTUI bool,
) *progressReporter {
	reporter := &progressReporter{
		writer: writer,
		input:  input,
		styles: newProgressStyles(writer, useColor, forceColor),
		useTUI: useTUI,
	}
	reporter.queue = newProgressRecordQueue(reporter.deliver)
	return reporter
}

func (application *app) colorEnabled(writer io.Writer) bool {
	switch application.color {
	case colorAlways:
		return true
	case colorNever:
		return false
	case colorAuto, "":
		return isTerminal(writer)
	default:
		return false
	}
}

func (application *app) colorForced() bool {
	return application.color == colorAlways
}

func (reporter *progressReporter) start(cancel context.CancelFunc) error {
	if reporter == nil {
		return nil
	}
	if !reporter.useTUI {
		reporter.queue.start()
		return nil
	}
	reporter.done = make(chan error, 1)
	options := []tea.ProgramOption{
		tea.WithOutput(reporter.writer),
		tea.WithoutSignalHandler(),
	}
	if reporter.input == nil {
		options = append(options, tea.WithInput(nil))
	} else {
		options = append(options, tea.WithInput(reporter.input))
	}
	reporter.program = tea.NewProgram(
		newProgressModel(reporter.styles, cancel),
		options...,
	)
	go func() {
		_, err := reporter.program.Run()
		reporter.done <- err
	}()
	reporter.queue.start()
	return nil
}

func (reporter *progressReporter) stop() error {
	if reporter == nil {
		return nil
	}
	reporter.stopOnce.Do(func() {
		if dropped := reporter.queue.close(); dropped > 0 {
			reporter.deliver(progressDroppedRecord(dropped))
		}
		if reporter.program == nil {
			return
		}
		reporter.program.Send(progressDoneMsg{})
		select {
		case reporter.stopErr = <-reporter.done:
		case <-time.After(2 * time.Second):
			reporter.program.Kill()
			select {
			case reporter.stopErr = <-reporter.done:
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
	return reporter.stopErr
}

func (reporter *progressReporter) Observe(_ context.Context, event sdk.Event) {
	if reporter == nil || reporter.writer == nil {
		return
	}
	record := progressRecordFromEvent(event)
	record.EventName = event.Name
	record.At = time.Now()
	if record.Label == "" && record.Detail == "" {
		return
	}
	reporter.queue.push(record)
}

func (reporter *progressReporter) deliver(record progressRecord) {
	if reporter.program != nil {
		reporter.program.Send(progressRecordMsg(record))
		return
	}
	reporter.writeLine(record)
}

func (reporter *progressReporter) writeLine(record progressRecord) {
	reporter.lineMu.Lock()
	defer reporter.lineMu.Unlock()
	label := reporter.styles.status(record.Status)
	if label == "" {
		label = "INFO"
	}
	prefix := reporter.styles.brand.Render("ag")
	_, _ = fmt.Fprintf(
		reporter.writer,
		"%s  %s  %s\n",
		prefix,
		label,
		record.line(),
	)
}
