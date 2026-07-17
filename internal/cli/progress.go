package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/lincyaw/ag/sdk"
	"github.com/muesli/termenv"
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
	return &progressReporter{
		writer: writer,
		input:  input,
		styles: newProgressStyles(writer, useColor, forceColor),
		useTUI: useTUI,
	}
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
	if reporter == nil || !reporter.useTUI {
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
	return nil
}

func (reporter *progressReporter) stop() error {
	if reporter == nil {
		return nil
	}
	reporter.stopOnce.Do(func() {
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

func (reporter *progressReporter) observe(_ context.Context, event sdk.Event) {
	if reporter == nil || reporter.writer == nil {
		return
	}
	record := reporter.record(event)
	record.EventName = event.Name
	record.At = time.Now()
	if record.Label == "" && record.Detail == "" {
		return
	}
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

func (reporter *progressReporter) record(event sdk.Event) progressRecord {
	switch event.Name {
	case sdk.EventAgentStart:
		return progressRecord{
			Status:    progressStatusRun,
			SessionID: event.SessionID,
			Label:     "session",
			Detail:    emptyAs(event.SessionID, "new"),
		}
	case sdk.EventTurnStart:
		var payload sdk.TurnStartPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		return progressRecord{
			Status: progressStatusModel,
			Turn:   payload.Turn + 1,
			Label:  fmt.Sprintf("turn %d", payload.Turn+1),
			Detail: "preparing model request",
		}
	case sdk.EventBeforeProvider:
		var payload sdk.BeforeProviderPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		detail := fmt.Sprintf(
			"%d message(s), %d tool(s) available",
			len(payload.Messages),
			len(payload.Tools),
		)
		return progressRecord{
			Status:   progressStatusModel,
			Turn:     payload.Turn + 1,
			Provider: payload.Provider,
			Label:    emptyAs(payload.Provider, "provider"),
			Detail:   detail,
		}
	case sdk.EventAfterProvider:
		var payload sdk.AfterProviderPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		if payload.Error != "" {
			return progressRecord{
				Status:   progressStatusError,
				Turn:     payload.Turn + 1,
				Provider: payload.Provider,
				Label:    emptyAs(payload.Provider, "provider"),
				Detail:   "provider failed: " + summarizeText(payload.Error, 180),
			}
		}
		if payload.Response == nil {
			return progressRecord{
				Status:   progressStatusModel,
				Turn:     payload.Turn + 1,
				Provider: payload.Provider,
				Label:    emptyAs(payload.Provider, "provider"),
				Detail:   "provider returned",
			}
		}
		if len(payload.Response.ToolCalls) == 0 {
			return progressRecord{
				Status:   progressStatusAnswer,
				Turn:     payload.Turn + 1,
				Provider: payload.Provider,
				Label:    "answer",
				Detail:   summarizeModelResponse(*payload.Response),
			}
		}
		return progressRecord{
			Status:   progressStatusPlan,
			Turn:     payload.Turn + 1,
			Provider: payload.Provider,
			Label:    "tool plan",
			Detail:   summarizeToolCalls(payload.Response.ToolCalls),
		}
	case sdk.EventBeforeTool:
		var payload sdk.BeforeToolPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		return progressRecord{
			Status:   progressStatusTool,
			Turn:     payload.Turn + 1,
			ToolName: payload.Call.Name,
			Label:    emptyAs(payload.Call.Name, "tool"),
			Detail:   summarizeArguments(payload.Call.Arguments),
		}
	case sdk.EventAfterTool:
		var payload sdk.AfterToolPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		status := progressStatusOK
		if payload.Result.IsError {
			status = progressStatusError
		}
		return progressRecord{
			Status:   status,
			Turn:     payload.Turn + 1,
			ToolName: payload.Call.Name,
			Label:    emptyAs(payload.Call.Name, "tool"),
			Detail:   summarizeToolResult(payload.Result),
		}
	case sdk.EventToolError:
		var payload sdk.ToolErrorPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		return progressRecord{
			Status:   progressStatusError,
			Turn:     payload.Turn + 1,
			ToolName: payload.Call.Name,
			Label:    emptyAs(payload.Call.Name, "tool"),
			Detail:   summarizeText(payload.Reason, 180),
		}
	case sdk.EventAgentEnd:
		var payload sdk.AgentEndPayload
		if decodeProgressPayload(event, &payload) != nil {
			return progressRecord{}
		}
		return progressRecord{
			Status:    progressStatusDone,
			SessionID: event.SessionID,
			Label:     "done",
			Detail:    emptyAs(payload.Cause.Code, "unknown"),
		}
	default:
		return progressRecord{}
	}
}

func decodeProgressPayload(event sdk.Event, target any) error {
	return json.Unmarshal(event.Payload, target)
}

const (
	progressStatusRun    = "run"
	progressStatusModel  = "model"
	progressStatusPlan   = "plan"
	progressStatusTool   = "tool"
	progressStatusOK     = "ok"
	progressStatusError  = "error"
	progressStatusAnswer = "answer"
	progressStatusDone   = "done"
)

const (
	progressTabOverview = iota
	progressTabTimeline
	progressTabDetails
	progressTabCount
	maxProgressRecords = 200
)

var progressTabNames = []string{"Overview", "Timeline", "Details"}

type progressRecord struct {
	EventName string
	At        time.Time
	Status    string
	Turn      int
	SessionID string
	Provider  string
	ToolName  string
	Label     string
	Detail    string
}

func (record progressRecord) line() string {
	var parts []string
	if record.Turn > 0 {
		parts = append(parts, fmt.Sprintf("turn=%d", record.Turn))
	}
	if record.Label != "" {
		parts = append(parts, record.Label)
	}
	if record.Detail != "" {
		parts = append(parts, record.Detail)
	}
	return strings.Join(parts, "  ")
}

type progressRecordMsg progressRecord
type progressDoneMsg struct{}

type progressModel struct {
	styles      progressStyles
	cancel      context.CancelFunc
	width       int
	height      int
	tab         int
	sessionID   string
	phase       string
	turn        int
	provider    string
	toolStarted int
	toolDone    int
	toolErrors  int
	history     []progressRecord
	selected    int
	follow      bool
	showHelp    bool
	done        bool
}

func newProgressModel(styles progressStyles, cancel context.CancelFunc) progressModel {
	return progressModel{
		styles:   styles,
		cancel:   cancel,
		phase:    "starting",
		selected: -1,
		follow:   true,
	}
}

func (model progressModel) Init() tea.Cmd { return nil }

func (model progressModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch value := message.(type) {
	case progressRecordMsg:
		record := progressRecord(value)
		model.apply(record)
		model.history = append(model.history, record)
		if len(model.history) > maxProgressRecords {
			removed := len(model.history) - maxProgressRecords
			model.history = model.history[removed:]
			model.selected -= removed
			if model.selected < 0 && len(model.history) > 0 {
				model.selected = 0
			}
		}
		if model.follow || model.selected < 0 || model.selected >= len(model.history) {
			model.selected = len(model.history) - 1
		}
		return model, nil
	case progressDoneMsg:
		model.done = true
		if model.phase == "" || model.phase == "starting" {
			model.phase = "done"
		}
		return model, tea.Quit
	case tea.WindowSizeMsg:
		model.width = value.Width
		model.height = value.Height
		return model, nil
	case tea.KeyMsg:
		return model.handleKey(value)
	default:
		return model, nil
	}
}

func (model progressModel) handleKey(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c":
		if model.cancel != nil {
			model.cancel()
		}
		model.done = true
		return model, tea.Quit
	case "q", "esc":
		model.done = true
		return model, tea.Quit
	case "tab", "right", "l":
		model.tab = (model.tab + 1) % progressTabCount
		return model, nil
	case "shift+tab", "left":
		model.tab = (model.tab + progressTabCount - 1) % progressTabCount
		return model, nil
	case "1":
		model.tab = progressTabOverview
		return model, nil
	case "2":
		model.tab = progressTabTimeline
		return model, nil
	case "3":
		model.tab = progressTabDetails
		return model, nil
	case "j", "down":
		model.moveSelection(1)
		return model, nil
	case "k", "up":
		model.moveSelection(-1)
		return model, nil
	case "g", "home":
		if len(model.history) > 0 {
			model.selected = 0
			model.follow = false
		}
		return model, nil
	case "G", "end":
		model.selectLatest()
		return model, nil
	case "f":
		model.follow = !model.follow
		if model.follow {
			model.selectLatest()
		}
		return model, nil
	case "?":
		model.showHelp = !model.showHelp
		return model, nil
	default:
		return model, nil
	}
}

func (model *progressModel) moveSelection(delta int) {
	if len(model.history) == 0 {
		return
	}
	if model.selected < 0 {
		model.selected = len(model.history) - 1
	}
	model.selected += delta
	if model.selected < 0 {
		model.selected = 0
	}
	if model.selected >= len(model.history) {
		model.selected = len(model.history) - 1
	}
	model.follow = model.selected == len(model.history)-1
}

func (model *progressModel) selectLatest() {
	if len(model.history) == 0 {
		return
	}
	model.selected = len(model.history) - 1
	model.follow = true
}

func (model *progressModel) apply(record progressRecord) {
	if record.SessionID != "" {
		model.sessionID = record.SessionID
	}
	if record.Turn > 0 {
		model.turn = record.Turn
	}
	if record.Provider != "" {
		model.provider = record.Provider
	}
	switch record.Status {
	case progressStatusRun:
		model.phase = "running"
	case progressStatusModel:
		model.phase = "asking model"
	case progressStatusPlan:
		model.phase = "planning tools"
	case progressStatusTool:
		model.phase = "using tools"
		model.toolStarted++
	case progressStatusOK:
		model.phase = "using tools"
		model.toolDone++
	case progressStatusError:
		model.phase = "needs attention"
		if record.ToolName != "" {
			model.toolDone++
		}
		model.toolErrors++
	case progressStatusAnswer:
		model.phase = "answering"
	case progressStatusDone:
		model.phase = "done"
	}
}

func (model progressModel) View() string {
	if model.done {
		return ""
	}
	width := model.effectiveWidth()
	var output strings.Builder
	output.WriteString(model.renderHeader(width))
	output.WriteByte('\n')
	output.WriteString(model.renderTabs())
	output.WriteByte('\n')
	output.WriteString(model.renderBody(width))
	output.WriteByte('\n')
	output.WriteString(model.renderFooter(width))
	if model.showHelp {
		output.WriteByte('\n')
		output.WriteString(model.renderHelp(width))
	}
	return output.String()
}

func (model progressModel) effectiveWidth() int {
	if model.width >= 50 {
		return model.width
	}
	return 96
}

func (model progressModel) bodyHeight() int {
	if model.height <= 0 {
		return 10
	}
	height := model.height - 8
	if model.showHelp {
		height -= 5
	}
	if height < 5 {
		return 5
	}
	if height > 14 {
		return 14
	}
	return height
}

func (model progressModel) renderHeader(width int) string {
	phase := model.styles.strong.Render(emptyAs(model.phase, "running"))
	title := model.styles.brand.Render("ag") + " " + phase
	stats := model.metricLine()
	if stats != "" {
		title += "  " + model.styles.muted.Render(fitProgressText(stats, width-18))
	}
	return fitProgressText(title, width)
}

func (model progressModel) metricLine() string {
	var stats []string
	if model.sessionID != "" {
		stats = append(stats, "session="+model.sessionID)
	}
	if model.turn > 0 {
		stats = append(stats, fmt.Sprintf("turn=%d", model.turn))
	}
	if model.provider != "" {
		stats = append(stats, "provider="+model.provider)
	}
	stats = append(stats, fmt.Sprintf("events=%d", len(model.history)))
	if model.toolStarted > 0 || model.toolDone > 0 {
		stats = append(stats, fmt.Sprintf("tools=%d/%d", model.toolDone, model.toolStarted))
	}
	if model.toolErrors > 0 {
		stats = append(stats, fmt.Sprintf("errors=%d", model.toolErrors))
	}
	if model.follow {
		stats = append(stats, "follow=on")
	} else {
		stats = append(stats, "follow=off")
	}
	return strings.Join(stats, "  ")
}

func (model progressModel) renderTabs() string {
	tabs := make([]string, 0, len(progressTabNames))
	for index, name := range progressTabNames {
		label := fmt.Sprintf("%d %s", index+1, name)
		if index == model.tab {
			tabs = append(tabs, model.styles.activeTab.Render(label))
		} else {
			tabs = append(tabs, model.styles.tab.Render(label))
		}
	}
	return strings.Join(tabs, " ")
}

func (model progressModel) renderBody(width int) string {
	switch model.tab {
	case progressTabTimeline:
		return model.renderTimeline(width, model.bodyHeight())
	case progressTabDetails:
		return model.renderDetails(width, model.bodyHeight())
	default:
		return model.renderOverview(width, model.bodyHeight())
	}
}

func (model progressModel) renderOverview(width int, height int) string {
	var lines []string
	lines = append(lines, model.styles.section.Render("Overview"))
	selected := model.selectedRecord()
	if selected != nil {
		lines = append(lines, "Current  "+model.formatRecord(*selected, width-9))
	} else {
		lines = append(lines, "Current  waiting for events")
	}
	lines = append(lines, "")
	lines = append(lines, model.styles.section.Render("Recent activity"))
	recent := model.recentRecords(height - len(lines))
	for _, record := range recent {
		lines = append(lines, "  "+model.formatRecord(record, width-2))
	}
	if len(recent) == 0 {
		lines = append(lines, "  no events yet")
	}
	return fitLines(lines, height)
}

func (model progressModel) renderTimeline(width int, height int) string {
	if len(model.history) == 0 {
		return "Timeline\n  waiting for events"
	}
	start, end := model.visibleRange(height)
	lines := make([]string, 0, end-start+2)
	lines = append(lines, model.styles.section.Render("Timeline"))
	for index := start; index < end; index++ {
		marker := " "
		lineStyle := model.styles.plain
		if index == model.selected {
			marker = ">"
			lineStyle = model.styles.selected
		}
		line := fmt.Sprintf(
			"%s %03d %s",
			marker,
			index+1,
			model.formatRecord(model.history[index], width-7),
		)
		lines = append(lines, lineStyle.Render(line))
	}
	return fitLines(lines, height)
}

func (model progressModel) renderDetails(width int, height int) string {
	record := model.selectedRecord()
	if record == nil {
		return "Details\n  no event selected"
	}
	lines := []string{
		model.styles.section.Render("Details"),
		"event: " + emptyAs(record.EventName, "-"),
		"status: " + record.Status,
	}
	if !record.At.IsZero() {
		lines = append(lines, "time: "+record.At.Format("15:04:05"))
	}
	if record.Turn > 0 {
		lines = append(lines, fmt.Sprintf("turn: %d", record.Turn))
	}
	if record.Provider != "" {
		lines = append(lines, "provider: "+record.Provider)
	}
	if record.ToolName != "" {
		lines = append(lines, "tool: "+record.ToolName)
	}
	if record.Label != "" {
		lines = append(lines, "label: "+record.Label)
	}
	if record.Detail != "" {
		lines = append(lines, "summary: "+record.Detail)
	}
	for index, line := range lines {
		lines[index] = fitProgressText(line, width)
	}
	return fitLines(lines, height)
}

func (model progressModel) renderFooter(width int) string {
	footer := "tab switch  j/k select  f follow  ? help  q hide  ctrl+c cancel"
	return model.styles.muted.Render(fitProgressText(footer, width))
}

func (model progressModel) renderHelp(width int) string {
	lines := []string{
		model.styles.section.Render("Keys"),
		"tab/right/l: next view    shift+tab/left: previous view",
		"j/down, k/up: select event    g/G: first/latest    f: toggle follow",
		"q/esc: hide dashboard and keep run going    ctrl+c: cancel run",
	}
	for index, line := range lines {
		lines[index] = fitProgressText(line, width)
	}
	return strings.Join(lines, "\n")
}

func (model progressModel) selectedRecord() *progressRecord {
	if model.selected < 0 || model.selected >= len(model.history) {
		return nil
	}
	return &model.history[model.selected]
}

func (model progressModel) recentRecords(limit int) []progressRecord {
	if limit <= 0 || len(model.history) == 0 {
		return nil
	}
	if limit > len(model.history) {
		limit = len(model.history)
	}
	return model.history[len(model.history)-limit:]
}

func (model progressModel) visibleRange(height int) (int, int) {
	rows := height - 1
	if rows < 1 {
		rows = 1
	}
	if rows > len(model.history) {
		rows = len(model.history)
	}
	selected := model.selected
	if selected < 0 {
		selected = len(model.history) - 1
	}
	start := selected - rows/2
	if start < 0 {
		start = 0
	}
	end := start + rows
	if end > len(model.history) {
		end = len(model.history)
		start = end - rows
		if start < 0 {
			start = 0
		}
	}
	return start, end
}

func (model progressModel) formatRecord(record progressRecord, width int) string {
	detailWidth := width - 7
	if detailWidth < 12 {
		detailWidth = 12
	}
	return model.styles.status(record.Status) + "  " +
		fitProgressText(record.line(), detailWidth)
}

type progressStyles struct {
	brand     lipgloss.Style
	strong    lipgloss.Style
	muted     lipgloss.Style
	plain     lipgloss.Style
	tab       lipgloss.Style
	activeTab lipgloss.Style
	selected  lipgloss.Style
	section   lipgloss.Style
	run       lipgloss.Style
	model     lipgloss.Style
	plan      lipgloss.Style
	tool      lipgloss.Style
	ok        lipgloss.Style
	err       lipgloss.Style
	answer    lipgloss.Style
	done      lipgloss.Style
}

func newProgressStyles(writer io.Writer, useColor bool, forceColor bool) progressStyles {
	renderer := lipgloss.NewRenderer(writer)
	switch {
	case forceColor:
		renderer.SetColorProfile(termenv.ANSI256)
	case !useColor:
		renderer.SetColorProfile(termenv.Ascii)
	}
	styles := progressStyles{
		brand:  renderer.NewStyle(),
		strong: renderer.NewStyle(),
		muted:  renderer.NewStyle(),
		plain:  renderer.NewStyle(),
		tab:    renderer.NewStyle().Padding(0, 1),
		activeTab: renderer.NewStyle().
			Bold(true).
			Padding(0, 1),
		selected: renderer.NewStyle(),
		section:  renderer.NewStyle().Bold(true),
		run:      renderer.NewStyle(),
		model:    renderer.NewStyle(),
		plan:     renderer.NewStyle(),
		tool:     renderer.NewStyle(),
		ok:       renderer.NewStyle(),
		err:      renderer.NewStyle(),
		answer:   renderer.NewStyle(),
		done:     renderer.NewStyle(),
	}
	if !useColor {
		return styles
	}
	styles.brand = styles.brand.Bold(true).Foreground(lipgloss.Color("69"))
	styles.strong = styles.strong.Bold(true).Foreground(lipgloss.Color("252"))
	styles.muted = styles.muted.Foreground(lipgloss.Color("245"))
	styles.tab = styles.tab.Foreground(lipgloss.Color("245"))
	styles.activeTab = styles.activeTab.
		Foreground(lipgloss.Color("16")).
		Background(lipgloss.Color("75"))
	styles.selected = styles.selected.
		Foreground(lipgloss.Color("255")).
		Background(lipgloss.Color("238"))
	styles.section = styles.section.Foreground(lipgloss.Color("252"))
	styles.run = styles.run.Bold(true).Foreground(lipgloss.Color("69"))
	styles.model = styles.model.Bold(true).Foreground(lipgloss.Color("141"))
	styles.plan = styles.plan.Bold(true).Foreground(lipgloss.Color("219"))
	styles.tool = styles.tool.Bold(true).Foreground(lipgloss.Color("75"))
	styles.ok = styles.ok.Bold(true).Foreground(lipgloss.Color("76"))
	styles.err = styles.err.Bold(true).Foreground(lipgloss.Color("196"))
	styles.answer = styles.answer.Bold(true).Foreground(lipgloss.Color("222"))
	styles.done = styles.done.Bold(true).Foreground(lipgloss.Color("76"))
	return styles
}

func (styles progressStyles) status(status string) string {
	switch status {
	case progressStatusRun:
		return styles.run.Render("RUN  ")
	case progressStatusModel:
		return styles.model.Render("MODEL")
	case progressStatusPlan:
		return styles.plan.Render("PLAN ")
	case progressStatusTool:
		return styles.tool.Render("TOOL ")
	case progressStatusOK:
		return styles.ok.Render("OK   ")
	case progressStatusError:
		return styles.err.Render("ERR  ")
	case progressStatusAnswer:
		return styles.answer.Render("ANS  ")
	case progressStatusDone:
		return styles.done.Render("DONE ")
	default:
		return ""
	}
}

func summarizeModelResponse(response sdk.ModelResponse) string {
	var parts []string
	if response.FinishReason != "" {
		parts = append(parts, "finish="+response.FinishReason)
	}
	if response.Usage.InputTokens > 0 || response.Usage.OutputTokens > 0 {
		parts = append(parts, fmt.Sprintf(
			"tokens=%d+%d",
			response.Usage.InputTokens,
			response.Usage.OutputTokens,
		))
	}
	if strings.TrimSpace(response.Content) != "" {
		parts = append(parts, summarizeText(response.Content, 180))
	}
	if len(parts) == 0 {
		return "model returned"
	}
	return strings.Join(parts, "  ")
}

func summarizeToolCalls(calls []sdk.ToolCall) string {
	if len(calls) == 0 {
		return "no tool calls"
	}
	values := make([]string, 0, len(calls))
	for index, call := range calls {
		if index >= 4 {
			values = append(values, fmt.Sprintf("+%d more", len(calls)-index))
			break
		}
		value := emptyAs(call.Name, "tool")
		if summary := summarizeArguments(call.Arguments); summary != "" {
			value += " " + summary
		}
		values = append(values, value)
	}
	return strings.Join(values, "; ")
}

func summarizeArguments(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return summarizeText(string(raw), 220)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return summarizeJSONValue(value, 220)
	}
	keys := slices.Sorted(maps.Keys(object))
	values := make([]string, 0, len(keys))
	for index, key := range keys {
		if index >= 6 {
			values = append(values, fmt.Sprintf("+%d more", len(keys)-index))
			break
		}
		values = append(values, key+"="+summarizeJSONValue(object[key], 90))
	}
	return strings.Join(values, " ")
}

func summarizeJSONValue(value any, limit int) string {
	switch typed := value.(type) {
	case string:
		return strconv.Quote(summarizeText(typed, limit))
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	case nil:
		return "null"
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return "<value>"
		}
		return summarizeText(string(raw), limit)
	}
}

func summarizeToolResult(result sdk.ToolResult) string {
	prefix := fmt.Sprintf(
		"%s, %d line(s)",
		formatBytes(len(result.Content)),
		lineCount(result.Content),
	)
	if strings.TrimSpace(result.Content) == "" {
		return prefix
	}
	return prefix + ": " + strconv.Quote(summarizeText(result.Content, 220))
}

func summarizeText(value string, limit int) string {
	value = strings.Join(strings.Fields(tableCell(value)), " ")
	if value == "" {
		return ""
	}
	return fitProgressText(value, limit)
}

func fitProgressText(value string, limit int) string {
	if limit <= 0 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	if limit <= 3 {
		return string(runes[:limit])
	}
	return string(runes[:limit-3]) + "..."
}

func fitLines(lines []string, limit int) string {
	if limit <= 0 || len(lines) <= limit {
		return strings.Join(lines, "\n")
	}
	if limit == 1 {
		return fitProgressText(lines[0], 80)
	}
	result := slices.Clone(lines[:limit])
	result[limit-1] = "..."
	return strings.Join(result, "\n")
}

func lineCount(value string) int {
	if value == "" {
		return 0
	}
	return strings.Count(value, "\n") + 1
}

func formatBytes(value int) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	if value < unit*unit {
		return fmt.Sprintf("%.1f KiB", float64(value)/unit)
	}
	return fmt.Sprintf("%.1f MiB", float64(value)/(unit*unit))
}
