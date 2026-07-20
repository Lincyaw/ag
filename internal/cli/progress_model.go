package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
)

const (
	progressTabOverview = iota
	progressTabTimeline
	progressTabDetails
	progressTabCount
	maxProgressRecords = 200
)

var progressTabNames = []string{"Overview", "Timeline", "Details"}

type progressModel struct {
	styles      progressStyles
	cancel      context.CancelFunc
	width       int
	height      int
	tab         int
	task        string
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
	case tea.KeyPressMsg:
		return model.handleKey(value)
	default:
		return model, nil
	}
}

func (model progressModel) handleKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
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
	if record.Task != "" {
		model.task = record.Task
	}
	if record.Turn > 0 {
		model.turn = record.Turn
	}
	if record.Provider != "" {
		model.provider = record.Provider
	}
	switch record.Status {
	case progressStatusRun:
		model.phase = "starting"
	case progressStatusModel:
		model.phase = "thinking"
	case progressStatusPlan:
		model.phase = "planning"
	case progressStatusTool:
		model.phase = "working"
		model.toolStarted++
	case progressStatusOK:
		model.phase = "working"
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

func (model progressModel) View() tea.View {
	if model.done {
		return tea.NewView("")
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
	return tea.NewView(output.String())
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
	phaseText := emptyAs(model.phase, "running")
	phase := model.styles.strong.Render(phaseText)
	title := model.styles.brand.Render("ag") + " " + phase
	stats := model.metricLine()
	if stats != "" {
		title += "  " + model.styles.muted.Render(
			fitProgressText(stats, width-len(phaseText)-6),
		)
	}
	return title
}

func (model progressModel) metricLine() string {
	var stats []string
	if model.sessionID != "" {
		stats = append(stats, "session="+shortIdentifier(model.sessionID))
	}
	if model.turn > 0 {
		stats = append(stats, fmt.Sprintf("turn=%d", model.turn))
	}
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
	if model.task != "" {
		lines = append(lines, "Task     "+fitProgressText(model.task, width-9))
	}
	current := model.currentOverviewRecord()
	if current != nil {
		lines = append(lines, "Current  "+model.formatRecord(*current, width-9))
	} else {
		lines = append(lines, "Current  starting")
	}
	if thought := model.latestThought(); thought != "" {
		lines = append(lines, "")
		lines = append(lines, model.styles.muted.Render(
			"  "+fitProgressText(thought, width-2),
		))
	}
	if lastErr := model.latestError(); lastErr != nil {
		lines = append(lines, "")
		lines = append(lines, "  "+model.styles.err.Render("error")+
			"  "+fitProgressText(lastErr.Detail, width-9))
	}
	lines = append(lines, "")
	lines = append(lines, model.styles.section.Render("Recent"))
	recent := model.recentOverviewRecords(height - len(lines))
	for _, record := range recent {
		lines = append(lines, "  "+model.formatRecord(record, width-2))
	}
	if len(recent) == 0 {
		lines = append(lines, "  no events yet")
	}
	return fitLines(lines, height)
}

func (model progressModel) latestThought() string {
	for i := len(model.history) - 1; i >= 0; i-- {
		r := model.history[i]
		if r.Status == progressStatusPlan && r.Detail != "" {
			return r.Detail
		}
		if r.Status == progressStatusAnswer && r.Detail != "" {
			return r.Detail
		}
	}
	return ""
}

func (model progressModel) latestError() *progressRecord {
	for i := len(model.history) - 1; i >= 0; i-- {
		r := model.history[i]
		if r.Status == progressStatusError {
			return &model.history[i]
		}
		if r.Status == progressStatusOK || r.Status == progressStatusPlan ||
			r.Status == progressStatusAnswer {
			return nil
		}
	}
	return nil
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
	if record.Task != "" {
		lines = append(lines, "task: "+record.Task)
	}
	if record.Label != "" {
		lines = append(lines, "label: "+record.Label)
	}
	if record.Detail != "" {
		lines = append(lines, "summary: "+record.Detail)
	}
	if record.Technical != "" {
		lines = append(lines, "trace: "+record.Technical)
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

func (model progressModel) currentOverviewRecord() *progressRecord {
	for index := len(model.history) - 1; index >= 0; index-- {
		if model.history[index].Overview {
			return &model.history[index]
		}
	}
	return nil
}

func (model progressModel) recentOverviewRecords(limit int) []progressRecord {
	if limit <= 0 || len(model.history) == 0 {
		return nil
	}
	records := make([]progressRecord, 0, limit)
	for index := len(model.history) - 1; index >= 0 && len(records) < limit; index-- {
		if model.history[index].Recent {
			records = append(records, model.history[index])
		}
	}
	slices.Reverse(records)
	return records
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
	status := model.styles.status(record.Status)
	detailWidth := width - visibleStatusWidth(record.Status) - 2
	if detailWidth < 12 {
		detailWidth = 12
	}
	return status + "  " + fitProgressText(record.display(), detailWidth)
}
