package cli

import (
	"io"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

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
		return styles.run.Render("Start")
	case progressStatusModel:
		return styles.model.Render("Think")
	case progressStatusPlan:
		return styles.plan.Render("Plan ")
	case progressStatusTool:
		return styles.tool.Render("Work ")
	case progressStatusOK:
		return styles.ok.Render("Done ")
	case progressStatusError:
		return styles.err.Render("Error")
	case progressStatusAnswer:
		return styles.answer.Render("Reply")
	case progressStatusDone:
		return styles.done.Render("Done ")
	default:
		return ""
	}
}

func visibleStatusWidth(_ string) int {
	return 5
}
