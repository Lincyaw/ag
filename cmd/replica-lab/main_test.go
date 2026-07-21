package main

import (
	"strings"
	"testing"
)

func TestNormalizeScreenPreservesFirstVisibleRow(t *testing.T) {
	raw := []byte("ready\n\n\n")
	got := string(normalizeScreen(raw, 40, 3))
	if got != "ready\n\n\n" {
		t.Fatalf("normalized screen = %q", got)
	}
}

func TestCompareCellsDetectsOneChangedCell(t *testing.T) {
	_, _, count, mismatches := compareCells("abc\n", "axc\n")
	if count != 3 || mismatches != 1 {
		t.Fatalf("count=%d mismatches=%d", count, mismatches)
	}
}

func TestCompareCellsUsesTerminalWidthForWideGraphemes(t *testing.T) {
	_, columns, count, mismatches := compareCells("界a\n", "界b\n")
	if columns != 3 || count != 3 || mismatches != 1 {
		t.Fatalf("columns=%d count=%d mismatches=%d", columns, count, mismatches)
	}
}

func TestCompareCellsCountsBothCellsOfChangedWideGrapheme(t *testing.T) {
	_, columns, count, mismatches := compareCells("界\n", "好\n")
	if columns != 2 || count != 2 || mismatches != 2 {
		t.Fatalf("columns=%d count=%d mismatches=%d", columns, count, mismatches)
	}
}

func TestNormalizeScreenDoesNotSplitWideGrapheme(t *testing.T) {
	if got := string(normalizeScreen([]byte("界a\n"), 2, 1)); got != "界\n" {
		t.Fatalf("normalized screen = %q", got)
	}
	if got := string(normalizeScreen([]byte("界a\n"), 1, 1)); got != "\n" {
		t.Fatalf("narrow normalized screen = %q", got)
	}
}

func TestANSIHTMLProjectsTrueColorAndText(t *testing.T) {
	got := ansiHTML("\x1b[38;2;1;2;3mhello\x1b[0m")
	if !strings.Contains(got, "color:#010203") || !strings.Contains(got, "hello") {
		t.Fatalf("ANSI HTML = %s", got)
	}
}

func TestActionListPreservesFlagOrder(t *testing.T) {
	var actions actionList
	if err := actions.Set(`{"text":"hello","wait_ms":10}`); err != nil {
		t.Fatal(err)
	}
	if err := actions.Set(`{"keys":["Enter"]}`); err != nil {
		t.Fatal(err)
	}
	if len(actions) != 2 || actions[0].Text != "hello" || actions[1].Keys[0] != "Enter" {
		t.Fatalf("actions = %#v", actions)
	}
}

func TestActionListRejectsEmptyAction(t *testing.T) {
	var actions actionList
	if err := actions.Set(`{}`); err == nil {
		t.Fatal("expected empty action to fail")
	}
}
