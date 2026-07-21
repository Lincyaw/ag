package tui

import (
	"testing"

	tea "charm.land/bubbletea/v2"
)

func TestIsTranscriptPageKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  tea.Key
		want bool
	}{
		{name: "page up", key: tea.Key{Code: tea.KeyPgUp}, want: true},
		{name: "page down", key: tea.Key{Code: tea.KeyPgDown}, want: true},
		{name: "arrow up remains editor input", key: tea.Key{Code: tea.KeyUp}},
		{name: "arrow down remains editor input", key: tea.Key{Code: tea.KeyDown}},
		{name: "home remains editor input", key: tea.Key{Code: tea.KeyHome}},
		{name: "end remains editor input", key: tea.Key{Code: tea.KeyEnd}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isTranscriptPageKey(tea.KeyPressMsg(tt.key))
			if got != tt.want {
				t.Fatalf("isTranscriptPageKey(%q) = %v, want %v", tt.key.String(), got, tt.want)
			}
		})
	}
}

func TestIsContentInteractionKey(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"j", "k", "c", "e", "g", "G"} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			key := tea.KeyPressMsg{Code: []rune(value)[0], Text: value}
			if !isContentInteractionKey(key) {
				t.Fatalf("isContentInteractionKey(%q) = false, want true", value)
			}
		})
	}

	for _, key := range []tea.KeyPressMsg{
		{Code: 'x', Text: "x"},
		{Code: ' ', Text: " "},
		{Code: tea.KeyEnter},
	} {
		if isContentInteractionKey(key) {
			t.Fatalf("isContentInteractionKey(%q) = true, want false", key.String())
		}
	}
}
