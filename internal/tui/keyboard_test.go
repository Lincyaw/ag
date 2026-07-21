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
