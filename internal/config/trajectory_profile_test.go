package config

import (
	"reflect"
	"testing"
	"time"
)

func TestTrajectoryRuntimeProfileAppliesScopedSettings(t *testing.T) {
	base := Config{
		OpenAI: OpenAI{
			APIKey:         "process-secret",
			DefaultHeaders: map[string]string{"Authorization": "process-header"},
		},
		Gateway: Gateway{Directory: "/gateway"},
	}
	want := Config{
		OpenAI: OpenAI{
			Enabled: true, Model: "trajectory-model",
			BaseURL: "https://model.invalid", MaxRetries: 4,
		},
		Workspace: Workspace{Enabled: true, Root: "/workspace", EnableWrite: true},
		Bash: Bash{
			Enabled: true, Shell: "/bin/sh", DefaultTimeout: 3 * time.Second,
			MaxTimeout: 9 * time.Second, MaxOutputBytes: 1234,
			Environment: []string{"PROFILE=value"},
		},
		Plugins: Plugins{Remote: []string{"remote=grpc://127.0.0.1:1"}},
	}
	got := NewTrajectoryRuntimeProfile(want).Apply(base)
	if got.OpenAI.APIKey != "process-secret" ||
		got.OpenAI.DefaultHeaders["Authorization"] != "process-header" {
		t.Fatalf("process credentials were replaced: %#v", got.OpenAI)
	}
	if !got.OpenAI.Enabled || got.OpenAI.Model != want.OpenAI.Model ||
		!reflect.DeepEqual(got.Workspace, want.Workspace) ||
		!reflect.DeepEqual(got.Bash, want.Bash) ||
		!reflect.DeepEqual(got.Plugins, want.Plugins) {
		t.Fatalf("applied profile = %#v", got)
	}
	if got.Gateway.Directory != base.Gateway.Directory {
		t.Fatalf("process gateway config changed: %#v", got.Gateway)
	}
}
