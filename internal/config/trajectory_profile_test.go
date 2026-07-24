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
		SystemPrompt: SystemPrompt{
			Enabled: true, PromptFile: "prompt.md", MaxFileBytes: 4096,
		},
		Skills: Skills{
			Enabled: true, Paths: []string{"skills"}, MaxReadBytes: 8192,
		},
		Memory: Memory{
			Enabled: true, Path: ".ag/memory", EnableWrite: true,
			IndexInSystemPrompt: true, MaxReadBytes: 4096, MaxIndexEntries: 20,
		},
		Subagent: Subagent{
			Enabled: true,
			Agents: []SubagentAgent{{
				Name: "reviewer", Description: "reviews code",
				MaxTurns: 3, Tools: []string{"read_file"},
			}},
		},
	}
	got := NewTrajectoryRuntimeProfile(want).Apply(base)
	if got.OpenAI.APIKey != "process-secret" ||
		got.OpenAI.DefaultHeaders["Authorization"] != "process-header" {
		t.Fatalf("process credentials were replaced: %#v", got.OpenAI)
	}
	if !got.OpenAI.Enabled || got.OpenAI.Model != want.OpenAI.Model ||
		!reflect.DeepEqual(got.Workspace, want.Workspace) ||
		!reflect.DeepEqual(got.Bash, want.Bash) ||
		!reflect.DeepEqual(got.SystemPrompt, want.SystemPrompt) ||
		!reflect.DeepEqual(got.Skills, want.Skills) ||
		!reflect.DeepEqual(got.Memory, want.Memory) ||
		!reflect.DeepEqual(got.Subagent, want.Subagent) ||
		!reflect.DeepEqual(got.Plugins, want.Plugins) {
		t.Fatalf("applied profile = %#v", got)
	}
	if got.Gateway.Directory != base.Gateway.Directory {
		t.Fatalf("process gateway config changed: %#v", got.Gateway)
	}
}
