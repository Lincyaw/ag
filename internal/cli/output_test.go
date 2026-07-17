package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

func TestHumanResourceRenderersExposeUsefulOperationalFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 9, 0, 0, 0, time.UTC)
	trajectory := sdk.Trajectory{
		ID:        "trajectory-1",
		Head:      "entry-2",
		CreatedAt: now,
		UpdatedAt: now.Add(time.Minute),
		Entries: []sdk.TrajectoryEntry{
			{
				ID: "entry-1", Kind: sdk.TrajectoryKindToolCall, Timestamp: now,
				Payload: json.RawMessage(
					`{"turn":0,"call":{"id":"call-1","name":"read_file","arguments":{"path":"README.md"}}}`,
				),
			},
			{
				ID: "entry-2", ParentID: "entry-1",
				Kind: sdk.TrajectoryKindToolResult, Timestamp: now.Add(time.Second),
				Payload: json.RawMessage(
					`{"turn":0,"call":{"id":"call-1","name":"read_file"},"result":{"content":"ok","is_error":false}}`,
				),
			},
		},
	}
	cases := []struct {
		name     string
		render   func(*app) error
		expected []string
	}{
		{
			name: "plugins",
			render: func(application *app) error {
				return application.writePlugins([]sdk.PluginDescriptor{{
					Name: "file", Scheme: "local", URI: "local://file",
					Description: "line one\nFAKE\t\x1b[31mred",
				}})
			},
			expected: []string{
				"NAME", "file", "local://file", `line one FAKE \u001b[31mred`,
			},
		},
		{
			name: "manifest",
			render: func(application *app) error {
				return application.writeManifest(sdk.Manifest{
					Name: "file", Version: "1.1.0", Description: "file tools",
					APIVersion: sdk.APIVersion, Registers: []string{"tool/read_file"},
				})
			},
			expected: []string{"Name:", "file", "Registers:", "tool/read_file"},
		},
		{
			name: "trajectory list",
			render: func(application *app) error {
				return application.writeTrajectoryList([]sdk.TrajectorySummary{{
					ID: "trajectory-1", Head: "entry-2", UpdatedAt: now, EntryCount: 2,
				}})
			},
			expected: []string{"ID", "ENTRIES", "trajectory-1", "entry-2"},
		},
		{
			name:   "trajectory show",
			render: func(application *app) error { return application.writeTrajectory(trajectory) },
			expected: []string{
				"Trajectory:", "trajectory-1", "tool=read_file", "status=ok",
			},
		},
		{
			name: "invocation graph",
			render: func(application *app) error {
				return application.writeInvocationGraph(
					sdk.InvocationGraph{
						RootID: "root-1",
						Operations: []sdk.OperationRecord{{
							Operation: sdk.Operation{
								State: sdk.OperationSucceeded,
							},
							Kind:     sdk.OperationKindAgent,
							Resource: "researcher",
							Invocation: sdk.Invocation{
								ID:              "agent-1",
								ParentID:        "tool-1",
								GroupID:         "group-1",
								SessionID:       "root-session",
								TargetSessionID: "child-session",
							},
						}},
					},
				)
			},
			expected: []string{
				"Invocation root:", "root-1", "researcher",
				"tool-1", "child-session",
			},
		},
		{
			name: "rollback",
			render: func(application *app) error {
				return application.writeRollback(rollbackOutput{
					TrajectoryID: "trajectory-1",
					CheckpointID: "checkpoint-1",
					Head:         "rollback-head",
				})
			},
			expected: []string{"Rolled back", "checkpoint-1", "rollback-head"},
		},
		{
			name: "state",
			render: func(application *app) error {
				return application.writeState(stateOutput{
					Backend: "file:///state", Namespace: "default",
					Capabilities: sdk.StorageCapabilities{
						Durable: true, Maintenance: true,
					},
				})
			},
			expected: []string{"Backend:", "file:///state", "Durable:", "yes"},
		},
		{
			name: "prune",
			render: func(application *app) error {
				return application.writePrune(sdk.PruneResult{
					Operations: 2, Deliveries: 3, Trajectories: 4,
				})
			},
			expected: []string{"State pruning complete.", "Operations deleted:", "2"},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout bytes.Buffer
			application := &app{stdout: &stdout, output: outputText}
			if err := test.render(application); err != nil {
				t.Fatal(err)
			}
			for _, expected := range test.expected {
				if !strings.Contains(stdout.String(), expected) {
					t.Fatalf("output %q missing %q", stdout.String(), expected)
				}
			}
			if json.Valid(stdout.Bytes()) {
				t.Fatalf("human output unexpectedly valid JSON: %q", stdout.String())
			}
			if strings.ContainsRune(stdout.String(), '\x1b') {
				t.Fatalf("human output contains terminal escape: %q", stdout.String())
			}
		})
	}
}

func TestJSONOnlyChangesRepresentation(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	application := &app{
		version: "1.2.3",
		stdout:  &stdout,
		output:  outputJSON,
	}
	if err := application.writeVersion(); err != nil {
		t.Fatal(err)
	}
	var version map[string]string
	if err := json.Unmarshal(stdout.Bytes(), &version); err != nil {
		t.Fatal(err)
	}
	if version["version"] != "1.2.3" {
		t.Fatalf("version = %#v", version)
	}

	stdout.Reset()
	if err := application.writePath("/tmp/config.toml"); err != nil {
		t.Fatal(err)
	}
	var path map[string]string
	if err := json.Unmarshal(stdout.Bytes(), &path); err != nil {
		t.Fatal(err)
	}
	if path["path"] != "/tmp/config.toml" {
		t.Fatalf("path = %#v", path)
	}
}

func TestConfigOutputRedactsURISecretsWithoutMutation(t *testing.T) {
	t.Parallel()
	config := appconfig.Config{
		OpenAI: appconfig.OpenAI{
			BaseURL: "https://openai:openai-password@example.com/v1?token=openai-token-value",
		},
		Plugins: appconfig.Plugins{
			Remote: []string{
				"file=grpc://remote:remote-password@example.com/plugin",
				"file@node-a",
			},
			RegistryURI: "grpc://registry:registry-password@example.com",
		},
		Registry: appconfig.Registry{
			AdvertiseURI: "grpc://advertise:advertise-password@example.com",
			BackendURI:   "etcd://example.com?client_secret=registry-secret-value",
		},
		State: appconfig.State{
			BackendURI: "postgres://state:state-password@example.com/ag?namespace=tenant",
		},
	}
	loaded := appconfig.Loaded{Config: config, File: "/tmp/config.toml"}
	for _, output := range []string{outputText, outputJSON} {
		t.Run(output, func(t *testing.T) {
			var stdout bytes.Buffer
			application := &app{stdout: &stdout, output: output}
			if err := application.writeConfig(loaded); err != nil {
				t.Fatal(err)
			}
			rendered := stdout.String()
			for _, secret := range []string{
				"openai-password",
				"openai-token-value",
				"remote-password",
				"registry-password",
				"advertise-password",
				"registry-secret-value",
				"state-password",
			} {
				if strings.Contains(rendered, secret) {
					t.Fatalf("%s output leaked %q: %s", output, secret, rendered)
				}
			}
			if !strings.Contains(rendered, "xxxxx") {
				t.Fatalf("%s output did not redact URI credentials: %s", output, rendered)
			}
			if !strings.Contains(rendered, "file@node-a") {
				t.Fatalf("%s output lost plugin selector: %s", output, rendered)
			}
		})
	}
	if config.Plugins.Remote[0] !=
		"file=grpc://remote:remote-password@example.com/plugin" {
		t.Fatalf("source config was mutated: %#v", config.Plugins.Remote)
	}
}

func TestRequestedOutputHonorsArgumentBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		args     []string
		expected string
	}{
		{
			name:     "explicit output after unknown flag",
			args:     []string{"run", "--unknown", "-o", "json"},
			expected: outputJSON,
		},
		{
			name:     "output-looking prompt value",
			args:     []string{"run", "--prompt", "-o", "json"},
			expected: outputText,
		},
		{
			name:     "unrelated command flag does not consume output",
			args:     []string{"version", "--prompt", "-o", "json"},
			expected: outputJSON,
		},
		{
			name:     "inherited flag consumes output-looking value",
			args:     []string{"version", "--config", "-o", "json"},
			expected: outputText,
		},
		{
			name:     "arguments after terminator",
			args:     []string{"version", "--", "-o", "json"},
			expected: outputText,
		},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := New(&bytes.Buffer{}, &bytes.Buffer{}, "test")
			if actual := requestedOutput(test.args, outputText, command); actual != test.expected {
				t.Fatalf("requestedOutput(%q) = %q, want %q", test.args, actual, test.expected)
			}
		})
	}
}

func TestHumanRunSummaryEscapesUntrustedFields(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	application := &app{stdout: &stdout, output: outputText}
	err := application.writeRun(
		"session\nFAKE\t\x1b[31mred",
		agentruntime.Result{
			Output: "answer",
			Cause:  sdk.Cause{Code: "model_end\nFAKE\t\x1b[31mred"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`session FAKE \u001b[31mred`,
		`model_end FAKE \u001b[31mred`,
	} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("output %q missing %q", stdout.String(), expected)
		}
	}
	if strings.ContainsRune(stdout.String(), '\x1b') {
		t.Fatalf("human output contains terminal escape: %q", stdout.String())
	}
}
