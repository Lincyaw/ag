package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
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
					Selection:          "legacy_file_fallback",
					LegacyFileFallback: true,
					Capabilities: sdk.StorageCapabilities{
						Durable: true, Maintenance: true,
					},
				})
			},
			expected: []string{
				"Backend:", "file:///state",
				"Selection:", "legacy_file_fallback",
				"legacy file state was detected",
				"Durable:", "yes",
			},
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

	stdout.Reset()
	rawMarkdown := "# Title\n\nThis is **bold**."
	if err := application.writeRun("session-1", agentruntime.Result{
		Output: rawMarkdown,
		Cause:  sdk.Cause{Code: "model_end"},
	}); err != nil {
		t.Fatal(err)
	}
	var run runOutput
	if err := json.Unmarshal(stdout.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if run.Result.Output != rawMarkdown {
		t.Fatalf("run output = %q, want raw markdown %q", run.Result.Output, rawMarkdown)
	}
}

func TestConfigOutputRedactsURISecretsWithoutMutation(t *testing.T) {
	t.Parallel()
	config := appconfig.Config{
		OpenAI: appconfig.OpenAI{
			APIKey:  "openai-api-key-value",
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
				"openai-api-key-value",
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
			if !strings.Contains(rendered, "<set>") {
				t.Fatalf("%s output did not summarize configured API key: %s", output, rendered)
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
	if config.OpenAI.APIKey != "openai-api-key-value" {
		t.Fatalf("source API key was mutated: %q", config.OpenAI.APIKey)
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

func TestHumanRunRendersMarkdownAnswer(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	application := &app{stdout: &stdout, output: outputText, color: colorNever}
	err := application.writeRun(
		"session-1",
		agentruntime.Result{
			Output: strings.Join([]string{
				"# Title",
				"",
				"## Details",
				"",
				"This is **bold** and `code`.",
				"",
				"- first",
				"- second",
				"",
				"```go",
				"fmt.Println(\"ok\")",
				"```",
			}, "\n"),
			Cause: sdk.Cause{Code: "model_end"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	rendered := stdout.String()
	for _, expected := range []string{
		"Title",
		"Details",
		"bold",
		"code",
		"first",
		"second",
		`fmt.Println("ok")`,
		"Session:",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered output %q missing %q", rendered, expected)
		}
	}
	for _, raw := range []string{
		"# Title",
		"## Details",
		"**bold**",
		"- first",
		"```go",
		"```",
	} {
		if strings.Contains(rendered, raw) {
			t.Fatalf("rendered output still contains raw markdown %q: %q", raw, rendered)
		}
	}
	if strings.ContainsRune(rendered, '\x1b') {
		t.Fatalf("human output contains terminal escape: %q", rendered)
	}

	stdout.Reset()
	application.color = colorAlways
	if err := application.writeRun("session-2", agentruntime.Result{
		Output: "# Colored\n\nThis is **bold**.",
		Cause:  sdk.Cause{Code: "model_end"},
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.ContainsRune(stdout.String(), '\x1b') {
		t.Fatalf("forced-color markdown output is not styled: %q", stdout.String())
	}
}

func TestProgressReporterExplainsRuntimeEvents(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	application := &app{
		stderr:   &stderr,
		output:   outputText,
		progress: progressPlain,
		color:    colorAlways,
	}
	reporter := application.progressReporter()
	if reporter == nil {
		t.Fatal("progress reporter was not created")
	}
	reporter.Observe(context.Background(), sdk.Event{
		Name:      sdk.EventBeforeTool,
		SessionID: "session-1",
		Payload: json.RawMessage(
			`{"turn":0,"call":{"id":"call-1","name":"custom_tool","arguments":{"path":"README.md","limit":3,"nested":{"owner":"me"}}}}`,
		),
	})
	reporter.Observe(context.Background(), sdk.Event{
		Name:      sdk.EventAfterTool,
		SessionID: "session-1",
		Payload: json.RawMessage(
			`{"turn":0,"call":{"id":"call-1","name":"custom_tool"},"result":{"content":"first line\nsecond line","is_error":false}}`,
		),
	})
	if err := reporter.stop(); err != nil {
		t.Fatal(err)
	}
	got := stderr.String()
	for _, expected := range []string{
		"Using",
		"README.md",
		"tool=custom_tool",
		`args=limit=3`,
		`path="README.md"`,
		"limit=3",
		"nested={\"owner\":\"me\"}",
		"Used",
		"2 line(s)",
		"first line second line",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("progress output %q missing %q", got, expected)
		}
	}
	if !strings.ContainsRune(got, '\x1b') {
		t.Fatalf("progress output is not colored: %q", got)
	}
}

func TestProgressReporterObserveDoesNotBlockOnSlowWriter(t *testing.T) {
	t.Parallel()
	writer := &blockingProgressWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	reporter := newProgressReporter(writer, nil, false, false, false)
	if err := reporter.start(func() {}); err != nil {
		t.Fatal(err)
	}
	reporter.Observe(context.Background(), sdk.Event{
		Name:      sdk.EventBeforeTool,
		SessionID: "session-1",
		Payload: json.RawMessage(
			`{"turn":0,"call":{"id":"call-1","name":"slow_tool","arguments":{"path":"README.md"}}}`,
		),
	})
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("progress writer was not reached")
	}
	observed := make(chan struct{})
	go func() {
		defer close(observed)
		reporter.Observe(context.Background(), sdk.Event{
			Name:      sdk.EventAfterTool,
			SessionID: "session-1",
			Payload: json.RawMessage(
				`{"turn":0,"call":{"id":"call-1","name":"slow_tool"},"result":{"content":"done","is_error":false}}`,
			),
		})
	}()
	select {
	case <-observed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Observe blocked on slow progress writer")
	}
	close(writer.release)
	if err := reporter.stop(); err != nil {
		t.Fatal(err)
	}
	got := writer.String()
	if !strings.Contains(got, "Using") || !strings.Contains(got, "Used") {
		t.Fatalf("drained progress output = %q", got)
	}
}

func TestProgressReporterReportsDroppedUpdates(t *testing.T) {
	t.Parallel()
	writer := &blockingProgressWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	reporter := newProgressReporter(writer, nil, false, false, false)
	if err := reporter.start(func() {}); err != nil {
		t.Fatal(err)
	}
	reporter.Observe(context.Background(), sdk.Event{
		Name:      sdk.EventBeforeTool,
		SessionID: "session-1",
		Payload: json.RawMessage(
			`{"turn":0,"call":{"id":"call-1","name":"slow_tool","arguments":{"path":"README.md"}}}`,
		),
	})
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("progress writer was not reached")
	}
	for range progressQueueSize + 4 {
		reporter.Observe(context.Background(), sdk.Event{
			Name:      sdk.EventAfterTool,
			SessionID: "session-1",
			Payload: json.RawMessage(
				`{"turn":0,"call":{"id":"call-1","name":"slow_tool"},"result":{"content":"done","is_error":false}}`,
			),
		})
	}
	close(writer.release)
	if err := reporter.stop(); err != nil {
		t.Fatal(err)
	}
	got := writer.String()
	for _, expected := range []string{
		"Progress limited",
		"dropped",
		"progress_queue_dropped=",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("progress output %q missing dropped-update detail %q", got, expected)
		}
	}
}

type blockingProgressWriter struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	buffer  bytes.Buffer
}

func (writer *blockingProgressWriter) Write(data []byte) (int, error) {
	select {
	case <-writer.entered:
	default:
		close(writer.entered)
	}
	<-writer.release
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.Write(data)
}

func (writer *blockingProgressWriter) String() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.String()
}

func TestProgressModelRendersInlineStatus(t *testing.T) {
	t.Parallel()
	cancelled := false
	model := newProgressModel(
		newProgressStyles(false),
		func() { cancelled = true },
	)
	updated, _ := model.Update(progressRecordMsg{
		Status:    progressStatusTool,
		Turn:      2,
		SessionID: "session-1",
		ToolName:  "remote_lookup",
		Label:     "remote_lookup",
		Detail:    `query="status"`,
		Overview:  true,
	})
	view := updated.(progressModel).View()
	for _, expected := range []string{
		"ag working",
		"Overview",
		"session=session-1",
		"turn=2",
		"tools=0/1",
		"remote_lookup",
		`query="status"`,
	} {
		if !strings.Contains(view.Content, expected) {
			t.Fatalf("progress view %q missing %q", view.Content, expected)
		}
	}
	if strings.ContainsRune(view.Content, '\x1b') {
		t.Fatalf("uncolored progress view contains terminal escape: %q", view.Content)
	}

	timeline, _ := updated.(progressModel).Update(tea.KeyPressMsg{Code: '\t'})
	if view := timeline.(progressModel).View(); !strings.Contains(view.Content, "Timeline") ||
		!strings.Contains(view.Content, "> 001") {
		t.Fatalf("timeline view = %q", view.Content)
	}
	help, _ := timeline.(progressModel).Update(tea.KeyPressMsg{Code: '?', Text: "?"})
	if view := help.(progressModel).View(); !strings.Contains(view.Content, "ctrl+c: cancel run") {
		t.Fatalf("help view = %q", view.Content)
	}
	cancelledModel, _ := help.(progressModel).Update(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
	_ = cancelledModel
	if !cancelled {
		t.Fatalf("ctrl+c cancelled=%v", cancelled)
	}

	closed, cmd := updated.(progressModel).Update(progressDoneMsg{})
	if cmd == nil {
		t.Fatal("progress done did not request Bubble Tea quit")
	}
	if view := closed.(progressModel).View(); view.Content != "" {
		t.Fatalf("closed progress view = %q, want empty", view.Content)
	}
}

func TestProgressReporterIgnoresJSONOutput(t *testing.T) {
	t.Parallel()
	application := &app{
		stderr:   &bytes.Buffer{},
		output:   outputJSON,
		progress: progressAlways,
		color:    colorAlways,
	}
	if reporter := application.progressReporter(); reporter != nil {
		t.Fatal("progress reporter was created for JSON output")
	}
}

func TestSchemaIncludesMachineReadableGlobalFlags(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	command := New(&stdout, &bytes.Buffer{}, "test-version")
	schema := schemaForCommand(command)
	if schema.Name != "ag" || len(schema.Commands) == 0 {
		t.Fatalf("schema = %#v", schema)
	}
	flags := map[string]flagSchema{}
	for _, flag := range schema.PersistentFlags {
		flags[flag.Name] = flag
	}
	if flags["dump-schema"].Name == "" || flags["version"].Name == "" {
		t.Fatalf("schema missing introspection flags: %#v", flags)
	}
	progress := flags["progress"]
	if !slicesEqual(progress.AllowedValues, []string{
		progressAuto,
		progressTUI,
		progressPlain,
		progressAlways,
		progressNever,
	}) {
		t.Fatalf("progress schema = %#v", progress)
	}
	color := flags["color"]
	if !slicesEqual(color.AllowedValues, []string{
		colorAuto,
		colorAlways,
		colorNever,
	}) {
		t.Fatalf("color schema = %#v", color)
	}
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
