package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/gateway"
	gatewaymanager "github.com/lincyaw/ag/gateway/manager"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

func TestCLIEndToEndToolsResumeInspectAndRollback(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cleanupManagedGatewayProcess(t, home)
	workspace := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(workspace, "input.txt"),
		[]byte("file-content-from-cli"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "cli-test-key")
	server := newScriptedOpenAIServer(t)
	defer server.Close()

	first := executeCLI(t,
		"--otel=false",
		"run",
		"--prompt", "use both tools",
		"--output", "json",
		"--base-url", server.URL+"/v1",
		"--model", "test-model",
		"--cwd", workspace,
		"--bash",
	)
	var firstOutput struct {
		TrajectoryID string              `json:"trajectory_id"`
		Result       agentruntime.Result `json:"result"`
	}
	decodeJSON(t, first.stdout, &firstOutput)
	trajectoryID := firstOutput.TrajectoryID
	if trajectoryID == "" || firstOutput.Result.Output != "first run complete" ||
		firstOutput.Result.Turns != 2 || firstOutput.Result.ToolCalls != 2 {
		t.Fatalf("first output = %#v", firstOutput)
	}
	configRoot, err := os.UserConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(configRoot, "ag", "state", "agent-state.db")
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("managed trajectory state missing: %v", err)
	}

	requests := server.requests(t)
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	assertSecondProviderRequestContainsToolResults(t, requests[1], workspace)

	shown := executeCLI(t,
		"trajectory", "show", trajectoryID, "-o", "json",
	)
	var trajectory sdk.Trajectory
	decodeJSON(t, shown.stdout, &trajectory)
	checkpoints := checkpointIDs(trajectory)
	if len(checkpoints) != 2 {
		t.Fatalf("checkpoint IDs = %v", checkpoints)
	}
	firstCheckpoint := executeCLI(t,
		"trajectory", "show", trajectoryID, "--head", checkpoints[0], "-o", "json",
	)
	var checkpointBranch sdk.Trajectory
	decodeJSON(t, firstCheckpoint.stdout, &checkpointBranch)
	if checkpointBranch.Head != checkpoints[0] ||
		checkpointBranch.Checkpoint != checkpoints[0] {
		t.Fatalf("checkpoint branch = %#v", checkpointBranch)
	}

	preview := executeCLI(t,
		"trajectory", "rollback", trajectoryID, checkpoints[0],
		"--dry-run", "-o", "json",
	)
	var rollbackPreview rollbackPreviewOutput
	decodeJSON(t, preview.stdout, &rollbackPreview)
	if !rollbackPreview.DryRun ||
		rollbackPreview.TrajectoryID != trajectoryID ||
		rollbackPreview.CheckpointID != checkpoints[0] {
		t.Fatalf("rollback preview = %#v", rollbackPreview)
	}

	second := executeCLI(t,
		"--otel=false",
		"run",
		trajectoryID,
		"--prompt", "continue from checkpoint",
		"--output", "json",
		"--base-url", server.URL+"/v1",
		"--model", "test-model",
		"--cwd", workspace,
		"--bash",
	)
	var secondOutput struct {
		TrajectoryID string              `json:"trajectory_id"`
		Result       agentruntime.Result `json:"result"`
	}
	decodeJSON(t, second.stdout, &secondOutput)
	if secondOutput.Result.Output != "resumed run complete" || secondOutput.Result.Turns != 1 {
		t.Fatalf("resumed output = %#v", secondOutput)
	}

	rolledBack := executeCLI(t,
		"trajectory", "rollback", trajectoryID, checkpoints[0],
		"--yes", "-o", "json",
	)
	var rollbackOutput map[string]string
	decodeJSON(t, rolledBack.stdout, &rollbackOutput)
	if rollbackOutput["checkpoint_id"] != checkpoints[0] || rollbackOutput["head"] == "" {
		t.Fatalf("rollback output = %#v", rollbackOutput)
	}

	branch := executeCLI(t,
		"trajectory", "show", trajectoryID,
		"--head", rollbackOutput["head"], "-o", "json",
	)
	var rollbackBranch sdk.Trajectory
	decodeJSON(t, branch.stdout, &rollbackBranch)
	if len(rollbackBranch.Entries) == 0 ||
		rollbackBranch.Entries[len(rollbackBranch.Entries)-1].Kind != sdk.TrajectoryKindRollback {
		t.Fatalf("rollback branch = %#v", rollbackBranch.Entries)
	}
	for _, entry := range rollbackBranch.Entries {
		if entry.ID == checkpoints[1] {
			t.Fatalf("rollback branch retained later checkpoint %s", checkpoints[1])
		}
	}

	paused := executeCLI(t, "trajectory", "pause", trajectoryID, "-o", "json")
	var control trajectoryControlOutput
	decodeJSON(t, paused.stdout, &control)
	if control.Status != agentStatusPaused {
		t.Fatalf("pause output = %#v", control)
	}
	submitted := executeCLI(
		t, "trajectory", "submit", trajectoryID,
		"--prompt", "queued while paused", "-o", "json",
	)
	control = trajectoryControlOutput{}
	decodeJSON(t, submitted.stdout, &control)
	if control.InputID == "" || control.Status != string(gateway.AgentInputQueued) {
		t.Fatalf("submit output = %#v", control)
	}
	pausedList := executeCLI(t, "trajectory", "list", "-o", "json")
	var pausedSummaries []managedTrajectorySummary
	decodeJSON(t, pausedList.stdout, &pausedSummaries)
	if len(pausedSummaries) != 1 ||
		pausedSummaries[0].Status != agentStatusPaused ||
		pausedSummaries[0].PendingInputs != 1 {
		t.Fatalf("paused summaries = %#v", pausedSummaries)
	}
	cancelled := executeCLI(t, "trajectory", "cancel", trajectoryID, "-o", "json")
	control = trajectoryControlOutput{}
	decodeJSON(t, cancelled.stdout, &control)
	if control.Affected != 1 {
		t.Fatalf("cancel output = %#v", control)
	}
	waited := executeCLI(t, "trajectory", "wait", trajectoryID, "-o", "json")
	control = trajectoryControlOutput{}
	decodeJSON(t, waited.stdout, &control)
	if control.Status != agentStatusPaused {
		t.Fatalf("wait output = %#v", control)
	}
	executeCLI(t, "trajectory", "resume", trajectoryID, "-o", "json")

	listed := executeCLI(t, "trajectory", "list", "-o", "json")
	var summaries []managedTrajectorySummary
	decodeJSON(t, listed.stdout, &summaries)
	if len(summaries) != 1 || summaries[0].ID != trajectoryID ||
		summaries[0].Status != agentStatusIdle {
		t.Fatalf("trajectory summaries = %#v", summaries)
	}
}

func TestCLIConfigPrecedencePluginCatalogAndUsageExit(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	configFile := filepath.Join(t.TempDir(), "config.toml")
	state := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(configFile, []byte(fmt.Sprintf(`
[agent]
provider = "from-file"
max_turns = 3

[openai]
enabled = false

[workspace]
enabled = false

[bash]
enabled = false

[compact]
enabled = false

[system_prompt]
enabled = false

[skills]
enabled = false

[memory]
enabled = false

[subagent]
enabled = false

[state]
directory = %q

[observability]
enabled = false
`, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTM_AGENT_PROVIDER", "from-env")
	result := executeCLI(t,
		"--config", configFile,
		"--state-dir", filepath.Join(state, "flag"),
		"config", "show", "-o", "json",
	)
	var shown map[string]any
	decodeJSON(t, result.stdout, &shown)
	config := shown["config"].(map[string]any)
	agent := config["agent"].(map[string]any)
	if agent["provider"] != "from-env" || agent["timeout"] != "0s" {
		t.Fatalf("effective agent config = %#v", agent)
	}
	stateConfig := config["state"].(map[string]any)
	if stateConfig["directory"] != filepath.Join(state, "flag") {
		t.Fatalf("effective state config = %#v", stateConfig)
	}

	plugins := executeCLI(t, "--config", configFile, "plugin", "list", "-o", "json")
	var descriptors []sdk.PluginDescriptor
	decodeJSON(t, plugins.stdout, &descriptors)
	if len(descriptors) != 0 {
		t.Fatalf("disabled local plugins still listed: %#v", descriptors)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"run"}, &stdout, &stderr, "test"); code != exitUsage {
		t.Fatalf("missing prompt exit = %d, stderr = %q", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "--prompt is required") {
		t.Fatalf("usage streams stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestCLIDefaultHumanOutputAndExplicitJSONContract(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cleanupManagedGatewayProcess(t, home)
	t.Setenv("OPENAI_API_KEY", "")
	configFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configFile, []byte(`
[openai]
api_key = "cli-config-key"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if got := request.Header.Get("Authorization"); got != "Bearer cli-config-key" {
			t.Errorf("authorization header = %q", got)
		}
		writer.Header().Set("Content-Type", "application/json")
		writeChatResponse(t, writer, "human-readable answer", "stop", nil)
	}))
	defer server.Close()

	human := executeCLI(t,
		"--config", configFile,
		"--otel=false",
		"run",
		"--session", "human-session",
		"--prompt", "answer once",
		"--base-url", server.URL+"/v1",
		"--model", "test-model",
		"--file=false",
	)
	for _, expected := range []string{
		"human-readable answer",
		"Trajectory:  human-session",
		"Turns:       1",
		"Tool calls:  0",
		"Cause:       model_end",
	} {
		if !strings.Contains(human.stdout, expected) {
			t.Fatalf("human stdout %q missing %q", human.stdout, expected)
		}
	}
	if json.Valid([]byte(human.stdout)) {
		t.Fatalf("default output unexpectedly became JSON: %q", human.stdout)
	}
	if human.stderr != "" {
		t.Fatalf("default runtime logs leaked to stderr: %q", human.stderr)
	}
	logs, err := os.ReadFile(filepath.Join(home, ".ag", "logs", "ag.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logs), `"msg":"plugin mounted"`) {
		t.Fatalf("default log file missing runtime records: %q", logs)
	}

	config := executeCLI(t, "--otel=false", "config", "show")
	for _, expected := range []string{
		"Effective configuration",
		"Agent",
		"Workspace",
		"Diagnostics",
	} {
		if !strings.Contains(config.stdout, expected) {
			t.Fatalf("config text %q missing %q", config.stdout, expected)
		}
	}

	version := executeCLI(t, "-o", "json", "version")
	var versionOutput map[string]string
	decodeJSON(t, version.stdout, &versionOutput)
	if versionOutput["version"] != "test-version" {
		t.Fatalf("version JSON = %#v", versionOutput)
	}

	rootVersion := executeCLI(t, "--version", "-o", "json")
	versionOutput = map[string]string{}
	decodeJSON(t, rootVersion.stdout, &versionOutput)
	if versionOutput["version"] != "test-version" {
		t.Fatalf("root version JSON = %#v", versionOutput)
	}

	schema := executeCLI(t, "--dump-schema")
	var commandTree commandSchema
	decodeJSON(t, schema.stdout, &commandTree)
	if commandTree.Name != "ag" || len(commandTree.Commands) == 0 {
		t.Fatalf("schema = %#v", commandTree)
	}
	subcommandSchema := executeCLI(t, "run", "--dump-schema")
	commandTree = commandSchema{}
	decodeJSON(t, subcommandSchema.stdout, &commandTree)
	if commandTree.Name != "ag" {
		t.Fatalf("subcommand schema = %#v", commandTree)
	}

	prunePreview := executeCLI(t,
		"--state-dir", t.TempDir(),
		"state", "prune", "--before", "720h",
		"--dry-run", "-o", "json",
	)
	var prune prunePreviewOutput
	decodeJSON(t, prunePreview.stdout, &prune)
	if !prune.DryRun || prune.Cutoff == "" {
		t.Fatalf("prune preview = %#v", prune)
	}

	logFile := filepath.Join(t.TempDir(), "custom.log")
	logConfig := executeCLI(t,
		"--log-file", logFile,
		"--log-console",
		"config", "show", "-o", "json",
	)
	var effective map[string]any
	decodeJSON(t, logConfig.stdout, &effective)
	effectiveConfig := effective["config"].(map[string]any)
	effectiveLogging := effectiveConfig["logging"].(map[string]any)
	if effectiveLogging["file"] != logFile ||
		effectiveLogging["console"] != true {
		t.Fatalf("effective logging config = %#v", effectiveLogging)
	}

	var stdout, stderr bytes.Buffer
	if code := Run(
		[]string{"run", "-o", "json"},
		&stdout,
		&stderr,
		"test-version",
	); code != exitUsage {
		t.Fatalf("JSON usage exit = %d, stderr = %q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("JSON error polluted stdout: %q", stdout.String())
	}
	var failure cliErrorOutput
	decodeJSON(t, stderr.String(), &failure)
	if failure.Error.Type != "usage" ||
		failure.Error.ExitCode != exitUsage ||
		!strings.Contains(failure.Error.Message, "--prompt is required") {
		t.Fatalf("JSON error = %#v", failure)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run(
		[]string{"run", "--unknown", "-o", "json"},
		&stdout,
		&stderr,
		"test-version",
	); code != exitUsage {
		t.Fatalf("late JSON flag exit = %d, stderr = %q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("late JSON flag polluted stdout: %q", stdout.String())
	}
	failure = cliErrorOutput{}
	decodeJSON(t, stderr.String(), &failure)
	if failure.Error.Type != "usage" ||
		!strings.Contains(failure.Error.Message, "unknown flag") {
		t.Fatalf("late JSON flag error = %#v", failure)
	}
}

func cleanupManagedGatewayProcess(t *testing.T, home string) {
	t.Helper()
	t.Cleanup(func() {
		readyPath := filepath.Join(
			home, ".ag", "gateway", gatewaymanager.DirectoryName,
			gatewaymanager.ReadyName,
		)
		raw, err := os.ReadFile(readyPath)
		if err != nil {
			return
		}
		var ready gatewaymanager.Ready
		if json.Unmarshal(raw, &ready) != nil || ready.PID <= 0 ||
			ready.PID == os.Getpid() {
			return
		}
		process, err := os.FindProcess(ready.PID)
		if err != nil {
			return
		}
		_ = process.Signal(os.Interrupt)
		time.Sleep(25 * time.Millisecond)
	})
}

type cliResult struct {
	stdout string
	stderr string
}

func executeCLI(t *testing.T, arguments ...string) cliResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := Run(arguments, &stdout, &stderr, "test-version"); code != exitOK {
		t.Fatalf("ag %v exited %d\nstdout:\n%s\nstderr:\n%s", arguments, code, stdout.String(), stderr.String())
	}
	return cliResult{stdout: stdout.String(), stderr: stderr.String()}
}

func decodeJSON(t *testing.T, raw string, target any) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode JSON %q: %v", raw, err)
	}
}

func checkpointIDs(trajectory sdk.Trajectory) []string {
	var result []string
	for _, entry := range trajectory.Entries {
		if entry.Kind == sdk.TrajectoryKindCheckpoint {
			result = append(result, entry.ID)
		}
	}
	return result
}

func assertSecondProviderRequestContainsToolResults(
	t *testing.T,
	request map[string]any,
	workspace string,
) {
	t.Helper()
	messages, ok := request["messages"].([]any)
	if !ok {
		t.Fatalf("messages = %#v", request["messages"])
	}
	joined := fmt.Sprint(messages)
	for _, expected := range []string{
		"file-content-from-cli",
		"bash-cwd=" + resolveTestPath(t, workspace),
		"exit_code: 0",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("provider messages %q missing %q", joined, expected)
		}
	}
}

type scriptedServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []map[string]any
}

func newScriptedOpenAIServer(t *testing.T) *scriptedServer {
	t.Helper()
	server := &scriptedServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		server.bodies = append(server.bodies, body)
		call := len(server.bodies)
		server.mu.Unlock()
		if request.URL.Path != "/v1/chat/completions" ||
			request.Header.Get("Authorization") != "Bearer cli-test-key" {
			http.Error(writer, "unexpected OpenAI request", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch call {
		case 1:
			writeChatResponse(t, writer, "", "tool_calls", []map[string]any{
				toolCall("file-call", "read_file", `{"path":"input.txt"}`),
				toolCall("bash-call", "bash", `{"command":"printf 'bash-cwd=%s\\n' \"$PWD\""}`),
			})
		case 2:
			writeChatResponse(t, writer, "first run complete", "stop", nil)
		case 3:
			writeChatResponse(t, writer, "resumed run complete", "stop", nil)
		default:
			http.Error(writer, fmt.Sprintf("unexpected call %d", call), http.StatusBadRequest)
		}
	}))
	return server
}

func (server *scriptedServer) requests(t *testing.T) []map[string]any {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	result := make([]map[string]any, len(server.bodies))
	copy(result, server.bodies)
	return result
}

func toolCall(id, name, arguments string) map[string]any {
	return map[string]any{
		"id": id, "type": "function",
		"function": map[string]any{"name": name, "arguments": arguments},
	}
}

func writeChatResponse(
	t *testing.T,
	writer http.ResponseWriter,
	content string,
	finishReason string,
	toolCalls []map[string]any,
) {
	t.Helper()
	response := map[string]any{
		"id": "chatcmpl-cli", "object": "chat.completion", "created": 1,
		"model": "test-model",
		"choices": []map[string]any{{
			"index": 0,
			"message": map[string]any{
				"role": "assistant", "content": content, "tool_calls": toolCalls,
			},
			"finish_reason": finishReason,
		}},
		"usage": map[string]any{
			"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
		},
	}
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		t.Errorf("write OpenAI response: %v", err)
	}
}

func resolveTestPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
