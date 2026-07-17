//go:build unix

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/cli"
	"github.com/lincyaw/ag/pluginrpc"
	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	pluginregistry "github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPythonPluginProcessImplementsCrossLanguageContract(t *testing.T) {
	if testing.Short() {
		t.Skip("creates a Python environment and starts a real plugin process")
	}
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Skip("uv is required for the Python cross-language integration test")
	}

	repository := repositoryRoot(t)
	project := filepath.Join(repository, "examples", "python-plugin")
	stubs := filepath.Join(t.TempDir(), "stubs")
	if err := os.MkdirAll(stubs, 0o700); err != nil {
		t.Fatal(err)
	}
	environment := []string{
		"AGENTM_PYTHON_STUBS=" + stubs,
		"UV_PROJECT_ENVIRONMENT=" + filepath.Join(t.TempDir(), "venv"),
	}
	generate := exec.Command(
		uv,
		"run", "--project", project, "--frozen",
		"python", "-m", "grpc_tools.protoc",
		"-I", filepath.Join(repository, "pluginrpc", "v1"),
		"--python_out", stubs,
		"--grpc_python_out", stubs,
		filepath.Join(repository, "pluginrpc", "v1", "plugin.proto"),
	)
	generate.Env = append(os.Environ(), environment...)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("generate Python protocol stubs: %v\n%s", err, output)
	}

	registryURI, registryClient, _ := startRegistry(t)
	eventsFile := filepath.Join(t.TempDir(), "deliveries.jsonl")
	process := startPluginProcessEnv(
		t,
		environment,
		uv,
		"run", "--project", project, "--frozen",
		"python", filepath.Join(project, "plugin.py"),
		"--listen", "127.0.0.1:0",
		"--events-file", eventsFile,
		"--registry-uri", registryURI,
		"--lease-ttl-ms", "300",
	)
	if process.ready.Name != "python-e2e" ||
		!strings.HasPrefix(process.ready.URI, "grpc://") {
		t.Fatalf("Python plugin ready = %#v", process.ready)
	}
	eventually(t, 2*time.Second, func() bool {
		page, listErr := registryClient.List(
			context.Background(),
			pluginregistry.DiscoveryQuery{},
			pluginregistry.PageRequest{},
		)
		return listErr == nil && len(page.Items) == 1 &&
			page.Items[0].Name == "python-e2e" &&
			page.Items[0].URI == process.ready.URI &&
			len(page.Items[0].Manifest.Registers) == 6
	})
	time.Sleep(700 * time.Millisecond)
	registrations, err := registryClient.List(
		context.Background(),
		pluginregistry.DiscoveryQuery{},
		pluginregistry.PageRequest{},
	)
	if err != nil || len(registrations.Items) != 1 {
		t.Fatalf(
			"Python lease was not renewed: registrations=%#v err=%v",
			registrations,
			err,
		)
	}

	sourceRegistry := sdk.NewPluginRegistry()
	if err := pluginrpc.RegisterDrivers(
		sourceRegistry,
		pluginrpc.ClientConfig{},
	); err != nil {
		t.Fatal(err)
	}
	if err := sourceRegistry.Register(sdk.PluginReference{
		Name:        registrations.Items[0].Name,
		URI:         registrations.Items[0].URI,
		Description: registrations.Items[0].Manifest.Description,
		Labels:      registrations.Items[0].Labels,
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{
		Storage:             sdkstorage.NewMemoryStateBackend(),
		OperationPoll:       time.Millisecond,
		DeliveryPoll:        time.Millisecond,
		DeliveryWorkers:     2,
		DeliveryMaxAttempts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	source, err := sourceRegistry.Resolve(context.Background(), "python-e2e")
	if err != nil {
		t.Fatalf("resolve Python plugin through lease registration: %v", err)
	}
	mount, err := runtime.Mount(context.Background(), source)
	if err != nil {
		t.Fatalf("mount Python plugin through lease registration: %v", err)
	}
	catalog := runtime.Catalog()
	if len(catalog.Providers) != 1 || len(catalog.Tools) != 2 ||
		len(catalog.Capabilities) != 1 || len(catalog.Hooks) != 1 ||
		len(catalog.Subscribers) != 1 {
		t.Fatalf("Python plugin catalog = %#v", catalog)
	}

	session, err := runtime.NewSession(
		context.Background(),
		agentruntime.SessionConfig{
			ID:       "python-session",
			Provider: "python-model",
			MaxTurns: 3,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Prompt(context.Background(), "run across languages")
	if err != nil {
		t.Fatalf("run session through Python plugin: %v", err)
	}
	if result.Output != "python-session-complete:python:from-python-provider" ||
		result.Turns != 2 || result.ToolCalls != 1 {
		t.Fatalf("Python session result = %#v", result)
	}

	capabilityOutput, err := runtime.InvokeCapability(
		context.Background(),
		"python-state",
		[]byte(`{"value":"from-go"}`),
	)
	if err != nil {
		t.Fatalf("invoke Python capability: %v", err)
	}
	var capability struct {
		Language string         `json:"language"`
		Input    map[string]any `json:"input"`
	}
	if err := json.Unmarshal(capabilityOutput, &capability); err != nil {
		t.Fatal(err)
	}
	if capability.Language != "python" || capability.Input["value"] != "from-go" {
		t.Fatalf("Python capability output = %#v", capability)
	}

	var delivery struct {
		Subscription string `json:"subscription"`
		Event        struct {
			Name      string `json:"name"`
			SessionID string `json:"session_id"`
		} `json:"event"`
	}
	eventually(t, 2*time.Second, func() bool {
		raw, readErr := os.ReadFile(eventsFile)
		if readErr != nil {
			return false
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(lines) == 0 || json.Unmarshal([]byte(lines[len(lines)-1]), &delivery) != nil {
			return false
		}
		return delivery.Subscription == "python-agent-end" &&
			delivery.Event.Name == sdk.EventAgentEnd &&
			delivery.Event.SessionID == session.ID()
	})

	deliveriesBefore := readPythonDeliveries(t, eventsFile)
	var stdout, stderr bytes.Buffer
	if code := cli.Run([]string{
		"--state-dir", filepath.Join(t.TempDir(), "cli-state"),
		"--otel=false",
		"run",
		"--openai=false",
		"--file=false",
		"--plugin", "python-e2e=" + process.ready.URI,
		"--provider", "python-model",
		"--session", "python-cli",
		"--output", "json",
		"--prompt", "run through the real CLI",
	}, &stdout, &stderr, "python-integration"); code != 0 {
		t.Fatalf(
			"Python CLI run exited %d\nstdout:\n%s\nstderr:\n%s",
			code,
			stdout.String(),
			stderr.String(),
		)
	}
	var cliOutput struct {
		SessionID string              `json:"session_id"`
		Result    agentruntime.Result `json:"result"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &cliOutput); err != nil {
		t.Fatalf("decode Python CLI output: %v\n%s", err, stdout.String())
	}
	if cliOutput.SessionID != "python-cli" ||
		cliOutput.Result.Output !=
			"python-session-complete:python:from-python-provider" {
		t.Fatalf("Python CLI output = %#v", cliOutput)
	}
	deliveriesAfter := readPythonDeliveries(t, eventsFile)
	if len(deliveriesAfter) != len(deliveriesBefore)+1 {
		t.Fatalf(
			"CLI returned before draining Python subscriber: before=%d after=%d",
			len(deliveriesBefore),
			len(deliveriesAfter),
		)
	}
	lastDelivery := deliveriesAfter[len(deliveriesAfter)-1]
	if lastDelivery.Event.Name != sdk.EventAgentEnd ||
		lastDelivery.Event.SessionID != "python-cli" {
		t.Fatalf("last Python CLI delivery = %#v", lastDelivery)
	}

	client := connectPlugin(t, process.ready.URI, nil)
	echoInput, err := structpb.NewStruct(map[string]any{"value": "idempotent"})
	if err != nil {
		t.Fatal(err)
	}
	submitEcho := func() *pluginv1.Operation {
		t.Helper()
		response, submitErr := client.SubmitOperation(
			context.Background(),
			&pluginv1.SubmitOperationRequest{
				Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
				Resource: "python-echo",
				Request: &pluginv1.OperationRequest{
					IdempotencyKey: "python-idempotency",
					Input:          echoInput,
				},
			},
		)
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		return response.GetOperation()
	}
	first, second := submitEcho(), submitEcho()
	if first.GetId() == "" || second.GetId() != first.GetId() {
		t.Fatalf("Python idempotent operations = %#v and %#v", first, second)
	}
	eventually(t, 2*time.Second, func() bool {
		response, pollErr := client.PollOperation(
			context.Background(),
			&pluginv1.PollOperationRequest{
				Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
				Resource: "python-echo",
				Id:       first.GetId(),
			},
		)
		return pollErr == nil &&
			response.GetOperation().GetState() ==
				pluginv1.OperationState_OPERATION_STATE_SUCCEEDED &&
			response.GetOperation().GetOutput().AsMap()["content"] == "python:idempotent"
	})

	slow, err := client.SubmitOperation(
		context.Background(),
		&pluginv1.SubmitOperationRequest{
			Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
			Resource: "python-slow",
			Request: &pluginv1.OperationRequest{
				IdempotencyKey: "python-cancel",
				Input:          &structpb.Struct{},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := client.CancelOperation(
		context.Background(),
		&pluginv1.CancelOperationRequest{
			Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
			Resource: "python-slow",
			Id:       slow.GetOperation().GetId(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.GetOperation().GetState() !=
		pluginv1.OperationState_OPERATION_STATE_CANCELLED {
		t.Fatalf("Python cancellation = %#v", cancelled.GetOperation())
	}
	eventually(t, 500*time.Millisecond, func() bool {
		response, pollErr := client.PollOperation(
			context.Background(),
			&pluginv1.PollOperationRequest{
				Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
				Resource: "python-slow",
				Id:       slow.GetOperation().GetId(),
			},
		)
		return pollErr == nil &&
			response.GetOperation().GetState() ==
				pluginv1.OperationState_OPERATION_STATE_CANCELLED
	})

	if err := mount.Unmount(context.Background()); err != nil {
		t.Fatalf("unmount Python plugin: %v", err)
	}
	process.stop(t)
	eventually(t, 2*time.Second, func() bool {
		page, listErr := registryClient.List(
			context.Background(),
			pluginregistry.DiscoveryQuery{},
			pluginregistry.PageRequest{},
		)
		return listErr == nil && len(page.Items) == 0
	})
}

type pythonDelivery struct {
	Subscription string `json:"subscription"`
	Event        struct {
		Name      string `json:"name"`
		SessionID string `json:"session_id"`
	} `json:"event"`
}

func readPythonDeliveries(t *testing.T, path string) []pythonDelivery {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	result := make([]pythonDelivery, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		var delivery pythonDelivery
		if err := json.Unmarshal([]byte(line), &delivery); err != nil {
			t.Fatalf("decode Python delivery %q: %v", line, err)
		}
		result = append(result, delivery)
	}
	return result
}
