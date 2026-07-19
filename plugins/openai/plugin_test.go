package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	agentsdk "github.com/lincyaw/ag/sdk"
	openaisdk "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

type providerRegistrar struct {
	agentsdk.Registrar
	provider agentsdk.Provider
}

func (registrar *providerRegistrar) RegisterProvider(
	provider agentsdk.Provider,
) error {
	registrar.provider = provider
	return nil
}

func TestPluginNormalizesModel(t *testing.T) {
	registrar := &providerRegistrar{}
	if err := New(Config{Model: " test-model "}).Install(
		t.Context(),
		registrar,
	); err != nil {
		t.Fatal(err)
	}
	if registrar.provider.Spec().Model != "test-model" {
		t.Fatalf("provider spec = %#v", registrar.provider.Spec())
	}
}

func TestPluginRespectsZeroMaxRetries(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		attempts++
		writer.Header().Set("Content-Type", "application/json")
		http.Error(writer, `{"error":{"message":"try again"}}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	registrar := &providerRegistrar{}
	plugin := New(Config{
		Model: "test-model", APIKey: "test",
		BaseURL: server.URL + "/v1", MaxRetries: 0,
		HTTPClient: server.Client(),
	})
	if err := plugin.Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}
	if registrar.provider == nil {
		t.Fatal("provider was not registered")
	}
	provider, ok := registrar.provider.(agentsdk.SyncProvider)
	if !ok {
		t.Fatal("registered provider is not synchronous")
	}
	_, err := provider.Complete(t.Context(), agentsdk.ModelRequest{
		Messages: []agentsdk.Message{{
			Role: agentsdk.RoleUser, Content: "hello",
		}},
	})
	if err == nil {
		t.Fatal("provider request unexpectedly succeeded")
	}
	if attempts != 1 {
		t.Fatalf("request attempts = %d, want 1", attempts)
	}
}

func TestProviderUsesOfficialSDKForTools(t *testing.T) {
	requestBody := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		requestBody <- body
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
			"id": "chatcmpl-test",
			"object": "chat.completion",
			"created": 1,
			"model": "test-model",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "",
					"tool_calls": [{
						"id": "call-1",
						"type": "function",
						"function": {
							"name": "read_file",
							"arguments": "{\"path\":\"README.md\"}"
						}
					}]
				},
				"finish_reason": "tool_calls",
				"logprobs": null
			}],
			"usage": {
				"prompt_tokens": 10,
				"completion_tokens": 5,
				"total_tokens": 15
			}
		}`))
	}))
	defer server.Close()

	client := openaisdk.NewClient(
		option.WithBaseURL(server.URL+"/v1"),
		option.WithAPIKey("test"),
		option.WithMaxRetries(0),
		option.WithHTTPClient(server.Client()),
	)
	model := &provider{client: client, model: "test-model"}
	response, err := model.Complete(context.Background(), agentsdk.ModelRequest{
		Messages: []agentsdk.Message{{
			Role:    agentsdk.RoleUser,
			Content: "read the README",
		}},
		Tools: []agentsdk.ToolSpec{{
			Name:              "read_file",
			Description:       "Read one file.",
			InterruptBehavior: agentsdk.ToolInterruptCancel,
			Parameters: map[string]any{
				"type": "object",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	body := <-requestBody
	if body["model"] != "test-model" {
		t.Fatalf("model = %v", body["model"])
	}
	tools, ok := body["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %#v", body["tools"])
	}
	function := tools[0].(map[string]any)["function"].(map[string]any)
	if _, exists := function["interrupt_behavior"]; exists {
		t.Fatalf("tool interrupt behavior leaked into OpenAI function: %#v", function)
	}
	if len(response.ToolCalls) != 1 {
		t.Fatalf("tool calls = %#v", response.ToolCalls)
	}
	if response.ToolCalls[0].Name != "read_file" {
		t.Fatalf("tool name = %q", response.ToolCalls[0].Name)
	}
	if response.Usage.InputTokens != 10 || response.Usage.OutputTokens != 5 {
		t.Fatalf("usage = %#v", response.Usage)
	}
}
