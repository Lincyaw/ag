package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	agentsdk "github.com/lincyaw/ag/sdk"
	openaisdk "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/shared"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Config struct {
	Model      string
	APIKey     string
	BaseURL    string
	MaxRetries int
	HTTPClient *http.Client
}

type plugin struct {
	config Config
}

func New(config Config) agentsdk.Plugin {
	return plugin{config: config}
}

func (plugin) Manifest() agentsdk.Manifest {
	return agentsdk.Manifest{
		Name:        "openai",
		Version:     "1.0.0",
		Description: "OpenAI chat completion provider using the official Go client",
		APIVersion:  agentsdk.APIVersion,
		Registers:   []string{agentsdk.ProviderResource("openai")},
	}
}

func (p plugin) Install(_ context.Context, registrar agentsdk.Registrar) error {
	if strings.TrimSpace(p.config.Model) == "" {
		return errors.New("OpenAI model is empty")
	}
	if p.config.MaxRetries < 0 {
		return errors.New("OpenAI max retries cannot be negative")
	}

	clientOptions := []option.RequestOption{
		option.WithMaxRetries(p.config.MaxRetries),
	}
	if p.config.APIKey != "" {
		clientOptions = append(clientOptions, option.WithAPIKey(p.config.APIKey))
	}
	if p.config.BaseURL != "" {
		clientOptions = append(clientOptions, option.WithBaseURL(p.config.BaseURL))
	}

	httpClient := p.config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}
	clientOptions = append(clientOptions, option.WithHTTPClient(httpClient))

	return registrar.RegisterProvider(&provider{
		client: openaisdk.NewClient(clientOptions...),
		model:  p.config.Model,
	})
}

type provider struct {
	client openaisdk.Client
	model  string
}

func (p *provider) Spec() agentsdk.ProviderSpec {
	return agentsdk.ProviderSpec{Name: "openai", Model: p.model}
}

func (p *provider) Complete(
	ctx context.Context,
	request agentsdk.ModelRequest,
) (agentsdk.ModelResponse, error) {
	messages, err := toMessages(request.Messages)
	if err != nil {
		return agentsdk.ModelResponse{}, err
	}
	tools := make([]openaisdk.ChatCompletionToolUnionParam, 0, len(request.Tools))
	for _, tool := range request.Tools {
		tools = append(tools, openaisdk.ChatCompletionFunctionTool(
			shared.FunctionDefinitionParam{
				Name:        tool.Name,
				Description: param.NewOpt(tool.Description),
				Parameters:  shared.FunctionParameters(tool.Parameters),
			},
		))
	}

	completion, err := p.client.Chat.Completions.New(
		ctx,
		openaisdk.ChatCompletionNewParams{
			Model:    openaisdk.ChatModel(p.model),
			Messages: messages,
			Tools:    tools,
		},
	)
	if err != nil {
		return agentsdk.ModelResponse{}, err
	}
	if len(completion.Choices) == 0 {
		return agentsdk.ModelResponse{}, errors.New("OpenAI returned no choices")
	}

	choice := completion.Choices[0]
	content := choice.Message.Content
	if content == "" && choice.Message.Refusal != "" {
		content = choice.Message.Refusal
	}

	calls := make([]agentsdk.ToolCall, 0, len(choice.Message.ToolCalls))
	for _, rawCall := range choice.Message.ToolCalls {
		if rawCall.Type != "function" {
			return agentsdk.ModelResponse{}, fmt.Errorf(
				"unsupported OpenAI tool call type %q",
				rawCall.Type,
			)
		}
		call := rawCall.AsFunction()
		calls = append(calls, agentsdk.ToolCall{
			ID:        call.ID,
			Name:      call.Function.Name,
			Arguments: []byte(call.Function.Arguments),
		})
	}

	return agentsdk.ModelResponse{
		Content:      content,
		ToolCalls:    calls,
		Model:        completion.Model,
		FinishReason: choice.FinishReason,
		Usage: agentsdk.Usage{
			InputTokens:  completion.Usage.PromptTokens,
			OutputTokens: completion.Usage.CompletionTokens,
		},
	}, nil
}

func toMessages(messages []agentsdk.Message) ([]openaisdk.ChatCompletionMessageParamUnion, error) {
	result := make([]openaisdk.ChatCompletionMessageParamUnion, 0, len(messages))
	for _, message := range messages {
		switch message.Role {
		case agentsdk.RoleSystem:
			result = append(result, openaisdk.SystemMessage(message.Content))
		case agentsdk.RoleUser:
			result = append(result, openaisdk.UserMessage(message.Content))
		case agentsdk.RoleAssistant:
			assistant := openaisdk.AssistantMessage(message.Content)
			for _, call := range message.ToolCalls {
				assistant.OfAssistant.ToolCalls = append(
					assistant.OfAssistant.ToolCalls,
					openaisdk.ChatCompletionMessageToolCallUnionParam{
						OfFunction: &openaisdk.ChatCompletionMessageFunctionToolCallParam{
							ID: call.ID,
							Function: openaisdk.ChatCompletionMessageFunctionToolCallFunctionParam{
								Name:      call.Name,
								Arguments: string(call.Arguments),
							},
						},
					},
				)
			}
			result = append(result, assistant)
		case agentsdk.RoleTool:
			if message.ToolCallID == "" {
				return nil, errors.New("tool message is missing tool call ID")
			}
			result = append(
				result,
				openaisdk.ToolMessage(message.Content, message.ToolCallID),
			)
		default:
			return nil, fmt.Errorf("unsupported message role %q", message.Role)
		}
	}
	return result, nil
}
