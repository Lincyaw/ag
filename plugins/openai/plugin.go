package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/lincyaw/ag/agent"
	sdk "github.com/openai/openai-go/v3"
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

func New(config Config) agent.Plugin {
	return plugin{config: config}
}

func (plugin) Name() string {
	return "openai"
}

func (p plugin) Install(host agent.Host) error {
	if strings.TrimSpace(p.config.Model) == "" {
		return errors.New("OpenAI model is empty")
	}
	if p.config.MaxRetries < 0 {
		return errors.New("OpenAI max retries cannot be negative")
	}
	if p.config.MaxRetries == 0 {
		p.config.MaxRetries = 2
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

	return host.RegisterProvider(&provider{
		client: sdk.NewClient(clientOptions...),
		model:  p.config.Model,
	})
}

type provider struct {
	client sdk.Client
	model  string
}

func (p *provider) Name() string {
	return "openai"
}

func (p *provider) Model() string {
	return p.model
}

func (p *provider) Complete(
	ctx context.Context,
	request agent.ModelRequest,
) (agent.ModelResponse, error) {
	messages, err := toMessages(request.Messages)
	if err != nil {
		return agent.ModelResponse{}, err
	}
	tools := make([]sdk.ChatCompletionToolUnionParam, 0, len(request.Tools))
	for _, tool := range request.Tools {
		tools = append(tools, sdk.ChatCompletionFunctionTool(
			shared.FunctionDefinitionParam{
				Name:        tool.Name,
				Description: param.NewOpt(tool.Description),
				Parameters:  shared.FunctionParameters(tool.Parameters),
			},
		))
	}

	completion, err := p.client.Chat.Completions.New(
		ctx,
		sdk.ChatCompletionNewParams{
			Model:    sdk.ChatModel(p.model),
			Messages: messages,
			Tools:    tools,
		},
	)
	if err != nil {
		return agent.ModelResponse{}, err
	}
	if len(completion.Choices) == 0 {
		return agent.ModelResponse{}, errors.New("OpenAI returned no choices")
	}

	choice := completion.Choices[0]
	content := choice.Message.Content
	if content == "" && choice.Message.Refusal != "" {
		content = choice.Message.Refusal
	}

	calls := make([]agent.ToolCall, 0, len(choice.Message.ToolCalls))
	for _, rawCall := range choice.Message.ToolCalls {
		if rawCall.Type != "function" {
			return agent.ModelResponse{}, fmt.Errorf(
				"unsupported OpenAI tool call type %q",
				rawCall.Type,
			)
		}
		call := rawCall.AsFunction()
		calls = append(calls, agent.ToolCall{
			ID:        call.ID,
			Name:      call.Function.Name,
			Arguments: []byte(call.Function.Arguments),
		})
	}

	return agent.ModelResponse{
		Content:      content,
		ToolCalls:    calls,
		Model:        completion.Model,
		FinishReason: choice.FinishReason,
		Usage: agent.Usage{
			InputTokens:  completion.Usage.PromptTokens,
			OutputTokens: completion.Usage.CompletionTokens,
		},
	}, nil
}

func toMessages(messages []agent.Message) ([]sdk.ChatCompletionMessageParamUnion, error) {
	result := make([]sdk.ChatCompletionMessageParamUnion, 0, len(messages))
	for _, message := range messages {
		switch message.Role {
		case agent.RoleSystem:
			result = append(result, sdk.SystemMessage(message.Content))
		case agent.RoleUser:
			result = append(result, sdk.UserMessage(message.Content))
		case agent.RoleAssistant:
			assistant := sdk.AssistantMessage(message.Content)
			for _, call := range message.ToolCalls {
				assistant.OfAssistant.ToolCalls = append(
					assistant.OfAssistant.ToolCalls,
					sdk.ChatCompletionMessageToolCallUnionParam{
						OfFunction: &sdk.ChatCompletionMessageFunctionToolCallParam{
							ID: call.ID,
							Function: sdk.ChatCompletionMessageFunctionToolCallFunctionParam{
								Name:      call.Name,
								Arguments: string(call.Arguments),
							},
						},
					},
				)
			}
			result = append(result, assistant)
		case agent.RoleTool:
			if message.ToolCallID == "" {
				return nil, errors.New("tool message is missing tool call ID")
			}
			result = append(
				result,
				sdk.ToolMessage(message.Content, message.ToolCallID),
			)
		default:
			return nil, fmt.Errorf("unsupported message role %q", message.Role)
		}
	}
	return result, nil
}
