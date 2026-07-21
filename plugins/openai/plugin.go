package openai

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strings"

	agentsdk "github.com/lincyaw/ag/sdk"
	openaisdk "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/azure"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/shared"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Config struct {
	Model          string
	APIKey         string
	BaseURL        string
	AzureEndpoint  string
	APIVersion     string
	DefaultHeaders map[string]string
	MaxRetries     int
	HTTPClient     *http.Client
}

type plugin struct {
	config Config
}

func New(config Config) agentsdk.Plugin {
	config.DefaultHeaders = maps.Clone(config.DefaultHeaders)
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
	p.config.Model = strings.TrimSpace(p.config.Model)
	if p.config.Model == "" {
		return errors.New("OpenAI model is empty")
	}
	if p.config.MaxRetries < 0 {
		return errors.New("OpenAI max retries cannot be negative")
	}
	baseURL := strings.TrimSpace(p.config.BaseURL)
	azureEndpoint := strings.TrimSpace(p.config.AzureEndpoint)
	if baseURL != "" && azureEndpoint != "" {
		return errors.New("OpenAI base URL and Azure endpoint cannot both be configured")
	}

	clientOptions := []option.RequestOption{
		option.WithMaxRetries(p.config.MaxRetries),
	}
	if azureEndpoint != "" {
		azureOptions, err := azureClientOptions(
			azureEndpoint,
			strings.TrimSpace(p.config.APIVersion),
			p.config.APIKey,
		)
		if err != nil {
			return err
		}
		clientOptions = append(clientOptions, azureOptions...)
	} else {
		if p.config.APIKey != "" {
			clientOptions = append(clientOptions, option.WithAPIKey(p.config.APIKey))
		}
		if baseURL != "" {
			clientOptions = append(clientOptions, option.WithBaseURL(baseURL))
		}
	}
	headerNames := make([]string, 0, len(p.config.DefaultHeaders))
	for name := range p.config.DefaultHeaders {
		if strings.TrimSpace(name) == "" {
			return errors.New("OpenAI default header name cannot be empty")
		}
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)
	for _, name := range headerNames {
		clientOptions = append(
			clientOptions,
			option.WithHeader(name, p.config.DefaultHeaders[name]),
		)
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

func azureClientOptions(
	endpoint string,
	apiVersion string,
	apiKey string,
) ([]option.RequestOption, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Azure endpoint %q", endpoint)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("Azure endpoint cannot contain credentials, query, or fragment")
	}

	prefixPath := strings.TrimRight(parsed.Path, "/")
	prefixRawPath := strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.Path = ""
	parsed.RawPath = ""

	clientOptions := []option.RequestOption{
		azure.WithEndpoint(parsed.String(), apiVersion),
	}
	if apiKey != "" {
		clientOptions = append(clientOptions, azure.WithAPIKey(apiKey))
	}
	if prefixPath == "" {
		return clientOptions, nil
	}

	clientOptions = append(clientOptions, option.WithMiddleware(func(
		request *http.Request,
		next option.MiddlewareNext,
	) (*http.Response, error) {
		requestPath := request.URL.Path
		requestRawPath := request.URL.EscapedPath()
		request.URL.Path = prefixPath + requestPath
		request.URL.RawPath = prefixRawPath + requestRawPath
		if request.URL.RawPath == request.URL.Path {
			request.URL.RawPath = ""
		}
		return next(request)
	}))
	return clientOptions, nil
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

	params := openaisdk.ChatCompletionNewParams{
		Model:    openaisdk.ChatModel(p.model),
		Messages: messages,
		Tools:    tools,
	}
	if request.ReasoningEffort != "" {
		params.ReasoningEffort = shared.ReasoningEffort(request.ReasoningEffort)
	}
	completion, err := p.client.Chat.Completions.New(
		ctx,
		params,
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
