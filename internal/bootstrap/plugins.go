package bootstrap

import (
	"log/slog"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/plugins/bash"
	"github.com/lincyaw/ag/plugins/compact"
	fileplugin "github.com/lincyaw/ag/plugins/file"
	"github.com/lincyaw/ag/plugins/memory"
	"github.com/lincyaw/ag/plugins/openai"
	otelplugin "github.com/lincyaw/ag/plugins/otel"
	"github.com/lincyaw/ag/plugins/skills"
	"github.com/lincyaw/ag/plugins/subagent"
	"github.com/lincyaw/ag/plugins/systemprompt"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func configuredLocalPlugins(
	config appconfig.Config,
	logger *slog.Logger,
	tracer trace.Tracer,
	meter metric.Meter,
) ([]sdk.Plugin, error) {
	plugins := make([]sdk.Plugin, 0, 9)
	if config.Observability.Enabled {
		plugin, err := otelplugin.New(otelplugin.Config{
			Logger: logger,
			Tracer: tracer,
			Meter:  meter,
		})
		if err != nil {
			return nil, err
		}
		plugins = append(plugins, plugin)
	}
	if config.OpenAI.Enabled {
		plugins = append(plugins, openai.New(openai.Config{
			Model:          config.OpenAI.Model,
			APIKey:         config.OpenAI.APIKey,
			BaseURL:        config.OpenAI.BaseURL,
			AzureEndpoint:  config.OpenAI.AzureEndpoint,
			APIVersion:     config.OpenAI.APIVersion,
			DefaultHeaders: config.OpenAI.DefaultHeaders,
			MaxRetries:     config.OpenAI.MaxRetries,
		}))
	}
	if config.Compact.Enabled {
		plugins = append(plugins, compact.New(compact.Config{
			TriggerTokens:      config.Compact.TriggerTokens,
			TargetTokens:       config.Compact.TargetTokens,
			KeepRecentMessages: config.Compact.KeepRecentMessages,
			MaxMessageChars:    config.Compact.MaxMessageChars,
			MaxToolResultChars: config.Compact.MaxToolResultChars,
		}))
	}
	if config.SystemPrompt.Enabled {
		plugins = append(plugins, systemprompt.New(systemprompt.Config{
			WorkspaceRoot: config.Workspace.Root,
			Prompt:        config.SystemPrompt.Prompt,
			PromptFile:    config.SystemPrompt.PromptFile,
			MaxFileBytes:  config.SystemPrompt.MaxFileBytes,
			Logger:        logger,
		}))
	}
	if config.Skills.Enabled {
		plugins = append(plugins, skills.New(skills.Config{
			WorkspaceRoot:   config.Workspace.Root,
			Paths:           config.Skills.Paths,
			IncludeDefaults: config.Skills.IncludeDefaults,
			MaxReadBytes:    config.Skills.MaxReadBytes,
			Logger:          logger,
		}))
	}
	if config.Memory.Enabled {
		plugins = append(plugins, memory.New(memory.Config{
			WorkspaceRoot:       config.Workspace.Root,
			Path:                config.Memory.Path,
			EnableWrite:         config.Memory.EnableWrite,
			IndexInSystemPrompt: config.Memory.IndexInSystemPrompt,
			MaxReadBytes:        config.Memory.MaxReadBytes,
			MaxIndexEntries:     config.Memory.MaxIndexEntries,
		}))
	}
	if config.Workspace.Enabled {
		plugins = append(plugins, fileplugin.New(fileplugin.Config{
			Root:          config.Workspace.Root,
			EnableWrite:   config.Workspace.EnableWrite,
			MaxReadBytes:  config.Workspace.MaxReadBytes,
			MaxWriteBytes: config.Workspace.MaxWriteBytes,
			MaxEntries:    config.Workspace.MaxEntries,
		}))
	}
	if config.Bash.Enabled {
		plugins = append(plugins, bash.New(bash.Config{
			Root:           config.Workspace.Root,
			Shell:          config.Bash.Shell,
			DefaultTimeout: config.Bash.DefaultTimeout,
			MaxTimeout:     config.Bash.MaxTimeout,
			MaxOutputBytes: config.Bash.MaxOutputBytes,
			Environment:    config.Bash.Environment,
		}))
	}
	if config.Subagent.Enabled {
		agents := make([]subagent.Agent, len(config.Subagent.Agents))
		for index, agent := range config.Subagent.Agents {
			var tools []string
			if agent.Tools != nil {
				tools = append(
					make([]string, 0, len(agent.Tools)),
					agent.Tools...,
				)
			}
			agents[index] = subagent.Agent{
				Name: agent.Name, Description: agent.Description,
				Provider: agent.Provider, System: agent.System,
				MaxTurns: agent.MaxTurns,
				Tools:    tools,
			}
		}
		plugins = append(plugins, subagent.New(subagent.Config{Agents: agents}))
	}
	return plugins, nil
}
