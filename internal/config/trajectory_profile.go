package config

import "time"

// TrajectoryRuntimeProfile is the execution configuration owned by one
// trajectory. Process-wide concerns (gateway storage, logging, credentials)
// intentionally stay in the managed gateway's base configuration.
type TrajectoryRuntimeProfile struct {
	OpenAI        TrajectoryOpenAIProfile `json:"openai"`
	Workspace     Workspace               `json:"workspace"`
	Bash          TrajectoryBashProfile   `json:"bash"`
	Compact       Compact                 `json:"compact"`
	Tree          Tree                    `json:"tree"`
	HostFS        HostFS                  `json:"hostfs"`
	Plugins       Plugins                 `json:"plugins"`
	Observability Observability           `json:"observability"`
}

type TrajectoryOpenAIProfile struct {
	Enabled       bool   `json:"enabled"`
	Model         string `json:"model"`
	BaseURL       string `json:"base_url"`
	AzureEndpoint string `json:"azure_endpoint,omitempty"`
	APIVersion    string `json:"api_version,omitempty"`
	MaxRetries    int    `json:"max_retries"`
}

type TrajectoryBashProfile struct {
	Enabled               bool     `json:"enabled"`
	Shell                 string   `json:"shell"`
	DefaultTimeoutNanosec int64    `json:"default_timeout_nanosec"`
	MaxTimeoutNanosec     int64    `json:"max_timeout_nanosec"`
	MaxOutputBytes        int64    `json:"max_output_bytes"`
	Environment           []string `json:"environment,omitempty"`
}

func NewTrajectoryRuntimeProfile(config Config) TrajectoryRuntimeProfile {
	return TrajectoryRuntimeProfile{
		OpenAI: TrajectoryOpenAIProfile{
			Enabled: config.OpenAI.Enabled, Model: config.OpenAI.Model,
			BaseURL:       config.OpenAI.BaseURL,
			AzureEndpoint: config.OpenAI.AzureEndpoint,
			APIVersion:    config.OpenAI.APIVersion,
			MaxRetries:    config.OpenAI.MaxRetries,
		},
		Workspace: config.Workspace,
		Bash: TrajectoryBashProfile{
			Enabled: config.Bash.Enabled, Shell: config.Bash.Shell,
			DefaultTimeoutNanosec: int64(config.Bash.DefaultTimeout),
			MaxTimeoutNanosec:     int64(config.Bash.MaxTimeout),
			MaxOutputBytes:        config.Bash.MaxOutputBytes,
			Environment:           append([]string(nil), config.Bash.Environment...),
		},
		Compact: config.Compact, Tree: config.Tree, HostFS: config.HostFS,
		Plugins: config.Plugins, Observability: config.Observability,
	}
}

// Apply overlays trajectory-scoped settings onto the gateway's base config.
// API credentials and default headers deliberately remain process-owned.
func (profile TrajectoryRuntimeProfile) Apply(base Config) Config {
	base.OpenAI.Enabled = profile.OpenAI.Enabled
	base.OpenAI.Model = profile.OpenAI.Model
	base.OpenAI.BaseURL = profile.OpenAI.BaseURL
	base.OpenAI.AzureEndpoint = profile.OpenAI.AzureEndpoint
	base.OpenAI.APIVersion = profile.OpenAI.APIVersion
	base.OpenAI.MaxRetries = profile.OpenAI.MaxRetries
	base.Workspace = profile.Workspace
	base.Bash = Bash{
		Enabled: profile.Bash.Enabled, Shell: profile.Bash.Shell,
		DefaultTimeout: time.Duration(profile.Bash.DefaultTimeoutNanosec),
		MaxTimeout:     time.Duration(profile.Bash.MaxTimeoutNanosec),
		MaxOutputBytes: profile.Bash.MaxOutputBytes,
		Environment:    append([]string(nil), profile.Bash.Environment...),
	}
	base.Compact = profile.Compact
	base.Tree = profile.Tree
	base.HostFS = profile.HostFS
	base.Plugins = profile.Plugins
	base.Observability = profile.Observability
	return base
}
