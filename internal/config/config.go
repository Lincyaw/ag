package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	AppName       = "ag"
	DefaultSystem = "You are a concise command-line agent. Use the read-only workspace tools when they are useful."
)

type Config struct {
	Agent     Agent     `mapstructure:"agent" json:"agent" yaml:"agent"`
	OpenAI    OpenAI    `mapstructure:"openai" json:"openai" yaml:"openai"`
	Workspace Workspace `mapstructure:"workspace" json:"workspace" yaml:"workspace"`
	Logging   Logging   `mapstructure:"logging" json:"logging" yaml:"logging"`
}

type Agent struct {
	System   string        `mapstructure:"system" json:"system" yaml:"system"`
	MaxTurns int           `mapstructure:"max_turns" json:"max_turns" yaml:"max_turns"`
	Timeout  time.Duration `mapstructure:"timeout" json:"timeout" yaml:"timeout"`
}

type OpenAI struct {
	Model      string `mapstructure:"model" json:"model" yaml:"model"`
	BaseURL    string `mapstructure:"base_url" json:"base_url" yaml:"base_url"`
	MaxRetries int    `mapstructure:"max_retries" json:"max_retries" yaml:"max_retries"`
}

type Workspace struct {
	Root         string `mapstructure:"root" json:"root" yaml:"root"`
	MaxReadBytes int64  `mapstructure:"max_read_bytes" json:"max_read_bytes" yaml:"max_read_bytes"`
}

type Logging struct {
	Level  string `mapstructure:"level" json:"level" yaml:"level"`
	Format string `mapstructure:"format" json:"format" yaml:"format"`
}

type LoadOptions struct {
	ConfigFile string
	Flags      *pflag.FlagSet
}

type Loaded struct {
	Config        Config
	File          string
	CandidateFile string
}

func Load(options LoadOptions) (Loaded, error) {
	v := viper.New()
	setDefaults(v)
	configureEnvironment(v)

	if options.Flags != nil {
		if err := bindFlags(v, options.Flags); err != nil {
			return Loaded{}, err
		}
	}

	configFile, candidate, required, err := resolveConfigFile(options.ConfigFile)
	if err != nil {
		return Loaded{}, err
	}
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return Loaded{}, fmt.Errorf("read config %q: %w", configFile, err)
		}
	} else if required {
		return Loaded{}, fmt.Errorf("config file %q does not exist", candidate)
	}

	var values Config
	if err := v.UnmarshalExact(&values); err != nil {
		return Loaded{}, fmt.Errorf("decode config: %w", err)
	}
	if err := values.Validate(); err != nil {
		return Loaded{}, err
	}

	return Loaded{
		Config:        values,
		File:          v.ConfigFileUsed(),
		CandidateFile: candidate,
	}, nil
}

func (c Config) Validate() error {
	if c.Agent.MaxTurns < 1 {
		return errors.New("agent.max_turns must be positive")
	}
	if c.Agent.Timeout <= 0 {
		return errors.New("agent.timeout must be positive")
	}
	if strings.TrimSpace(c.OpenAI.Model) == "" {
		return errors.New("openai.model is required")
	}
	if c.OpenAI.MaxRetries < 0 {
		return errors.New("openai.max_retries cannot be negative")
	}
	if strings.TrimSpace(c.Workspace.Root) == "" {
		return errors.New("workspace.root is required")
	}
	if c.Workspace.MaxReadBytes < 1 {
		return errors.New("workspace.max_read_bytes must be positive")
	}
	switch strings.ToLower(strings.TrimSpace(c.Logging.Level)) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return errors.New(
			`logging.level must be "debug", "info", "warn", or "error"`,
		)
	}
	switch strings.ToLower(strings.TrimSpace(c.Logging.Format)) {
	case "json", "text":
	default:
		return errors.New(`logging.format must be "json" or "text"`)
	}
	return nil
}

func (l Loaded) Path() string {
	if l.File != "" {
		return l.File
	}
	return l.CandidateFile
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("agent.system", DefaultSystem)
	v.SetDefault("agent.max_turns", 8)
	v.SetDefault("agent.timeout", "5m")
	v.SetDefault("openai.model", "gpt-5-mini")
	v.SetDefault("openai.base_url", "")
	v.SetDefault("openai.max_retries", 2)
	v.SetDefault("workspace.root", ".")
	v.SetDefault("workspace.max_read_bytes", 64<<10)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
}

func configureEnvironment(v *viper.Viper) {
	v.SetEnvPrefix("AGENTM")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// The official OpenAI names remain valid compatibility aliases. API keys are
	// deliberately absent: the official SDK reads OPENAI_API_KEY directly.
	_ = v.BindEnv("openai.model", "AGENTM_OPENAI_MODEL", "OPENAI_MODEL")
	_ = v.BindEnv(
		"openai.base_url",
		"AGENTM_OPENAI_BASE_URL",
		"OPENAI_BASE_URL",
	)
}

func bindFlags(v *viper.Viper, flags *pflag.FlagSet) error {
	bindings := map[string]string{
		"agent.system":             "system",
		"agent.max_turns":          "max-turns",
		"agent.timeout":            "timeout",
		"openai.model":             "model",
		"openai.base_url":          "base-url",
		"openai.max_retries":       "max-retries",
		"workspace.root":           "cwd",
		"workspace.max_read_bytes": "max-read-bytes",
		"logging.level":            "log-level",
		"logging.format":           "log-format",
	}
	for key, name := range bindings {
		flag := flags.Lookup(name)
		if flag == nil {
			continue
		}
		if err := v.BindPFlag(key, flag); err != nil {
			return fmt.Errorf("bind flag --%s: %w", name, err)
		}
	}
	return nil
}

func resolveConfigFile(explicit string) (
	configFile string,
	candidate string,
	required bool,
	err error,
) {
	requested := strings.TrimSpace(explicit)
	if requested == "" {
		requested = strings.TrimSpace(os.Getenv("AGENTM_CONFIG"))
	}
	if requested != "" {
		absolute, absErr := filepath.Abs(requested)
		if absErr != nil {
			return "", "", false, fmt.Errorf("resolve config path: %w", absErr)
		}
		if _, statErr := os.Stat(absolute); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return "", absolute, true, nil
			}
			return "", "", false, fmt.Errorf("stat config %q: %w", absolute, statErr)
		}
		return absolute, absolute, true, nil
	}

	configDir, dirErr := os.UserConfigDir()
	if dirErr != nil {
		return "", "", false, fmt.Errorf("resolve user config directory: %w", dirErr)
	}
	base := filepath.Join(configDir, AppName, "config")
	for _, extension := range []string{".toml", ".yaml", ".yml", ".json"} {
		path := base + extension
		if _, statErr := os.Stat(path); statErr == nil {
			return path, base + ".toml", false, nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return "", "", false, fmt.Errorf("stat config %q: %w", path, statErr)
		}
	}
	return "", base + ".toml", false, nil
}
