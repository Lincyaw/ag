package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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
	Agent         Agent         `mapstructure:"agent" json:"agent" yaml:"agent"`
	OpenAI        OpenAI        `mapstructure:"openai" json:"openai" yaml:"openai"`
	Workspace     Workspace     `mapstructure:"workspace" json:"workspace" yaml:"workspace"`
	Bash          Bash          `mapstructure:"bash" json:"bash" yaml:"bash"`
	Plugins       Plugins       `mapstructure:"plugins" json:"plugins" yaml:"plugins"`
	Registry      Registry      `mapstructure:"registry" json:"registry" yaml:"registry"`
	Gateway       Gateway       `mapstructure:"gateway" json:"gateway" yaml:"gateway"`
	State         State         `mapstructure:"state" json:"state" yaml:"state"`
	Observability Observability `mapstructure:"observability" json:"observability" yaml:"observability"`
	Logging       Logging       `mapstructure:"logging" json:"logging" yaml:"logging"`
}

type Agent struct {
	System   string        `mapstructure:"system" json:"system" yaml:"system"`
	Provider string        `mapstructure:"provider" json:"provider" yaml:"provider"`
	MaxTurns int           `mapstructure:"max_turns" json:"max_turns" yaml:"max_turns"`
	Timeout  time.Duration `mapstructure:"timeout" json:"timeout" yaml:"timeout"`
}

func (agent Agent) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		System   string `json:"system"`
		Provider string `json:"provider"`
		MaxTurns int    `json:"max_turns"`
		Timeout  string `json:"timeout"`
	}{
		System: agent.System, Provider: agent.Provider,
		MaxTurns: agent.MaxTurns, Timeout: agent.Timeout.String(),
	})
}

type OpenAI struct {
	Enabled    bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Model      string `mapstructure:"model" json:"model" yaml:"model"`
	BaseURL    string `mapstructure:"base_url" json:"base_url" yaml:"base_url"`
	MaxRetries int    `mapstructure:"max_retries" json:"max_retries" yaml:"max_retries"`
}

type Workspace struct {
	Enabled       bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Root          string `mapstructure:"root" json:"root" yaml:"root"`
	EnableWrite   bool   `mapstructure:"enable_write" json:"enable_write" yaml:"enable_write"`
	MaxReadBytes  int64  `mapstructure:"max_read_bytes" json:"max_read_bytes" yaml:"max_read_bytes"`
	MaxWriteBytes int64  `mapstructure:"max_write_bytes" json:"max_write_bytes" yaml:"max_write_bytes"`
	MaxEntries    int    `mapstructure:"max_entries" json:"max_entries" yaml:"max_entries"`
}

type Bash struct {
	Enabled        bool          `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Shell          string        `mapstructure:"shell" json:"shell" yaml:"shell"`
	DefaultTimeout time.Duration `mapstructure:"default_timeout" json:"default_timeout" yaml:"default_timeout"`
	MaxTimeout     time.Duration `mapstructure:"max_timeout" json:"max_timeout" yaml:"max_timeout"`
	MaxOutputBytes int64         `mapstructure:"max_output_bytes" json:"max_output_bytes" yaml:"max_output_bytes"`
	Environment    []string      `mapstructure:"environment" json:"environment" yaml:"environment"`
}

func (bash Bash) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Enabled        bool     `json:"enabled"`
		Shell          string   `json:"shell"`
		DefaultTimeout string   `json:"default_timeout"`
		MaxTimeout     string   `json:"max_timeout"`
		MaxOutputBytes int64    `json:"max_output_bytes"`
		Environment    []string `json:"environment"`
	}{
		Enabled: bash.Enabled, Shell: bash.Shell,
		DefaultTimeout: bash.DefaultTimeout.String(), MaxTimeout: bash.MaxTimeout.String(),
		MaxOutputBytes: bash.MaxOutputBytes, Environment: bash.Environment,
	})
}

type Plugins struct {
	Remote            []string `mapstructure:"remote" json:"remote" yaml:"remote"`
	RegistryURI       string   `mapstructure:"registry_uri" json:"registry_uri" yaml:"registry_uri"`
	RegistryNamespace string   `mapstructure:"registry_namespace" json:"registry_namespace" yaml:"registry_namespace"`
}

type Registry struct {
	Listen          string `mapstructure:"listen" json:"listen" yaml:"listen"`
	AdvertiseURI    string `mapstructure:"advertise_uri" json:"advertise_uri,omitempty" yaml:"advertise_uri,omitempty"`
	BackendURI      string `mapstructure:"backend_uri" json:"backend_uri" yaml:"backend_uri"`
	TLSCertFile     string `mapstructure:"tls_cert_file" json:"tls_cert_file,omitempty" yaml:"tls_cert_file,omitempty"`
	TLSKeyFile      string `mapstructure:"tls_key_file" json:"tls_key_file,omitempty" yaml:"tls_key_file,omitempty"`
	MaxMessageBytes int    `mapstructure:"max_message_bytes" json:"max_message_bytes" yaml:"max_message_bytes"`
}

type Gateway struct {
	Listen            string        `mapstructure:"listen" json:"listen" yaml:"listen"`
	Directory         string        `mapstructure:"directory" json:"directory" yaml:"directory"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout" json:"read_header_timeout" yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout" json:"idle_timeout" yaml:"idle_timeout"`
	ShutdownTimeout   time.Duration `mapstructure:"shutdown_timeout" json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

func (gateway Gateway) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Listen            string `json:"listen"`
		Directory         string `json:"directory"`
		ReadHeaderTimeout string `json:"read_header_timeout"`
		IdleTimeout       string `json:"idle_timeout"`
		ShutdownTimeout   string `json:"shutdown_timeout"`
	}{
		Listen: gateway.Listen, Directory: gateway.Directory,
		ReadHeaderTimeout: gateway.ReadHeaderTimeout.String(),
		IdleTimeout:       gateway.IdleTimeout.String(),
		ShutdownTimeout:   gateway.ShutdownTimeout.String(),
	})
}

type State struct {
	Directory  string `mapstructure:"directory" json:"directory" yaml:"directory"`
	BackendURI string `mapstructure:"backend_uri" json:"backend_uri,omitempty" yaml:"backend_uri,omitempty"`
	Namespace  string `mapstructure:"namespace" json:"namespace,omitempty" yaml:"namespace,omitempty"`
}

type Observability struct {
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
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
	if strings.TrimSpace(c.Agent.Provider) == "" {
		return errors.New("agent.provider is required")
	}
	if c.Agent.MaxTurns < 1 {
		return errors.New("agent.max_turns must be positive")
	}
	if c.Agent.Timeout <= 0 {
		return errors.New("agent.timeout must be positive")
	}
	if c.OpenAI.Enabled && strings.TrimSpace(c.OpenAI.Model) == "" {
		return errors.New("openai.model is required")
	}
	if c.OpenAI.MaxRetries < 0 {
		return errors.New("openai.max_retries cannot be negative")
	}
	if c.Workspace.Enabled && strings.TrimSpace(c.Workspace.Root) == "" {
		return errors.New("workspace.root is required")
	}
	if c.Workspace.Enabled && (c.Workspace.MaxReadBytes < 1 ||
		c.Workspace.MaxWriteBytes < 1 || c.Workspace.MaxEntries < 1) {
		return errors.New("workspace limits must be positive")
	}
	if c.Bash.Enabled && (strings.TrimSpace(c.Bash.Shell) == "" ||
		c.Bash.DefaultTimeout <= 0 || c.Bash.MaxTimeout < c.Bash.DefaultTimeout ||
		c.Bash.MaxOutputBytes < 1) {
		return errors.New("bash configuration and limits are invalid")
	}
	if strings.TrimSpace(c.State.Directory) == "" &&
		strings.TrimSpace(c.State.BackendURI) == "" {
		return errors.New("state.directory or state.backend_uri is required")
	}
	for _, remote := range c.Plugins.Remote {
		if strings.TrimSpace(remote) == "" {
			return errors.New("plugins.remote contains an empty entry")
		}
	}
	if strings.TrimSpace(c.Plugins.RegistryNamespace) == "" {
		return errors.New("plugins.registry_namespace is required")
	}
	if strings.TrimSpace(c.Registry.Listen) == "" {
		return errors.New("registry.listen is required")
	}
	if strings.TrimSpace(c.Registry.BackendURI) == "" {
		return errors.New("registry.backend_uri is required")
	}
	if (strings.TrimSpace(c.Registry.TLSCertFile) == "") !=
		(strings.TrimSpace(c.Registry.TLSKeyFile) == "") {
		return errors.New(
			"registry.tls_cert_file and registry.tls_key_file must be configured together",
		)
	}
	if c.Registry.MaxMessageBytes < 0 {
		return errors.New("registry.max_message_bytes cannot be negative")
	}
	if strings.TrimSpace(c.Gateway.Listen) == "" {
		return errors.New("gateway.listen is required")
	}
	if strings.TrimSpace(c.Gateway.Directory) == "" {
		return errors.New("gateway.directory is required")
	}
	if c.Gateway.ReadHeaderTimeout <= 0 ||
		c.Gateway.IdleTimeout <= 0 ||
		c.Gateway.ShutdownTimeout <= 0 {
		return errors.New("gateway timeouts must be positive")
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
	v.SetDefault("agent.provider", "openai")
	v.SetDefault("agent.max_turns", 8)
	v.SetDefault("agent.timeout", "5m")
	v.SetDefault("openai.enabled", true)
	v.SetDefault("openai.model", "gpt-5-mini")
	v.SetDefault("openai.base_url", "")
	v.SetDefault("openai.max_retries", 2)
	v.SetDefault("workspace.root", ".")
	v.SetDefault("workspace.enabled", true)
	v.SetDefault("workspace.enable_write", false)
	v.SetDefault("workspace.max_read_bytes", 1<<20)
	v.SetDefault("workspace.max_write_bytes", 1<<20)
	v.SetDefault("workspace.max_entries", 1000)
	v.SetDefault("bash.enabled", false)
	v.SetDefault("bash.shell", "/bin/sh")
	v.SetDefault("bash.default_timeout", "30s")
	v.SetDefault("bash.max_timeout", "5m")
	v.SetDefault("bash.max_output_bytes", 1<<20)
	v.SetDefault("bash.environment", []string{})
	v.SetDefault("plugins.remote", []string{})
	v.SetDefault("plugins.registry_uri", "")
	v.SetDefault("plugins.registry_namespace", "default")
	v.SetDefault("registry.listen", "127.0.0.1:9090")
	v.SetDefault("registry.advertise_uri", "")
	v.SetDefault("registry.backend_uri", defaultRegistryBackendURI())
	v.SetDefault("registry.tls_cert_file", "")
	v.SetDefault("registry.tls_key_file", "")
	v.SetDefault("registry.max_message_bytes", 0)
	v.SetDefault("gateway.listen", "127.0.0.1:8080")
	v.SetDefault("gateway.directory", defaultGatewayDirectory())
	v.SetDefault("gateway.read_header_timeout", "5s")
	v.SetDefault("gateway.idle_timeout", "1m")
	v.SetDefault("gateway.shutdown_timeout", "10s")
	v.SetDefault("state.directory", defaultStateDirectory())
	v.SetDefault("state.backend_uri", "")
	v.SetDefault("state.namespace", "")
	v.SetDefault("observability.enabled", true)
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
		"agent.system":                "system",
		"agent.provider":              "provider",
		"agent.max_turns":             "max-turns",
		"agent.timeout":               "timeout",
		"openai.enabled":              "openai",
		"openai.model":                "model",
		"openai.base_url":             "base-url",
		"openai.max_retries":          "max-retries",
		"workspace.enabled":           "file",
		"workspace.root":              "cwd",
		"workspace.enable_write":      "write",
		"workspace.max_read_bytes":    "max-read-bytes",
		"workspace.max_write_bytes":   "max-write-bytes",
		"workspace.max_entries":       "max-entries",
		"bash.enabled":                "bash",
		"bash.shell":                  "shell",
		"bash.default_timeout":        "bash-timeout",
		"bash.max_timeout":            "bash-max-timeout",
		"bash.max_output_bytes":       "bash-max-output-bytes",
		"plugins.remote":              "plugin",
		"plugins.registry_uri":        "registry-uri",
		"plugins.registry_namespace":  "registry-namespace",
		"registry.listen":             "listen",
		"registry.advertise_uri":      "advertise-uri",
		"registry.backend_uri":        "registry-backend",
		"registry.tls_cert_file":      "tls-cert",
		"registry.tls_key_file":       "tls-key",
		"registry.max_message_bytes":  "max-message-bytes",
		"gateway.listen":              "gateway-listen",
		"gateway.directory":           "gateway-dir",
		"gateway.read_header_timeout": "read-header-timeout",
		"gateway.idle_timeout":        "idle-timeout",
		"gateway.shutdown_timeout":    "shutdown-timeout",
		"state.directory":             "state-dir",
		"state.backend_uri":           "storage",
		"state.namespace":             "state-namespace",
		"observability.enabled":       "otel",
		"logging.level":               "log-level",
		"logging.format":              "log-format",
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

func defaultStateDirectory() string {
	directory, err := os.UserConfigDir()
	if err != nil {
		return filepath.Join(".ag", "state")
	}
	return filepath.Join(directory, AppName, "state")
}

func defaultRegistryBackendURI() string {
	directory, err := os.UserHomeDir()
	if err != nil {
		directory = ".ag"
	} else {
		directory = filepath.Join(directory, ".ag")
	}
	return (&url.URL{
		Scheme: "file",
		Path:   filepath.Join(directory, "registry"),
	}).String()
}

func defaultGatewayDirectory() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".ag", "gateway")
	}
	return filepath.Join(home, ".ag", "gateway")
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

	home, dirErr := os.UserHomeDir()
	if dirErr != nil {
		return "", "", false, fmt.Errorf("resolve user home directory: %w", dirErr)
	}
	base := filepath.Join(home, "."+AppName, "config")
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
