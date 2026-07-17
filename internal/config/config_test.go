package config

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestLoadPrecedenceFlagEnvFileDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[agent]
max_turns = 3
timeout = "90s"

[openai]
model = "file-model"

[workspace]
root = "."

[logging]
level = "debug"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTM_OPENAI_MODEL", "env-model")

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("model", "flag-default", "")
	flags.Int("max-turns", 8, "")
	if err := flags.Set("max-turns", "11"); err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(LoadOptions{ConfigFile: path, Flags: flags})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.OpenAI.Model != "env-model" {
		t.Fatalf("model = %q", loaded.Config.OpenAI.Model)
	}
	if loaded.Config.Agent.MaxTurns != 11 {
		t.Fatalf("max turns = %d", loaded.Config.Agent.MaxTurns)
	}
	if loaded.Config.Agent.Timeout != 90*time.Second {
		t.Fatalf("timeout = %s", loaded.Config.Agent.Timeout)
	}
	if loaded.Config.Logging.Format != "json" {
		t.Fatalf("default log format = %q", loaded.Config.Logging.Format)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[agent]
max_turns = 3
unknown = true
`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Load(LoadOptions{ConfigFile: path})
	if err == nil {
		t.Fatal("expected unknown key to fail")
	}
}

func TestExplicitMissingConfigFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.toml")
	_, err := Load(LoadOptions{ConfigFile: path})
	if err == nil {
		t.Fatal("expected missing explicit config to fail")
	}
}

func TestDefaultConfigPathUsesDotAGInHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AGENTM_CONFIG", "")

	configFile, candidate, required, err := resolveConfigFile("")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, ".ag", "config.toml")
	if configFile != "" || candidate != want || required {
		t.Fatalf(
			"default config resolution = file %q candidate %q required %t; want empty, %q, false",
			configFile,
			candidate,
			required,
			want,
		)
	}
}

func TestDefaultRegistryConfigurationUsesDotAG(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AGENTM_CONFIG", "")
	loaded, err := Load(LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(loaded.Config.Registry.BackendURI)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Registry.Listen != "127.0.0.1:9090" ||
		parsed.Scheme != "file" ||
		parsed.Path != filepath.Join(home, ".ag", "registry") ||
		loaded.Config.Plugins.RegistryNamespace != "default" {
		t.Fatalf(
			"registry defaults = %#v, plugins = %#v",
			loaded.Config.Registry,
			loaded.Config.Plugins,
		)
	}
}

func TestDefaultGatewayConfigurationUsesDotAG(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AGENTM_CONFIG", "")
	loaded, err := Load(LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Gateway.Listen != "127.0.0.1:8080" ||
		loaded.Config.Gateway.Directory != filepath.Join(home, ".ag", "gateway") ||
		loaded.Config.Gateway.ReadHeaderTimeout != 5*time.Second ||
		loaded.Config.Gateway.IdleTimeout != time.Minute ||
		loaded.Config.Gateway.ShutdownTimeout != 10*time.Second {
		t.Fatalf("gateway defaults = %#v", loaded.Config.Gateway)
	}
}

func TestDefaultLoggingUsesFileWithoutConsole(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AGENTM_CONFIG", "")
	loaded, err := Load(LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Logging.File !=
		filepath.Join(home, ".ag", "logs", "ag.log") {
		t.Fatalf("log file = %q", loaded.Config.Logging.File)
	}
	if loaded.Config.Logging.Console {
		t.Fatal("console logging is enabled by default")
	}
}
