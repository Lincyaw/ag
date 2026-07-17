package cli

import (
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"

	appconfig "github.com/lincyaw/ag/internal/config"
)

type configOutput struct {
	File   string           `json:"file"`
	Config appconfig.Config `json:"config"`
}

func (application *app) writeConfig(loaded appconfig.Loaded) error {
	config := configForDisplay(loaded.Config)
	value := configOutput{File: loaded.File, Config: config}
	return application.render(value, func(writer io.Writer) error {
		source := loaded.Path()
		if loaded.File == "" {
			source += " (defaults; file not present)"
		}
		if _, err := fmt.Fprintf(writer, "Effective configuration\n\n"); err != nil {
			return err
		}
		if err := writeSection(writer, "Source",
			[2]string{"File", source},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Agent",
			[2]string{"Provider", config.Agent.Provider},
			[2]string{"Max turns", fmt.Sprint(config.Agent.MaxTurns)},
			[2]string{"Timeout", config.Agent.Timeout.String()},
			[2]string{"System", fmt.Sprintf("%q", config.Agent.System)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "OpenAI",
			[2]string{"Enabled", yesNo(config.OpenAI.Enabled)},
			[2]string{"Model", config.OpenAI.Model},
			[2]string{"API key", config.OpenAI.APIKey},
			[2]string{"Base URL", emptyAs(config.OpenAI.BaseURL, "provider default")},
			[2]string{"Max retries", fmt.Sprint(config.OpenAI.MaxRetries)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Workspace",
			[2]string{"Enabled", yesNo(config.Workspace.Enabled)},
			[2]string{"Root", config.Workspace.Root},
			[2]string{"Writes", yesNo(config.Workspace.EnableWrite)},
			[2]string{"Max read bytes", fmt.Sprint(config.Workspace.MaxReadBytes)},
			[2]string{"Max write bytes", fmt.Sprint(config.Workspace.MaxWriteBytes)},
			[2]string{"Max entries", fmt.Sprint(config.Workspace.MaxEntries)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Bash",
			[2]string{"Enabled", yesNo(config.Bash.Enabled)},
			[2]string{"Shell", config.Bash.Shell},
			[2]string{"Default timeout", config.Bash.DefaultTimeout.String()},
			[2]string{"Max timeout", config.Bash.MaxTimeout.String()},
			[2]string{"Max output bytes", fmt.Sprint(config.Bash.MaxOutputBytes)},
			[2]string{"Environment", listOrNone(config.Bash.Environment)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Plugins",
			[2]string{"Remote", listOrNone(config.Plugins.Remote)},
			[2]string{"Registry", emptyAs(config.Plugins.RegistryURI, "none")},
			[2]string{"Registry namespace", config.Plugins.RegistryNamespace},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Registry service",
			[2]string{"Listen", config.Registry.Listen},
			[2]string{"Advertise URI", emptyAs(config.Registry.AdvertiseURI, "derived")},
			[2]string{"Backend URI", config.Registry.BackendURI},
			[2]string{"TLS", yesNo(config.Registry.TLSCertFile != "")},
			[2]string{"Max message bytes", fmt.Sprint(config.Registry.MaxMessageBytes)},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "Gateway service",
			[2]string{"Listen", config.Gateway.Listen},
			[2]string{"Directory", config.Gateway.Directory},
			[2]string{
				"Read header timeout",
				config.Gateway.ReadHeaderTimeout.String(),
			},
			[2]string{"Idle timeout", config.Gateway.IdleTimeout.String()},
			[2]string{
				"Shutdown timeout",
				config.Gateway.ShutdownTimeout.String(),
			},
		); err != nil {
			return err
		}
		if err := writeSection(writer, "State",
			[2]string{"Backend URI", emptyAs(config.State.BackendURI, "file")},
			[2]string{"Directory", config.State.Directory},
			[2]string{"Namespace", emptyAs(config.State.Namespace, "default")},
		); err != nil {
			return err
		}
		return writeSection(writer, "Diagnostics",
			[2]string{"OpenTelemetry", yesNo(config.Observability.Enabled)},
			[2]string{"Log level", config.Logging.Level},
			[2]string{"Log format", config.Logging.Format},
			[2]string{"Log file", config.Logging.File},
			[2]string{"Log console", yesNo(config.Logging.Console)},
		)
	})
}

func configForDisplay(config appconfig.Config) appconfig.Config {
	config.OpenAI.APIKey = secretForDisplay(config.OpenAI.APIKey)
	config.OpenAI.BaseURL = uriForDisplay(config.OpenAI.BaseURL)
	config.Plugins.Remote = slices.Clone(config.Plugins.Remote)
	for index := range config.Plugins.Remote {
		config.Plugins.Remote[index] = pluginReferenceForDisplay(
			config.Plugins.Remote[index],
		)
	}
	config.Plugins.RegistryURI = uriForDisplay(
		config.Plugins.RegistryURI,
	)
	config.Registry.AdvertiseURI = uriForDisplay(
		config.Registry.AdvertiseURI,
	)
	config.Registry.BackendURI = uriForDisplay(config.Registry.BackendURI)
	config.State.BackendURI = uriForDisplay(config.State.BackendURI)
	return config
}

func secretForDisplay(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "<unset>"
	}
	return "<set>"
}

func pluginReferenceForDisplay(raw string) string {
	value := strings.TrimSpace(raw)
	name, uri, found := strings.Cut(value, "=")
	if !found {
		return value
	}
	return name + "=" + uriForDisplay(uri)
}

func uriForDisplay(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" {
		return "<invalid URI>"
	}
	query := parsed.Query()
	for key, values := range query {
		if !sensitiveQueryParameter(key) {
			continue
		}
		for index := range values {
			values[index] = "<redacted>"
		}
		query[key] = values
	}
	parsed.RawQuery = query.Encode()
	return parsed.Redacted()
}

func sensitiveQueryParameter(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "").Replace(
		strings.ToLower(strings.TrimSpace(key)),
	)
	switch normalized {
	case "password", "passwd", "pass", "token", "apikey", "secret",
		"clientsecret", "accesstoken", "refreshtoken", "credential":
		return true
	default:
		return false
	}
}
