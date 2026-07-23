package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	gatewaymanager "github.com/lincyaw/ag/gateway/manager"
	appconfig "github.com/lincyaw/ag/internal/config"
)

func TestCLIImportsGatewayManagerForAutomaticStartup(t *testing.T) {
	config := managedGatewayTestConfig(t)
	const target = "grpc://127.0.0.1:19003"
	application := &app{
		probeGateway: func(context.Context, string) error { return nil },
		launchGateway: func(
			configPath string,
			readyPath string,
			_ string,
		) (<-chan error, error) {
			raw, err := os.ReadFile(configPath)
			if err != nil {
				return nil, err
			}
			var loaded map[string]any
			if err := json.Unmarshal(raw, &loaded); err != nil {
				return nil, err
			}
			workspace := loaded["workspace"].(map[string]any)
			if workspace["root"] != config.Workspace.Root {
				t.Fatalf("managed config workspace = %q", workspace["root"])
			}
			executable, executableSHA256, err := gatewaymanager.CurrentExecutableIdentity()
			if err != nil {
				return nil, err
			}
			ready, err := json.Marshal(gatewaymanager.Ready{
				Target:     target,
				Executable: executable, ExecutableSHA256: executableSHA256,
			})
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(readyPath, ready, 0o600); err != nil {
				return nil, err
			}
			return make(chan error), nil
		},
	}
	got, err := application.ensureManagedGateway(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if got != target {
		t.Fatalf("target = %q", got)
	}
}

func TestNormalizeAgentViewConfigMakesWorkspaceAbsolute(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	config, err := normalizeAgentViewConfig(appconfig.Config{
		Workspace: appconfig.Workspace{Root: "workspace"},
		Gateway:   appconfig.Gateway{Directory: "gateway"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Workspace.Root != filepath.Join(workingDirectory, "workspace") ||
		config.Gateway.Directory != filepath.Join(workingDirectory, "gateway") {
		t.Fatalf("normalized config = %#v", config)
	}
}

func managedGatewayTestConfig(t *testing.T) appconfig.Config {
	t.Helper()
	return appconfig.Config{
		Workspace: appconfig.Workspace{Root: t.TempDir()},
		Gateway: appconfig.Gateway{
			Directory: t.TempDir(), ShutdownTimeout: 100_000_000,
		},
	}
}
