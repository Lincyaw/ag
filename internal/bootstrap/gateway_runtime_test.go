package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/lincyaw/ag/gateway"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestGatewayRuntimeBuilderSessionBindingOverridesLocalPlugin(t *testing.T) {
	t.Parallel()
	remote := fileOverridePlugin()
	uri := serveBootstrapPlugin(t, remote)
	backend := sdkstorage.NewMemoryStateBackend()
	runtime, err := GatewayRuntimeBuilder(
		appconfig.Config{
			Workspace: appconfig.Workspace{
				Enabled: true,
				Root:    t.TempDir(),
			},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		nil,
		"test",
	)(
		t.Context(),
		gateway.RuntimeBuildSpec{
			Plugins: []gateway.PluginBinding{{
				Name:       "file",
				InstanceID: "remote",
				URI:        uri,
				Manifest:   remote.Manifest(),
			}},
		},
		backend,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})

	catalog := runtime.Catalog()
	if len(catalog.Plugins) != 1 ||
		catalog.Plugins[0].Name != "file" ||
		catalog.Plugins[0].Version != "9.9.0" {
		t.Fatalf("plugins = %#v", catalog.Plugins)
	}
	if !catalogHasTool(catalog, "remote_marker") {
		t.Fatalf("remote override tool missing: %#v", catalog.Tools)
	}
	if catalogHasTool(catalog, "read_file") {
		t.Fatalf("local file plugin was mounted despite session override: %#v", catalog.Tools)
	}
}

func fileOverridePlugin() sdk.Plugin {
	return sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "file",
			Version:     "9.9.0",
			Description: "remote file override for gateway composition tests",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("remote_marker")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(markerTool{})
		},
	}
}

type markerTool struct{}

func (markerTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "remote_marker",
		Description: "marks the remote file override",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (markerTool) Call(context.Context, json.RawMessage) (sdk.ToolResult, error) {
	return sdk.ToolResult{Content: "remote"}, nil
}

func serveBootstrapPlugin(t *testing.T, plugin sdk.Plugin) string {
	t.Helper()
	adapter, err := pluginrpc.NewServer(t.Context(), pluginrpc.ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
		InboxPoll:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := pluginrpc.NewGRPCServer(adapter, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("serve plugin: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("plugin server did not stop")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := adapter.Close(ctx); err != nil {
			t.Errorf("close plugin adapter: %v", err)
		}
	})
	return "grpc://" + listener.Addr().String()
}

func catalogHasTool(catalog agentruntime.CatalogSnapshot, name string) bool {
	for _, tool := range catalog.Tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}
