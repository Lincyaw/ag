package cli

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/bootstrap"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

func TestSelectPluginInstanceRequiresExplicitReplica(t *testing.T) {
	t.Parallel()
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	t.Cleanup(func() { _ = directory.Close(context.Background()) })
	for _, instanceID := range []string{"node-b", "node-a"} {
		if _, err := directory.Register(
			context.Background(),
			registry.PluginRegistration{
				Namespace:  registry.DefaultNamespace,
				Name:       "shared",
				InstanceID: instanceID,
				URI:        "grpc://127.0.0.1:9000",
				Manifest: sdk.Manifest{
					Name:        "shared",
					Version:     "1.0.0",
					Description: "shared test plugin",
					APIVersion:  sdk.APIVersion,
				},
			},
			registry.LeaseOptions{TTL: time.Minute},
		); err != nil {
			t.Fatal(err)
		}
	}

	_, err := bootstrap.SelectPluginInstance(
		context.Background(),
		directory,
		registry.DefaultNamespace,
		"shared",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "shared@node-a=") ||
		!strings.Contains(err.Error(), "shared@node-b=") {
		t.Fatalf("ambiguous replica error = %v", err)
	}
	selected, err := bootstrap.SelectPluginInstance(
		context.Background(),
		directory,
		registry.DefaultNamespace,
		"shared@node-b",
	)
	if err != nil {
		t.Fatal(err)
	}
	if selected.InstanceID != "node-b" {
		t.Fatalf("selected = %#v", selected)
	}
}

func TestGatewayConfiguredToolsMatchesRuntimeComposition(t *testing.T) {
	got := gatewayConfiguredTools(appconfig.Config{
		Workspace: appconfig.Workspace{Enabled: true, EnableWrite: true},
		Tree:      appconfig.Tree{Enabled: true},
		Bash:      appconfig.Bash{Enabled: true},
		HostFS:    appconfig.HostFS{Enabled: true},
	})
	want := []string{
		"ask_user", "bash", "edit_file", "hostfs_read_file", "hostfs_tree",
		"read_file", "search_files", "workspace_tree", "write_file",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gateway tools = %#v, want %#v", got, want)
	}
}

func TestTrajectoryShowUsesTUIOnlyForInteractiveCurrentView(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		output        string
		branchHead    string
		inputTerminal bool
		want          bool
	}{
		{name: "terminal text", output: outputText, inputTerminal: true, want: true},
		{name: "json", output: outputJSON, inputTerminal: true},
		{name: "historical branch", output: outputText, branchHead: "checkpoint", inputTerminal: true, want: true},
		{name: "piped input", output: outputText},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := trajectoryShowUsesTUI(
				test.output, test.branchHead, test.inputTerminal,
			)
			if got != test.want {
				t.Fatalf("trajectoryShowUsesTUI() = %v, want %v", got, test.want)
			}
		})
	}
}
