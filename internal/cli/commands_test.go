package cli

import (
	"context"
	"strings"
	"testing"
	"time"

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

	_, err := selectPluginInstance(
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
	selected, err := selectPluginInstance(
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
