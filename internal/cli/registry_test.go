package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

func TestRegistryServeCommandPublishesReadyDirectory(t *testing.T) {
	t.Parallel()
	stdoutReader, stdoutWriter := io.Pipe()
	defer stdoutReader.Close()
	var stderr bytes.Buffer
	command := New(stdoutWriter, &stderr, "test-version")
	backendURI := (&url.URL{
		Scheme: "file",
		Path:   filepath.Join(t.TempDir(), "registry"),
	}).String()
	command.SetArgs([]string{
		"registry", "serve",
		"--listen", "127.0.0.1:0",
		"--registry-backend", backendURI,
		"--log-format", "text",
		"-o", "json",
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- command.ExecuteContext(ctx)
		_ = stdoutWriter.Close()
	}()

	var ready registryReady
	if err := json.NewDecoder(stdoutReader).Decode(&ready); err != nil {
		cancel()
		t.Fatalf("decode ready record: %v; stderr: %s", err, stderr.String())
	}
	if ready.URI == "" || ready.Backend != backendURI ||
		!ready.Capabilities.Durable || ready.PID == 0 {
		cancel()
		t.Fatalf("ready = %#v", ready)
	}

	client, err := pluginrpc.NewRegistryClient(
		context.Background(),
		ready.URI,
		pluginrpc.ClientConfig{},
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	lease, err := client.Register(
		context.Background(),
		registry.PluginRegistration{
			Namespace:  registry.DefaultNamespace,
			Name:       "cli-registry-test",
			InstanceID: "node-a",
			URI:        "grpc://127.0.0.1:9999",
			Manifest: sdk.Manifest{
				Name:        "cli-registry-test",
				Version:     "1.0.0",
				Description: "CLI registry integration test",
				APIVersion:  sdk.APIVersion,
			},
		},
		registry.LeaseOptions{TTL: time.Minute},
	)
	if err != nil {
		_ = client.Close(context.Background())
		cancel()
		t.Fatal(err)
	}
	page, err := client.List(
		context.Background(),
		registry.DiscoveryQuery{Name: "cli-registry-test"},
		registry.PageRequest{},
	)
	if err != nil || len(page.Items) != 1 ||
		page.Items[0].InstanceID != lease.Key.InstanceID {
		_ = client.Close(context.Background())
		cancel()
		t.Fatalf("registry page = %#v, %v", page, err)
	}
	var discoverStdout, discoverStderr bytes.Buffer
	discover := New(
		&discoverStdout,
		&discoverStderr,
		"test-version",
	)
	discover.SetArgs([]string{
		"plugin", "discover",
		"--registry-uri", ready.URI,
		"--name", "cli-registry-test",
		"--instance-id", "node-a",
		"-o", "json",
	})
	if err := discover.ExecuteContext(context.Background()); err != nil {
		_ = client.Close(context.Background())
		cancel()
		t.Fatalf(
			"discover command: %v; stderr: %s",
			err,
			discoverStderr.String(),
		)
	}
	var discovered []pluginDiscovery
	if err := json.Unmarshal(discoverStdout.Bytes(), &discovered); err != nil {
		_ = client.Close(context.Background())
		cancel()
		t.Fatalf(
			"decode discovery %q: %v",
			discoverStdout.String(),
			err,
		)
	}
	if len(discovered) != 1 ||
		discovered[0].Namespace != registry.DefaultNamespace ||
		discovered[0].InstanceID != "node-a" ||
		discovered[0].Scheme != "grpc" {
		_ = client.Close(context.Background())
		cancel()
		t.Fatalf("discovered = %#v", discovered)
	}
	selectionCatalog := sdk.NewPluginRegistry()
	if err := pluginrpc.RegisterDrivers(
		selectionCatalog,
		pluginrpc.ClientConfig{},
	); err != nil {
		_ = client.Close(context.Background())
		cancel()
		t.Fatal(err)
	}
	selected, err := resolvePluginSelection(
		context.Background(),
		selectionCatalog,
		appconfig.Plugins{
			RegistryURI:       ready.URI,
			RegistryNamespace: registry.DefaultNamespace,
		},
		"cli-registry-test@node-a",
	)
	if err != nil || selected.String() != "grpc://127.0.0.1:9999" {
		_ = client.Close(context.Background())
		cancel()
		t.Fatalf("selected source = %v, %v", selected, err)
	}
	catalog, names, err := buildRegistry(
		context.Background(),
		appconfig.Config{
			Plugins: appconfig.Plugins{
				Remote:            []string{"cli-registry-test@node-a"},
				RegistryURI:       ready.URI,
				RegistryNamespace: registry.DefaultNamespace,
			},
		},
		slog.Default(),
		nil,
		nil,
	)
	if err != nil {
		_ = client.Close(context.Background())
		cancel()
		t.Fatal(err)
	}
	descriptors, err := catalog.Discover(
		context.Background(),
		sdk.DiscoveryQuery{},
	)
	if err != nil || len(names) != 1 ||
		names[0] != "cli-registry-test" ||
		len(descriptors) != 1 ||
		descriptors[0].URI != "grpc://127.0.0.1:9999" {
		_ = client.Close(context.Background())
		cancel()
		t.Fatalf(
			"selected catalog names=%#v descriptors=%#v err=%v",
			names,
			descriptors,
			err,
		)
	}
	if err := client.Close(context.Background()); err != nil {
		cancel()
		t.Fatal(err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("registry command: %v; stderr: %s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("registry command did not stop")
	}
}
