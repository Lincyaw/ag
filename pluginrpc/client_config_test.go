package pluginrpc

import (
	"crypto/tls"
	"testing"

	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc"
)

func TestClientConstructorsSnapshotConfig(t *testing.T) {
	t.Parallel()
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "original.example",
	}
	options := []grpc.DialOption{grpc.WithAuthority("original.example")}
	config := ClientConfig{
		TLSConfig:       tlsConfig,
		DialOptions:     options,
		RegistryURI:     "  grpcs://registry.example  ",
		MaxMessageBytes: 1024,
	}
	source, err := newSource("grpcs://plugin.example", config)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := newDriver("grpcs", config)
	if err != nil {
		t.Fatal(err)
	}

	tlsConfig.ServerName = "mutated.example"
	options[0] = nil
	for name, snapshot := range map[string]ClientConfig{
		"source": source.config,
		"driver": driver.config,
	} {
		if snapshot.TLSConfig == tlsConfig ||
			snapshot.TLSConfig.ServerName != "original.example" {
			t.Errorf("%s TLS config was not snapshotted", name)
		}
		if len(snapshot.DialOptions) != 1 || snapshot.DialOptions[0] == nil {
			t.Errorf("%s dial options were not snapshotted", name)
		}
		if snapshot.RegistryURI != "grpcs://registry.example" {
			t.Errorf("%s registry URI = %q", name, snapshot.RegistryURI)
		}
	}
}

func TestRegisterDriversRejectsInvalidConfigBeforeMutation(t *testing.T) {
	t.Parallel()
	registry := sdk.NewPluginRegistry()
	err := RegisterDrivers(registry, ClientConfig{MaxMessageBytes: -1})
	if err == nil || err.Error() != "RPC max message bytes must be positive" {
		t.Fatalf("register drivers error = %v", err)
	}
	if _, err := registry.Resolve(
		t.Context(),
		"grpc://127.0.0.1:1",
	); err == nil || err.Error() != `no plugin driver registered for scheme "grpc"` {
		t.Fatalf("registry mutation after failure: %v", err)
	}
}

func TestRegisterDriversRejectsConflictWithoutPartialMutation(t *testing.T) {
	t.Parallel()
	registry := sdk.NewPluginRegistry()
	occupied, err := NewDriver("grpcs", ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDrivers(occupied); err != nil {
		t.Fatal(err)
	}

	err = RegisterDrivers(registry, ClientConfig{})
	if err == nil || err.Error() != `plugin driver "grpcs" is already registered` {
		t.Fatalf("RegisterDrivers() error = %v", err)
	}
	if _, err := registry.Resolve(
		t.Context(),
		"grpc://127.0.0.1:1",
	); err == nil || err.Error() != `no plugin driver registered for scheme "grpc"` {
		t.Fatalf("registry was partially mutated: %v", err)
	}
}
