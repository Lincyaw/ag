package bootstrap

import (
	"testing"

	appconfig "github.com/lincyaw/ag/internal/config"
)

func TestGatewayPluginDirectoryEmbedsConfiguredBackend(t *testing.T) {
	directory, location, err := OpenGatewayPluginDirectory(
		t.Context(),
		appconfig.Config{
			Registry: appconfig.Registry{BackendURI: "memory://local"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.Close(t.Context()); err != nil {
			t.Errorf("close embedded registry: %v", err)
		}
	})
	if location != "memory://local" || directory.String() != location {
		t.Fatalf("location=%q directory=%q", location, directory.String())
	}
}
