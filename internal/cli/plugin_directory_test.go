package cli

import (
	"context"
	"strings"
	"testing"

	appconfig "github.com/lincyaw/ag/internal/config"
)

func TestOpenPluginDirectoryDoesNotEchoCredentials(t *testing.T) {
	t.Parallel()
	_, err := openPluginDirectory(
		context.Background(),
		appconfig.Plugins{
			RegistryURI: "grpc://user:secret@registry.example",
		},
	)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("registry connection error = %v", err)
	}
}
