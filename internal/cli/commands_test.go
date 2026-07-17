package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

type ambiguousPluginDriver struct {
	scheme string
}

func (driver ambiguousPluginDriver) Scheme() string { return driver.scheme }

func (ambiguousPluginDriver) Resolve(
	context.Context,
	sdk.PluginReference,
) (sdk.Source, error) {
	return nil, nil
}

func (driver ambiguousPluginDriver) Discover(
	_ context.Context,
	query sdk.DiscoveryQuery,
) ([]sdk.PluginReference, error) {
	return []sdk.PluginReference{{
		Name: query.Name,
		URI:  driver.scheme + "://plugin",
	}}, nil
}

func TestResolvePluginReportsAmbiguousDiscovery(t *testing.T) {
	t.Parallel()
	registry := sdk.NewPluginRegistry()
	if err := registry.RegisterDrivers(
		ambiguousPluginDriver{scheme: "alpha"},
		ambiguousPluginDriver{scheme: "zeta"},
	); err != nil {
		t.Fatal(err)
	}

	_, err := resolvePlugin(t.Context(), registry, "shared")
	if err == nil ||
		!strings.Contains(err.Error(), `plugin "shared" is ambiguous`) ||
		!strings.Contains(err.Error(), "alpha://plugin") ||
		!strings.Contains(err.Error(), "zeta://plugin") {
		t.Fatalf("resolve ambiguous plugin error = %v", err)
	}
}
