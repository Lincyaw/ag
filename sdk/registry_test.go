package sdk

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type discoveryTestDriver struct {
	scheme     string
	references []PluginReference
	calls      *[]string
	discover   func(DiscoveryQuery)
	resolve    func(PluginReference) Source
}

func (driver discoveryTestDriver) Scheme() string { return driver.scheme }

func (driver discoveryTestDriver) Resolve(
	_ context.Context,
	reference PluginReference,
) (Source, error) {
	if driver.resolve != nil {
		return driver.resolve(reference), nil
	}
	return nil, nil
}

func (driver discoveryTestDriver) Discover(
	_ context.Context,
	query DiscoveryQuery,
) ([]PluginReference, error) {
	if driver.calls != nil {
		*driver.calls = append(*driver.calls, driver.scheme)
	}
	if driver.discover != nil {
		driver.discover(query)
	}
	return driver.references, nil
}

type discoveryTestSource struct{}

func (discoveryTestSource) Open(context.Context) (Connection, error) {
	return nil, nil
}

func (discoveryTestSource) String() string { return "test" }

func TestPluginDiscoveryPreservesAmbiguityAndOrdersDrivers(t *testing.T) {
	t.Parallel()
	var calls []string
	registry := NewPluginRegistry()
	for _, driver := range []PluginDriver{
		discoveryTestDriver{
			scheme: "zeta",
			references: []PluginReference{{
				Name: "shared", URI: "zeta://plugin", Description: "zeta source",
			}},
			calls: &calls,
		},
		discoveryTestDriver{
			scheme: "alpha",
			references: []PluginReference{{
				Name: "shared", URI: "alpha://plugin", Description: "alpha source",
			}},
			calls: &calls,
		},
	} {
		if err := registry.RegisterDrivers(driver); err != nil {
			t.Fatal(err)
		}
	}

	got, err := registry.Discover(t.Context(), DiscoveryQuery{
		Name: "shared", IncludeDrivers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []PluginDescriptor{
		{
			Name: "shared", URI: "alpha://plugin",
			Description: "alpha source", Scheme: "alpha",
		},
		{
			Name: "shared", URI: "zeta://plugin",
			Description: "zeta source", Scheme: "zeta",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("discovered = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(calls, []string{"alpha", "zeta"}) {
		t.Fatalf("driver call order = %v", calls)
	}
}

func TestPluginRegistryRegistersDriversAtomically(t *testing.T) {
	t.Parallel()
	registry := NewPluginRegistry()
	if err := registry.RegisterDrivers(discoveryTestDriver{scheme: "zeta"}); err != nil {
		t.Fatal(err)
	}
	err := registry.RegisterDrivers(
		discoveryTestDriver{scheme: "alpha"},
		discoveryTestDriver{scheme: "zeta"},
	)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("RegisterDrivers() error = %v", err)
	}
	if _, err := registry.Resolve(t.Context(), "alpha://plugin"); err == nil ||
		err.Error() != `no plugin driver registered for scheme "alpha"` {
		t.Fatalf("partially registered alpha driver: %v", err)
	}
}

func TestPluginRegistryRejectsInvalidReferencesConsistently(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		reference PluginReference
		want      string
	}{
		{
			name:      "invalid name",
			reference: PluginReference{Name: "invalid name", URI: "test://plugin"},
			want:      "plugin reference name",
		},
		{
			name: "source and URI",
			reference: PluginReference{
				Name: "plugin", URI: "test://plugin", Source: discoveryTestSource{},
			},
			want: "exactly one of Source or URI",
		},
		{
			name:      "missing source and URI",
			reference: PluginReference{Name: "plugin"},
			want:      "exactly one of Source or URI",
		},
		{
			name:      "URI without scheme",
			reference: PluginReference{Name: "plugin", URI: "plugin"},
			want:      "has no scheme",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			registerErr := NewPluginRegistry().Register(test.reference)
			if registerErr == nil || !strings.Contains(registerErr.Error(), test.want) {
				t.Fatalf("Register() error = %v, want containing %q", registerErr, test.want)
			}

			registry := NewPluginRegistry()
			err := registry.RegisterDrivers(discoveryTestDriver{
				scheme: "test", references: []PluginReference{test.reference},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, discoverErr := registry.Discover(t.Context(), DiscoveryQuery{
				IncludeDrivers: true,
			})
			if discoverErr == nil || !strings.Contains(discoverErr.Error(), test.want) {
				t.Fatalf(
					"Discover() error = %v, want containing %q",
					discoverErr,
					test.want,
				)
			}
		})
	}
}

func TestPluginRegistrySnapshotsNormalizedReference(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"environment": "test"}
	registry := NewPluginRegistry()
	if err := registry.Register(PluginReference{
		Name: "plugin", URI: "  test://plugin  ", Labels: labels,
	}); err != nil {
		t.Fatal(err)
	}
	labels["environment"] = "changed"

	got, err := registry.Discover(t.Context(), DiscoveryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	want := []PluginDescriptor{{
		Name: "plugin", URI: "test://plugin",
		Labels: map[string]string{"environment": "test"}, Scheme: "test",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("discovered = %#v, want %#v", got, want)
	}
}

func TestPluginDiscoveryIsolatesMutableQueriesBetweenDrivers(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"environment": "production"}
	var secondDriverLabel string
	registry := NewPluginRegistry()
	for _, driver := range []PluginDriver{
		discoveryTestDriver{
			scheme: "alpha",
			references: []PluginReference{{
				Name: "alpha", URI: "alpha://plugin",
				Labels: map[string]string{"environment": "production"},
			}},
			discover: func(query DiscoveryQuery) {
				query.Labels["environment"] = "changed"
			},
		},
		discoveryTestDriver{
			scheme: "zeta",
			references: []PluginReference{{
				Name: "zeta", URI: "zeta://plugin",
				Labels: map[string]string{"environment": "production"},
			}},
			discover: func(query DiscoveryQuery) {
				secondDriverLabel = query.Labels["environment"]
			},
		},
	} {
		if err := registry.RegisterDrivers(driver); err != nil {
			t.Fatal(err)
		}
	}

	got, err := registry.Discover(t.Context(), DiscoveryQuery{
		Labels: labels, IncludeDrivers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("discovered = %#v, want both matching drivers", got)
	}
	if secondDriverLabel != "production" {
		t.Fatalf("second driver label = %q, want isolated query", secondDriverLabel)
	}
	if labels["environment"] != "production" {
		t.Fatalf("caller labels were mutated: %#v", labels)
	}
}

func TestPluginRegistryFreezesDriverScheme(t *testing.T) {
	t.Parallel()
	var calls []string
	alpha := &discoveryTestDriver{
		scheme: "alpha",
		references: []PluginReference{{
			Name: "alpha", URI: "alpha://plugin",
		}},
		discover: func(DiscoveryQuery) {
			calls = append(calls, "alpha")
		},
	}
	beta := &discoveryTestDriver{
		scheme: "beta",
		references: []PluginReference{{
			Name: "beta", URI: "beta://plugin",
		}},
		discover: func(DiscoveryQuery) {
			calls = append(calls, "beta")
		},
	}
	registry := NewPluginRegistry()
	for _, driver := range []PluginDriver{alpha, beta} {
		if err := registry.RegisterDrivers(driver); err != nil {
			t.Fatal(err)
		}
	}
	alpha.scheme = "zeta"

	if _, err := registry.Discover(t.Context(), DiscoveryQuery{
		IncludeDrivers: true,
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"alpha", "beta"}) {
		t.Fatalf("driver call order = %v, want frozen registration order", calls)
	}
}

func TestPluginResolveIsolatesRegisteredLabels(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"environment": "production"}
	registry := NewPluginRegistry()
	if err := registry.RegisterDrivers(discoveryTestDriver{
		scheme: "test",
		resolve: func(reference PluginReference) Source {
			reference.Labels["environment"] = "changed"
			return discoveryTestSource{}
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(PluginReference{
		Name: "plugin", URI: "test://plugin", Labels: labels,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := registry.Resolve(t.Context(), "plugin"); err != nil {
		t.Fatal(err)
	}
	discovered, err := registry.Discover(t.Context(), DiscoveryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(discovered) != 1 ||
		discovered[0].Labels["environment"] != "production" ||
		labels["environment"] != "production" {
		t.Fatalf("registered labels changed through driver: %#v", discovered)
	}
}
