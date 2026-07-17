package sdk

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type testPluginDriver struct {
	scheme     string
	references []PluginReference
	calls      *[]string
	discover   func(DiscoveryQuery)
	resolve    func(PluginReference) Source
}

func (driver testPluginDriver) Scheme() string { return driver.scheme }

func (driver testPluginDriver) Resolve(
	_ context.Context,
	reference PluginReference,
) (Source, error) {
	if driver.resolve != nil {
		return driver.resolve(reference), nil
	}
	return nil, nil
}

func (driver testPluginDriver) Discover(
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

type testPluginSource struct{}

func (testPluginSource) Open(context.Context) (Connection, error) {
	return nil, nil
}

func (testPluginSource) String() string { return "test" }

func TestPluginDiscoveryOrdersDriversAndIsolatesQueries(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"environment": "production"}
	var calls []string
	var secondDriverLabel string
	registry := NewPluginRegistry()
	for _, driver := range []PluginDriver{
		testPluginDriver{
			scheme: "zeta",
			references: []PluginReference{{
				Name: "shared", URI: "zeta://plugin",
				Labels: map[string]string{"environment": "production"},
			}},
			calls: &calls,
			discover: func(query DiscoveryQuery) {
				secondDriverLabel = query.Labels["environment"]
			},
		},
		testPluginDriver{
			scheme: "alpha",
			references: []PluginReference{{
				Name: "shared", URI: "alpha://plugin",
				Labels: map[string]string{"environment": "production"},
			}},
			calls: &calls,
			discover: func(query DiscoveryQuery) {
				query.Labels["environment"] = "changed"
			},
		},
	} {
		if err := registry.RegisterDrivers(driver); err != nil {
			t.Fatal(err)
		}
	}

	got, err := registry.Discover(t.Context(), DiscoveryQuery{
		Name: "shared", Labels: labels, IncludeDrivers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 ||
		got[0].URI != "alpha://plugin" ||
		got[1].URI != "zeta://plugin" {
		t.Fatalf("discovered = %#v", got)
	}
	if !reflect.DeepEqual(calls, []string{"alpha", "zeta"}) {
		t.Fatalf("driver call order = %v", calls)
	}
	if secondDriverLabel != "production" ||
		labels["environment"] != "production" {
		t.Fatalf(
			"mutable query escaped boundary: second=%q caller=%#v",
			secondDriverLabel,
			labels,
		)
	}
}

func TestPluginRegistryRegistersDriversAtomically(t *testing.T) {
	t.Parallel()
	registry := NewPluginRegistry()
	if err := registry.RegisterDrivers(testPluginDriver{scheme: "zeta"}); err != nil {
		t.Fatal(err)
	}
	err := registry.RegisterDrivers(
		testPluginDriver{scheme: "alpha"},
		testPluginDriver{scheme: "zeta"},
	)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("RegisterDrivers() error = %v", err)
	}
	if _, err := registry.Resolve(t.Context(), "alpha://plugin"); err == nil ||
		err.Error() != `no plugin driver registered for scheme "alpha"` {
		t.Fatalf("partially registered alpha driver: %v", err)
	}
}

func TestPluginRegistryRejectsInvalidReferences(t *testing.T) {
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
				Name: "plugin", URI: "test://plugin", Source: testPluginSource{},
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
			if err := registry.RegisterDrivers(testPluginDriver{
				scheme: "test", references: []PluginReference{test.reference},
			}); err != nil {
				t.Fatal(err)
			}
			_, discoverErr := registry.Discover(t.Context(), DiscoveryQuery{
				IncludeDrivers: true,
			})
			if discoverErr == nil ||
				!strings.Contains(discoverErr.Error(), test.want) {
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

func TestPluginRegistryFreezesDriverScheme(t *testing.T) {
	t.Parallel()
	driver := &testPluginDriver{
		scheme: "alpha",
		resolve: func(PluginReference) Source {
			return testPluginSource{}
		},
	}
	registry := NewPluginRegistry()
	if err := registry.RegisterDrivers(driver); err != nil {
		t.Fatal(err)
	}
	driver.scheme = "zeta"
	if _, err := registry.Resolve(t.Context(), "alpha://plugin"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(t.Context(), "zeta://plugin"); err == nil ||
		err.Error() != `no plugin driver registered for scheme "zeta"` {
		t.Fatalf("mutated driver scheme was registered: %v", err)
	}
}

func TestPluginResolveIsolatesRegisteredLabels(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"environment": "production"}
	registry := NewPluginRegistry()
	if err := registry.RegisterDrivers(testPluginDriver{
		scheme: "test",
		resolve: func(reference PluginReference) Source {
			reference.Labels["environment"] = "changed"
			return testPluginSource{}
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
	listed, err := registry.Discover(t.Context(), DiscoveryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 ||
		listed[0].Labels["environment"] != "production" ||
		labels["environment"] != "production" {
		t.Fatalf("registered labels changed through driver: %#v", listed)
	}
}
