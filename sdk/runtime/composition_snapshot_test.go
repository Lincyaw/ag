package runtime

// Composition tests cover immutable published snapshots.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type mutableSpecTool struct {
	spec sdk.ToolSpec
}

func (tool *mutableSpecTool) Spec() sdk.ToolSpec {
	return tool.spec
}

func (*mutableSpecTool) Call(
	context.Context,
	json.RawMessage,
) (sdk.ToolResult, error) {
	return sdk.ToolResult{Content: "ok"}, nil
}

func TestBuiltinEventContractsAreIsolatedBetweenSnapshots(t *testing.T) {
	t.Parallel()
	first := initialSnapshot()
	contract := first.events[sdk.EventBeforeAgentStart]
	contract.contract.MutableFields[0] = "mutated"

	second := initialSnapshot()
	if field := second.events[sdk.EventBeforeAgentStart].contract.MutableFields[0]; field != "messages" {
		t.Fatalf("second snapshot mutable field = %q, want messages", field)
	}
}

func TestBuiltinTrajectoryEnvironmentEventScope(t *testing.T) {
	t.Parallel()
	tests := map[string]bool{
		sdk.EventBeforeProvider:    true,
		sdk.EventProviderOutcome:   false,
		sdk.EventTrajectoryAppend:  true,
		sdk.EventAgentEnd:          true,
		sdk.EventPluginMounted:     false,
		sdk.EventPluginUnmounted:   false,
		"plugin.unknown.execution": false,
	}
	for name, want := range tests {
		if got := builtinEventInTrajectoryEnvironment(name); got != want {
			t.Fatalf("builtinEventInTrajectoryEnvironment(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestBuiltinSessionExecutionEventScope(t *testing.T) {
	t.Parallel()
	tests := map[string]bool{
		sdk.EventBeforeProvider:    true,
		sdk.EventProviderOutcome:   true,
		sdk.EventTrajectoryAppend:  true,
		sdk.EventPluginMounted:     false,
		sdk.EventPluginUnmounted:   false,
		"plugin.unknown.execution": false,
	}
	for name, want := range tests {
		if got := builtinEventInSessionExecution(name); got != want {
			t.Fatalf("builtinEventInSessionExecution(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestMountedSpecsAreFrozenAndCatalogIsDefensive(t *testing.T) {
	t.Parallel()
	valueSchema := map[string]any{"type": "string"}
	tool := &mutableSpecTool{spec: sdk.ToolSpec{
		Name:        "mutable",
		Description: "mutates its spec after mount",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"value": valueSchema,
			},
		},
	}}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "mutable-spec",
			Version:     "1.0.0",
			Description: "verifies immutable registry specs",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("mutable")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(tool)
		},
	}
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}

	tool.spec.Name = "changed"
	valueSchema["type"] = "number"
	plugin.PluginManifest.Registers[0] = sdk.ToolResource("changed")
	catalog := runtime.Catalog()
	first := catalog.Tools
	if len(first) != 1 || first[0].Name != "mutable" {
		t.Fatalf("catalog tools after plugin mutation = %#v", first)
	}
	if got := catalog.Plugins[0].Registers; len(got) != 1 ||
		got[0] != sdk.ToolResource("mutable") {
		t.Fatalf("catalog manifest after plugin mutation = %#v", got)
	}
	firstValue := first[0].Parameters["properties"].(map[string]any)["value"].(map[string]any)
	if firstValue["type"] != "string" {
		t.Fatalf("frozen value schema = %#v", firstValue)
	}

	firstValue["type"] = "boolean"
	secondValue := runtime.Catalog().Tools[0].Parameters["properties"].(map[string]any)["value"].(map[string]any)
	if secondValue["type"] != "string" {
		t.Fatalf("catalog mutation leaked into snapshot: %#v", secondValue)
	}
}

func TestRegistrySnapshotCloneOwnsMutableSpecs(t *testing.T) {
	t.Parallel()
	original := initialSnapshot()
	original.tools["tool"] = ownedResource[sdk.AsyncTool, sdk.ToolSpec]{
		spec: sdk.ToolSpec{
			Name: "tool",
			Parameters: map[string]any{
				"properties": map[string]any{
					"value": map[string]any{"type": "string"},
				},
			},
		},
	}
	original.agents["agent"] = ownedAgent{
		spec: sdk.AgentSpec{Name: "agent", Tools: []string{"tool"}},
	}
	original.capabilities["capability"] = ownedResource[
		sdk.AsyncCapability,
		sdk.CapabilitySpec,
	]{
		spec: sdk.CapabilitySpec{
			Name:        "capability",
			InputSchema: map[string]any{"type": "object"},
		},
	}

	cloned := original.clone()
	cloned.tools["tool"].spec.Parameters["properties"].(map[string]any)["value"].(map[string]any)["type"] = "number"
	cloned.agents["agent"].spec.Tools[0] = "changed"
	cloned.capabilities["capability"].spec.InputSchema["type"] = "array"

	toolValue := original.tools["tool"].spec.Parameters["properties"].(map[string]any)["value"].(map[string]any)
	if toolValue["type"] != "string" {
		t.Fatalf("tool spec mutation leaked into original: %#v", toolValue)
	}
	if original.agents["agent"].spec.Tools[0] != "tool" {
		t.Fatalf("agent spec mutation leaked into original: %#v", original.agents["agent"].spec)
	}
	if original.capabilities["capability"].spec.InputSchema["type"] != "object" {
		t.Fatalf("capability spec mutation leaked into original: %#v", original.capabilities["capability"].spec)
	}
}
