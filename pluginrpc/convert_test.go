package pluginrpc

import (
	"reflect"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestSchemaConversionNormalizesGoJSONValues(t *testing.T) {
	t.Parallel()

	parameters := map[string]any{
		"type":     "object",
		"required": []string{"path", "depth"},
		"properties": map[string]any{
			"path":  map[string]string{"type": "string"},
			"depth": map[string]any{"type": "integer", "maximum": int32(4)},
		},
	}
	converted, err := toProtoToolSpec(sdk.ToolSpec{
		Name:              "list_files",
		Parameters:        parameters,
		InterruptBehavior: sdk.ToolInterruptCancel,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := fromProtoToolSpec(converted)
	want := map[string]any{
		"type":     "object",
		"required": []any{"path", "depth"},
		"properties": map[string]any{
			"path":  map[string]any{"type": "string"},
			"depth": map[string]any{"type": "integer", "maximum": float64(4)},
		},
	}
	if !reflect.DeepEqual(got.Parameters, want) {
		t.Fatalf("parameters = %#v, want %#v", got.Parameters, want)
	}
	if got.InterruptBehavior != sdk.ToolInterruptCancel {
		t.Fatalf("interrupt behavior = %q", got.InterruptBehavior)
	}

	capability, err := toProtoCapabilitySpec(sdk.CapabilitySpec{
		Name:         "example",
		InputSchema:  parameters,
		OutputSchema: map[string]any{"required": []string{"result"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := capability.GetOutputSchema().AsMap()["required"]; !reflect.DeepEqual(got, []any{"result"}) {
		t.Fatalf("capability required = %#v", got)
	}
}

func TestManifestAPIRangeRoundTripsThroughProtocol(t *testing.T) {
	t.Parallel()
	manifest := sdk.Manifest{
		Name:          "range-aware",
		Version:       "1.2.3",
		Description:   "supports a protocol range",
		MinAPIVersion: sdk.APIVersion,
		MaxAPIVersion: sdk.APIVersion + 1,
		Requires:      []string{sdk.ToolResource("reader")},
		Registers:     []string{sdk.ProviderResource("model")},
	}
	roundTripped, err := fromProtoManifest(toProtoManifest(manifest))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTripped, manifest) {
		t.Fatalf("manifest round trip = %#v, want %#v", roundTripped, manifest)
	}
}

func TestInvocationRoundTripsThroughProtocol(t *testing.T) {
	t.Parallel()
	invocation := sdk.Invocation{
		ID:              "agent-call",
		RootID:          "root-call",
		ParentID:        "tool-call",
		GroupID:         "agent-group",
		SessionID:       "root-session",
		TargetSessionID: "child-session",
		ExecutionID:     "root-execution",
		Dependencies:    []string{"previous-agent"},
		Ordinal:         2,
	}
	roundTripped := fromProtoInvocation(
		toProtoInvocation(invocation),
	)
	if !reflect.DeepEqual(roundTripped, invocation) {
		t.Fatalf(
			"invocation round trip = %#v, want %#v",
			roundTripped,
			invocation,
		)
	}
	roundTripped.Dependencies[0] = "mutated"
	if invocation.Dependencies[0] != "previous-agent" {
		t.Fatal("invocation conversion aliased dependencies")
	}
	if converted := toProtoInvocation(sdk.Invocation{}); converted != nil {
		t.Fatalf("zero invocation converted to %#v", converted)
	}
}
