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
		Name: "list_files", Parameters: parameters,
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
