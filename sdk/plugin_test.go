package sdk

import (
	"strings"
	"testing"
)

func TestManifestAcceptsCompatibleAPIRange(t *testing.T) {
	t.Parallel()
	manifest := Manifest{
		Name:          "compatible",
		Version:       "1.0.0",
		Description:   "supports an SDK API range",
		MinAPIVersion: APIVersion,
		MaxAPIVersion: APIVersion + 1,
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	minimum, maximum := manifest.APIRange()
	if minimum != APIVersion || maximum != APIVersion+1 {
		t.Fatalf("API range = %d..%d", minimum, maximum)
	}
}

func TestManifestRejectsIncompatibleOrInvalidAPIRange(t *testing.T) {
	t.Parallel()
	for _, manifest := range []Manifest{
		{
			Name:          "future-only",
			Version:       "1.0.0",
			Description:   "requires a future API",
			MinAPIVersion: APIVersion + 1,
			MaxAPIVersion: APIVersion + 2,
		},
		{
			Name:          "reversed-range",
			Version:       "1.0.0",
			Description:   "declares an invalid API range",
			MinAPIVersion: APIVersion + 1,
			MaxAPIVersion: APIVersion,
		},
	} {
		if err := manifest.Validate(); err == nil ||
			!strings.Contains(err.Error(), "API version") {
			t.Fatalf("manifest %#v validation error = %v", manifest, err)
		}
	}
}

func TestManifestValidationDoesNotMutateResourceSlices(t *testing.T) {
	t.Parallel()
	requires := []string{ToolResource("reader"), "untouched"}
	manifest := Manifest{
		Name:        "non-mutating",
		Version:     "1.0.0",
		Description: "keeps caller-owned resource slices unchanged",
		APIVersion:  APIVersion,
		Requires:    requires[:1],
		Conflicts:   []string{ToolResource("writer")},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	if requires[1] != "untouched" {
		t.Fatalf("validation changed caller-owned capacity to %q", requires[1])
	}
}

func TestManifestValidatesCommandDeclarations(t *testing.T) {
	t.Parallel()
	command := CommandSpec{
		Name:        "review",
		Description: "Review a target",
		Instruction: "Review $ARGUMENTS",
	}
	manifest := Manifest{
		Name:        "commands",
		Version:     "1.0.0",
		Description: "contributes commands",
		APIVersion:  APIVersion,
		Registers:   []string{CommandResource(command.Name)},
		Commands:    []CommandSpec{command},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	manifest.Registers = nil
	if err := manifest.Validate(); err == nil ||
		!strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing command resource validation error = %v", err)
	}
}

func TestResourceRevisionEncodingIsStable(t *testing.T) {
	t.Parallel()
	revision := ResourceRevision(
		Manifest{Name: "plugin", Version: "1.0.0"},
		"tool",
		"echo",
		nil,
	)
	const expected = "eaa8a305bf12ee42c5247855690f25f434fd98d4559b1be6dde0ed9a3b7bd47e"
	if revision != expected {
		t.Fatalf("resource revision = %q, want %q", revision, expected)
	}
}
