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
