package sdk

// Agent tests cover declarative agent contracts and defensive ownership.

import (
	"encoding/json"
	"testing"
)

func TestAgentSpecToolsJSONPreservesInheritanceBoundary(t *testing.T) {
	t.Parallel()
	inherited, err := json.Marshal(AgentSpec{Name: "inherited"})
	if err != nil {
		t.Fatal(err)
	}
	none, err := json.Marshal(AgentSpec{
		Name:  "none",
		Tools: []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(inherited) == string(none) {
		t.Fatalf(
			"inherit and empty allowlist encoded identically: %s",
			inherited,
		)
	}
	var inheritedSpec AgentSpec
	if err := json.Unmarshal(inherited, &inheritedSpec); err != nil {
		t.Fatal(err)
	}
	if inheritedSpec.Tools != nil {
		t.Fatalf("inherited tools = %#v, want nil", inheritedSpec.Tools)
	}
	var noneSpec AgentSpec
	if err := json.Unmarshal(none, &noneSpec); err != nil {
		t.Fatal(err)
	}
	if noneSpec.Tools == nil || len(noneSpec.Tools) != 0 {
		t.Fatalf(
			"empty tools = %#v, want non-nil empty allowlist",
			noneSpec.Tools,
		)
	}
}
