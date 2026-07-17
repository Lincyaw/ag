package plugincontract

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestRegistrarsExposeAgentCapabilityExplicitly(t *testing.T) {
	base := NewRegistrar()
	if _, ok := any(base).(sdk.AgentRegistrar); ok {
		t.Fatal("base registrar unexpectedly supports same-process agents")
	}
	agents := NewAgentRegistrar()
	if _, ok := any(agents).(sdk.AgentRegistrar); !ok {
		t.Fatal("agent registrar does not implement sdk.AgentRegistrar")
	}
	if err := agents.RegisterAgent(sdk.AgentSpec{
		Name:        "worker",
		Description: "same-process worker",
	}); err != nil {
		t.Fatal(err)
	}
	if got := agents.Resources(); !slices.Equal(
		got,
		[]string{sdk.AgentResource("worker")},
	) {
		t.Fatalf("agent resources = %v", got)
	}
}

func TestRegistrarPreservesHookRegistrationOrder(t *testing.T) {
	registrar := NewRegistrar()
	for _, name := range []string{"second", "first"} {
		err := registrar.RegisterHook(sdk.HookFunc{
			HookSpec: sdk.HookSpec{
				Name:  name,
				Event: "example.event",
			},
			HandleFunc: func(
				context.Context,
				sdk.Event,
			) (sdk.Effect, error) {
				return sdk.Effect{}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(registrar.HookOrder, []string{"second", "first"}) {
		t.Fatalf("hook order = %v", registrar.HookOrder)
	}
	if got := registrar.Resources(); !slices.Equal(got, []string{
		sdk.HookResource("first"),
		sdk.HookResource("second"),
	}) {
		t.Fatalf("sorted resources = %v", got)
	}
}

func TestRegistrarRejectsDuplicateEventFields(t *testing.T) {
	err := NewRegistrar().RegisterEvent(sdk.EventContract{
		Name:          "mutable-event",
		MutableFields: []string{"first", "second", "first"},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate mutable fields") {
		t.Fatalf("validation error = %v", err)
	}
}
