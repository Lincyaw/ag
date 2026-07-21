package cli

import (
	"bytes"
	"testing"
)

func TestGatewayIsNotAPublicCommand(t *testing.T) {
	command := New(&bytes.Buffer{}, &bytes.Buffer{}, "test")
	for _, child := range command.Commands() {
		if child.Name() == "gateway" || child.Name() == "tui" {
			t.Fatalf("private command %q is exposed", child.Name())
		}
	}
	found, _, _ := command.Find([]string{"gateway"})
	if found != command {
		t.Fatalf("ag gateway resolved to %q", found.CommandPath())
	}
}

func TestRunExposesOnlyTrajectoryViewOptions(t *testing.T) {
	command := New(&bytes.Buffer{}, &bytes.Buffer{}, "test")
	run, _, err := command.Find([]string{"run"})
	if err != nil {
		t.Fatal(err)
	}
	if run.Flags().Lookup("interactive") == nil {
		t.Fatal("ag run is missing --interactive")
	}
	for _, removed := range []string{
		"gateway-url", "user", "list", "attach",
	} {
		if run.Flags().Lookup(removed) != nil {
			t.Fatalf("ag run still exposes --%s", removed)
		}
	}
	for _, compatibility := range []string{"session", "resume"} {
		flag := run.Flags().Lookup(compatibility)
		if flag == nil || !flag.Hidden {
			t.Fatalf("compatibility --%s is not hidden", compatibility)
		}
	}
}

func TestGatewayRPCTargetUsesLoopbackForWildcardListener(t *testing.T) {
	for _, listen := range []string{":8080", "0.0.0.0:8080", "[::]:8080"} {
		got, err := gatewayRPCTarget(listen)
		if err != nil {
			t.Fatal(err)
		}
		if got != "grpc://127.0.0.1:8080" {
			t.Fatalf("gatewayRPCTarget(%q) = %q", listen, got)
		}
	}
}
