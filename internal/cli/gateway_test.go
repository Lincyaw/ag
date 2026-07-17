package cli

import (
	"bytes"
	"testing"
)

func TestGatewayServeFlagsMatchItsRuntimeContract(t *testing.T) {
	command := New(&bytes.Buffer{}, &bytes.Buffer{}, "test")
	serve, _, err := command.Find([]string{"gateway", "serve"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"gateway-listen",
		"gateway-dir",
		"provider",
		"registry-uri",
		"openai",
		"file",
		"bash",
	} {
		if serve.Flags().Lookup(name) == nil {
			t.Fatalf("gateway serve is missing --%s", name)
		}
	}
	for _, ignored := range []string{"plugin", "timeout"} {
		if serve.Flags().Lookup(ignored) != nil {
			t.Fatalf("gateway serve exposes ignored --%s", ignored)
		}
	}
}
