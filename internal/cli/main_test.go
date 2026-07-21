package cli

import (
	"os"
	"testing"

	gatewaymanager "github.com/lincyaw/ag/gateway/manager"
)

// The managed gateway launches the current executable through a private
// environment protocol. In package tests the current executable is the Go test
// binary, so TestMain mirrors cmd/ag's single call into cli.Run.
func TestMain(m *testing.M) {
	if os.Getenv(gatewaymanager.ChildModeEnvironment) == "1" {
		os.Exit(Run(nil, os.Stdout, os.Stderr, "test-version"))
	}
	os.Exit(m.Run())
}
