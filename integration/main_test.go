package integration_test

import (
	"os"
	"testing"

	"github.com/lincyaw/ag/gateway/manager"
	"github.com/lincyaw/ag/internal/cli"
)

// The managed gateway relaunches the current executable. When an integration
// test invokes cli.Run in-process, the executable is this test binary, so it
// mirrors cmd/ag's private child entrypoint before entering the test runner.
func TestMain(m *testing.M) {
	if os.Getenv(manager.ChildModeEnvironment) == "1" {
		os.Exit(cli.Run(nil, os.Stdout, os.Stderr, "integration-test"))
	}
	os.Exit(m.Run())
}
