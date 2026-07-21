//go:build !unix

package manager

import (
	"context"
	"errors"
	"os"
	"os/exec"
)

func configureProcess(_ *exec.Cmd) {}

func stopManagedGatewayProcess(_ context.Context, ready Ready) error {
	process, err := os.FindProcess(ready.PID)
	if err != nil {
		return err
	}
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
