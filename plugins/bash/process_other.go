//go:build !unix

package bash

import (
	"os"
	"os/exec"
)

func configureProcess(command *exec.Cmd) {
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		return command.Process.Kill()
	}
}
