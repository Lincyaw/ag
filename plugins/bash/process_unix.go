//go:build unix

package bash

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}

func waitProcessGroup(command *exec.Cmd, timeout time.Duration) error {
	if command.Process == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Kill(-command.Process.Pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("process group %d still exists after %s", command.Process.Pid, timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
