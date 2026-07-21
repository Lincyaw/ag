//go:build unix

package manager

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"
)

func configureProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

func stopManagedGatewayProcess(ctx context.Context, ready Ready) error {
	if err := signalManagedProcess(ready.PID, syscall.SIGTERM); err != nil &&
		!errors.Is(err, syscall.ESRCH) {
		return err
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		err := syscall.Kill(ready.PID, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return err
		}
		select {
		case <-ctx.Done():
			if err := signalManagedProcess(ready.PID, syscall.SIGKILL); err != nil &&
				!errors.Is(err, syscall.ESRCH) {
				return errors.Join(ctx.Err(), err)
			}
			return nil
		case <-ticker.C:
		}
	}
}

func signalManagedProcess(pid int, signal syscall.Signal) error {
	// Managed gateways are session leaders (configureProcess uses Setsid), so
	// signal the process group to avoid leaving plugin children behind. Fall
	// back to the leader for readiness files created by older installations.
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return syscall.Kill(pid, signal)
	}
	return err
}
