//go:build unix

package manager

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestStopManagedGatewayEscalatesAfterGracePeriod(t *testing.T) {
	command := exec.Command("sh", "-c", "trap '' TERM; while :; do sleep 1; done")
	configureProcess(command)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		_ = signalManagedProcess(command.Process.Pid, 9)
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	// Give the shell time to install its TERM trap before requesting shutdown.
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := stopManagedGatewayProcess(ctx, Ready{PID: command.Process.Pid}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("managed process survived forced shutdown")
	}
}
