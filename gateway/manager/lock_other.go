//go:build !unix

package manager

import (
	"context"
	"os"
	"time"
)

func withStartupLock(ctx context.Context, path string, action func() error) error {
	lockDirectory := path + ".d"
	for {
		if err := os.Mkdir(lockDirectory, 0o700); err == nil {
			defer os.Remove(lockDirectory)
			return action()
		} else if !os.IsExist(err) {
			return err
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
