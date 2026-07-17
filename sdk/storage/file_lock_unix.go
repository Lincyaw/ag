//go:build unix

package storage

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const fileLocksAreMultiProcessSafe = true

func withFileLock(path string, exclusive bool, action func() error) error {
	return withProcessFileLock(path, exclusive, func() error {
		lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("open state lock %q: %w", path, err)
		}
		defer lock.Close()
		operation := unix.LOCK_SH
		if exclusive {
			operation = unix.LOCK_EX
		}
		if err := unix.Flock(int(lock.Fd()), operation); err != nil {
			return fmt.Errorf("acquire state lock %q: %w", path, err)
		}
		defer func() {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		}()
		return action()
	})
}
