//go:build unix

package filestate

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const MultiProcessSafe = true

func withFileLock(path string, exclusive bool, action func() error) error {
	return withProcessLock(path, exclusive, func() error {
		lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("open file-state lock %q: %w", path, err)
		}
		defer lock.Close()
		operation := unix.LOCK_SH
		if exclusive {
			operation = unix.LOCK_EX
		}
		if err := unix.Flock(int(lock.Fd()), operation); err != nil {
			return fmt.Errorf("acquire file-state lock %q: %w", path, err)
		}
		defer func() {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		}()
		return action()
	})
}
