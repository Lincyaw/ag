package registry

import (
	"hash/crc32"
	"sync"
)

const processFileLockCount = 64

var processFileLocks [processFileLockCount]sync.RWMutex

func withProcessFileLock(
	path string,
	exclusive bool,
	action func() error,
) error {
	index := crc32.ChecksumIEEE([]byte(path)) % processFileLockCount
	lock := &processFileLocks[index]
	if exclusive {
		lock.Lock()
		defer lock.Unlock()
	} else {
		lock.RLock()
		defer lock.RUnlock()
	}
	return action()
}
