package filestate

import (
	"hash/crc32"
	"sync"
)

const processLockCount = 64

var processLocks [processLockCount]sync.RWMutex

func WithSharedLock(path string, action func() error) error {
	return withFileLock(path, false, action)
}

func WithExclusiveLock(path string, action func() error) error {
	return withFileLock(path, true, action)
}

func withProcessLock(
	path string,
	exclusive bool,
	action func() error,
) error {
	index := crc32.ChecksumIEEE([]byte(path)) % processLockCount
	lock := &processLocks[index]
	if exclusive {
		lock.Lock()
		defer lock.Unlock()
	} else {
		lock.RLock()
		defer lock.RUnlock()
	}
	return action()
}
