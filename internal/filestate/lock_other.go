//go:build !unix

package filestate

const MultiProcessSafe = false

func withFileLock(path string, exclusive bool, action func() error) error {
	return withProcessLock(path, exclusive, action)
}
