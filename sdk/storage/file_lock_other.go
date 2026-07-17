//go:build !unix

package storage

const fileLocksAreMultiProcessSafe = false

func withFileLock(_ string, _ bool, action func() error) error {
	return action()
}

func WithFileLock(path string, exclusive bool, action func() error) error {
	return withFileLock(path, exclusive, action)
}
