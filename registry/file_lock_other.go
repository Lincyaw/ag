//go:build !unix

package registry

const fileLocksAreMultiProcessSafe = false

func withFileLock(path string, exclusive bool, action func() error) error {
	return withProcessFileLock(path, exclusive, action)
}
