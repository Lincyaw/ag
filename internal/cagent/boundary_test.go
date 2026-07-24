package cagent

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLegacyCagentDoesNotLeakIntoMaintainedKernel(t *testing.T) {
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve boundary test path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
	for _, directory := range []string{
		"gateway",
		"pluginrpc",
		"plugins",
		"registry",
		"sdk",
	} {
		err := filepath.WalkDir(
			filepath.Join(root, directory),
			func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || filepath.Ext(path) != ".go" {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if strings.Contains(
					string(raw),
					"github.com/lincyaw/ag/internal/cagent",
				) {
					t.Errorf("maintained package imports legacy cagent: %s", path)
				}
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
}
