package workspace

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lincyaw/ag/agent"
)

type collectingHost struct {
	tools map[string]agent.Tool
}

func (h *collectingHost) RegisterProvider(agent.Provider) error {
	return nil
}

func (h *collectingHost) RegisterTool(tool agent.Tool) error {
	if h.tools == nil {
		h.tools = make(map[string]agent.Tool)
	}
	h.tools[tool.Spec().Name] = tool
	return nil
}

func TestReadFileIsRootConfined(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "workspace")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(root, "hello.txt"),
		[]byte("hello"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(parent, "secret.txt"),
		[]byte("secret"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	host := &collectingHost{}
	if err := New(Config{Root: root}).Install(host); err != nil {
		t.Fatal(err)
	}
	read := host.tools["read_file"]

	content, err := read.Call(
		context.Background(),
		json.RawMessage(`{"path":"hello.txt"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if content != "hello" {
		t.Fatalf("content = %q", content)
	}

	_, err = read.Call(
		context.Background(),
		json.RawMessage(`{"path":"../secret.txt"}`),
	)
	if err == nil {
		t.Fatal("expected workspace escape to fail")
	}
}
