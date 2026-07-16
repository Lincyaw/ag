package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/plugins/bash"
	fileplugin "github.com/lincyaw/ag/plugins/file"
	"github.com/lincyaw/ag/sdk"
)

func TestFileAndBashPluginsRunThroughRemoteOperationProtocol(t *testing.T) {
	t.Parallel()

	t.Run("file", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		client := startToolPlugin(t, fileplugin.New(fileplugin.Config{
			Root:        root,
			EnableWrite: true,
		}))
		write := callRemoteTool(t, client, "write_file", "file-write", map[string]any{
			"path":    "remote.txt",
			"content": "written through rpc",
		})
		if write.IsError || !strings.Contains(write.Content, "wrote 19 bytes") {
			t.Fatalf("remote write result = %#v", write)
		}
		read := callRemoteTool(t, client, "read_file", "file-read", map[string]any{
			"path": "remote.txt",
		})
		if read.IsError || read.Content != "written through rpc" {
			t.Fatalf("remote read result = %#v", read)
		}
		onDisk, err := os.ReadFile(filepath.Join(root, "remote.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if string(onDisk) != read.Content {
			t.Fatalf("disk = %q, rpc = %q", onDisk, read.Content)
		}
	})

	t.Run("bash", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		client := startToolPlugin(t, bash.New(bash.Config{Root: root}))
		result := callRemoteTool(t, client, "bash", "bash-run", map[string]any{
			"command": `printf 'rpc-cwd=%s\n' "$PWD"; printf 'remote-stderr\n' >&2`,
		})
		if result.IsError {
			t.Fatalf("remote bash failed: %s", result.Content)
		}
		for _, expected := range []string{
			"rpc-cwd=" + resolvePath(t, root),
			"stderr:\nremote-stderr",
			"exit_code: 0",
		} {
			if !strings.Contains(result.Content, expected) {
				t.Fatalf("remote bash result %q missing %q", result.Content, expected)
			}
		}
	})
}

func startToolPlugin(t *testing.T, plugin sdk.Plugin) pluginv1.PluginServiceClient {
	t.Helper()
	adapter, err := NewServer(context.Background(), ServerConfig{
		Plugin: plugin,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewGRPCServer(adapter, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = listener.Close()
		select {
		case serveErr := <-serveErrors:
			if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
				t.Errorf("serve plugin: %v", serveErr)
			}
		case <-time.After(time.Second):
			t.Error("plugin gRPC server did not stop")
		}
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := adapter.Close(closeContext); err != nil {
			t.Errorf("close plugin adapter: %v", err)
		}
	})
	parsed, err := parseSourceURI("grpc://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dial(context.Background(), parsed, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close plugin connection: %v", err)
		}
	})
	return pluginv1.NewPluginServiceClient(connection)
}

func callRemoteTool(
	t *testing.T,
	client pluginv1.PluginServiceClient,
	resource string,
	idempotencyKey string,
	arguments map[string]any,
) sdk.ToolResult {
	t.Helper()
	raw, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	input, err := rawToStruct(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submitted, err := client.SubmitOperation(ctx, &pluginv1.SubmitOperationRequest{
		Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
		Resource: resource,
		Request: &pluginv1.OperationRequest{
			IdempotencyKey: idempotencyKey,
			Input:          input,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	operationID := submitted.GetOperation().GetId()
	for {
		polled, pollErr := client.PollOperation(ctx, &pluginv1.PollOperationRequest{
			Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
			Resource: resource,
			Id:       operationID,
		})
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		operation := polled.GetOperation()
		switch operation.GetState() {
		case pluginv1.OperationState_OPERATION_STATE_SUCCEEDED:
			output, outputErr := structToRaw(operation.GetOutput())
			if outputErr != nil {
				t.Fatal(outputErr)
			}
			var result sdk.ToolResult
			if unmarshalErr := json.Unmarshal(output, &result); unmarshalErr != nil {
				t.Fatal(unmarshalErr)
			}
			return result
		case pluginv1.OperationState_OPERATION_STATE_FAILED,
			pluginv1.OperationState_OPERATION_STATE_CANCELLED:
			t.Fatalf("remote operation %s ended in %s: %s", operationID, operation.GetState(), operation.GetError())
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func resolvePath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
