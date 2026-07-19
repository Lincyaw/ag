//go:build unix

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/pluginhost"
	"github.com/lincyaw/ag/pluginrpc"
	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStandaloneFileAndBashProcessesWithLeaseAndPolling(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and executes standalone plugin binaries")
	}
	repository := repositoryRoot(t)
	bin := t.TempDir()
	fileBinary := filepath.Join(bin, "agentm-plugin-file")
	bashBinary := filepath.Join(bin, "agentm-plugin-bash")
	build := exec.Command(
		"go", "build",
		"-o", fileBinary, "./cmd/agentm-plugin-file",
	)
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build file plugin: %v\n%s", err, output)
	}
	build = exec.Command(
		"go", "build",
		"-o", bashBinary, "./cmd/agentm-plugin-bash",
	)
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build bash plugin: %v\n%s", err, output)
	}

	registryURI, registryClient, _ := startRegistry(t)
	root := t.TempDir()
	fileState := filepath.Join(t.TempDir(), "file-state")
	fileProcess := startPluginProcess(t, fileBinary,
		"--root", root,
		"--write",
		"--state-dir", fileState,
		"--registry-uri", registryURI,
		"--lease-ttl", "300ms",
	)
	if fileProcess.ready.Name != "file" || !strings.HasPrefix(fileProcess.ready.URI, "grpc://") {
		t.Fatalf("file ready = %#v", fileProcess.ready)
	}
	assertPluginLogContains(
		t,
		fileProcess,
		"agentm-plugin-file",
		"plugin RPC server ready",
	)
	eventually(t, 2*time.Second, func() bool {
		page, err := registryClient.List(
			context.Background(),
			registry.DiscoveryQuery{},
			registry.PageRequest{},
		)
		return err == nil && len(page.Items) == 1 &&
			page.Items[0].Name == "file" &&
			page.Items[0].URI == fileProcess.ready.URI
	})
	fileClient := connectPlugin(t, fileProcess.ready.URI, nil)
	write := callTool(t, fileClient, "write_file", "write-once", map[string]any{
		"path": "standalone.txt", "content": "from standalone process",
	})
	if write.IsError || !strings.Contains(write.Content, "bytes: 23") {
		t.Fatalf("standalone write = %#v", write)
	}
	read := callTool(t, fileClient, "read_file", "read-once", map[string]any{
		"path": "standalone.txt",
	})
	if read.IsError || !strings.Contains(read.Content, "1\tfrom standalone process") {
		t.Fatalf("standalone read = %#v", read)
	}
	revision := standaloneFileRevision(t, read.Content)
	edit := callTool(t, fileClient, "edit_file", "edit-once", map[string]any{
		"path":            "standalone.txt",
		"expected_sha256": revision,
		"old_text":        "standalone",
		"new_text":        "remote",
	})
	if edit.IsError || !strings.Contains(edit.Content, "1\tfrom remote process") {
		t.Fatalf("standalone edit = %#v", edit)
	}
	search := callTool(t, fileClient, "search_files", "search-once", map[string]any{
		"path": "standalone.txt", "query": "remote",
	})
	if search.IsError ||
		!strings.Contains(search.Content, "standalone.txt:1:6: from remote process") {
		t.Fatalf("standalone search = %#v", search)
	}
	fileProcess.stop(t)
	eventually(t, 2*time.Second, func() bool {
		page, err := registryClient.List(
			context.Background(),
			registry.DiscoveryQuery{},
			registry.PageRequest{},
		)
		return err == nil && len(page.Items) == 0
	})
	if _, err := os.Stat(filepath.Join(fileState, "operations", "operations.json")); err != nil {
		t.Fatalf("durable operation state missing: %v", err)
	}

	certificate, privateKey, roots := createServerIdentity(t)
	bashProcess := startPluginProcess(t, bashBinary,
		"--root", root,
		"--state-dir", filepath.Join(t.TempDir(), "bash-state"),
		"--timeout", "2s",
		"--max-timeout", "3s",
		"--tls-cert", certificate,
		"--tls-key", privateKey,
	)
	if !strings.HasPrefix(bashProcess.ready.URI, "grpcs://") {
		t.Fatalf("TLS bash ready = %#v", bashProcess.ready)
	}
	assertPluginLogContains(
		t,
		bashProcess,
		"agentm-plugin-bash",
		"plugin RPC server ready",
	)
	bashClient := connectPlugin(t, bashProcess.ready.URI, roots)
	bashResult := callTool(t, bashClient, "bash", "bash-once", map[string]any{
		"command": `printf 'standalone=%s\n' "$PWD"; printf 'rpc-stderr\n' >&2`,
	})
	if bashResult.IsError {
		t.Fatalf("standalone bash = %#v", bashResult)
	}
	for _, expected := range []string{
		"standalone=" + resolvePath(t, root),
		"stderr:\nrpc-stderr",
		"exit_code: 0",
	} {
		if !strings.Contains(bashResult.Content, expected) {
			t.Fatalf("bash result %q missing %q", bashResult.Content, expected)
		}
	}
	bashProcess.stop(t)
}

func standaloneFileRevision(t *testing.T, content string) string {
	t.Helper()
	for _, line := range strings.Split(content, "\n") {
		if revision, found := strings.CutPrefix(line, "sha256: "); found {
			if len(revision) != 64 {
				t.Fatalf("invalid file revision %q", revision)
			}
			return revision
		}
	}
	t.Fatalf("file result has no revision: %q", content)
	return ""
}

type childProcess struct {
	command *exec.Cmd
	ready   pluginhost.Ready
	stderr  *safeBuffer
	home    string
}

type safeBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *safeBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *safeBuffer) Len() int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Len()
}

func (buffer *safeBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

func startPluginProcess(t *testing.T, binary string, arguments ...string) *childProcess {
	home := t.TempDir()
	process := startPluginProcessEnv(t, []string{"HOME=" + home}, binary, arguments...)
	process.home = home
	return process
}

func startPluginProcessEnv(
	t *testing.T,
	environment []string,
	binary string,
	arguments ...string,
) *childProcess {
	t.Helper()
	command := exec.Command(binary, arguments...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if len(environment) > 0 {
		command.Env = append(os.Environ(), environment...)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := &safeBuffer{}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			killProcessGroup(command, syscall.SIGKILL)
			_ = command.Wait()
		}
	})
	ready, err := waitPluginReady(stdout, 15*time.Second)
	if err != nil {
		killProcessGroup(command, syscall.SIGKILL)
		_ = command.Wait()
		t.Fatalf("%v\nstderr:\n%s", err, stderr.String())
	}
	return &childProcess{command: command, ready: ready, stderr: stderr}
}

func (process *childProcess) stop(t *testing.T) {
	t.Helper()
	if err := signalProcessGroup(process.command, syscall.SIGINT); err != nil {
		t.Fatalf("interrupt plugin: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- process.command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("plugin exit: %v\nstderr:\n%s", err, process.stderr.String())
		}
	case <-time.After(5 * time.Second):
		killProcessGroup(process.command, syscall.SIGKILL)
		t.Fatalf("plugin did not stop\nstderr:\n%s", process.stderr.String())
	}
}

func waitPluginReady(
	stdout io.Reader,
	timeout time.Duration,
) (pluginhost.Ready, error) {
	type readyResult struct {
		ready pluginhost.Ready
		err   error
	}
	ready := make(chan readyResult, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if !scanner.Scan() {
			ready <- readyResult{
				err: fmt.Errorf(
					"plugin exited before ready: %v",
					scanner.Err(),
				),
			}
			return
		}
		var parsed pluginhost.Ready
		if err := json.Unmarshal(scanner.Bytes(), &parsed); err != nil {
			ready <- readyResult{
				err: fmt.Errorf(
					"decode plugin ready %q: %w",
					scanner.Text(),
					err,
				),
			}
			return
		}
		ready <- readyResult{ready: parsed}
	}()
	select {
	case result := <-ready:
		return result.ready, result.err
	case <-time.After(timeout):
		return pluginhost.Ready{}, errors.New("plugin did not become ready")
	}
}

func signalProcessGroup(command *exec.Cmd, signal syscall.Signal) error {
	if command == nil || command.Process == nil {
		return errors.New("plugin process is not started")
	}
	if err := syscall.Kill(-command.Process.Pid, signal); err == nil ||
		!errors.Is(err, syscall.ESRCH) {
		return err
	}
	return command.Process.Signal(signal)
}

func killProcessGroup(command *exec.Cmd, signal syscall.Signal) {
	if command == nil || command.Process == nil {
		return
	}
	if err := syscall.Kill(-command.Process.Pid, signal); err != nil &&
		!errors.Is(err, syscall.ESRCH) {
		_ = command.Process.Signal(signal)
	}
}

func assertPluginLogContains(
	t *testing.T,
	process *childProcess,
	commandName string,
	needle string,
) {
	t.Helper()
	if process.stderr.Len() != 0 {
		t.Fatalf("plugin stderr is not quiet:\n%s", process.stderr.String())
	}
	logPath := filepath.Join(process.home, ".ag", "logs", commandName+".log")
	eventually(t, time.Second, func() bool {
		content, err := os.ReadFile(logPath)
		return err == nil && strings.Contains(string(content), needle)
	})
}

func startRegistry(
	t *testing.T,
) (string, registry.Directory, registry.Directory) {
	t.Helper()
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	adapter, err := pluginrpc.NewRegistryServer(directory)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pluginv1.RegisterRegistryServiceServer(server, adapter)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = listener.Close()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("registry serve: %v", err)
		}
		if err := directory.Close(context.Background()); err != nil {
			t.Errorf("registry close: %v", err)
		}
	})
	uri := "grpc://" + listener.Addr().String()
	client, err := pluginrpc.NewRegistryClient(context.Background(), uri, pluginrpc.ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return uri, client, directory
}

func connectPlugin(
	t *testing.T,
	uri string,
	roots *x509.CertPool,
) pluginv1.PluginServiceClient {
	t.Helper()
	address := strings.TrimPrefix(strings.TrimPrefix(uri, "grpc://"), "grpcs://")
	transport := credentials.TransportCredentials(insecure.NewCredentials())
	if strings.HasPrefix(uri, "grpcs://") {
		transport = credentials.NewTLS(&tls.Config{
			RootCAs: roots, MinVersion: tls.VersionTLS12,
		})
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return pluginv1.NewPluginServiceClient(connection)
}

func createServerIdentity(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "agentm-plugin-test"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	directory := t.TempDir()
	certificatePath := filepath.Join(directory, "server.crt")
	privateKeyPath := filepath.Join(directory, "server.key")
	if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(privateKeyPath, privateKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("append test root certificate")
	}
	return certificatePath, privateKeyPath, roots
}

func callTool(
	t *testing.T,
	client pluginv1.PluginServiceClient,
	resource string,
	idempotencyKey string,
	arguments map[string]any,
) sdk.ToolResult {
	t.Helper()
	input, err := structpb.NewStruct(arguments)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submitted, err := client.SubmitOperation(ctx, &pluginv1.SubmitOperationRequest{
		Kind: pluginv1.OperationKind_OPERATION_KIND_TOOL, Resource: resource,
		Request: &pluginv1.OperationRequest{IdempotencyKey: idempotencyKey, Input: input},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := submitted.GetOperation().GetId()
	for {
		polled, err := client.PollOperation(ctx, &pluginv1.PollOperationRequest{
			Kind: pluginv1.OperationKind_OPERATION_KIND_TOOL, Resource: resource, Id: id,
		})
		if err != nil {
			t.Fatal(err)
		}
		operation := polled.GetOperation()
		switch operation.GetState() {
		case pluginv1.OperationState_OPERATION_STATE_SUCCEEDED:
			raw, err := json.Marshal(operation.GetOutput().AsMap())
			if err != nil {
				t.Fatal(err)
			}
			var result sdk.ToolResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			return result
		case pluginv1.OperationState_OPERATION_STATE_FAILED,
			pluginv1.OperationState_OPERATION_STATE_CANCELLED:
			t.Fatalf("operation %s ended as %s: %s", id, operation.GetState(), operation.GetError())
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root %q: %v", root, err)
	}
	return root
}

func resolvePath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
