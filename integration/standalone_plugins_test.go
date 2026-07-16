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
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/pluginhost"
	"github.com/lincyaw/ag/pluginrpc"
	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
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

	registryURI, registryClient := startRegistry(t)
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
	eventually(t, 2*time.Second, func() bool {
		registrations, err := registryClient.List(context.Background())
		return err == nil && len(registrations) == 1 &&
			registrations[0].Name == "file" && registrations[0].URI == fileProcess.ready.URI
	})
	fileClient := connectPlugin(t, fileProcess.ready.URI, nil)
	write := callTool(t, fileClient, "write_file", "write-once", map[string]any{
		"path": "standalone.txt", "content": "from standalone process",
	})
	if write.IsError || !strings.Contains(write.Content, "wrote 23 bytes") {
		t.Fatalf("standalone write = %#v", write)
	}
	read := callTool(t, fileClient, "read_file", "read-once", map[string]any{
		"path": "standalone.txt",
	})
	if read.IsError || read.Content != "from standalone process" {
		t.Fatalf("standalone read = %#v", read)
	}
	fileProcess.stop(t)
	eventually(t, 2*time.Second, func() bool {
		registrations, err := registryClient.List(context.Background())
		return err == nil && len(registrations) == 0
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

type childProcess struct {
	command *exec.Cmd
	ready   pluginhost.Ready
	stderr  *bytes.Buffer
}

func startPluginProcess(t *testing.T, binary string, arguments ...string) *childProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	command := exec.CommandContext(ctx, binary, arguments...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		cancel()
		_ = command.Wait()
		t.Fatalf("plugin exited before ready: %v\nstderr:\n%s", scanner.Err(), stderr.String())
	}
	var ready pluginhost.Ready
	if err := json.Unmarshal(scanner.Bytes(), &ready); err != nil {
		t.Fatalf("decode plugin ready %q: %v", scanner.Text(), err)
	}
	return &childProcess{command: command, ready: ready, stderr: stderr}
}

func (process *childProcess) stop(t *testing.T) {
	t.Helper()
	if err := process.command.Process.Signal(os.Interrupt); err != nil {
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
		_ = process.command.Process.Kill()
		t.Fatalf("plugin did not stop\nstderr:\n%s", process.stderr.String())
	}
}

func startRegistry(t *testing.T) (string, *pluginrpc.RegistryClient) {
	t.Helper()
	leaseRegistry := sdk.NewLeaseRegistry(sdk.LeaseRegistryConfig{})
	adapter, err := pluginrpc.NewRegistryServer(leaseRegistry)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	if err := pluginrpc.RegisterRegistryService(server, adapter); err != nil {
		t.Fatal(err)
	}
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
	})
	uri := "grpc://" + listener.Addr().String()
	client, err := pluginrpc.NewRegistryClient(context.Background(), uri, pluginrpc.ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return uri, client
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
