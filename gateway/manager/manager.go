// Package manager owns the lifecycle of ag's private background gateway.
// Frontends import this package; users never start a gateway command.
package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	gatewayclient "github.com/lincyaw/ag/gateway/client"
)

const (
	DirectoryName = "managed"
	ReadyName     = "ready.json"
	ConfigName    = "config.json"
	LogName       = "gateway.stderr.log"
	LockName      = "start.lock"

	ChildModeEnvironment   = "AG_INTERNAL_GATEWAY_DAEMON"
	ChildConfigEnvironment = "AG_INTERNAL_GATEWAY_CONFIG"

	// Replacement gets independent stop and start budgets. The gateway's own
	// shutdown path uses two bounded phases (drain, then forced close), so the
	// manager allows both to finish before escalating the exact process group.
	defaultStopTimeout  = 25 * time.Second
	defaultStartTimeout = 30 * time.Second
	pollInterval        = 50 * time.Millisecond
	defaultProbeTimeout = 750 * time.Millisecond
)

type Ready struct {
	Target              string `json:"target"`
	Listen              string `json:"listen"`
	Directory           string `json:"directory"`
	Registry            string `json:"registry"`
	Executable          string `json:"executable,omitempty"`
	ExecutableSHA256    string `json:"executable_sha256,omitempty"`
	RecoveredExecutions int    `json:"recovered_executions"`
	PID                 int    `json:"pid"`
}

type Launcher func(
	configPath string,
	readyPath string,
	logPath string,
) (<-chan error, error)

type Probe func(context.Context, string) error

type Stopper func(context.Context, Ready) error

type Config struct {
	Directory     string
	RuntimeConfig []byte
	Executable    string
	Environment   []string
	StartTimeout  time.Duration
	StopTimeout   time.Duration
	ProbeTimeout  time.Duration
	Launcher      Launcher
	Probe         Probe
	Stopper       Stopper
}

type Manager struct {
	config           Config
	executable       string
	executableSHA256 string
}

func New(config Config) (*Manager, error) {
	config.Directory = strings.TrimSpace(config.Directory)
	if config.Directory == "" {
		return nil, errors.New("managed gateway directory is empty")
	}
	if !filepath.IsAbs(config.Directory) {
		return nil, errors.New("managed gateway directory must be absolute")
	}
	config.Directory = filepath.Clean(config.Directory)
	if !json.Valid(config.RuntimeConfig) {
		return nil, errors.New("managed gateway runtime config is not valid JSON")
	}
	if config.StartTimeout == 0 {
		config.StartTimeout = defaultStartTimeout
	}
	if config.StopTimeout == 0 {
		config.StopTimeout = defaultStopTimeout
	}
	if config.ProbeTimeout == 0 {
		config.ProbeTimeout = defaultProbeTimeout
	}
	if config.StartTimeout < 0 || config.StopTimeout < 0 || config.ProbeTimeout < 0 {
		return nil, errors.New("managed gateway timeouts must be positive")
	}
	config.RuntimeConfig = append([]byte(nil), config.RuntimeConfig...)
	config.Environment = append([]string(nil), config.Environment...)
	executable, executableSHA256, err := executableIdentity(config.Executable)
	if err != nil {
		return nil, err
	}
	manager := &Manager{
		config:           config,
		executable:       executable,
		executableSHA256: executableSHA256,
	}
	if manager.config.Probe == nil {
		manager.config.Probe = probeGRPC
	}
	if manager.config.Launcher == nil {
		manager.config.Launcher = manager.launch
	}
	if manager.config.Stopper == nil {
		manager.config.Stopper = stopManagedGatewayProcess
	}
	return manager, nil
}

func (manager *Manager) Ensure(ctx context.Context) (Ready, error) {
	managedDirectory := filepath.Join(manager.config.Directory, DirectoryName)
	if err := os.MkdirAll(managedDirectory, 0o700); err != nil {
		return Ready{}, fmt.Errorf("create managed gateway directory: %w", err)
	}
	if err := os.Chmod(managedDirectory, 0o700); err != nil {
		return Ready{}, fmt.Errorf("secure managed gateway directory: %w", err)
	}
	readyPath := filepath.Join(managedDirectory, ReadyName)
	if ready, ok := manager.healthy(ctx, readyPath); ok {
		return ready, nil
	}

	var resolved Ready
	err := withStartupLock(
		ctx,
		filepath.Join(managedDirectory, LockName),
		func() error {
			if ready, ok := manager.healthy(ctx, readyPath); ok {
				resolved = ready
				return nil
			}
			if err := manager.stopRecordedInstance(ctx, readyPath); err != nil {
				return err
			}
			configPath := filepath.Join(managedDirectory, ConfigName)
			if err := writeRuntimeConfig(configPath, manager.config.RuntimeConfig); err != nil {
				return err
			}
			logPath := filepath.Join(managedDirectory, LogName)
			if err := truncateReady(readyPath); err != nil {
				return err
			}
			processDone, err := manager.config.Launcher(
				configPath, readyPath, logPath,
			)
			if err != nil {
				return fmt.Errorf("start managed gateway: %w", err)
			}
			resolved, err = manager.wait(ctx, readyPath, logPath, processDone)
			return err
		},
	)
	if err != nil {
		return Ready{}, err
	}
	return resolved, nil
}

func (manager *Manager) stopRecordedInstance(
	ctx context.Context,
	readyPath string,
) error {
	ready, err := readReady(readyPath)
	if err != nil {
		return nil
	}
	if ready.PID <= 0 || ready.PID == os.Getpid() ||
		filepath.Clean(ready.Directory) != manager.config.Directory {
		return nil
	}
	stopCtx, cancel := context.WithTimeout(ctx, manager.config.StopTimeout)
	defer cancel()
	if err := manager.config.Stopper(stopCtx, ready); err != nil {
		return fmt.Errorf(
			"stop stale managed gateway process %d: %w",
			ready.PID,
			err,
		)
	}
	return nil
}

func (manager *Manager) healthy(ctx context.Context, readyPath string) (Ready, bool) {
	ready, err := readReady(readyPath)
	if err != nil || !validTarget(ready.Target) {
		return Ready{}, false
	}
	if ready.Executable != manager.executable ||
		ready.ExecutableSHA256 != manager.executableSHA256 {
		return Ready{}, false
	}
	probeContext, cancel := context.WithTimeout(ctx, manager.config.ProbeTimeout)
	defer cancel()
	if err := manager.config.Probe(probeContext, ready.Target); err != nil {
		return Ready{}, false
	}
	return ready, true
}

func readReady(path string) (Ready, error) {
	file, err := os.Open(path)
	if err != nil {
		return Ready{}, err
	}
	var ready Ready
	decodeErr := json.NewDecoder(io.LimitReader(file, 1<<20)).Decode(&ready)
	closeErr := file.Close()
	return ready, errors.Join(decodeErr, closeErr)
}

func (manager *Manager) wait(
	ctx context.Context,
	readyPath string,
	logPath string,
	processDone <-chan error,
) (Ready, error) {
	timeout := time.NewTimer(manager.config.StartTimeout)
	defer timeout.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if ready, ok := manager.healthy(ctx, readyPath); ok {
			return ready, nil
		}
		select {
		case <-ctx.Done():
			return Ready{}, ctx.Err()
		case err, open := <-processDone:
			if !open && err == nil {
				err = errors.New("managed gateway exited")
			}
			return Ready{}, startupError(err, logPath)
		case <-timeout.C:
			return Ready{}, startupError(
				errors.New("timed out waiting for readiness"), logPath,
			)
		case <-ticker.C:
		}
	}
}

func (manager *Manager) launch(
	configPath string,
	readyPath string,
	logPath string,
) (<-chan error, error) {
	stdout, err := os.OpenFile(
		readyPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open gateway readiness output: %w", err)
	}
	stderr, err := os.OpenFile(
		logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600,
	)
	if err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("open gateway diagnostic log: %w", err)
	}
	command := exec.Command(manager.executable)
	command.Stdin = nil
	command.Stdout = stdout
	command.Stderr = stderr
	environment := manager.config.Environment
	if environment == nil {
		environment = os.Environ()
	}
	command.Env = setEnvironment(
		setEnvironment(environment, ChildModeEnvironment, "1"),
		ChildConfigEnvironment,
		configPath,
	)
	configureProcess(command)
	if err := command.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	_ = stdout.Close()
	_ = stderr.Close()
	done := make(chan error, 1)
	go func() {
		done <- command.Wait()
		close(done)
	}()
	return done, nil
}

func CurrentExecutableIdentity() (string, string, error) {
	return executableIdentity("")
}

func executableIdentity(configured string) (string, string, error) {
	executable := strings.TrimSpace(configured)
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return "", "", fmt.Errorf("locate ag executable: %w", err)
		}
	}
	absolute, err := filepath.Abs(executable)
	if err != nil {
		return "", "", fmt.Errorf("resolve ag executable path: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(absolute); err == nil {
		absolute = resolved
	}
	file, err := os.Open(absolute)
	if err != nil {
		return "", "", fmt.Errorf("open ag executable: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", "", fmt.Errorf("hash ag executable: %w", err)
	}
	return filepath.Clean(absolute), hex.EncodeToString(hash.Sum(nil)), nil
}

func ChildRequestFromEnvironment() (string, bool, error) {
	if os.Getenv(ChildModeEnvironment) != "1" {
		return "", false, nil
	}
	configPath := strings.TrimSpace(os.Getenv(ChildConfigEnvironment))
	if configPath == "" {
		return "", true, errors.New("private gateway child config is empty")
	}
	if !filepath.IsAbs(configPath) {
		return "", true, errors.New("private gateway child config must be absolute")
	}
	return filepath.Clean(configPath), true, nil
}

func writeRuntimeConfig(path string, raw []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".config-*.tmp")
	if err != nil {
		return fmt.Errorf("create managed gateway config: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure managed gateway config: %w", err)
	}
	if _, err := temporary.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write managed gateway config: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync managed gateway config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close managed gateway config: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish managed gateway config: %w", err)
	}
	return nil
}

func truncateReady(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("prepare managed gateway readiness file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure managed gateway readiness file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close managed gateway readiness file: %w", err)
	}
	return nil
}

func probeGRPC(ctx context.Context, target string) error {
	client, err := gatewayclient.New(gatewayclient.Config{
		Target: target, UserID: "health",
	})
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Health(ctx)
}

func validTarget(target string) bool {
	parsed, err := url.Parse(strings.TrimSpace(target))
	return err == nil && (parsed.Scheme == "grpc" || parsed.Scheme == "grpcs") &&
		parsed.Host != ""
}

func startupError(cause error, logPath string) error {
	diagnostic, err := os.ReadFile(logPath)
	if err == nil {
		const maxDiagnosticBytes = 16 << 10
		if len(diagnostic) > maxDiagnosticBytes {
			diagnostic = diagnostic[len(diagnostic)-maxDiagnosticBytes:]
		}
		message := strings.TrimSpace(string(diagnostic))
		if message != "" {
			return fmt.Errorf("managed gateway failed to start: %w\n%s", cause, message)
		}
	}
	return fmt.Errorf("managed gateway failed to start: %w", cause)
}

func setEnvironment(environment []string, name string, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}
