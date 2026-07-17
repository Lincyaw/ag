package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type FileConfig struct {
	Directory    string
	Clock        func() time.Time
	PollInterval time.Duration
	MaxChanges   int
}

type FileDirectory struct {
	directory    string
	statePath    string
	lockPath     string
	clock        func() time.Time
	pollInterval time.Duration
	maxChanges   int
	closed       atomic.Bool
}

func NewFileDirectory(config FileConfig) (*FileDirectory, error) {
	directory := strings.TrimSpace(config.Directory)
	if directory == "" {
		return nil, errors.New("registry directory is empty")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("resolve registry directory: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create registry directory: %w", err)
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.MaxChanges < 1 {
		config.MaxChanges = 1024
	}
	result := &FileDirectory{
		directory:    absolute,
		statePath:    filepath.Join(absolute, "registry.json"),
		lockPath:     filepath.Join(absolute, "registry.lock"),
		clock:        config.Clock,
		pollInterval: config.PollInterval,
		maxChanges:   config.MaxChanges,
	}
	if err := result.withState(context.Background(), func(
		state *directoryState,
	) (bool, error) {
		return false, state.validate()
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (directory *FileDirectory) Register(
	ctx context.Context,
	registration PluginRegistration,
	options LeaseOptions,
) (PluginLease, error) {
	var lease PluginLease
	err := directory.withState(ctx, func(
		state *directoryState,
	) (bool, error) {
		var changed bool
		var err error
		lease, changed, err = state.register(
			registration,
			options.TTL,
			directory.clock().UTC(),
			directory.maxChanges,
		)
		return changed, err
	})
	return lease, err
}

func (directory *FileDirectory) Renew(
	ctx context.Context,
	credential LeaseCredential,
	ttl time.Duration,
) (PluginLease, error) {
	var lease PluginLease
	err := directory.withState(ctx, func(
		state *directoryState,
	) (bool, error) {
		var changed bool
		var err error
		lease, changed, err = state.renew(
			credential,
			ttl,
			directory.clock().UTC(),
			directory.maxChanges,
		)
		return changed, err
	})
	return lease, err
}

func (directory *FileDirectory) Unregister(
	ctx context.Context,
	credential LeaseCredential,
) error {
	return directory.withState(ctx, func(
		state *directoryState,
	) (bool, error) {
		return state.unregister(
			credential,
			directory.clock().UTC(),
			directory.maxChanges,
		)
	})
}

func (directory *FileDirectory) Get(
	ctx context.Context,
	key InstanceKey,
) (PluginInstance, error) {
	var instance PluginInstance
	err := directory.withState(ctx, func(
		state *directoryState,
	) (bool, error) {
		var changed bool
		var err error
		instance, changed, err = state.get(
			key,
			directory.clock().UTC(),
			directory.maxChanges,
		)
		return changed, err
	})
	return instance, err
}

func (directory *FileDirectory) List(
	ctx context.Context,
	query DiscoveryQuery,
	request PageRequest,
) (DiscoveryPage, error) {
	var page DiscoveryPage
	err := directory.withState(ctx, func(
		state *directoryState,
	) (bool, error) {
		var changed bool
		var err error
		page, changed, err = state.list(
			query,
			request,
			directory.clock().UTC(),
			directory.maxChanges,
		)
		return changed, err
	})
	return page, err
}

func (directory *FileDirectory) Poll(
	ctx context.Context,
	request ChangePollRequest,
) (ChangePage, error) {
	request, err := validatePoll(request)
	if err != nil {
		return ChangePage{}, err
	}
	deadline := time.Now().Add(request.Wait)
	for {
		var page ChangePage
		err := directory.withState(ctx, func(
			state *directoryState,
		) (bool, error) {
			var changed bool
			var pollErr error
			page, changed, pollErr = state.poll(
				request,
				directory.clock().UTC(),
				directory.maxChanges,
			)
			return changed, pollErr
		})
		if err != nil || len(page.Changes) > 0 ||
			page.NextRevision > request.AfterRevision ||
			request.Wait == 0 {
			return page, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return page, nil
		}
		timer := time.NewTimer(min(directory.pollInterval, remaining))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ChangePage{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (*FileDirectory) Capabilities() Capabilities {
	return Capabilities{
		Durable:          true,
		MultiProcessSafe: fileLocksAreMultiProcessSafe,
		Poll:             true,
	}
}

func (directory *FileDirectory) String() string {
	return (&url.URL{Scheme: "file", Path: directory.directory}).String()
}

func (directory *FileDirectory) Close(context.Context) error {
	directory.closed.Store(true)
	return nil
}

func (directory *FileDirectory) withState(
	ctx context.Context,
	action func(*directoryState) (bool, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if directory.closed.Load() {
		return ErrClosed
	}
	return withFileLock(directory.lockPath, true, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if directory.closed.Load() {
			return ErrClosed
		}
		state, err := directory.readState()
		if err != nil {
			return err
		}
		changed, actionErr := action(&state)
		if changed {
			if writeErr := directory.writeState(ctx, state); writeErr != nil {
				return errors.Join(actionErr, writeErr)
			}
		}
		return actionErr
	})
}

func (directory *FileDirectory) readState() (directoryState, error) {
	raw, err := os.ReadFile(directory.statePath)
	if errors.Is(err, fs.ErrNotExist) {
		return newDirectoryState(), nil
	}
	if err != nil {
		return directoryState{}, fmt.Errorf("read registry state: %w", err)
	}
	var state directoryState
	if err := json.Unmarshal(raw, &state); err != nil {
		return directoryState{}, fmt.Errorf("decode registry state: %w", err)
	}
	if err := state.validate(); err != nil {
		return directoryState{}, fmt.Errorf("validate registry state: %w", err)
	}
	return state, nil
}

func (directory *FileDirectory) writeState(
	ctx context.Context,
	state directoryState,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode registry state: %w", err)
	}
	temporary, err := os.CreateTemp(directory.directory, ".registry-*.tmp")
	if err != nil {
		return fmt.Errorf("create registry temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure registry temporary file: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write registry state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync registry state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close registry state: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, directory.statePath); err != nil {
		return fmt.Errorf("publish registry state: %w", err)
	}
	removeTemporary = false
	handle, err := os.Open(directory.directory)
	if err != nil {
		return fmt.Errorf("open registry directory for sync: %w", err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("sync registry directory: %w", err)
	}
	return nil
}
