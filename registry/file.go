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
	"sync/atomic"
	"time"

	"github.com/lincyaw/ag/internal/filestate"
)

type FileConfig struct {
	Directory    string
	Clock        func() time.Time
	PollInterval time.Duration
	MaxChanges   int
}

type fileDirectory struct {
	directory    string
	statePath    string
	lockPath     string
	clock        func() time.Time
	pollInterval time.Duration
	maxChanges   int
	closed       atomic.Bool
}

func NewFileDirectory(config FileConfig) (Directory, error) {
	absolute, err := filestate.PrepareDirectory(
		"registry",
		config.Directory,
	)
	if err != nil {
		return nil, err
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
	result := &fileDirectory{
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

func (directory *fileDirectory) Register(
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

func (directory *fileDirectory) Renew(
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

func (directory *fileDirectory) Unregister(
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

func (directory *fileDirectory) Get(
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

func (directory *fileDirectory) List(
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

func (directory *fileDirectory) Poll(
	ctx context.Context,
	request ChangePollRequest,
) (ChangePage, error) {
	request, err := validatePoll(request)
	if err != nil {
		return ChangePage{}, invalidRequest(err)
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

func (*fileDirectory) Capabilities() Capabilities {
	return Capabilities{
		Durable:          true,
		MultiProcessSafe: filestate.MultiProcessSafe,
	}
}

func (directory *fileDirectory) String() string {
	return (&url.URL{Scheme: "file", Path: directory.directory}).String()
}

func (directory *fileDirectory) Close(context.Context) error {
	directory.closed.Store(true)
	return nil
}

func (directory *fileDirectory) withState(
	ctx context.Context,
	action func(*directoryState) (bool, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if directory.closed.Load() {
		return ErrClosed
	}
	return filestate.WithExclusiveLock(directory.lockPath, func() error {
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

func (directory *fileDirectory) readState() (directoryState, error) {
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

func (directory *fileDirectory) writeState(
	ctx context.Context,
	state directoryState,
) error {
	return filestate.WriteJSON(
		ctx,
		directory.directory,
		directory.statePath,
		"registry state",
		state,
	)
}
