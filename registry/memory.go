package registry

import (
	"context"
	"sync"
	"time"
)

type MemoryConfig struct {
	Clock      func() time.Time
	MaxChanges int
}

type memoryDirectory struct {
	mu         sync.Mutex
	state      directoryState
	clock      func() time.Time
	maxChanges int
	changed    chan struct{}
	closed     bool
}

func NewMemoryDirectory(config MemoryConfig) Directory {
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	if config.MaxChanges < 1 {
		config.MaxChanges = 1024
	}
	return &memoryDirectory{
		state:      newDirectoryState(),
		clock:      config.Clock,
		maxChanges: config.MaxChanges,
		changed:    make(chan struct{}),
	}
}

func (directory *memoryDirectory) Register(
	ctx context.Context,
	registration PluginRegistration,
	options LeaseOptions,
) (PluginLease, error) {
	if err := ctx.Err(); err != nil {
		return PluginLease{}, err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.closed {
		return PluginLease{}, ErrClosed
	}
	before := directory.state.Revision
	lease, _, err := directory.state.register(
		registration,
		options.TTL,
		directory.clock().UTC(),
		directory.maxChanges,
	)
	directory.signalIfChanged(before)
	return lease, err
}

func (directory *memoryDirectory) Renew(
	ctx context.Context,
	credential LeaseCredential,
	ttl time.Duration,
) (PluginLease, error) {
	if err := ctx.Err(); err != nil {
		return PluginLease{}, err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.closed {
		return PluginLease{}, ErrClosed
	}
	before := directory.state.Revision
	lease, _, err := directory.state.renew(
		credential,
		ttl,
		directory.clock().UTC(),
		directory.maxChanges,
	)
	directory.signalIfChanged(before)
	return lease, err
}

func (directory *memoryDirectory) Unregister(
	ctx context.Context,
	credential LeaseCredential,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.closed {
		return ErrClosed
	}
	before := directory.state.Revision
	_, err := directory.state.unregister(
		credential,
		directory.clock().UTC(),
		directory.maxChanges,
	)
	directory.signalIfChanged(before)
	return err
}

func (directory *memoryDirectory) Get(
	ctx context.Context,
	key InstanceKey,
) (PluginInstance, error) {
	if err := ctx.Err(); err != nil {
		return PluginInstance{}, err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.closed {
		return PluginInstance{}, ErrClosed
	}
	before := directory.state.Revision
	instance, _, err := directory.state.get(
		key,
		directory.clock().UTC(),
		directory.maxChanges,
	)
	directory.signalIfChanged(before)
	return instance, err
}

func (directory *memoryDirectory) List(
	ctx context.Context,
	query DiscoveryQuery,
	request PageRequest,
) (DiscoveryPage, error) {
	if err := ctx.Err(); err != nil {
		return DiscoveryPage{}, err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.closed {
		return DiscoveryPage{}, ErrClosed
	}
	before := directory.state.Revision
	page, _, err := directory.state.list(
		query,
		request,
		directory.clock().UTC(),
		directory.maxChanges,
	)
	directory.signalIfChanged(before)
	return page, err
}

func (directory *memoryDirectory) Poll(
	ctx context.Context,
	request ChangePollRequest,
) (ChangePage, error) {
	request, err := validatePoll(request)
	if err != nil {
		return ChangePage{}, invalidRequest(err)
	}
	deadline := time.Now().Add(request.Wait)
	for {
		if err := ctx.Err(); err != nil {
			return ChangePage{}, err
		}
		directory.mu.Lock()
		if directory.closed {
			directory.mu.Unlock()
			return ChangePage{}, ErrClosed
		}
		before := directory.state.Revision
		page, _, pollErr := directory.state.poll(
			request,
			directory.clock().UTC(),
			directory.maxChanges,
		)
		directory.signalIfChanged(before)
		if pollErr != nil || len(page.Changes) > 0 ||
			page.NextRevision > request.AfterRevision ||
			request.Wait == 0 {
			directory.mu.Unlock()
			return page, pollErr
		}
		changed := directory.changed
		nextExpiry := directory.state.nextExpiry(directory.clock().UTC())
		directory.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return page, nil
		}
		wakeForExpiry := nextExpiry > 0 && nextExpiry < remaining
		if wakeForExpiry {
			remaining = min(remaining, nextExpiry)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ChangePage{}, ctx.Err()
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			if !wakeForExpiry {
				return page, nil
			}
		}
	}
}

func (*memoryDirectory) Capabilities() Capabilities {
	return Capabilities{}
}

func (*memoryDirectory) String() string { return "memory://local" }

func (directory *memoryDirectory) Close(context.Context) error {
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.closed {
		return nil
	}
	directory.closed = true
	close(directory.changed)
	return nil
}

func (directory *memoryDirectory) signalIfChanged(before uint64) {
	if directory.state.Revision == before {
		return
	}
	close(directory.changed)
	directory.changed = make(chan struct{})
}
