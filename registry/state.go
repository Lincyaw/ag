package registry

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const stateSchemaVersion uint32 = 1

type storedInstance struct {
	PluginInstance
	LeaseID string `json:"lease_id"`
}

type storedLease struct {
	PluginLease
}

type directoryState struct {
	SchemaVersion uint32                    `json:"schema_version"`
	Revision      uint64                    `json:"revision"`
	Instances     map[string]storedInstance `json:"instances"`
	Leases        map[string]storedLease    `json:"leases"`
	Epochs        map[string]uint64         `json:"epochs"`
	Changes       []PluginChange            `json:"changes"`
}

func newDirectoryState() directoryState {
	return directoryState{
		SchemaVersion: stateSchemaVersion,
		Instances:     make(map[string]storedInstance),
		Leases:        make(map[string]storedLease),
		Epochs:        make(map[string]uint64),
	}
}

func (state *directoryState) normalize() {
	if state.SchemaVersion == 0 {
		state.SchemaVersion = stateSchemaVersion
	}
	if state.Instances == nil {
		state.Instances = make(map[string]storedInstance)
	}
	if state.Leases == nil {
		state.Leases = make(map[string]storedLease)
	}
	if state.Epochs == nil {
		state.Epochs = make(map[string]uint64)
	}
	if state.Changes == nil {
		state.Changes = []PluginChange{}
	}
}

func (state *directoryState) register(
	registration PluginRegistration,
	ttl time.Duration,
	now time.Time,
	maxChanges int,
) (PluginLease, bool, error) {
	if ttl <= 0 {
		return PluginLease{}, false, errors.New("plugin lease TTL must be positive")
	}
	registration, err := normalizeRegistration(registration)
	if err != nil {
		return PluginLease{}, false, err
	}
	changed := state.pruneExpired(now, maxChanges)
	key := instanceMapKey(registration.Key())
	if _, exists := state.Instances[key]; exists {
		return PluginLease{}, changed, fmt.Errorf(
			"%w: %s",
			ErrInstanceConflict,
			registration.Key(),
		)
	}
	token, err := newLeaseToken()
	if err != nil {
		return PluginLease{}, changed, err
	}
	epoch := state.Epochs[key] + 1
	state.Epochs[key] = epoch
	state.Revision++
	lease := PluginLease{
		ID:        sdk.NewID(),
		Token:     token,
		Key:       registration.Key(),
		ExpiresAt: now.Add(ttl),
		Epoch:     epoch,
	}
	instance := PluginInstance{
		PluginRegistration: registration,
		RegisteredAt:       now,
		UpdatedAt:          now,
		ExpiresAt:          lease.ExpiresAt,
		Revision:           state.Revision,
		Epoch:              epoch,
	}
	state.Instances[key] = storedInstance{
		PluginInstance: instance,
		LeaseID:        lease.ID,
	}
	state.Leases[lease.ID] = storedLease{PluginLease: lease}
	state.appendChange(PluginChange{
		Revision: state.Revision,
		Kind:     ChangeUpsert,
		Instance: instance,
	}, maxChanges)
	return lease, true, nil
}

func (state *directoryState) renew(
	credential LeaseCredential,
	ttl time.Duration,
	now time.Time,
	maxChanges int,
) (PluginLease, bool, error) {
	if ttl <= 0 {
		return PluginLease{}, false, errors.New("plugin lease TTL must be positive")
	}
	lease, exists := state.Leases[credential.ID]
	if !exists {
		return PluginLease{}, false, fmt.Errorf(
			"%w: %s",
			ErrLeaseNotFound,
			credential.ID,
		)
	}
	if !lease.ExpiresAt.After(now) {
		state.expireLease(lease, now, maxChanges)
		return PluginLease{}, true, fmt.Errorf(
			"%w: %s",
			ErrLeaseExpired,
			credential.ID,
		)
	}
	if !validLeaseToken(lease.Token, credential.Token) {
		return PluginLease{}, false, fmt.Errorf(
			"%w: %s",
			ErrLeaseFenced,
			credential.ID,
		)
	}
	key := instanceMapKey(lease.Key)
	instance, exists := state.Instances[key]
	if !exists || instance.LeaseID != lease.ID {
		return PluginLease{}, false, fmt.Errorf(
			"registry state has no instance for lease %q",
			lease.ID,
		)
	}
	lease.ExpiresAt = now.Add(ttl)
	state.Leases[credential.ID] = lease
	instance.ExpiresAt = lease.ExpiresAt
	instance.UpdatedAt = now
	state.Instances[key] = instance
	return lease.PluginLease, true, nil
}

func (state *directoryState) unregister(
	credential LeaseCredential,
	now time.Time,
	maxChanges int,
) (bool, error) {
	lease, exists := state.Leases[credential.ID]
	if !exists {
		return false, fmt.Errorf("%w: %s", ErrLeaseNotFound, credential.ID)
	}
	if !lease.ExpiresAt.After(now) {
		state.expireLease(lease, now, maxChanges)
		return true, fmt.Errorf("%w: %s", ErrLeaseExpired, credential.ID)
	}
	if !validLeaseToken(lease.Token, credential.Token) {
		return false, fmt.Errorf("%w: %s", ErrLeaseFenced, credential.ID)
	}
	state.removeLease(lease, ChangeDelete, now, maxChanges)
	return true, nil
}

func (state *directoryState) get(
	key InstanceKey,
	now time.Time,
	maxChanges int,
) (PluginInstance, bool, error) {
	if strings.TrimSpace(key.Namespace) == "" {
		key.Namespace = DefaultNamespace
	}
	if err := validateKey(key); err != nil {
		return PluginInstance{}, false, err
	}
	changed := state.pruneExpired(now, maxChanges)
	instance, exists := state.Instances[instanceMapKey(key)]
	if !exists {
		return PluginInstance{}, changed, fmt.Errorf(
			"%w: %s",
			ErrInstanceNotFound,
			key,
		)
	}
	return cloneInstance(instance.PluginInstance), changed, nil
}

func (state *directoryState) list(
	query DiscoveryQuery,
	request PageRequest,
	now time.Time,
	maxChanges int,
) (DiscoveryPage, bool, error) {
	query, err := normalizeQuery(query)
	if err != nil {
		return DiscoveryPage{}, false, err
	}
	request, after, err := validatePage(request)
	if err != nil {
		return DiscoveryPage{}, false, err
	}
	changed := state.pruneExpired(now, maxChanges)
	keys := make([]string, 0, len(state.Instances))
	for key, instance := range state.Instances {
		if key > after && matches(instance.PluginInstance, query) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	page := DiscoveryPage{Revision: state.Revision}
	limit := min(request.Limit, len(keys))
	page.Items = make([]PluginInstance, 0, limit)
	for _, key := range keys[:limit] {
		page.Items = append(
			page.Items,
			cloneInstance(state.Instances[key].PluginInstance),
		)
	}
	if len(keys) > request.Limit {
		page.Next = encodePageCursor(keys[request.Limit-1])
	}
	return page, changed, nil
}

func (state *directoryState) poll(
	request ChangePollRequest,
	now time.Time,
	maxChanges int,
) (ChangePage, bool, error) {
	request, err := validatePoll(request)
	if err != nil {
		return ChangePage{}, false, err
	}
	changed := state.pruneExpired(now, maxChanges)
	if request.AfterRevision > state.Revision {
		return ChangePage{}, changed, fmt.Errorf(
			"poll revision %d is ahead of current revision %d",
			request.AfterRevision,
			state.Revision,
		)
	}
	if request.AfterRevision > 0 && len(state.Changes) > 0 &&
		request.AfterRevision+1 < state.Changes[0].Revision {
		return ChangePage{}, changed, fmt.Errorf(
			"%w: revision %d",
			ErrCursorExpired,
			request.AfterRevision,
		)
	}
	page := ChangePage{
		NextRevision:    request.AfterRevision,
		CurrentRevision: state.Revision,
	}
	for _, change := range state.Changes {
		if change.Revision <= request.AfterRevision {
			continue
		}
		page.NextRevision = change.Revision
		if matches(change.Instance, request.Query) {
			page.Changes = append(page.Changes, cloneChange(change))
			if len(page.Changes) == request.Limit {
				return page, changed, nil
			}
		}
	}
	page.NextRevision = state.Revision
	return page, changed, nil
}

func (state *directoryState) pruneExpired(now time.Time, maxChanges int) bool {
	var expired []string
	for id, lease := range state.Leases {
		if !lease.ExpiresAt.After(now) {
			expired = append(expired, id)
		}
	}
	slices.Sort(expired)
	for _, id := range expired {
		lease, exists := state.Leases[id]
		if exists {
			state.expireLease(lease, now, maxChanges)
		}
	}
	return len(expired) > 0
}

func (state *directoryState) nextExpiry(now time.Time) time.Duration {
	var result time.Duration
	for _, lease := range state.Leases {
		remaining := lease.ExpiresAt.Sub(now)
		if remaining <= 0 {
			return 0
		}
		if result == 0 || remaining < result {
			result = remaining
		}
	}
	return result
}

func (state *directoryState) expireLease(
	lease storedLease,
	now time.Time,
	maxChanges int,
) {
	state.removeLease(lease, ChangeExpire, now, maxChanges)
}

func (state *directoryState) removeLease(
	lease storedLease,
	kind ChangeKind,
	now time.Time,
	maxChanges int,
) {
	key := instanceMapKey(lease.Key)
	instance, exists := state.Instances[key]
	delete(state.Leases, lease.ID)
	if !exists || instance.LeaseID != lease.ID {
		return
	}
	delete(state.Instances, key)
	state.Revision++
	instance.UpdatedAt = now
	instance.Revision = state.Revision
	state.appendChange(PluginChange{
		Revision: state.Revision,
		Kind:     kind,
		Instance: instance.PluginInstance,
	}, maxChanges)
}

func (state *directoryState) appendChange(
	change PluginChange,
	maxChanges int,
) {
	state.Changes = append(state.Changes, cloneChange(change))
	if overflow := len(state.Changes) - maxChanges; overflow > 0 {
		state.Changes = slices.Clone(state.Changes[overflow:])
	}
}

func validLeaseToken(expected, actual string) bool {
	if expected == "" || len(expected) != len(actual) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}

func newLeaseToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate plugin lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func (state *directoryState) validate() error {
	state.normalize()
	if state.SchemaVersion > stateSchemaVersion {
		return fmt.Errorf(
			"registry schema version %d exceeds supported version %d",
			state.SchemaVersion,
			stateSchemaVersion,
		)
	}
	for key, stored := range state.Instances {
		registration, err := normalizeRegistration(stored.PluginRegistration)
		if err != nil {
			return fmt.Errorf("validate registry instance %q: %w", key, err)
		}
		if instanceMapKey(registration.Key()) != key {
			return fmt.Errorf("registry instance key %q does not match payload", key)
		}
		lease, exists := state.Leases[stored.LeaseID]
		if !exists || lease.Key != registration.Key() ||
			lease.Epoch != stored.Epoch ||
			lease.ExpiresAt != stored.ExpiresAt {
			return fmt.Errorf("registry instance %q has an invalid lease", key)
		}
		if state.Epochs[key] < stored.Epoch {
			return fmt.Errorf("registry instance %q exceeds recorded epoch", key)
		}
	}
	for id, lease := range state.Leases {
		if id != lease.ID || lease.Token == "" {
			return fmt.Errorf("registry lease %q is invalid", id)
		}
		instance, exists := state.Instances[instanceMapKey(lease.Key)]
		if !exists || instance.LeaseID != id {
			return fmt.Errorf("registry lease %q has no matching instance", id)
		}
	}
	var revision uint64
	for _, change := range state.Changes {
		if change.Revision <= revision || change.Revision > state.Revision {
			return errors.New("registry change revisions are invalid")
		}
		switch change.Kind {
		case ChangeUpsert, ChangeDelete, ChangeExpire:
		default:
			return fmt.Errorf("registry change kind %q is invalid", change.Kind)
		}
		revision = change.Revision
	}
	return nil
}
