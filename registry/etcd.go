package registry

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultEtcdPrefix = "/ag/registry/v1"

type EtcdConfig struct {
	Endpoints   []string
	Prefix      string
	DialTimeout time.Duration
	Username    string
	Password    string
	TLS         *tls.Config
	Clock       func() time.Time
	DisplayURI  string
}

type etcdDirectory struct {
	client         *clientv3.Client
	prefix         string
	instancePrefix string
	leasePrefix    string
	epochPrefix    string
	clock          func() time.Time
	displayURI     string
	closed         atomic.Bool
	closeOnce      sync.Once
	closeErr       error
}

type etcdInstanceRecord struct {
	PluginInstance
	LeaseID    string     `json:"lease_id"`
	Token      string     `json:"token"`
	TTLSeconds int64      `json:"ttl_seconds"`
	Removal    ChangeKind `json:"removal,omitempty"`
}

type etcdLeaseIndex struct {
	Key        InstanceKey `json:"key"`
	Token      string      `json:"token"`
	Epoch      uint64      `json:"epoch"`
	TTLSeconds int64       `json:"ttl_seconds"`
}

func NewEtcdDirectory(config EtcdConfig) (Directory, error) {
	if len(config.Endpoints) == 0 {
		return nil, errors.New("etcd registry endpoints are empty")
	}
	endpoints := make([]string, 0, len(config.Endpoints))
	for _, endpoint := range config.Endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return nil, errors.New("etcd registry endpoint is empty")
		}
		endpoints = append(endpoints, endpoint)
	}
	prefix, err := normalizeEtcdPrefix(config.Prefix)
	if err != nil {
		return nil, err
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.DialTimeout < 0 {
		return nil, errors.New("etcd registry dial timeout cannot be negative")
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	tlsConfig := config.TLS
	if tlsConfig != nil {
		tlsConfig = tlsConfig.Clone()
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: config.DialTimeout,
		Username:    config.Username,
		Password:    config.Password,
		TLS:         tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd registry client: %w", err)
	}
	display := strings.TrimSpace(config.DisplayURI)
	if display == "" {
		display = (&url.URL{
			Scheme: "etcd",
			Host:   endpoints[0],
			Path:   prefix,
		}).String()
	}
	return &etcdDirectory{
		client:         client,
		prefix:         prefix,
		instancePrefix: prefix + "/instances/",
		leasePrefix:    prefix + "/leases/",
		epochPrefix:    prefix + "/epochs/",
		clock:          config.Clock,
		displayURI:     display,
	}, nil
}

func (directory *etcdDirectory) Register(
	ctx context.Context,
	registration PluginRegistration,
	options LeaseOptions,
) (PluginLease, error) {
	if err := directory.ready(ctx); err != nil {
		return PluginLease{}, err
	}
	registration, err := normalizeRegistration(registration)
	if err != nil {
		return PluginLease{}, err
	}
	ttlSeconds, err := etcdTTLSeconds(options.TTL)
	if err != nil {
		return PluginLease{}, err
	}
	key := registration.Key()
	instancePath := directory.instancePath(key)
	epochPath := directory.epochPath(key)
	for {
		if err := ctx.Err(); err != nil {
			return PluginLease{}, err
		}
		epochResponse, err := directory.client.Get(ctx, epochPath)
		if err != nil {
			return PluginLease{}, fmt.Errorf(
				"read etcd plugin epoch: %w",
				err,
			)
		}
		epoch, epochCompare, err := nextEtcdEpoch(
			epochPath,
			epochResponse,
		)
		if err != nil {
			return PluginLease{}, err
		}
		grant, err := directory.client.Grant(ctx, ttlSeconds)
		if err != nil {
			return PluginLease{}, fmt.Errorf(
				"grant etcd plugin lease: %w",
				err,
			)
		}
		leaseID := strconv.FormatInt(int64(grant.ID), 10)
		token, err := newLeaseToken()
		if err != nil {
			directory.revokeLease(grant.ID)
			return PluginLease{}, err
		}
		now := directory.clock().UTC()
		expiresAt := now.Add(time.Duration(grant.TTL) * time.Second)
		record := etcdInstanceRecord{
			PluginInstance: PluginInstance{
				PluginRegistration: registration,
				RegisteredAt:       now,
				UpdatedAt:          now,
				ExpiresAt:          expiresAt,
				Epoch:              epoch,
			},
			LeaseID:    leaseID,
			Token:      token,
			TTLSeconds: ttlSeconds,
		}
		index := etcdLeaseIndex{
			Key: key, Token: token, Epoch: epoch,
			TTLSeconds: ttlSeconds,
		}
		recordJSON, err := json.Marshal(record)
		if err != nil {
			directory.revokeLease(grant.ID)
			return PluginLease{}, fmt.Errorf(
				"encode etcd plugin instance: %w",
				err,
			)
		}
		indexJSON, err := json.Marshal(index)
		if err != nil {
			directory.revokeLease(grant.ID)
			return PluginLease{}, fmt.Errorf(
				"encode etcd plugin lease index: %w",
				err,
			)
		}
		transaction, err := directory.client.Txn(ctx).
			If(
				clientv3.Compare(
					clientv3.CreateRevision(instancePath),
					"=",
					0,
				),
				epochCompare,
			).
			Then(
				clientv3.OpPut(
					epochPath,
					strconv.FormatUint(epoch, 10),
				),
				clientv3.OpPut(
					instancePath,
					string(recordJSON),
					clientv3.WithLease(grant.ID),
				),
				clientv3.OpPut(
					directory.leasePath(leaseID),
					string(indexJSON),
					clientv3.WithLease(grant.ID),
				),
			).
			Commit()
		if err != nil {
			directory.revokeLease(grant.ID)
			return PluginLease{}, fmt.Errorf(
				"register etcd plugin instance: %w",
				err,
			)
		}
		if transaction.Succeeded {
			return PluginLease{
				ID: leaseID, Token: token, Key: key,
				ExpiresAt: expiresAt, Epoch: epoch,
			}, nil
		}
		directory.revokeLease(grant.ID)
		existing, err := directory.client.Get(ctx, instancePath)
		if err != nil {
			return PluginLease{}, fmt.Errorf(
				"check etcd plugin registration conflict: %w",
				err,
			)
		}
		if len(existing.Kvs) != 0 {
			return PluginLease{}, fmt.Errorf(
				"%w: %s",
				ErrInstanceConflict,
				key,
			)
		}
	}
}

func (directory *etcdDirectory) Renew(
	ctx context.Context,
	credential LeaseCredential,
	ttl time.Duration,
) (PluginLease, error) {
	if err := directory.ready(ctx); err != nil {
		return PluginLease{}, err
	}
	ttlSeconds, err := etcdTTLSeconds(ttl)
	if err != nil {
		return PluginLease{}, err
	}
	index, indexRevision, record, recordRevision, err :=
		directory.loadLease(ctx, credential)
	if err != nil {
		return PluginLease{}, err
	}
	if record.Removal != "" {
		return PluginLease{}, fmt.Errorf(
			"%w: %s",
			ErrLeaseFenced,
			credential.ID,
		)
	}
	if ttlSeconds == index.TTLSeconds {
		leaseID, err := parseEtcdLeaseID(credential.ID)
		if err != nil {
			return PluginLease{}, err
		}
		keepAlive, err := directory.client.KeepAliveOnce(ctx, leaseID)
		if err != nil {
			return PluginLease{}, classifyEtcdLeaseError(
				credential.ID,
				err,
			)
		}
		if keepAlive == nil || keepAlive.TTL <= 0 {
			return PluginLease{}, fmt.Errorf(
				"%w: %s",
				ErrLeaseExpired,
				credential.ID,
			)
		}
		now := directory.clock().UTC()
		record.UpdatedAt = now
		record.ExpiresAt = now.Add(
			time.Duration(keepAlive.TTL) * time.Second,
		)
		recordJSON, err := json.Marshal(record)
		if err != nil {
			return PluginLease{}, err
		}
		transaction, err := directory.client.Txn(ctx).
			If(
				clientv3.Compare(
					clientv3.ModRevision(
						directory.instancePath(index.Key),
					),
					"=",
					recordRevision,
				),
				clientv3.Compare(
					clientv3.ModRevision(
						directory.leasePath(credential.ID),
					),
					"=",
					indexRevision,
				),
			).
			Then(clientv3.OpPut(
				directory.instancePath(index.Key),
				string(recordJSON),
				clientv3.WithLease(leaseID),
			)).
			Commit()
		if err != nil {
			return PluginLease{}, fmt.Errorf(
				"update etcd plugin lease metadata: %w",
				err,
			)
		}
		if !transaction.Succeeded {
			return PluginLease{}, fmt.Errorf(
				"%w: %s",
				ErrLeaseFenced,
				credential.ID,
			)
		}
		return PluginLease{
			ID: credential.ID, Token: credential.Token,
			Key: index.Key, ExpiresAt: record.ExpiresAt,
			Epoch: index.Epoch,
		}, nil
	}
	return directory.replaceLease(
		ctx,
		credential,
		index,
		indexRevision,
		record,
		recordRevision,
		ttlSeconds,
	)
}

func (directory *etcdDirectory) Unregister(
	ctx context.Context,
	credential LeaseCredential,
) error {
	if err := directory.ready(ctx); err != nil {
		return err
	}
	index, indexRevision, record, recordRevision, err :=
		directory.loadLease(ctx, credential)
	if err != nil {
		return err
	}
	leaseID, err := parseEtcdLeaseID(credential.ID)
	if err != nil {
		return err
	}
	record.Removal = ChangeDelete
	record.UpdatedAt = directory.clock().UTC()
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode etcd plugin delete intent: %w", err)
	}
	instancePath := directory.instancePath(index.Key)
	leasePath := directory.leasePath(credential.ID)
	marked, err := directory.client.Txn(ctx).
		If(
			clientv3.Compare(
				clientv3.ModRevision(instancePath),
				"=",
				recordRevision,
			),
			clientv3.Compare(
				clientv3.ModRevision(leasePath),
				"=",
				indexRevision,
			),
		).
		Then(clientv3.OpPut(
			instancePath,
			string(recordJSON),
			clientv3.WithLease(leaseID),
		)).
		Commit()
	if err != nil {
		return fmt.Errorf("mark etcd plugin unregister: %w", err)
	}
	if !marked.Succeeded {
		return fmt.Errorf("%w: %s", ErrLeaseFenced, credential.ID)
	}
	deleted, err := directory.client.Txn(ctx).
		If(clientv3.Compare(
			clientv3.ModRevision(instancePath),
			"=",
			marked.Header.Revision,
		)).
		Then(
			clientv3.OpDelete(instancePath),
			clientv3.OpDelete(leasePath),
		).
		Commit()
	if err != nil {
		return fmt.Errorf("delete etcd plugin registration: %w", err)
	}
	if !deleted.Succeeded {
		return fmt.Errorf("%w: %s", ErrLeaseFenced, credential.ID)
	}
	directory.revokeLease(leaseID)
	return nil
}

func (directory *etcdDirectory) Get(
	ctx context.Context,
	key InstanceKey,
) (PluginInstance, error) {
	if err := directory.ready(ctx); err != nil {
		return PluginInstance{}, err
	}
	if strings.TrimSpace(key.Namespace) == "" {
		key.Namespace = DefaultNamespace
	}
	if err := validateKey(key); err != nil {
		return PluginInstance{}, err
	}
	response, err := directory.client.Get(
		ctx,
		directory.instancePath(key),
	)
	if err != nil {
		return PluginInstance{}, fmt.Errorf(
			"get etcd plugin instance: %w",
			err,
		)
	}
	if len(response.Kvs) == 0 {
		return PluginInstance{}, fmt.Errorf(
			"%w: %s",
			ErrInstanceNotFound,
			key,
		)
	}
	instance, removal, err := decodeEtcdInstance(response.Kvs[0])
	if err != nil {
		return PluginInstance{}, err
	}
	if removal != "" {
		return PluginInstance{}, fmt.Errorf(
			"%w: %s",
			ErrInstanceNotFound,
			key,
		)
	}
	return instance, nil
}

func (directory *etcdDirectory) List(
	ctx context.Context,
	query DiscoveryQuery,
	request PageRequest,
) (DiscoveryPage, error) {
	if err := directory.ready(ctx); err != nil {
		return DiscoveryPage{}, err
	}
	query, err := normalizeQuery(query)
	if err != nil {
		return DiscoveryPage{}, err
	}
	request, after, err := validatePage(request)
	if err != nil {
		return DiscoveryPage{}, err
	}
	start := directory.instancePrefix
	if after != "" {
		if !strings.HasPrefix(after, directory.instancePrefix) {
			return DiscoveryPage{}, errors.New(
				"registry page cursor belongs to another backend",
			)
		}
		start = after + "\x00"
	}
	rangeEnd := clientv3.GetPrefixRangeEnd(directory.instancePrefix)
	scanSize := int64(max(request.Limit+1, 128))
	var (
		items            []PluginInstance
		itemKeys         []string
		snapshotRevision int64
	)
	for len(items) <= request.Limit {
		options := []clientv3.OpOption{
			clientv3.WithRange(rangeEnd),
			clientv3.WithLimit(scanSize),
			clientv3.WithSort(
				clientv3.SortByKey,
				clientv3.SortAscend,
			),
		}
		if snapshotRevision > 0 {
			options = append(
				options,
				clientv3.WithRev(snapshotRevision),
			)
		}
		response, err := directory.client.Get(ctx, start, options...)
		if err != nil {
			return DiscoveryPage{}, classifyEtcdCursorError(err)
		}
		if snapshotRevision == 0 {
			snapshotRevision = response.Header.Revision
		}
		for _, keyValue := range response.Kvs {
			instance, removal, err := decodeEtcdInstance(keyValue)
			if err != nil {
				return DiscoveryPage{}, err
			}
			if removal == "" && matches(instance, query) {
				items = append(items, instance)
				itemKeys = append(
					itemKeys,
					string(keyValue.Key),
				)
				if len(items) > request.Limit {
					break
				}
			}
		}
		if len(items) > request.Limit ||
			!response.More ||
			len(response.Kvs) == 0 {
			break
		}
		start = string(
			response.Kvs[len(response.Kvs)-1].Key,
		) + "\x00"
	}
	page := DiscoveryPage{
		Items:    items,
		Revision: uint64(snapshotRevision),
	}
	if len(items) > request.Limit {
		page.Items = items[:request.Limit]
		page.Next = encodePageCursor(
			itemKeys[request.Limit-1],
		)
	}
	return page, nil
}

func (directory *etcdDirectory) Poll(
	ctx context.Context,
	request ChangePollRequest,
) (ChangePage, error) {
	if err := directory.ready(ctx); err != nil {
		return ChangePage{}, err
	}
	request, err := validatePoll(request)
	if err != nil {
		return ChangePage{}, err
	}
	if request.AfterRevision > math.MaxInt64 {
		return ChangePage{}, errors.New(
			"registry poll revision exceeds etcd revision range",
		)
	}
	currentResponse, err := directory.client.Get(
		ctx,
		directory.instancePrefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(1),
	)
	if err != nil {
		return ChangePage{}, fmt.Errorf(
			"read etcd registry revision: %w",
			err,
		)
	}
	currentRevision := uint64(currentResponse.Header.Revision)
	if request.AfterRevision > currentRevision {
		return ChangePage{}, fmt.Errorf(
			"poll revision %d is ahead of current revision %d",
			request.AfterRevision,
			currentRevision,
		)
	}
	page := ChangePage{
		NextRevision:    request.AfterRevision,
		CurrentRevision: currentRevision,
	}
	if request.AfterRevision == currentRevision &&
		request.Wait == 0 {
		return page, nil
	}
	watchContext, cancel := context.WithCancel(ctx)
	defer cancel()
	watch := directory.client.Watch(
		watchContext,
		directory.instancePrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(int64(request.AfterRevision+1)),
		clientv3.WithPrevKV(),
		clientv3.WithCreatedNotify(),
	)
	var timeout <-chan time.Time
	var timer *time.Timer
	if request.Wait > 0 {
		timer = time.NewTimer(request.Wait)
		timeout = timer.C
		defer timer.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return ChangePage{}, ctx.Err()
		case <-timeout:
			return page, nil
		case response, ok := <-watch:
			if !ok {
				if err := ctx.Err(); err != nil {
					return ChangePage{}, err
				}
				return ChangePage{}, errors.New(
					"etcd registry watch closed",
				)
			}
			if response.CompactRevision > 0 ||
				errors.Is(response.Err(), rpctypes.ErrCompacted) {
				return ChangePage{}, fmt.Errorf(
					"%w: revision %d compacted at %d",
					ErrCursorExpired,
					request.AfterRevision,
					response.CompactRevision,
				)
			}
			if err := response.Err(); err != nil {
				return ChangePage{}, fmt.Errorf(
					"watch etcd registry: %w",
					err,
				)
			}
			if response.Created {
				if err := directory.client.RequestProgress(ctx); err != nil {
					return ChangePage{}, fmt.Errorf(
						"request etcd registry watch progress: %w",
						err,
					)
				}
				continue
			}
			if response.IsProgressNotify() {
				revision := uint64(response.Header.Revision)
				page.CurrentRevision = max(
					page.CurrentRevision,
					revision,
				)
				if request.AfterRevision < currentRevision {
					page.NextRevision = max(
						page.NextRevision,
						min(revision, currentRevision),
					)
					return page, nil
				}
				continue
			}
			for _, event := range response.Events {
				revision := etcdEventRevision(
					event,
					response.Header.Revision,
				)
				if len(page.Changes) >= request.Limit &&
					uint64(revision) > page.NextRevision {
					return page, nil
				}
				page.NextRevision = max(
					page.NextRevision,
					uint64(revision),
				)
				page.CurrentRevision = max(
					page.CurrentRevision,
					uint64(response.Header.Revision),
				)
				change, include, err := etcdPluginChange(event)
				if err != nil {
					return ChangePage{}, err
				}
				if include && matches(change.Instance, request.Query) {
					page.Changes = append(page.Changes, change)
				}
			}
			if len(response.Events) > 0 &&
				(len(page.Changes) >= request.Limit ||
					page.NextRevision > request.AfterRevision) {
				return page, nil
			}
		}
	}
}

func (*etcdDirectory) Capabilities() Capabilities {
	return Capabilities{
		Durable:          true,
		MultiProcessSafe: true,
		Distributed:      true,
	}
}

func (directory *etcdDirectory) String() string {
	if directory == nil {
		return ""
	}
	return directory.displayURI
}

func (directory *etcdDirectory) Close(context.Context) error {
	if directory == nil {
		return nil
	}
	directory.closeOnce.Do(func() {
		directory.closed.Store(true)
		directory.closeErr = directory.client.Close()
	})
	return directory.closeErr
}

func (directory *etcdDirectory) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if directory == nil || directory.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (directory *etcdDirectory) replaceLease(
	ctx context.Context,
	credential LeaseCredential,
	index etcdLeaseIndex,
	indexRevision int64,
	record etcdInstanceRecord,
	recordRevision int64,
	ttlSeconds int64,
) (PluginLease, error) {
	grant, err := directory.client.Grant(ctx, ttlSeconds)
	if err != nil {
		return PluginLease{}, fmt.Errorf(
			"grant replacement etcd plugin lease: %w",
			err,
		)
	}
	newLeaseID := strconv.FormatInt(int64(grant.ID), 10)
	now := directory.clock().UTC()
	record.LeaseID = newLeaseID
	record.TTLSeconds = ttlSeconds
	record.UpdatedAt = now
	record.ExpiresAt = now.Add(
		time.Duration(grant.TTL) * time.Second,
	)
	newIndex := index
	newIndex.TTLSeconds = ttlSeconds
	recordJSON, err := json.Marshal(record)
	if err != nil {
		directory.revokeLease(grant.ID)
		return PluginLease{}, err
	}
	indexJSON, err := json.Marshal(newIndex)
	if err != nil {
		directory.revokeLease(grant.ID)
		return PluginLease{}, err
	}
	transaction, err := directory.client.Txn(ctx).
		If(
			clientv3.Compare(
				clientv3.ModRevision(
					directory.instancePath(index.Key),
				),
				"=",
				recordRevision,
			),
			clientv3.Compare(
				clientv3.ModRevision(
					directory.leasePath(credential.ID),
				),
				"=",
				indexRevision,
			),
		).
		Then(
			clientv3.OpPut(
				directory.instancePath(index.Key),
				string(recordJSON),
				clientv3.WithLease(grant.ID),
			),
			clientv3.OpPut(
				directory.leasePath(newLeaseID),
				string(indexJSON),
				clientv3.WithLease(grant.ID),
			),
			clientv3.OpDelete(
				directory.leasePath(credential.ID),
			),
		).
		Commit()
	if err != nil {
		directory.revokeLease(grant.ID)
		return PluginLease{}, fmt.Errorf(
			"replace etcd plugin lease: %w",
			err,
		)
	}
	if !transaction.Succeeded {
		directory.revokeLease(grant.ID)
		return PluginLease{}, fmt.Errorf(
			"%w: %s",
			ErrLeaseFenced,
			credential.ID,
		)
	}
	if oldLeaseID, parseErr := parseEtcdLeaseID(
		credential.ID,
	); parseErr == nil {
		directory.revokeLease(oldLeaseID)
	}
	return PluginLease{
		ID: newLeaseID, Token: credential.Token,
		Key: index.Key, ExpiresAt: record.ExpiresAt,
		Epoch: index.Epoch,
	}, nil
}

func (directory *etcdDirectory) loadLease(
	ctx context.Context,
	credential LeaseCredential,
) (
	etcdLeaseIndex,
	int64,
	etcdInstanceRecord,
	int64,
	error,
) {
	if strings.TrimSpace(credential.ID) == "" {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("%w: empty lease ID", ErrLeaseNotFound)
	}
	indexResponse, err := directory.client.Get(
		ctx,
		directory.leasePath(credential.ID),
	)
	if err != nil {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("read etcd plugin lease: %w", err)
	}
	if len(indexResponse.Kvs) == 0 {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("%w: %s", ErrLeaseNotFound, credential.ID)
	}
	var index etcdLeaseIndex
	if err := json.Unmarshal(
		indexResponse.Kvs[0].Value,
		&index,
	); err != nil {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("decode etcd plugin lease: %w", err)
	}
	if !validLeaseToken(index.Token, credential.Token) {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("%w: %s", ErrLeaseFenced, credential.ID)
	}
	instanceResponse, err := directory.client.Get(
		ctx,
		directory.instancePath(index.Key),
	)
	if err != nil {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("read etcd leased plugin instance: %w", err)
	}
	if len(instanceResponse.Kvs) == 0 {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("%w: %s", ErrLeaseExpired, credential.ID)
	}
	var record etcdInstanceRecord
	if err := json.Unmarshal(
		instanceResponse.Kvs[0].Value,
		&record,
	); err != nil {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("decode etcd leased plugin instance: %w", err)
	}
	if record.LeaseID != credential.ID ||
		record.Epoch != index.Epoch ||
		!validLeaseToken(record.Token, credential.Token) {
		return etcdLeaseIndex{}, 0, etcdInstanceRecord{}, 0,
			fmt.Errorf("%w: %s", ErrLeaseFenced, credential.ID)
	}
	record.Revision = uint64(
		instanceResponse.Kvs[0].CreateRevision,
	)
	return index,
		indexResponse.Kvs[0].ModRevision,
		record,
		instanceResponse.Kvs[0].ModRevision,
		nil
}

func (directory *etcdDirectory) instancePath(key InstanceKey) string {
	return directory.instancePrefix +
		key.Namespace + "/" + key.Name + "/" + key.InstanceID
}

func (directory *etcdDirectory) leasePath(id string) string {
	return directory.leasePrefix + id
}

func (directory *etcdDirectory) epochPath(key InstanceKey) string {
	return directory.epochPrefix +
		key.Namespace + "/" + key.Name + "/" + key.InstanceID
}

func (directory *etcdDirectory) revokeLease(id clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = directory.client.Revoke(ctx, id)
}

func normalizeEtcdPrefix(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = defaultEtcdPrefix
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	normalized := path.Clean(value)
	if normalized == "/" {
		return "", errors.New("etcd registry prefix cannot be root")
	}
	return normalized, nil
}

func nextEtcdEpoch(
	key string,
	response *clientv3.GetResponse,
) (uint64, clientv3.Cmp, error) {
	if len(response.Kvs) == 0 {
		return 1,
			clientv3.Compare(clientv3.Version(key), "=", 0),
			nil
	}
	epoch, err := strconv.ParseUint(
		string(response.Kvs[0].Value),
		10,
		64,
	)
	if err != nil || epoch == math.MaxUint64 {
		return 0, clientv3.Cmp{}, fmt.Errorf(
			"decode etcd plugin epoch %q",
			response.Kvs[0].Value,
		)
	}
	return epoch + 1,
		clientv3.Compare(
			clientv3.ModRevision(key),
			"=",
			response.Kvs[0].ModRevision,
		),
		nil
}

func etcdTTLSeconds(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("plugin lease TTL must be positive")
	}
	seconds := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return seconds, nil
}

func decodeEtcdInstance(
	keyValue *mvccpb.KeyValue,
) (PluginInstance, ChangeKind, error) {
	if keyValue == nil {
		return PluginInstance{}, "", errors.New(
			"etcd plugin instance is missing",
		)
	}
	var record etcdInstanceRecord
	if err := json.Unmarshal(keyValue.Value, &record); err != nil {
		return PluginInstance{}, "", fmt.Errorf(
			"decode etcd plugin instance %q: %w",
			keyValue.Key,
			err,
		)
	}
	record.Revision = uint64(keyValue.CreateRevision)
	return cloneInstance(record.PluginInstance), record.Removal, nil
}

func etcdPluginChange(
	event *clientv3.Event,
) (PluginChange, bool, error) {
	if event == nil {
		return PluginChange{}, false, nil
	}
	revision := etcdEventRevision(event, 0)
	switch event.Type {
	case mvccpb.PUT:
		if event.PrevKv != nil {
			return PluginChange{}, false, nil
		}
		instance, removal, err := decodeEtcdInstance(event.Kv)
		if err != nil {
			return PluginChange{}, false, err
		}
		if removal != "" {
			return PluginChange{}, false, nil
		}
		instance.Revision = uint64(revision)
		return PluginChange{
			Revision: uint64(revision),
			Kind:     ChangeUpsert,
			Instance: instance,
		}, true, nil
	case mvccpb.DELETE:
		if event.PrevKv == nil {
			return PluginChange{}, false, fmt.Errorf(
				"%w: previous value for delete revision %d is unavailable",
				ErrCursorExpired,
				revision,
			)
		}
		instance, removal, err := decodeEtcdInstance(
			event.PrevKv,
		)
		if err != nil {
			return PluginChange{}, false, err
		}
		kind := ChangeExpire
		if removal == ChangeDelete {
			kind = ChangeDelete
		}
		instance.UpdatedAt = time.Now().UTC()
		instance.Revision = uint64(revision)
		return PluginChange{
			Revision: uint64(revision),
			Kind:     kind,
			Instance: instance,
		}, true, nil
	default:
		return PluginChange{}, false, nil
	}
}

func etcdEventRevision(event *clientv3.Event, fallback int64) int64 {
	if event != nil && event.Kv != nil &&
		event.Kv.ModRevision > 0 {
		return event.Kv.ModRevision
	}
	return fallback
}

func parseEtcdLeaseID(value string) (clientv3.LeaseID, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf(
			"%w: invalid etcd lease ID %q",
			ErrLeaseNotFound,
			value,
		)
	}
	return clientv3.LeaseID(parsed), nil
}

func classifyEtcdLeaseError(id string, err error) error {
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return fmt.Errorf("%w: %s", ErrLeaseExpired, id)
	}
	return fmt.Errorf("renew etcd plugin lease %q: %w", id, err)
}

func classifyEtcdCursorError(err error) error {
	if errors.Is(err, rpctypes.ErrCompacted) {
		return fmt.Errorf("%w: %v", ErrCursorExpired, err)
	}
	return fmt.Errorf("list etcd plugin instances: %w", err)
}
