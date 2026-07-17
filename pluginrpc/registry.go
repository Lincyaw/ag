package pluginrpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registryServer struct {
	pluginv1.UnimplementedRegistryServiceServer
	directory registry.Directory
}

func NewRegistryServer(
	directory registry.Directory,
) (pluginv1.RegistryServiceServer, error) {
	if directory == nil {
		return nil, errors.New("plugin directory is nil")
	}
	return &registryServer{directory: directory}, nil
}

func (server *registryServer) Register(
	ctx context.Context,
	request *pluginv1.RegisterRequest,
) (*pluginv1.RegisterResponse, error) {
	registration, err := fromProtoRegistration(request.GetRegistration())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ttl, err := positiveMillis(request.GetTtlMillis(), "plugin lease TTL")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	lease, err := server.directory.Register(
		ctx,
		registration,
		registry.LeaseOptions{TTL: ttl},
	)
	if err != nil {
		return nil, registryRPCError(err)
	}
	return &pluginv1.RegisterResponse{Lease: toProtoLease(lease)}, nil
}

func (server *registryServer) Renew(
	ctx context.Context,
	request *pluginv1.RenewRequest,
) (*pluginv1.RenewResponse, error) {
	ttl, err := positiveMillis(request.GetTtlMillis(), "plugin lease TTL")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	lease, err := server.directory.Renew(
		ctx,
		registry.LeaseCredential{
			ID: request.GetId(), Token: request.GetToken(),
		},
		ttl,
	)
	if err != nil {
		return nil, registryRPCError(err)
	}
	return &pluginv1.RenewResponse{Lease: toProtoLease(lease)}, nil
}

func (server *registryServer) Unregister(
	ctx context.Context,
	request *pluginv1.UnregisterRequest,
) (*pluginv1.UnregisterResponse, error) {
	if err := server.directory.Unregister(ctx, registry.LeaseCredential{
		ID: request.GetId(), Token: request.GetToken(),
	}); err != nil {
		return nil, registryRPCError(err)
	}
	return &pluginv1.UnregisterResponse{}, nil
}

func (server *registryServer) GetRegistration(
	ctx context.Context,
	request *pluginv1.GetRegistrationRequest,
) (*pluginv1.GetRegistrationResponse, error) {
	instance, err := server.directory.Get(
		ctx,
		fromProtoInstanceKey(request.GetKey()),
	)
	if err != nil {
		return nil, registryRPCError(err)
	}
	return &pluginv1.GetRegistrationResponse{
		Instance: toProtoInstance(instance),
	}, nil
}

func (server *registryServer) ListRegistrations(
	ctx context.Context,
	request *pluginv1.ListRegistrationsRequest,
) (*pluginv1.ListRegistrationsResponse, error) {
	if request.GetPageSize() > uint32(registry.MaxPageSize) {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"registry page size cannot exceed %d",
			registry.MaxPageSize,
		)
	}
	page, err := server.directory.List(
		ctx,
		fromProtoDiscoveryQuery(request.GetQuery()),
		registry.PageRequest{
			After: request.GetPageToken(),
			Limit: int(request.GetPageSize()),
		},
	)
	if err != nil {
		return nil, registryRPCError(err)
	}
	response := &pluginv1.ListRegistrationsResponse{
		NextPageToken: page.Next,
		Revision:      page.Revision,
		Instances:     make([]*pluginv1.PluginInstance, 0, len(page.Items)),
		Registrations: make([]*pluginv1.Registration, 0, len(page.Items)),
	}
	for _, instance := range page.Items {
		response.Instances = append(
			response.Instances,
			toProtoInstance(instance),
		)
		response.Registrations = append(
			response.Registrations,
			toProtoRegistration(instance.PluginRegistration),
		)
	}
	return response, nil
}

func (server *registryServer) PollRegistrations(
	ctx context.Context,
	request *pluginv1.PollRegistrationsRequest,
) (*pluginv1.PollRegistrationsResponse, error) {
	if request.GetLimit() > uint32(registry.MaxPageSize) {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"registry poll limit cannot exceed %d",
			registry.MaxPageSize,
		)
	}
	wait, err := nonNegativeMillis(
		request.GetWaitMillis(),
		"registry poll wait",
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	page, err := server.directory.Poll(ctx, registry.ChangePollRequest{
		Query:         fromProtoDiscoveryQuery(request.GetQuery()),
		AfterRevision: request.GetAfterRevision(),
		Limit:         int(request.GetLimit()),
		Wait:          wait,
	})
	if err != nil {
		return nil, registryRPCError(err)
	}
	response := &pluginv1.PollRegistrationsResponse{
		Changes:         make([]*pluginv1.RegistrationChange, 0, len(page.Changes)),
		NextRevision:    page.NextRevision,
		CurrentRevision: page.CurrentRevision,
	}
	for _, change := range page.Changes {
		response.Changes = append(response.Changes, toProtoChange(change))
	}
	return response, nil
}

type RegistryClient struct {
	uri        string
	connection *grpc.ClientConn
	client     pluginv1.RegistryServiceClient
}

func NewRegistryClient(
	ctx context.Context,
	uri string,
	config ClientConfig,
) (*RegistryClient, error) {
	parsed, err := parseSourceURI(uri)
	if err != nil {
		return nil, err
	}
	connection, err := dial(ctx, parsed, config)
	if err != nil {
		return nil, err
	}
	return &RegistryClient{
		uri:        parsed.String(),
		connection: connection,
		client:     pluginv1.NewRegistryServiceClient(connection),
	}, nil
}

func (client *RegistryClient) Register(
	ctx context.Context,
	registration registry.PluginRegistration,
	options registry.LeaseOptions,
) (registry.PluginLease, error) {
	ttlMillis, err := durationMillis(options.TTL, false, "plugin lease TTL")
	if err != nil {
		return registry.PluginLease{}, err
	}
	response, err := client.client.Register(ctx, &pluginv1.RegisterRequest{
		Registration: toProtoRegistration(registration),
		TtlMillis:    ttlMillis,
	})
	if err != nil {
		return registry.PluginLease{}, err
	}
	return fromProtoLease(response.GetLease())
}

func (client *RegistryClient) Renew(
	ctx context.Context,
	credential registry.LeaseCredential,
	ttl time.Duration,
) (registry.PluginLease, error) {
	ttlMillis, err := durationMillis(ttl, false, "plugin lease TTL")
	if err != nil {
		return registry.PluginLease{}, err
	}
	response, err := client.client.Renew(ctx, &pluginv1.RenewRequest{
		Id: credential.ID, Token: credential.Token,
		TtlMillis: ttlMillis,
	})
	if err != nil {
		return registry.PluginLease{}, err
	}
	return fromProtoLease(response.GetLease())
}

func (client *RegistryClient) Unregister(
	ctx context.Context,
	credential registry.LeaseCredential,
) error {
	_, err := client.client.Unregister(ctx, &pluginv1.UnregisterRequest{
		Id: credential.ID, Token: credential.Token,
	})
	return err
}

func (client *RegistryClient) Get(
	ctx context.Context,
	key registry.InstanceKey,
) (registry.PluginInstance, error) {
	response, err := client.client.GetRegistration(
		ctx,
		&pluginv1.GetRegistrationRequest{Key: toProtoInstanceKey(key)},
	)
	if err != nil {
		return registry.PluginInstance{}, err
	}
	return fromProtoInstance(response.GetInstance())
}

func (client *RegistryClient) List(
	ctx context.Context,
	query registry.DiscoveryQuery,
	request registry.PageRequest,
) (registry.DiscoveryPage, error) {
	pageSize, err := sizeToUint32(
		request.Limit,
		registry.MaxPageSize,
		"registry page size",
	)
	if err != nil {
		return registry.DiscoveryPage{}, err
	}
	response, err := client.client.ListRegistrations(
		ctx,
		&pluginv1.ListRegistrationsRequest{
			Query:     toProtoDiscoveryQuery(query),
			PageToken: request.After,
			PageSize:  pageSize,
		},
	)
	if err != nil {
		return registry.DiscoveryPage{}, err
	}
	page := registry.DiscoveryPage{
		Next:     response.GetNextPageToken(),
		Revision: response.GetRevision(),
		Items:    make([]registry.PluginInstance, 0, len(response.GetInstances())),
	}
	if len(response.GetInstances()) == 0 {
		for _, registration := range response.GetRegistrations() {
			converted, convertErr := fromProtoRegistration(registration)
			if convertErr != nil {
				return registry.DiscoveryPage{}, convertErr
			}
			page.Items = append(page.Items, registry.PluginInstance{
				PluginRegistration: converted,
			})
		}
		return page, nil
	}
	for _, instance := range response.GetInstances() {
		converted, convertErr := fromProtoInstance(instance)
		if convertErr != nil {
			return registry.DiscoveryPage{}, convertErr
		}
		page.Items = append(page.Items, converted)
	}
	return page, nil
}

func (client *RegistryClient) Poll(
	ctx context.Context,
	request registry.ChangePollRequest,
) (registry.ChangePage, error) {
	limit, err := sizeToUint32(
		request.Limit,
		registry.MaxPageSize,
		"registry poll limit",
	)
	if err != nil {
		return registry.ChangePage{}, err
	}
	waitMillis, err := durationMillis(
		request.Wait,
		true,
		"registry poll wait",
	)
	if err != nil {
		return registry.ChangePage{}, err
	}
	response, err := client.client.PollRegistrations(
		ctx,
		&pluginv1.PollRegistrationsRequest{
			Query:         toProtoDiscoveryQuery(request.Query),
			AfterRevision: request.AfterRevision,
			Limit:         limit,
			WaitMillis:    waitMillis,
		},
	)
	if err != nil {
		return registry.ChangePage{}, err
	}
	page := registry.ChangePage{
		Changes:         make([]registry.PluginChange, 0, len(response.GetChanges())),
		NextRevision:    response.GetNextRevision(),
		CurrentRevision: response.GetCurrentRevision(),
	}
	for _, change := range response.GetChanges() {
		converted, convertErr := fromProtoChange(change)
		if convertErr != nil {
			return registry.ChangePage{}, convertErr
		}
		page.Changes = append(page.Changes, converted)
	}
	return page, nil
}

func (*RegistryClient) Capabilities() registry.Capabilities {
	return registry.Capabilities{
		Distributed: true,
		Poll:        true,
	}
}

func (client *RegistryClient) String() string {
	if client == nil {
		return ""
	}
	return client.uri
}

func (client *RegistryClient) Close(context.Context) error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func toProtoRegistration(
	registration registry.PluginRegistration,
) *pluginv1.Registration {
	return &pluginv1.Registration{
		Namespace:  registration.Namespace,
		Name:       registration.Name,
		InstanceId: registration.InstanceID,
		Uri:        registration.URI,
		Manifest:   toProtoManifest(registration.Manifest),
		Labels:     registration.Labels,
	}
}

func fromProtoRegistration(
	registration *pluginv1.Registration,
) (registry.PluginRegistration, error) {
	if registration == nil {
		return registry.PluginRegistration{}, errors.New("registration is required")
	}
	manifest, err := fromProtoManifest(registration.GetManifest())
	if err != nil {
		return registry.PluginRegistration{}, err
	}
	return registry.PluginRegistration{
		Namespace:  registration.GetNamespace(),
		Name:       registration.GetName(),
		InstanceID: registration.GetInstanceId(),
		URI:        registration.GetUri(),
		Manifest:   manifest,
		Labels:     registration.GetLabels(),
	}, nil
}

func toProtoInstanceKey(key registry.InstanceKey) *pluginv1.InstanceKey {
	return &pluginv1.InstanceKey{
		Namespace: key.Namespace, Name: key.Name, InstanceId: key.InstanceID,
	}
}

func fromProtoInstanceKey(key *pluginv1.InstanceKey) registry.InstanceKey {
	if key == nil {
		return registry.InstanceKey{}
	}
	return registry.InstanceKey{
		Namespace:  key.GetNamespace(),
		Name:       key.GetName(),
		InstanceID: key.GetInstanceId(),
	}
}

func toProtoInstance(instance registry.PluginInstance) *pluginv1.PluginInstance {
	return &pluginv1.PluginInstance{
		Registration:        toProtoRegistration(instance.PluginRegistration),
		RegisteredUnixMilli: unixMilli(instance.RegisteredAt),
		UpdatedUnixMilli:    unixMilli(instance.UpdatedAt),
		ExpiresUnixMilli:    unixMilli(instance.ExpiresAt),
		Revision:            instance.Revision,
		Epoch:               instance.Epoch,
	}
}

func fromProtoInstance(
	instance *pluginv1.PluginInstance,
) (registry.PluginInstance, error) {
	if instance == nil {
		return registry.PluginInstance{}, errors.New("plugin instance is missing")
	}
	registration, err := fromProtoRegistration(instance.GetRegistration())
	if err != nil {
		return registry.PluginInstance{}, err
	}
	return registry.PluginInstance{
		PluginRegistration: registration,
		RegisteredAt:       fromUnixMilli(instance.GetRegisteredUnixMilli()),
		UpdatedAt:          fromUnixMilli(instance.GetUpdatedUnixMilli()),
		ExpiresAt:          fromUnixMilli(instance.GetExpiresUnixMilli()),
		Revision:           instance.GetRevision(),
		Epoch:              instance.GetEpoch(),
	}, nil
}

func toProtoLease(lease registry.PluginLease) *pluginv1.Lease {
	return &pluginv1.Lease{
		Id: lease.ID, Token: lease.Token,
		Key:              toProtoInstanceKey(lease.Key),
		ExpiresUnixMilli: unixMilli(lease.ExpiresAt),
		Epoch:            lease.Epoch,
	}
}

func fromProtoLease(
	lease *pluginv1.Lease,
) (registry.PluginLease, error) {
	if lease == nil || lease.GetId() == "" || lease.GetToken() == "" {
		return registry.PluginLease{}, errors.New("plugin lease is missing")
	}
	return registry.PluginLease{
		ID:        lease.GetId(),
		Token:     lease.GetToken(),
		Key:       fromProtoInstanceKey(lease.GetKey()),
		ExpiresAt: fromUnixMilli(lease.GetExpiresUnixMilli()),
		Epoch:     lease.GetEpoch(),
	}, nil
}

func toProtoDiscoveryQuery(
	query registry.DiscoveryQuery,
) *pluginv1.DiscoveryQuery {
	return &pluginv1.DiscoveryQuery{
		Namespace: query.Namespace,
		Name:      query.Name,
		Version:   query.Version,
		Resource:  query.Resource,
		Labels:    query.Labels,
	}
}

func fromProtoDiscoveryQuery(
	query *pluginv1.DiscoveryQuery,
) registry.DiscoveryQuery {
	if query == nil {
		return registry.DiscoveryQuery{}
	}
	return registry.DiscoveryQuery{
		Namespace: query.GetNamespace(),
		Name:      query.GetName(),
		Version:   query.GetVersion(),
		Resource:  query.GetResource(),
		Labels:    query.GetLabels(),
	}
}

func toProtoChange(change registry.PluginChange) *pluginv1.RegistrationChange {
	var kind pluginv1.RegistrationChangeKind
	switch change.Kind {
	case registry.ChangeUpsert:
		kind = pluginv1.RegistrationChangeKind_REGISTRATION_CHANGE_KIND_UPSERT
	case registry.ChangeDelete:
		kind = pluginv1.RegistrationChangeKind_REGISTRATION_CHANGE_KIND_DELETE
	case registry.ChangeExpire:
		kind = pluginv1.RegistrationChangeKind_REGISTRATION_CHANGE_KIND_EXPIRE
	}
	return &pluginv1.RegistrationChange{
		Revision: change.Revision,
		Kind:     kind,
		Instance: toProtoInstance(change.Instance),
	}
}

func fromProtoChange(
	change *pluginv1.RegistrationChange,
) (registry.PluginChange, error) {
	if change == nil {
		return registry.PluginChange{}, errors.New("registry change is missing")
	}
	var kind registry.ChangeKind
	switch change.GetKind() {
	case pluginv1.RegistrationChangeKind_REGISTRATION_CHANGE_KIND_UPSERT:
		kind = registry.ChangeUpsert
	case pluginv1.RegistrationChangeKind_REGISTRATION_CHANGE_KIND_DELETE:
		kind = registry.ChangeDelete
	case pluginv1.RegistrationChangeKind_REGISTRATION_CHANGE_KIND_EXPIRE:
		kind = registry.ChangeExpire
	default:
		return registry.PluginChange{}, fmt.Errorf(
			"registry change kind %s is unsupported",
			change.GetKind(),
		)
	}
	instance, err := fromProtoInstance(change.GetInstance())
	if err != nil {
		return registry.PluginChange{}, err
	}
	return registry.PluginChange{
		Revision: change.GetRevision(),
		Kind:     kind,
		Instance: instance,
	}, nil
}

func positiveMillis(value int64, name string) (time.Duration, error) {
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return millis(value, name)
}

func nonNegativeMillis(value int64, name string) (time.Duration, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s cannot be negative", name)
	}
	return millis(value, name)
}

func millis(value int64, name string) (time.Duration, error) {
	if value > math.MaxInt64/int64(time.Millisecond) {
		return 0, fmt.Errorf("%s is too large", name)
	}
	return time.Duration(value) * time.Millisecond, nil
}

func sizeToUint32(value, maximum int, name string) (uint32, error) {
	if value < 0 || value > maximum || uint64(value) > math.MaxUint32 {
		return 0, fmt.Errorf("%s is out of range", name)
	}
	return uint32(value), nil
}

func durationMillis(
	value time.Duration,
	allowZero bool,
	name string,
) (int64, error) {
	if value < 0 || (!allowZero && value == 0) {
		if allowZero {
			return 0, fmt.Errorf("%s cannot be negative", name)
		}
		return 0, fmt.Errorf("%s must be positive", name)
	}
	if value == 0 {
		return 0, nil
	}
	result := value / time.Millisecond
	if value%time.Millisecond != 0 {
		result++
	}
	return int64(result), nil
}

func registryRPCError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, registry.ErrInstanceNotFound),
		errors.Is(err, registry.ErrLeaseNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, registry.ErrInstanceConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, registry.ErrLeaseExpired),
		errors.Is(err, registry.ErrLeaseFenced):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, registry.ErrCursorExpired):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, registry.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

var _ registry.Directory = (*RegistryClient)(nil)
