package pluginrpc

import (
	"context"
	"errors"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registryServer struct {
	pluginv1.UnimplementedRegistryServiceServer
	registry *sdk.LeaseRegistry
}

func NewRegistryServer(
	registry *sdk.LeaseRegistry,
) (pluginv1.RegistryServiceServer, error) {
	if registry == nil {
		return nil, errors.New("lease registry is nil")
	}
	return &registryServer{registry: registry}, nil
}

func (server *registryServer) Register(
	ctx context.Context,
	request *pluginv1.RegisterRequest,
) (*pluginv1.RegisterResponse, error) {
	registration := request.GetRegistration()
	if registration == nil {
		return nil, status.Error(codes.InvalidArgument, "registration is required")
	}
	manifest, err := fromProtoManifest(registration.GetManifest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	lease, err := server.registry.Register(ctx, sdk.PluginRegistration{
		Name: registration.GetName(), URI: registration.GetUri(), Manifest: manifest,
	}, time.Duration(request.GetTtlMillis())*time.Millisecond)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pluginv1.RegisterResponse{Lease: toProtoLease(lease)}, nil
}

func (server *registryServer) Renew(
	ctx context.Context,
	request *pluginv1.RenewRequest,
) (*pluginv1.RenewResponse, error) {
	lease, err := server.registry.Renew(
		ctx,
		request.GetId(),
		time.Duration(request.GetTtlMillis())*time.Millisecond,
	)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pluginv1.RenewResponse{Lease: toProtoLease(lease)}, nil
}

func (server *registryServer) Unregister(
	ctx context.Context,
	request *pluginv1.UnregisterRequest,
) (*pluginv1.UnregisterResponse, error) {
	if err := server.registry.Unregister(ctx, request.GetId()); err != nil {
		return nil, rpcError(err)
	}
	return &pluginv1.UnregisterResponse{}, nil
}

func (server *registryServer) ListRegistrations(
	ctx context.Context,
	_ *pluginv1.ListRegistrationsRequest,
) (*pluginv1.ListRegistrationsResponse, error) {
	registrations, err := server.registry.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &pluginv1.ListRegistrationsResponse{}
	for _, registration := range registrations {
		response.Registrations = append(response.Registrations, &pluginv1.Registration{
			Name: registration.Name, Uri: registration.URI,
			Manifest: toProtoManifest(registration.Manifest),
		})
	}
	return response, nil
}

type RegistryClient struct {
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
		connection: connection,
		client:     pluginv1.NewRegistryServiceClient(connection),
	}, nil
}

func (client *RegistryClient) Register(
	ctx context.Context,
	registration sdk.PluginRegistration,
	ttl time.Duration,
) (sdk.PluginLease, error) {
	response, err := client.client.Register(ctx, &pluginv1.RegisterRequest{
		Registration: &pluginv1.Registration{
			Name: registration.Name, Uri: registration.URI,
			Manifest: toProtoManifest(registration.Manifest),
		},
		TtlMillis: ttl.Milliseconds(),
	})
	if err != nil {
		return sdk.PluginLease{}, err
	}
	return fromProtoLease(response.GetLease())
}

func (client *RegistryClient) Renew(
	ctx context.Context,
	id string,
	ttl time.Duration,
) (sdk.PluginLease, error) {
	response, err := client.client.Renew(ctx, &pluginv1.RenewRequest{
		Id: id, TtlMillis: ttl.Milliseconds(),
	})
	if err != nil {
		return sdk.PluginLease{}, err
	}
	return fromProtoLease(response.GetLease())
}

func (client *RegistryClient) Unregister(ctx context.Context, id string) error {
	_, err := client.client.Unregister(ctx, &pluginv1.UnregisterRequest{Id: id})
	return err
}

func (client *RegistryClient) List(
	ctx context.Context,
) ([]sdk.PluginRegistration, error) {
	response, err := client.client.ListRegistrations(ctx, &pluginv1.ListRegistrationsRequest{})
	if err != nil {
		return nil, err
	}
	result := make([]sdk.PluginRegistration, 0, len(response.GetRegistrations()))
	for _, registration := range response.GetRegistrations() {
		manifest, err := fromProtoManifest(registration.GetManifest())
		if err != nil {
			return nil, err
		}
		result = append(result, sdk.PluginRegistration{
			Name: registration.GetName(), URI: registration.GetUri(), Manifest: manifest,
		})
	}
	return result, nil
}

func (client *RegistryClient) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func toProtoLease(lease sdk.PluginLease) *pluginv1.Lease {
	return &pluginv1.Lease{Id: lease.ID, ExpiresUnixMilli: unixMilli(lease.ExpiresAt)}
}

func fromProtoLease(lease *pluginv1.Lease) (sdk.PluginLease, error) {
	if lease == nil || lease.GetId() == "" {
		return sdk.PluginLease{}, errors.New("lease is missing")
	}
	return sdk.PluginLease{ID: lease.GetId(), ExpiresAt: fromUnixMilli(lease.GetExpiresUnixMilli())}, nil
}
