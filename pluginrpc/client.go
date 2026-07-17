package pluginrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultMaxMessageBytes = 16 << 20

type ClientConfig struct {
	TLSConfig       *tls.Config
	MaxMessageBytes int
	DialOptions     []grpc.DialOption
	RegistryURI     string
}

type source struct {
	uri    url.URL
	config ClientConfig
}

func normalizeClientConfig(config ClientConfig) (ClientConfig, error) {
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.MaxMessageBytes < 1 {
		return ClientConfig{}, errors.New("RPC max message bytes must be positive")
	}
	config.RegistryURI = strings.TrimSpace(config.RegistryURI)
	config.DialOptions = append([]grpc.DialOption(nil), config.DialOptions...)
	if config.TLSConfig != nil {
		config.TLSConfig = config.TLSConfig.Clone()
	}
	return config, nil
}

func NewSource(uri string, config ClientConfig) (sdk.Source, error) {
	return newSource(uri, config)
}

func newSource(uri string, config ClientConfig) (*source, error) {
	parsed, err := parseSourceURI(uri)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "grpc" && parsed.Scheme != "grpcs" {
		return nil, fmt.Errorf("unsupported plugin RPC scheme %q", parsed.Scheme)
	}
	config, err = normalizeClientConfig(config)
	if err != nil {
		return nil, err
	}
	return &source{uri: *parsed, config: config}, nil
}

func (source *source) String() string {
	if source == nil {
		return ""
	}
	return source.uri.String()
}

func (source *source) Open(ctx context.Context) (sdk.Connection, error) {
	if source == nil {
		return nil, errors.New("RPC source is nil")
	}
	connection, err := dial(ctx, &source.uri, source.config)
	if err != nil {
		return nil, fmt.Errorf("create plugin RPC client: %w", err)
	}
	client := pluginv1.NewPluginServiceClient(connection)
	description, err := client.Describe(ctx, &pluginv1.DescribeRequest{})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("describe remote plugin: %w", err),
			connection.Close(),
		)
	}
	remote, err := newRemoteConnection(connection, client, description)
	if err != nil {
		return nil, errors.Join(err, connection.Close())
	}
	return remote, nil
}

type driver struct {
	scheme string
	config ClientConfig
}

func NewDriver(scheme string, config ClientConfig) (sdk.PluginDriver, error) {
	return newDriver(scheme, config)
}

func newDriver(scheme string, config ClientConfig) (*driver, error) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if scheme != "grpc" && scheme != "grpcs" {
		return nil, fmt.Errorf("unsupported plugin RPC driver scheme %q", scheme)
	}
	var err error
	config, err = normalizeClientConfig(config)
	if err != nil {
		return nil, err
	}
	return &driver{scheme: scheme, config: config}, nil
}

func RegisterDrivers(registry *sdk.PluginRegistry, config ClientConfig) error {
	if registry == nil {
		return errors.New("plugin registry is nil")
	}
	drivers := make([]sdk.PluginDriver, 0, 2)
	for _, scheme := range []string{"grpc", "grpcs"} {
		driver, err := NewDriver(scheme, config)
		if err != nil {
			return err
		}
		drivers = append(drivers, driver)
	}
	return registry.RegisterDrivers(drivers...)
}

func (driver *driver) Scheme() string { return driver.scheme }

func (driver *driver) Resolve(
	_ context.Context,
	reference sdk.PluginReference,
) (sdk.Source, error) {
	source, err := newSource(reference.URI, driver.config)
	if err != nil {
		return nil, err
	}
	if source.uri.Scheme != driver.scheme {
		return nil, fmt.Errorf("%s driver cannot resolve %q", driver.scheme, reference.URI)
	}
	return source, nil
}

func (driver *driver) Discover(
	ctx context.Context,
	query sdk.DiscoveryQuery,
) ([]sdk.PluginReference, error) {
	if strings.TrimSpace(driver.config.RegistryURI) == "" {
		return nil, nil
	}
	parsed, err := parseSourceURI(driver.config.RegistryURI)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != driver.scheme {
		return nil, nil
	}
	if len(query.Labels) != 0 {
		return nil, nil
	}
	client, err := NewRegistryClient(ctx, driver.config.RegistryURI, driver.config)
	if err != nil {
		return nil, err
	}
	var instances []registry.PluginInstance
	pageRequest := registry.PageRequest{Limit: registry.MaxPageSize}
	for {
		page, listErr := client.List(ctx, registry.DiscoveryQuery{
			Name: query.Name, Labels: query.Labels,
		}, pageRequest)
		if listErr != nil {
			err = listErr
			break
		}
		instances = append(instances, page.Items...)
		if page.Next == "" {
			break
		}
		pageRequest.After = page.Next
	}
	err = errors.Join(err, client.Close(ctx))
	if err != nil {
		return nil, err
	}
	result := make([]sdk.PluginReference, 0, len(instances))
	for _, instance := range instances {
		result = append(result, sdk.PluginReference{
			Name: instance.Name, URI: instance.URI,
			Description: instance.Manifest.Description,
			Labels:      instance.Labels,
		})
	}
	return result, nil
}

type remoteConnection struct {
	connection   *grpc.ClientConn
	client       pluginv1.PluginServiceClient
	manifest     sdk.Manifest
	providers    []sdk.ProviderSpec
	tools        []sdk.ToolSpec
	hooks        []sdk.HookSpec
	subscribers  []sdk.SubscriberSpec
	capabilities []sdk.CapabilitySpec
	events       []sdk.EventContract
}

func newRemoteConnection(
	connection *grpc.ClientConn,
	client pluginv1.PluginServiceClient,
	description *pluginv1.DescribeResponse,
) (*remoteConnection, error) {
	manifest, err := fromProtoManifest(description.GetManifest())
	if err != nil {
		return nil, fmt.Errorf("decode remote manifest: %w", err)
	}
	remote := &remoteConnection{connection: connection, client: client, manifest: manifest}
	for _, spec := range description.GetProviders() {
		remote.providers = append(remote.providers, fromProtoProviderSpec(spec))
	}
	for _, spec := range description.GetTools() {
		remote.tools = append(remote.tools, fromProtoToolSpec(spec))
	}
	for _, spec := range description.GetHooks() {
		remote.hooks = append(remote.hooks, fromProtoHookSpec(spec))
	}
	for _, spec := range description.GetSubscribers() {
		remote.subscribers = append(remote.subscribers, fromProtoSubscriberSpec(spec))
	}
	for _, spec := range description.GetCapabilities() {
		remote.capabilities = append(remote.capabilities, fromProtoCapabilitySpec(spec))
	}
	for _, contract := range description.GetEvents() {
		remote.events = append(remote.events, fromProtoEventContract(contract))
	}
	return remote, nil
}

func (remote *remoteConnection) Manifest() sdk.Manifest { return remote.manifest }

func (remote *remoteConnection) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	for _, spec := range remote.providers {
		if err := registrar.RegisterProvider(remoteProvider{spec: spec, client: remote.client}); err != nil {
			return err
		}
	}
	for _, spec := range remote.tools {
		if err := registrar.RegisterTool(remoteTool{spec: spec, client: remote.client}); err != nil {
			return err
		}
	}
	for _, spec := range remote.hooks {
		if err := registrar.RegisterHook(remoteHook{spec: spec, client: remote.client}); err != nil {
			return err
		}
	}
	for _, spec := range remote.subscribers {
		if err := registrar.RegisterSubscriber(remoteSubscriber{spec: spec, client: remote.client}); err != nil {
			return err
		}
	}
	for _, spec := range remote.capabilities {
		if err := registrar.RegisterCapability(remoteCapability{spec: spec, client: remote.client}); err != nil {
			return err
		}
	}
	for _, contract := range remote.events {
		if err := registrar.RegisterEvent(contract); err != nil {
			return err
		}
	}
	return nil
}

func (remote *remoteConnection) Close(context.Context) error {
	return remote.connection.Close()
}

type remoteProvider struct {
	spec   sdk.ProviderSpec
	client pluginv1.PluginServiceClient
}

func (provider remoteProvider) Spec() sdk.ProviderSpec { return provider.spec }

func (provider remoteProvider) SubmitCompletion(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return submitRemote(ctx, provider.client, sdk.OperationKindProvider, provider.spec.Name, request)
}

func (provider remoteProvider) PollCompletion(
	ctx context.Context,
	id string,
	afterRevision uint64,
) (sdk.Operation, error) {
	return pollRemote(ctx, provider.client, sdk.OperationKindProvider, provider.spec.Name, id, afterRevision)
}

func (provider remoteProvider) CancelCompletion(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return cancelRemote(ctx, provider.client, sdk.OperationKindProvider, provider.spec.Name, id)
}

type remoteTool struct {
	spec   sdk.ToolSpec
	client pluginv1.PluginServiceClient
}

func (tool remoteTool) Spec() sdk.ToolSpec { return tool.spec }

func (tool remoteTool) SubmitCall(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return submitRemote(ctx, tool.client, sdk.OperationKindTool, tool.spec.Name, request)
}

func (tool remoteTool) PollCall(
	ctx context.Context,
	id string,
	afterRevision uint64,
) (sdk.Operation, error) {
	return pollRemote(ctx, tool.client, sdk.OperationKindTool, tool.spec.Name, id, afterRevision)
}

func (tool remoteTool) CancelCall(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return cancelRemote(ctx, tool.client, sdk.OperationKindTool, tool.spec.Name, id)
}

type remoteHook struct {
	spec   sdk.HookSpec
	client pluginv1.PluginServiceClient
}

func (hook remoteHook) Spec() sdk.HookSpec { return hook.spec }

func (hook remoteHook) Handle(ctx context.Context, event sdk.Event) (sdk.Effect, error) {
	converted, err := toProtoEvent(event)
	if err != nil {
		return sdk.Effect{}, err
	}
	response, err := hook.client.HandleHook(ctx, &pluginv1.HandleHookRequest{
		Hook: hook.spec.Name, Event: converted,
	})
	if err != nil {
		return sdk.Effect{}, err
	}
	return fromProtoEffect(response.GetEffect())
}

type remoteSubscriber struct {
	spec   sdk.SubscriberSpec
	client pluginv1.PluginServiceClient
}

func (subscriber remoteSubscriber) Spec() sdk.SubscriberSpec { return subscriber.spec }

func (subscriber remoteSubscriber) Receive(ctx context.Context, delivery sdk.Delivery) error {
	converted, err := toProtoDelivery(delivery)
	if err != nil {
		return err
	}
	response, err := subscriber.client.Deliver(ctx, &pluginv1.DeliverRequest{Delivery: converted})
	if err != nil {
		return err
	}
	if !response.GetAccepted() {
		return errors.New("remote inbox did not accept delivery")
	}
	return nil
}

type remoteCapability struct {
	spec   sdk.CapabilitySpec
	client pluginv1.PluginServiceClient
}

func (capability remoteCapability) Spec() sdk.CapabilitySpec { return capability.spec }

func (capability remoteCapability) SubmitInvoke(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return submitRemote(ctx, capability.client, sdk.OperationKindCapability, capability.spec.Name, request)
}

func (capability remoteCapability) PollInvoke(
	ctx context.Context,
	id string,
	afterRevision uint64,
) (sdk.Operation, error) {
	return pollRemote(ctx, capability.client, sdk.OperationKindCapability, capability.spec.Name, id, afterRevision)
}

func (capability remoteCapability) CancelInvoke(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return cancelRemote(ctx, capability.client, sdk.OperationKindCapability, capability.spec.Name, id)
}

func submitRemote(
	ctx context.Context,
	client pluginv1.PluginServiceClient,
	kind sdk.OperationKind,
	resource string,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	input, err := rawToStruct(request.Input)
	if err != nil {
		return sdk.Operation{}, err
	}
	response, err := client.SubmitOperation(ctx, &pluginv1.SubmitOperationRequest{
		Kind:     toProtoOperationKind(kind),
		Resource: resource,
		Request: &pluginv1.OperationRequest{
			IdempotencyKey: request.IdempotencyKey,
			Input:          input,
		},
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	return fromProtoOperation(response.GetOperation())
}

func pollRemote(
	ctx context.Context,
	client pluginv1.PluginServiceClient,
	kind sdk.OperationKind,
	resource string,
	id string,
	afterRevision uint64,
) (sdk.Operation, error) {
	response, err := client.PollOperation(ctx, &pluginv1.PollOperationRequest{
		Kind: toProtoOperationKind(kind), Resource: resource,
		Id: id, AfterRevision: afterRevision,
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	return fromProtoOperation(response.GetOperation())
}

func cancelRemote(
	ctx context.Context,
	client pluginv1.PluginServiceClient,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	response, err := client.CancelOperation(ctx, &pluginv1.CancelOperationRequest{
		Kind: toProtoOperationKind(kind), Resource: resource, Id: id,
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	return fromProtoOperation(response.GetOperation())
}

func parseSourceURI(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("parse plugin RPC URI: %w", err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Host == "" {
		return nil, fmt.Errorf("plugin RPC URI %q has no host", raw)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return nil, fmt.Errorf("plugin RPC URI %q must not contain a path", raw)
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed, nil
}

func dial(
	ctx context.Context,
	parsed *url.URL,
	config ClientConfig,
) (*grpc.ClientConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := normalizeClientConfig(config)
	if err != nil {
		return nil, err
	}
	var transportCredentials credentials.TransportCredentials
	if parsed.Scheme == "grpc" {
		transportCredentials = insecure.NewCredentials()
	} else if parsed.Scheme == "grpcs" {
		configuration := config.TLSConfig
		if configuration == nil {
			configuration = &tls.Config{
				MinVersion: tls.VersionTLS12,
				ServerName: parsed.Hostname(),
			}
		} else {
			configuration = configuration.Clone()
			if configuration.ServerName == "" {
				configuration.ServerName = parsed.Hostname()
			}
		}
		transportCredentials = credentials.NewTLS(configuration)
	} else {
		return nil, fmt.Errorf("unsupported plugin RPC scheme %q", parsed.Scheme)
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(config.MaxMessageBytes),
		),
	}
	options = append(options, config.DialOptions...)
	connection, err := grpc.NewClient(parsed.Host, options...)
	if err != nil {
		return nil, fmt.Errorf("create RPC client: %w", err)
	}
	return connection, nil
}
