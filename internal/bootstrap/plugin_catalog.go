package bootstrap

import (
	"context"
	"errors"
	"io"
	"strings"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

type PluginCatalog struct {
	catalog *sdk.PluginRegistry
	config  appconfig.Plugins
	logFile io.Closer
}

type PluginInstanceQuery struct {
	Name       string
	InstanceID string
	Version    string
	Resource   string
	Labels     map[string]string
}

func OpenPluginCatalog(
	ctx context.Context,
	config appconfig.Config,
	stderr io.Writer,
) (*PluginCatalog, error) {
	logger, logFile, err := OpenConfiguredLogger(config.Logging, stderr)
	if err != nil {
		return nil, err
	}
	plan, err := BuildPluginPlan(ctx, config, logger, nil, nil)
	if err != nil {
		return nil, errors.Join(err, logFile.Close())
	}
	return &PluginCatalog{
		catalog: plan.Catalog,
		config:  config.Plugins,
		logFile: logFile,
	}, nil
}

func (catalog *PluginCatalog) Close() error {
	if catalog == nil || catalog.logFile == nil {
		return nil
	}
	return catalog.logFile.Close()
}

func (catalog *PluginCatalog) Discover(
	ctx context.Context,
	query sdk.DiscoveryQuery,
) ([]sdk.PluginDescriptor, error) {
	if err := catalog.mustBeOpen(); err != nil {
		return nil, err
	}
	return catalog.catalog.Discover(ctx, query)
}

func (catalog *PluginCatalog) DiscoverConfigured(
	ctx context.Context,
) ([]sdk.PluginDescriptor, error) {
	return catalog.Discover(ctx, sdk.DiscoveryQuery{})
}

func (catalog *PluginCatalog) Resolve(
	ctx context.Context,
	nameOrURI string,
) (sdk.Source, error) {
	if err := catalog.mustBeOpen(); err != nil {
		return nil, err
	}
	return ResolvePluginSelection(ctx, catalog.catalog, catalog.config, nameOrURI)
}

func (catalog *PluginCatalog) Manifest(
	ctx context.Context,
	nameOrURI string,
) (sdk.Manifest, error) {
	source, err := catalog.Resolve(ctx, nameOrURI)
	if err != nil {
		return sdk.Manifest{}, err
	}
	connection, err := source.Open(ctx)
	if err != nil {
		return sdk.Manifest{}, err
	}
	manifest := sdk.CloneManifest(connection.Manifest())
	closeCtx, cancel := closeContext(ctx)
	defer cancel()
	return manifest, connection.Close(closeCtx)
}

func ResolvePluginSelection(
	ctx context.Context,
	catalog *sdk.PluginRegistry,
	config appconfig.Plugins,
	nameOrURI string,
) (sdk.Source, error) {
	if catalog == nil {
		return nil, errors.New("plugin catalog is nil")
	}
	source, err := catalog.Resolve(ctx, nameOrURI)
	if err == nil || strings.Contains(nameOrURI, "://") {
		return source, err
	}
	if strings.TrimSpace(config.RegistryURI) == "" {
		return nil, err
	}
	directory, openErr := OpenPluginDirectory(ctx, config)
	if openErr != nil {
		return nil, openErr
	}
	instance, selectErr := SelectPluginInstance(
		ctx,
		directory,
		config.RegistryNamespace,
		nameOrURI,
	)
	closeCtx, cancel := closeContext(ctx)
	closeErr := directory.Close(closeCtx)
	cancel()
	if selectErr != nil || closeErr != nil {
		return nil, errors.Join(selectErr, closeErr)
	}
	return catalog.Resolve(ctx, instance.URI)
}

func DiscoverPluginInstances(
	ctx context.Context,
	config appconfig.Plugins,
	query PluginInstanceQuery,
) ([]registry.PluginInstance, error) {
	directory, err := OpenPluginDirectory(ctx, config)
	if err != nil {
		return nil, err
	}
	instances, listErr := ListPluginInstances(
		ctx,
		directory,
		registry.DiscoveryQuery{
			Namespace: config.RegistryNamespace,
			Name:      query.Name,
			Version:   query.Version,
			Resource:  query.Resource,
			Labels:    query.Labels,
		},
	)
	if listErr == nil && query.InstanceID != "" {
		instances = filterPluginInstances(instances, query.InstanceID)
	}
	closeCtx, cancel := closeContext(ctx)
	defer cancel()
	return instances, errors.Join(
		listErr,
		directory.Close(closeCtx),
	)
}

func filterPluginInstances(
	instances []registry.PluginInstance,
	instanceID string,
) []registry.PluginInstance {
	if instanceID == "" {
		return instances
	}
	filtered := instances[:0]
	for _, instance := range instances {
		if instance.InstanceID == instanceID {
			filtered = append(filtered, instance)
		}
	}
	return filtered
}

func (catalog *PluginCatalog) mustBeOpen() error {
	if catalog == nil || catalog.catalog == nil {
		return errors.New("plugin catalog is not open")
	}
	return nil
}
