package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

type PluginSelector struct {
	Name       string
	InstanceID string
}

func ParsePluginSelector(raw string) (PluginSelector, error) {
	value := strings.TrimSpace(raw)
	name, instanceID, hasInstance := strings.Cut(value, "@")
	name = strings.TrimSpace(name)
	instanceID = strings.TrimSpace(instanceID)
	if err := sdk.ValidateResourceName("plugin", name); err != nil {
		return PluginSelector{}, err
	}
	if hasInstance {
		if instanceID == "" {
			return PluginSelector{}, errors.New(
				"plugin selector instance ID is empty",
			)
		}
		if err := sdk.ValidateResourceName(
			"plugin instance",
			instanceID,
		); err != nil {
			return PluginSelector{}, err
		}
	}
	return PluginSelector{Name: name, InstanceID: instanceID}, nil
}

func OpenPluginDirectory(
	ctx context.Context,
	config appconfig.Plugins,
) (registry.Directory, error) {
	uri := strings.TrimSpace(config.RegistryURI)
	if uri == "" {
		return nil, errors.New(
			"plugin registry URI is not configured; set --registry-uri",
		)
	}
	directory, err := pluginrpc.NewRegistryClient(
		ctx,
		uri,
		pluginrpc.ClientConfig{},
	)
	if err != nil {
		return nil, fmt.Errorf("connect plugin registry: %w", err)
	}
	return directory, nil
}

func OpenGatewayPluginDirectory(
	ctx context.Context,
	config appconfig.Config,
) (registry.Directory, string, error) {
	if strings.TrimSpace(config.Plugins.RegistryURI) != "" {
		directory, err := OpenPluginDirectory(ctx, config.Plugins)
		return directory, config.Plugins.RegistryURI, err
	}
	directory, err := registry.NewDefaultBackendRegistry().Open(
		ctx,
		config.Registry.BackendURI,
	)
	if err != nil {
		return nil, "", fmt.Errorf(
			"open embedded gateway plugin directory: %w",
			err,
		)
	}
	return directory, directory.String(), nil
}

func ListPluginInstances(
	ctx context.Context,
	directory registry.Directory,
	query registry.DiscoveryQuery,
) ([]registry.PluginInstance, error) {
	request := registry.PageRequest{Limit: registry.MaxPageSize}
	var result []registry.PluginInstance
	for {
		page, err := directory.List(ctx, query, request)
		if err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.Next == "" {
			return result, nil
		}
		request.After = page.Next
	}
}

func SelectPluginInstance(
	ctx context.Context,
	directory registry.Directory,
	namespace string,
	rawSelector string,
) (registry.PluginInstance, error) {
	selector, err := ParsePluginSelector(rawSelector)
	if err != nil {
		return registry.PluginInstance{}, err
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = registry.DefaultNamespace
	}
	if selector.InstanceID != "" {
		instance, err := directory.Get(ctx, registry.InstanceKey{
			Namespace:  namespace,
			Name:       selector.Name,
			InstanceID: selector.InstanceID,
		})
		if err != nil {
			return registry.PluginInstance{}, fmt.Errorf(
				"select plugin %q: %w",
				rawSelector,
				err,
			)
		}
		return instance, nil
	}
	instances, err := ListPluginInstances(
		ctx,
		directory,
		registry.DiscoveryQuery{
			Namespace: namespace,
			Name:      selector.Name,
		},
	)
	if err != nil {
		return registry.PluginInstance{}, fmt.Errorf(
			"discover plugin %q: %w",
			selector.Name,
			err,
		)
	}
	switch len(instances) {
	case 0:
		return registry.PluginInstance{}, fmt.Errorf(
			"plugin %q has no active instance in namespace %q",
			selector.Name,
			namespace,
		)
	case 1:
		return instances[0], nil
	default:
		candidates := make([]string, 0, len(instances))
		for _, instance := range instances {
			candidates = append(candidates, fmt.Sprintf(
				"%s@%s=%s",
				instance.Name,
				instance.InstanceID,
				instance.URI,
			))
		}
		slices.Sort(candidates)
		return registry.PluginInstance{}, fmt.Errorf(
			"plugin %q is ambiguous in namespace %q; select one: %s",
			selector.Name,
			namespace,
			strings.Join(candidates, ", "),
		)
	}
}
