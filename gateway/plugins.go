package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

var (
	ErrForbidden       = errors.New("gateway session access forbidden")
	ErrPluginAmbiguous = errors.New("gateway plugin selection is ambiguous")
	ErrPluginNotBound  = errors.New("gateway plugin is not bound")
	ErrBindingStale    = errors.New("gateway plugin binding is stale")
)

type IdleGuard func(context.Context, Session) error

type ManagerConfig struct {
	Store            SessionStore
	Directory        registry.Directory
	DefaultNamespace string
	RequireIdle      IdleGuard
}

type Manager struct {
	store            SessionStore
	directory        registry.Directory
	defaultNamespace string
	requireIdle      IdleGuard
}

func NewManager(config ManagerConfig) (*Manager, error) {
	if config.Store == nil {
		return nil, errors.New("gateway session store is nil")
	}
	if config.Directory == nil {
		return nil, errors.New("gateway plugin directory is nil")
	}
	config.DefaultNamespace = strings.TrimSpace(config.DefaultNamespace)
	if config.DefaultNamespace == "" {
		config.DefaultNamespace = registry.DefaultNamespace
	}
	if err := sdk.ValidateResourceName(
		"registry namespace",
		config.DefaultNamespace,
	); err != nil {
		return nil, err
	}
	return &Manager{
		store:            config.Store,
		directory:        config.Directory,
		defaultNamespace: config.DefaultNamespace,
		requireIdle:      config.RequireIdle,
	}, nil
}

func (manager *Manager) Discover(
	ctx context.Context,
	query registry.DiscoveryQuery,
	request registry.PageRequest,
) (registry.DiscoveryPage, error) {
	if strings.TrimSpace(query.Namespace) == "" {
		query.Namespace = manager.defaultNamespace
	}
	return manager.directory.List(ctx, query, request)
}

func (manager *Manager) AttachPlugin(
	ctx context.Context,
	userID string,
	sessionID string,
	rawSelector string,
	expectedRevision uint64,
) (Session, error) {
	session, err := manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Session{}, err
	}
	if err := manager.checkIdle(ctx, session); err != nil {
		return Session{}, err
	}
	instance, err := manager.selectInstance(
		ctx,
		manager.defaultNamespace,
		rawSelector,
	)
	if err != nil {
		return Session{}, err
	}
	binding := bindingFromInstance(instance)
	replaced := false
	for index := range session.Plugins {
		if session.Plugins[index].Name == binding.Name {
			session.Plugins[index] = binding
			replaced = true
			break
		}
	}
	if !replaced {
		session.Plugins = append(session.Plugins, binding)
	}
	return manager.store.Save(ctx, session, expectedRevision)
}

func (manager *Manager) DetachPlugin(
	ctx context.Context,
	userID string,
	sessionID string,
	name string,
	expectedRevision uint64,
) (Session, error) {
	session, err := manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Session{}, err
	}
	if err := manager.checkIdle(ctx, session); err != nil {
		return Session{}, err
	}
	name = strings.TrimSpace(name)
	if err := sdk.ValidateResourceName("plugin", name); err != nil {
		return Session{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	filtered := make([]PluginBinding, 0, len(session.Plugins))
	found := false
	for _, binding := range session.Plugins {
		if binding.Name == name {
			found = true
			continue
		}
		filtered = append(filtered, binding)
	}
	if !found {
		return Session{}, fmt.Errorf("%w: %s", ErrPluginNotBound, name)
	}
	session.Plugins = filtered
	return manager.store.Save(ctx, session, expectedRevision)
}

func (manager *Manager) ResolvePlugins(
	ctx context.Context,
	session Session,
) ([]sdk.PluginReference, error) {
	references := make([]sdk.PluginReference, 0, len(session.Plugins))
	for _, binding := range session.Plugins {
		instance, err := manager.directory.Get(ctx, registry.InstanceKey{
			Namespace:  binding.Namespace,
			Name:       binding.Name,
			InstanceID: binding.InstanceID,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %s/%s@%s is unavailable",
				ErrBindingStale,
				binding.Namespace,
				binding.Name,
				binding.InstanceID,
			)
		}
		if instance.Epoch != binding.Epoch || instance.URI != binding.URI {
			return nil, fmt.Errorf(
				"%w: %s/%s@%s changed (bound epoch %d, current epoch %d)",
				ErrBindingStale,
				binding.Namespace,
				binding.Name,
				binding.InstanceID,
				binding.Epoch,
				instance.Epoch,
			)
		}
		references = append(references, sdk.PluginReference{
			Name:        binding.Name,
			URI:         binding.URI,
			Description: binding.Manifest.Description,
			Labels:      cloneLabels(binding.Labels),
		})
	}
	return references, nil
}

func (manager *Manager) ownedSession(
	ctx context.Context,
	userID string,
	sessionID string,
) (Session, error) {
	session, err := manager.store.Get(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if strings.TrimSpace(userID) == "" || session.UserID != userID {
		return Session{}, fmt.Errorf("%w: %s", ErrForbidden, sessionID)
	}
	return session, nil
}

func (manager *Manager) checkIdle(ctx context.Context, session Session) error {
	if manager.requireIdle == nil {
		return nil
	}
	if err := manager.requireIdle(ctx, session); err != nil {
		return fmt.Errorf(
			"change gateway session %s composition: %w",
			session.ID,
			err,
		)
	}
	return nil
}

func (manager *Manager) selectInstance(
	ctx context.Context,
	namespace string,
	rawSelector string,
) (registry.PluginInstance, error) {
	name, instanceID, err := parseSelector(rawSelector)
	if err != nil {
		return registry.PluginInstance{}, fmt.Errorf(
			"%w: %v",
			ErrInvalidRequest,
			err,
		)
	}
	if instanceID != "" {
		instance, err := manager.directory.Get(ctx, registry.InstanceKey{
			Namespace:  namespace,
			Name:       name,
			InstanceID: instanceID,
		})
		if err != nil {
			return registry.PluginInstance{}, fmt.Errorf(
				"select gateway plugin %q: %w",
				rawSelector,
				err,
			)
		}
		return instance, nil
	}
	request := registry.PageRequest{Limit: registry.MaxPageSize}
	var instances []registry.PluginInstance
	for {
		page, err := manager.directory.List(
			ctx,
			registry.DiscoveryQuery{Namespace: namespace, Name: name},
			request,
		)
		if err != nil {
			return registry.PluginInstance{}, err
		}
		instances = append(instances, page.Items...)
		if page.Next == "" {
			break
		}
		request.After = page.Next
	}
	switch len(instances) {
	case 0:
		return registry.PluginInstance{}, fmt.Errorf(
			"%w: plugin %q has no active instance",
			registry.ErrInstanceNotFound,
			name,
		)
	case 1:
		return instances[0], nil
	default:
		candidates := make([]string, 0, len(instances))
		for _, instance := range instances {
			candidates = append(
				candidates,
				instance.Name+"@"+instance.InstanceID,
			)
		}
		slices.Sort(candidates)
		return registry.PluginInstance{}, fmt.Errorf(
			"%w: plugin %q has multiple active instances; select one of %s",
			ErrPluginAmbiguous,
			name,
			strings.Join(candidates, ", "),
		)
	}
}

func parseSelector(raw string) (string, string, error) {
	value := strings.TrimSpace(raw)
	name, instanceID, hasInstance := strings.Cut(value, "@")
	name = strings.TrimSpace(name)
	instanceID = strings.TrimSpace(instanceID)
	if err := sdk.ValidateResourceName("plugin", name); err != nil {
		return "", "", err
	}
	if hasInstance {
		if instanceID == "" {
			return "", "", errors.New("plugin selector instance ID is empty")
		}
		if err := sdk.ValidateResourceName(
			"plugin instance",
			instanceID,
		); err != nil {
			return "", "", err
		}
	}
	return name, instanceID, nil
}

func bindingFromInstance(instance registry.PluginInstance) PluginBinding {
	return PluginBinding{
		Namespace:  instance.Namespace,
		Name:       instance.Name,
		InstanceID: instance.InstanceID,
		URI:        instance.URI,
		Manifest:   cloneManifest(instance.Manifest),
		Labels:     cloneLabels(instance.Labels),
		Epoch:      instance.Epoch,
	}
}

func cloneLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
}
