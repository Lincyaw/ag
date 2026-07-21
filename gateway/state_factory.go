package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type StateBackendFactory interface {
	Open(context.Context, Session) (sdk.StateBackend, error)
}

type StateBackendFactoryFunc func(
	context.Context,
	Session,
) (sdk.StateBackend, error)

func (function StateBackendFactoryFunc) Open(
	ctx context.Context,
	session Session,
) (sdk.StateBackend, error) {
	return function(ctx, session)
}

func NewDuckDBSessionStateFactory(
	root string,
) (StateBackendFactory, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("gateway session state root is empty")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve gateway session state root: %w", err)
	}
	registry := sdkstorage.NewDefaultStorageRegistry()
	return StateBackendFactoryFunc(func(
		ctx context.Context,
		session Session,
	) (sdk.StateBackend, error) {
		if err := sdk.ValidateResourceName("gateway session", session.ID); err != nil {
			return nil, err
		}
		uri := (&url.URL{
			Scheme: "duckdb",
			Path:   filepath.Join(absolute, session.ID+".duckdb"),
			RawQuery: url.Values{
				"namespace": {session.ID},
			}.Encode(),
		}).String()
		return registry.Open(ctx, uri)
	}), nil
}

func NewFileSessionStateFactory(
	root string,
) (StateBackendFactory, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("gateway session state root is empty")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve gateway session state root: %w", err)
	}
	registry := sdkstorage.NewDefaultStorageRegistry()
	return StateBackendFactoryFunc(func(
		ctx context.Context,
		session Session,
	) (sdk.StateBackend, error) {
		uri := (&url.URL{
			Scheme: "file",
			Path:   absolute,
			RawQuery: url.Values{
				"namespace": {session.ID},
			}.Encode(),
		}).String()
		return registry.Open(ctx, uri)
	}), nil
}

// NewStorageSessionStateFactory opens the configured durable backend with one
// namespace per gateway session. A configured base namespace is retained as a
// prefix so multiple gateway deployments can safely share one backend.
func NewStorageSessionStateFactory(
	rawURI string,
) (StateBackendFactory, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil {
		return nil, fmt.Errorf("parse gateway state backend URI: %w", err)
	}
	if strings.TrimSpace(parsed.Scheme) == "" {
		return nil, errors.New("gateway state backend URI has no scheme")
	}
	configuredURI := parsed.String()
	registry := sdkstorage.NewDefaultStorageRegistry()
	return StateBackendFactoryFunc(func(
		ctx context.Context,
		session Session,
	) (sdk.StateBackend, error) {
		sessionURI, err := sessionStateBackendURI(configuredURI, session.ID)
		if err != nil {
			return nil, err
		}
		return registry.Open(ctx, sessionURI)
	}), nil
}

func sessionStateBackendURI(rawURI string, sessionID string) (string, error) {
	if err := sdk.ValidateResourceName("gateway session", sessionID); err != nil {
		return "", err
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil {
		return "", fmt.Errorf("parse gateway state backend URI: %w", err)
	}
	baseNamespace := strings.TrimSpace(parsed.Query().Get("namespace"))
	namespace := sessionID
	if baseNamespace != "" {
		namespace = baseNamespace + "-" + sessionID
	}
	if err := sdk.ValidateResourceName(
		"gateway state namespace",
		namespace,
	); err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("namespace", namespace)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
