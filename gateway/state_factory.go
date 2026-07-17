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
		return sdkstorage.NewDefaultStorageRegistry().Open(ctx, uri)
	}), nil
}
