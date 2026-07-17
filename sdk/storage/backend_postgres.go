package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/lincyaw/ag/sdk"
	postgresstore "github.com/lincyaw/ag/sdk/storage/postgres"
)

func NewPostgresStateBackend(
	ctx context.Context,
	connectionString string,
) (sdk.StateBackend, error) {
	return postgresstore.NewStateBackend(ctx, connectionString)
}

type postgresStorageDriver struct {
	scheme string
}

func (driver postgresStorageDriver) Scheme() string {
	return driver.scheme
}

func (driver postgresStorageDriver) Open(
	ctx context.Context,
	parsed *url.URL,
) (sdk.StateBackend, error) {
	if parsed == nil {
		return nil, errors.New("PostgreSQL storage URI is nil")
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return nil, fmt.Errorf(
			"unsupported PostgreSQL storage scheme %q",
			parsed.Scheme,
		)
	}
	namespace := strings.TrimSpace(parsed.Query().Get("namespace"))
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName(
		"storage namespace",
		namespace,
	); err != nil {
		return nil, err
	}
	connectionURL := *parsed
	connectionQuery := connectionURL.Query()
	connectionQuery.Del("namespace")
	connectionURL.RawQuery = connectionQuery.Encode()
	displayURL := *parsed
	displayQuery := displayURL.Query()
	displayQuery.Set("namespace", namespace)
	displayURL.RawQuery = displayQuery.Encode()
	return postgresstore.Open(ctx, postgresstore.Config{
		ConnectionString: connectionURL.String(),
		Namespace:        namespace,
		DisplayURI:       displayURL.Redacted(),
	})
}
