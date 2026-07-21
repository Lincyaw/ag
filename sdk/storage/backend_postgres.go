package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/storage/gormstore"
)

func NewPostgresStateBackend(
	ctx context.Context,
	connectionString string,
) (sdk.StateBackend, error) {
	parsed, err := url.Parse(connectionString)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL connection string: %w", err)
	}
	return postgresStorageDriver{scheme: parsed.Scheme}.Open(ctx, parsed)
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
	return openGORMPostgres(ctx, &connectionURL, namespace)
}

func openGORMPostgres(
	ctx context.Context,
	connectionURL *url.URL,
	namespace string,
) (sdk.StateBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	displayURL := *connectionURL
	displayQuery := displayURL.Query()
	displayQuery.Set("namespace", namespace)
	displayURL.RawQuery = displayQuery.Encode()
	return gormstore.OpenPostgres(
		connectionURL.String(),
		namespace,
		displayURL.Redacted(),
	)
}
