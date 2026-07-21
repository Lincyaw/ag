// Package postgres preserves the direct PostgreSQL constructor while the
// implementation is shared with SQLite through storage/gormstore.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/storage/gormstore"
)

type Config struct {
	ConnectionString string
	Namespace        string
	DisplayURI       string
}

type Backend = gormstore.Store

func NewStateBackend(
	ctx context.Context,
	connectionString string,
) (*Backend, error) {
	return Open(ctx, Config{
		ConnectionString: connectionString,
		Namespace:        "default",
	})
}

func Open(ctx context.Context, config Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connectionString := strings.TrimSpace(config.ConnectionString)
	if connectionString == "" {
		return nil, errors.New("PostgreSQL connection string is empty")
	}
	parsed, err := url.Parse(connectionString)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL connection string: %w", err)
	}
	namespace := strings.TrimSpace(config.Namespace)
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}
	query := parsed.Query()
	query.Del("namespace")
	parsed.RawQuery = query.Encode()
	displayURI := strings.TrimSpace(config.DisplayURI)
	if displayURI == "" {
		display := *parsed
		displayQuery := display.Query()
		displayQuery.Set("namespace", namespace)
		display.RawQuery = displayQuery.Encode()
		displayURI = display.Redacted()
	}
	return gormstore.OpenPostgres(
		parsed.String(),
		namespace,
		displayURI,
	)
}
