package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lincyaw/ag/sdk"
)

type queryer interface {
	Exec(
		context.Context,
		string,
		...any,
	) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

func normalizePageRequest(
	request sdk.PageRequest,
) (sdk.PageRequest, error) {
	if request.Limit == 0 {
		request.Limit = sdk.DefaultPageSize
	}
	if request.Limit < 0 {
		return sdk.PageRequest{}, errors.New(
			"page limit cannot be negative",
		)
	}
	if request.Limit > sdk.MaxPageSize {
		return sdk.PageRequest{}, fmt.Errorf(
			"page limit %d exceeds maximum %d",
			request.Limit,
			sdk.MaxPageSize,
		)
	}
	return request, nil
}

func isUniqueViolation(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		postgresError.Code == "23505"
}
