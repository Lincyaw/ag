package gormstore

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// OpenPostgres opens the shared GORM state backend with the PostgreSQL
// dialect. displayURI must already be credential-redacted.
func OpenPostgres(
	dsn string,
	namespace string,
	displayURI string,
) (*Store, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database: %w", err)
	}
	return openStore(db, namespace, displayURI)
}
