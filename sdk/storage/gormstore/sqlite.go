package gormstore

import (
	"fmt"

	"github.com/glebarez/sqlite"
	"github.com/lincyaw/ag/internal/sqlitecoord"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func openSQLite(dsn string) (*gorm.DB, error) {
	openMu := sqlitecoord.OpenMutex()
	openMu.Lock()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	openMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}

	// Enable WAL mode for concurrent reads and busy timeout for write contention.
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB from gorm: %w", err)
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := sqlDB.Exec(pragma); err != nil {
			_ = sqlDB.Close()
			return nil, fmt.Errorf("execute %s: %w", pragma, err)
		}
	}

	return db, nil
}
