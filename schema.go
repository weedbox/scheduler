package scheduler

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// schemaLockKey identifies the PostgreSQL advisory lock that serializes the
// scheduler's schema migration across instances ("wbsched" in ASCII).
const schemaLockKey int64 = 0x7762736368656400

// autoMigrate runs GORM's AutoMigrate for models. On PostgreSQL it runs in a
// transaction holding a transaction-scoped advisory lock, so instances that
// start at the same time on an empty database migrate one after another
// instead of racing on CREATE TABLE / CREATE INDEX (the loser would otherwise
// fail with a duplicate key error on the system catalogs). PostgreSQL DDL is
// transactional, and the lock is released on commit or rollback, so it can
// never stay held on a pooled connection. Other dialects migrate directly.
func autoMigrate(ctx context.Context, db *gorm.DB, models ...any) error {
	db = db.WithContext(ctx)
	if db.Dialector.Name() != "postgres" {
		return db.AutoMigrate(models...)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", schemaLockKey).Error; err != nil {
			return fmt.Errorf("scheduler: acquire schema migration lock: %w", err)
		}
		return tx.AutoMigrate(models...)
	})
}
