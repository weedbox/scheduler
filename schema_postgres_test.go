package scheduler

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Instances starting at the same time on an empty database must not race on
// CREATE TABLE / CREATE INDEX. Each goroutine uses its own *gorm.DB (its own
// pool), like separate processes would.
func TestPostgresConcurrentSchemaMigration(t *testing.T) {
	const instances = 6

	cases := []struct {
		name  string
		start func(db *gorm.DB) error
	}{
		{"GormStorage", func(db *gorm.DB) error {
			return NewGormStorage(db).Initialize(context.Background())
		}},
		{"PostgresScheduler", func(db *gorm.DB) error {
			s := NewPostgresScheduler(db, func(context.Context, JobEvent) error { return nil }, NewBasicScheduleCodec())
			if err := s.Start(context.Background()); err != nil {
				return err
			}
			return s.Stop(context.Background())
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetPostgresTables(t, newPostgresTestDB(t))

			// Open the pools up front so connecting is not part of the race window.
			dbs := make([]*gorm.DB, instances)
			for i := range dbs {
				dbs[i] = newPostgresTestDB(t)
				sqlDB, err := dbs[i].DB()
				require.NoError(t, err)
				require.NoError(t, sqlDB.Ping())
			}
			errs := make([]error, instances)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range instances {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs[i] = tc.start(dbs[i])
				}()
			}
			close(start)
			wg.Wait()

			for i, err := range errs {
				require.NoError(t, err, "instance %d", i)
			}
		})
	}
}
