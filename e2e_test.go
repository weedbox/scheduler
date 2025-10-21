package scheduler

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestGormStoragePersistenceAcrossRestart verifies that jobs persisted with GORM
// are restored and continue executing after the scheduler restarts.
func TestGormStoragePersistenceAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler.db")

	openDB := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
		require.NoError(t, err)
		return db
	}

	state := newTestHandlerState()
	var runCount int32
	state.SetJobFunc(func(ctx context.Context, event JobEvent) error {
		if event.ID() == "persisted-job" {
			atomic.AddInt32(&runCount, 1)
		}
		return nil
	})

	interval, err := NewIntervalSchedule(200 * time.Millisecond)
	require.NoError(t, err)

	// First scheduler instance: add job and wait for at least one execution.
	db1 := openDB()
	storage1 := NewGormStorage(db1)
	scheduler1 := NewScheduler(storage1, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler1.Start(ctx))
	require.NoError(t, scheduler1.AddJob("persisted-job", interval, nil))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&runCount) >= 1
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, scheduler1.Stop(ctx))

	beforeRestart := atomic.LoadInt32(&runCount)
	require.GreaterOrEqual(t, beforeRestart, int32(1))

	// Second scheduler instance: ensure job is reloaded and runs again.
	db2 := openDB()
	storage2 := NewGormStorage(db2)
	scheduler2 := NewScheduler(storage2, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler2.Start(ctx))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&runCount) >= beforeRestart+1
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, scheduler2.Stop(ctx))
}

// TestGormStorageOnceJobExecutedOnlyOnce verifies that a once schedule executes on the first run
// and does not execute again after the scheduler restarts.
func TestGormStorageOnceJobExecutedOnlyOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler_once.db")

	openDB := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
		require.NoError(t, err)
		return db
	}

	state := newTestHandlerState()
	var runCount int32
	state.SetJobFunc(func(ctx context.Context, event JobEvent) error {
		if event.ID() == "one-time-job" {
			atomic.AddInt32(&runCount, 1)
		}
		return nil
	})

	runAt := time.Now().Add(200 * time.Millisecond)
	onceSchedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)

	// First scheduler instance: add job and wait for execution.
	db1 := openDB()
	storage1 := NewGormStorage(db1)
	scheduler1 := NewScheduler(storage1, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler1.Start(ctx))
	require.NoError(t, scheduler1.AddJob("one-time-job", onceSchedule, nil))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&runCount) == 1
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, scheduler1.Stop(ctx))

	beforeRestart := atomic.LoadInt32(&runCount)
	require.Equal(t, int32(1), beforeRestart)

	// Second scheduler instance: ensure once job does not execute again.
	db2 := openDB()
	storage2 := NewGormStorage(db2)
	scheduler2 := NewScheduler(storage2, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler2.Start(ctx))

	// Wait longer than scheduler tick interval to ensure it would run if scheduled.
	time.Sleep(2 * time.Second)

	require.Equal(t, beforeRestart, atomic.LoadInt32(&runCount))
	require.NoError(t, scheduler2.Stop(ctx))
}

// TestGormStorageOnceJobExecutesAfterRestart verifies that a once schedule can be delayed
// until after the scheduler restarts.
func TestGormStorageOnceJobExecutesAfterRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler_once_after.db")

	openDB := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
		require.NoError(t, err)
		return db
	}

	state := newTestHandlerState()
	var runCount int32
	state.SetJobFunc(func(ctx context.Context, event JobEvent) error {
		if event.ID() == "delayed-once-job" {
			atomic.AddInt32(&runCount, 1)
		}
		return nil
	})

	runAt := time.Now().Add(2 * time.Second)
	onceSchedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)

	// First scheduler instance: add job but stop before it executes.
	db1 := openDB()
	storage1 := NewGormStorage(db1)
	scheduler1 := NewScheduler(storage1, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler1.Start(ctx))
	require.NoError(t, scheduler1.AddJob("delayed-once-job", onceSchedule, nil))

	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int32(0), atomic.LoadInt32(&runCount))

	require.NoError(t, scheduler1.Stop(ctx))
	require.Equal(t, int32(0), atomic.LoadInt32(&runCount))

	// Second scheduler instance: ensure the once job executes after restart.
	db2 := openDB()
	storage2 := NewGormStorage(db2)
	scheduler2 := NewScheduler(storage2, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler2.Start(ctx))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&runCount) == 1
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, scheduler2.Stop(ctx))
	require.Equal(t, int32(1), atomic.LoadInt32(&runCount))
}
