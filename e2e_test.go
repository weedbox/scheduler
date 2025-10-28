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

// TestGormStorageCronJobPersistenceAcrossRestart verifies that cron jobs persisted with GORM
// are restored and continue executing after the scheduler restarts.
func TestGormStorageCronJobPersistenceAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler_cron.db")

	openDB := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
		require.NoError(t, err)
		return db
	}

	state := newTestHandlerState()
	var runCount int32
	state.SetJobFunc(func(ctx context.Context, event JobEvent) error {
		if event.ID() == "cron-job" {
			atomic.AddInt32(&runCount, 1)
		}
		return nil
	})

	// Every minute (for testing purposes, we'll use a short interval in actual test)
	// In reality, cron "* * * * *" runs every minute at :00 seconds
	cronSchedule, err := NewCronSchedule("* * * * *")
	require.NoError(t, err)

	// First scheduler instance: add cron job and wait for at least one execution.
	db1 := openDB()
	storage1 := NewGormStorage(db1)
	scheduler1 := NewScheduler(storage1, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler1.Start(ctx))
	require.NoError(t, scheduler1.AddJob("cron-job", cronSchedule, map[string]string{"type": "cron"}))

	// Verify the job was persisted with correct schedule type and config
	jobData, err := storage1.GetJob(ctx, "cron-job")
	require.NoError(t, err)
	require.Equal(t, "cron", jobData.ScheduleType)
	require.Equal(t, "* * * * *", jobData.ScheduleConfig)

	// Wait up to 70 seconds for the job to run at least once
	// (since cron runs at the top of the minute)
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&runCount) >= 1
	}, 70*time.Second, 1*time.Second)

	require.NoError(t, scheduler1.Stop(ctx))

	beforeRestart := atomic.LoadInt32(&runCount)
	require.GreaterOrEqual(t, beforeRestart, int32(1))

	// Second scheduler instance: ensure cron job is reloaded and continues to run.
	db2 := openDB()
	storage2 := NewGormStorage(db2)
	scheduler2 := NewScheduler(storage2, state.Handler(), NewBasicScheduleCodec())

	require.NoError(t, scheduler2.Start(ctx))

	// Verify the job was loaded from storage
	jobs := scheduler2.ListJobs()
	require.Len(t, jobs, 1)
	require.Equal(t, "cron-job", jobs[0].ID())

	// Wait for at least one more execution after restart
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&runCount) >= beforeRestart+1
	}, 70*time.Second, 1*time.Second)

	require.NoError(t, scheduler2.Stop(ctx))
}
