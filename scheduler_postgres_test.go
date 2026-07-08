package scheduler

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// testPostgresDSN returns the PostgreSQL DSN for tests, skipping the test
// when none is configured. Example:
//
//	SCHEDULER_TEST_POSTGRES_DSN="host=localhost port=5432 user=postgres password=test dbname=postgres sslmode=disable" go test ./...
func testPostgresDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("SCHEDULER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SCHEDULER_TEST_POSTGRES_DSN to run PostgresScheduler tests")
	}
	return dsn
}

func newPostgresTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.Open(testPostgresDSN(t)), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	})

	return db
}

// resetPostgresTables drops the shared tables so each test starts clean.
// Tests in this file must not call t.Parallel(): they share one database.
func resetPostgresTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS scheduler_jobs, scheduler_executions").Error)
}

func startPostgresScheduler(t *testing.T, db *gorm.DB, handler JobHandler, opts ...PostgresSchedulerOption) Scheduler {
	t.Helper()

	base := []PostgresSchedulerOption{WithPostgresPollInterval(30 * time.Millisecond)}
	s := NewPostgresScheduler(db, handler, NewBasicScheduleCodec(), append(base, opts...)...)
	require.NoError(t, s.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	return s
}

func getPostgresJobRow(t *testing.T, db *gorm.DB, id string) *postgresJobModel {
	t.Helper()

	var m postgresJobModel
	res := db.Raw("SELECT * FROM scheduler_jobs WHERE id = ?", id).Scan(&m)
	require.NoError(t, res.Error)
	if res.RowsAffected == 0 {
		return nil
	}
	return &m
}

// TestPostgresScheduler_BasicExecution verifies a recurring job fires
// repeatedly, advances next_run, and persists execution records.
func TestPostgresScheduler_BasicExecution(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	schedule, err := NewStartAtIntervalSchedule(time.Now(), 150*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("basic-job", schedule, map[string]string{"k": "v"}))

	require.Eventually(t, func() bool {
		return executions.Load() >= 2
	}, 10*time.Second, 20*time.Millisecond, "recurring job should fire repeatedly")

	job, err := s.GetJob("basic-job")
	require.NoError(t, err)
	assert.True(t, job.NextRun().After(time.Now().Add(-time.Second)), "next_run should keep advancing")

	// Records are written after the handler returns, so the second record
	// may still be in flight when the execution counter reaches 2.
	require.Eventually(t, func() bool {
		var count int64
		if err := db.Raw("SELECT COUNT(*) FROM scheduler_executions WHERE job_id = ?", "basic-job").Scan(&count).Error; err != nil {
			return false
		}
		return count >= 2
	}, 10*time.Second, 20*time.Millisecond, "execution records should be persisted")
}

// TestPostgresScheduler_MultiInstanceSingleExecution verifies that with two
// instances sharing the database, a due once-job is executed exactly once.
func TestPostgresScheduler_MultiInstanceSingleExecution(t *testing.T) {
	dbA := newPostgresTestDB(t)
	dbB := newPostgresTestDB(t)
	resetPostgresTables(t, dbA)

	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	sA := startPostgresScheduler(t, dbA, handler)
	startPostgresScheduler(t, dbB, handler)

	require.NoError(t, sA.AddJob("once-job", mustOnce(t, time.Now()), nil))

	require.Eventually(t, func() bool {
		return executions.Load() >= 1
	}, 10*time.Second, 20*time.Millisecond)

	// Give both instances several poll cycles to (incorrectly) re-claim.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), executions.Load(), "once job must execute exactly once across instances")

	row := getPostgresJobRow(t, dbA, "once-job")
	require.NotNil(t, row)
	assert.Equal(t, string(JobStatusCompleted), row.Status)
	assert.True(t, row.NextRun.After(time.Now().Add(365*24*time.Hour)), "completed once job should be parked far in the future")
}

// TestPostgresScheduler_LeaseTakeover verifies a job left running with an
// expired lease by a crashed instance is re-claimed and executed.
func TestPostgresScheduler_LeaseTakeover(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	// A future-dated job the crashed peer supposedly owned.
	schedule, err := NewStartAtIntervalSchedule(time.Now(), time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("orphan-job", schedule, nil))

	// Simulate the crashed peer: mark the row running with an expired lease.
	expired := time.Now().UTC().Add(-time.Minute)
	require.NoError(t, db.Exec(
		"UPDATE scheduler_jobs SET status = ?, lease_expires_at = ?, claim_seq = 5, next_run = ? WHERE id = ?",
		string(JobStatusRunning), expired, time.Now().UTC(), "orphan-job").Error)

	require.Eventually(t, func() bool {
		return executions.Load() >= 1
	}, 10*time.Second, 20*time.Millisecond, "expired lease should be taken over")

	require.Eventually(t, func() bool {
		row := getPostgresJobRow(t, db, "orphan-job")
		return row != nil && row.Status == string(JobStatusCompleted)
	}, 5*time.Second, 20*time.Millisecond)

	row := getPostgresJobRow(t, db, "orphan-job")
	assert.Greater(t, row.ClaimSeq, int64(5), "takeover must bump the fencing token")
	assert.Nil(t, row.LeaseExpiresAt, "lease should be cleared after completion")
}

// TestPostgresScheduler_StaleClaimCannotClobber verifies that when a claim is
// superseded mid-execution (fencing token bumped, as a lease takeover or
// schedule update would), the original handler's completion is discarded.
func TestPostgresScheduler_StaleClaimCannotClobber(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	require.NoError(t, s.AddJob("clobber-job", mustOnce(t, time.Now()), nil))
	<-handlerStarted

	// Supersede the claim while the handler is blocked, as another instance
	// taking over after a lease expiry would.
	supersededNextRun := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Microsecond)
	require.NoError(t, db.Exec(
		"UPDATE scheduler_jobs SET claim_seq = claim_seq + 1, status = ?, lease_expires_at = NULL, next_run = ? WHERE id = ?",
		string(JobStatusPending), supersededNextRun, "clobber-job").Error)

	close(releaseHandler)

	// The stale write-back must be discarded: status and next_run keep the
	// superseding values.
	time.Sleep(500 * time.Millisecond)
	row := getPostgresJobRow(t, db, "clobber-job")
	require.NotNil(t, row)
	assert.Equal(t, string(JobStatusPending), row.Status, "stale claim must not overwrite status")
	assert.WithinDuration(t, supersededNextRun, row.NextRun.UTC(), time.Millisecond, "stale claim must not overwrite next_run")
}

// TestPostgresScheduler_RecreatedJobNotClobbered verifies the delete +
// re-create ABA case: a job is removed and re-added under the same ID while
// its old incarnation is still executing. The re-created row restarts
// claim_seq at zero, so after one claim its token collides with the one the
// stale owner still holds; the created_at guard must discard the stale
// write-back anyway.
func TestPostgresScheduler_RecreatedJobNotClobbered(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	require.NoError(t, s.AddJob("aba-job", mustOnce(t, time.Now()), nil))
	<-handlerStarted // old incarnation claimed: its owner holds claim_seq = 1

	// Re-create the job under the same ID while the old handler is blocked.
	// The far-future run time keeps the local claim loop away from it.
	require.NoError(t, s.RemoveJob("aba-job"))
	require.NoError(t, s.AddJob("aba-job", mustOnce(t, time.Now().Add(time.Hour)), nil))

	// Simulate another instance claiming the new incarnation: its claim_seq
	// becomes 1 — colliding with the token the stale owner is holding.
	require.NoError(t, db.Exec(
		"UPDATE scheduler_jobs SET status = ?, claim_seq = claim_seq + 1, lease_expires_at = now() + interval '5 minutes' WHERE id = ?",
		string(JobStatusRunning), "aba-job").Error)
	fresh := getPostgresJobRow(t, db, "aba-job")
	require.NotNil(t, fresh)
	require.EqualValues(t, 1, fresh.ClaimSeq, "test setup: fencing tokens must collide")

	close(releaseHandler)

	// The stale write-back matches id, status, and claim_seq but not
	// created_at; it must be discarded, leaving the new claim untouched.
	time.Sleep(500 * time.Millisecond)
	row := getPostgresJobRow(t, db, "aba-job")
	require.NotNil(t, row)
	assert.Equal(t, string(JobStatusRunning), row.Status, "stale owner must not complete the re-created job")
	assert.EqualValues(t, 1, row.ClaimSeq)
	assert.WithinDuration(t, fresh.NextRun, row.NextRun, time.Millisecond, "stale owner must not advance next_run")
}

// TestPostgresScheduler_UpdateOwnsChain verifies UpdateJobSchedule from a
// second instance while the job is executing: the updater owns the next tick
// and the in-flight execution cannot advance the old chain.
func TestPostgresScheduler_UpdateOwnsChain(t *testing.T) {
	dbA := newPostgresTestDB(t)
	dbB := newPostgresTestDB(t)
	resetPostgresTables(t, dbA)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	blockingHandler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}
	idleHandler := func(ctx context.Context, event JobEvent) error { return nil }

	sA := startPostgresScheduler(t, dbA, blockingHandler)
	sB := startPostgresScheduler(t, dbB, idleHandler,
		// Effectively disable B's claim loop so A owns the execution.
		WithPostgresPollInterval(time.Hour))

	require.NoError(t, sA.AddJob("update-job", mustOnce(t, time.Now()), nil))
	<-handlerStarted

	newRunAt := time.Now().Add(72 * time.Hour)
	require.NoError(t, sB.UpdateJobSchedule("update-job", mustOnce(t, newRunAt)))

	close(releaseHandler)
	time.Sleep(500 * time.Millisecond)

	row := getPostgresJobRow(t, dbA, "update-job")
	require.NotNil(t, row)
	assert.Equal(t, string(JobStatusPending), row.Status, "updater owns the chain; completion must be discarded")
	assert.WithinDuration(t, newRunAt.UTC(), row.NextRun.UTC(), time.Second, "next_run must reflect the updated schedule")
}

// TestPostgresScheduler_RemoveJobNoResurrection verifies removing a job while
// its handler runs does not resurrect the row on completion.
func TestPostgresScheduler_RemoveJobNoResurrection(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	require.NoError(t, s.AddJob("removed-job", mustOnce(t, time.Now()), nil))
	<-handlerStarted

	require.NoError(t, s.RemoveJob("removed-job"))
	close(releaseHandler)

	time.Sleep(500 * time.Millisecond)
	assert.Nil(t, getPostgresJobRow(t, db, "removed-job"), "completed handler must not resurrect a removed job")

	_, err := s.GetJob("removed-job")
	assert.ErrorIs(t, err, ErrJobNotFound)
}

// TestPostgresScheduler_PreStartJobsPersisted verifies jobs added before
// Start are persisted and executed after Start.
func TestPostgresScheduler_PreStartJobsPersisted(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	s := NewPostgresScheduler(db, handler, NewBasicScheduleCodec(),
		WithPostgresPollInterval(30*time.Millisecond))

	require.NoError(t, s.AddJob("prestart-job", mustOnce(t, time.Now()), nil))
	require.ErrorIs(t, s.AddJob("prestart-job", mustOnce(t, time.Now()), nil), ErrJobAlreadyExists)

	job, err := s.GetJob("prestart-job")
	require.NoError(t, err)
	assert.Equal(t, "prestart-job", job.ID())

	require.NoError(t, s.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})

	require.Eventually(t, func() bool {
		return executions.Load() >= 1
	}, 10*time.Second, 20*time.Millisecond, "pre-start job should execute after Start")
}

// TestPostgresScheduler_FailureAndPanicAdvanceNextRun verifies that a failing
// and a panicking handler both mark the job failed and advance next_run, so
// broken jobs cannot re-fire in a hot loop.
func TestPostgresScheduler_FailureAndPanicAdvanceNextRun(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	var failExecs, panicExecs atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		switch event.ID() {
		case "fail-job":
			failExecs.Add(1)
			return errors.New("boom")
		case "panic-job":
			panicExecs.Add(1)
			panic("kaboom")
		}
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	// startAt must still be in the future when AddJob computes Next(), or
	// the first fire lands one full interval away.
	failSchedule, err := NewStartAtIntervalSchedule(time.Now().Add(50*time.Millisecond), time.Hour)
	require.NoError(t, err)
	panicSchedule, err := NewStartAtIntervalSchedule(time.Now().Add(80*time.Millisecond), time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("fail-job", failSchedule, nil))
	require.NoError(t, s.AddJob("panic-job", panicSchedule, nil))

	require.Eventually(t, func() bool {
		return failExecs.Load() >= 1 && panicExecs.Load() >= 1
	}, 10*time.Second, 20*time.Millisecond)

	// Any additional execution could only come from a broken failure path
	// re-claiming a non-advanced next_run.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), failExecs.Load())
	assert.Equal(t, int32(1), panicExecs.Load())

	for _, id := range []string{"fail-job", "panic-job"} {
		row := getPostgresJobRow(t, db, id)
		require.NotNil(t, row, id)
		assert.Equal(t, string(JobStatusFailed), row.Status, id)
		assert.True(t, row.NextRun.After(time.Now().Add(30*time.Minute)), "%s next_run should advance ~1h", id)
	}

	var errMsg string
	require.NoError(t, db.Raw(
		"SELECT error FROM scheduler_executions WHERE job_id = ? ORDER BY start_time DESC LIMIT 1",
		"panic-job").Scan(&errMsg).Error)
	assert.Contains(t, errMsg, "kaboom", "panic should be recorded in the execution record")
}

// TestPostgresScheduler_MissedTicksCollapse verifies ticks missed while all
// instances were down collapse into a single catch-up execution.
func TestPostgresScheduler_MissedTicksCollapse(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	schedule, err := NewStartAtIntervalSchedule(time.Now().Add(50*time.Millisecond), time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("missed-job", schedule, nil))

	require.Eventually(t, func() bool { return executions.Load() >= 1 }, 10*time.Second, 20*time.Millisecond)

	// Backdate next_run as if 10 hourly ticks were missed during an outage.
	require.NoError(t, db.Exec(
		"UPDATE scheduler_jobs SET next_run = ?, status = ? WHERE id = ?",
		time.Now().UTC().Add(-10*time.Hour), string(JobStatusCompleted), "missed-job").Error)

	require.Eventually(t, func() bool { return executions.Load() >= 2 }, 10*time.Second, 20*time.Millisecond)

	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(2), executions.Load(), "missed ticks must collapse into one catch-up execution")

	row := getPostgresJobRow(t, db, "missed-job")
	require.NotNil(t, row)
	assert.True(t, row.NextRun.After(time.Now()), "next_run must be recomputed from completion time, not replayed")
}

// TestPostgresScheduler_StopHonorsContext verifies Stop returns the context
// error while a handler is still running, and completes once released.
func TestPostgresScheduler_StopHonorsContext(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := NewPostgresScheduler(db, handler, NewBasicScheduleCodec(),
		WithPostgresPollInterval(30*time.Millisecond))
	require.NoError(t, s.Start(context.Background()))

	require.NoError(t, s.AddJob("stop-job", mustOnce(t, time.Now()), nil))
	<-handlerStarted

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := s.Stop(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "Stop must give up when the context expires")

	close(releaseHandler)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return s.Stop(ctx) == nil || !s.IsRunning()
	}, 10*time.Second, 50*time.Millisecond, "Stop should succeed after the handler finishes")
}

// TestPostgresScheduler_LifecycleErrors verifies the Start/Stop guard rails.
func TestPostgresScheduler_LifecycleErrors(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	noHandler := NewPostgresScheduler(db, nil, nil)
	assert.ErrorIs(t, noHandler.Start(context.Background()), ErrHandlerNotDefined)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	s := NewPostgresScheduler(db, handler, nil, WithPostgresPollInterval(30*time.Millisecond))

	assert.ErrorIs(t, s.Stop(context.Background()), ErrSchedulerNotStarted)
	require.NoError(t, s.Start(context.Background()))
	assert.ErrorIs(t, s.Start(context.Background()), ErrSchedulerAlreadyStarted)
	require.NoError(t, s.WaitUntilRunning(context.Background()))
	assert.True(t, s.IsRunning())

	assert.ErrorIs(t, s.AddJob("", mustOnce(t, time.Now()), nil), ErrEmptyJobID)
	assert.ErrorIs(t, s.AddJob("x", nil, nil), ErrInvalidInterval)
	assert.ErrorIs(t, s.RemoveJob("nope"), ErrJobNotFound)
	assert.ErrorIs(t, s.UpdateJobSchedule("nope", mustOnce(t, time.Now())), ErrJobNotFound)
	_, err := s.GetJob("nope")
	assert.ErrorIs(t, err, ErrJobNotFound)

	require.NoError(t, s.Stop(context.Background()))
	assert.False(t, s.IsRunning())

	// The scheduler must be restartable after a clean Stop.
	require.NoError(t, s.Start(context.Background()))
	require.NoError(t, s.Stop(context.Background()))
}

// TestPostgresScheduler_SharedTableWithGormStorage verifies a job created by
// the single-node GORM scheduler is picked up unchanged after switching to
// the PostgresScheduler (in-place migration path).
func TestPostgresScheduler_SharedTableWithGormStorage(t *testing.T) {
	db := newPostgresTestDB(t)
	resetPostgresTables(t, db)

	// Seed the table via plain GormStorage, as a single-node deployment
	// running NewScheduler(NewGormStorage(db), ...) would have.
	storage := NewGormStorage(db)
	require.NoError(t, storage.Initialize(context.Background()))
	scheduleType, scheduleConfig, err := NewBasicScheduleCodec().Encode(mustStartAtInterval(t, time.Now(), time.Hour))
	require.NoError(t, err)
	require.NoError(t, storage.SaveJob(context.Background(), &JobData{
		ID:             "legacy-job",
		Status:         JobStatusPending,
		NextRun:        time.Now().Add(-time.Second),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		ScheduleType:   scheduleType,
		ScheduleConfig: scheduleConfig,
		Metadata:       map[string]string{"origin": "gorm"},
	}))

	var gotMetadata atomic.Value
	handler := func(ctx context.Context, event JobEvent) error {
		gotMetadata.Store(event.Metadata())
		return nil
	}

	s := startPostgresScheduler(t, db, handler)

	require.Eventually(t, func() bool {
		return gotMetadata.Load() != nil
	}, 10*time.Second, 20*time.Millisecond, "legacy row should be claimed after AutoMigrate adds claim columns")

	metadata, _ := gotMetadata.Load().(map[string]string)
	assert.Equal(t, "gorm", metadata["origin"], "metadata must survive the backend switch")

	jobs := s.ListJobs()
	require.Len(t, jobs, 1)
	assert.Equal(t, "legacy-job", jobs[0].ID())
}

func mustStartAtInterval(t *testing.T, startAt time.Time, interval time.Duration) Schedule {
	t.Helper()
	schedule, err := NewStartAtIntervalSchedule(startAt, interval)
	require.NoError(t, err)
	return schedule
}

func mustOnce(t *testing.T, runAt time.Time) Schedule {
	t.Helper()
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	return schedule
}
