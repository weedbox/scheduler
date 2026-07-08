package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNATSScheduler_ExecuteJobDoesNotClobberPeerScheduleUpdate simulates a
// peer running UpdateJobSchedule while this peer's handler is mid-execution.
// The post-handler KV write must merge (keep the peer's new schedule and
// NextRun, record only LastRun/Status), not blind-Put the stale schedule back.
func TestNATSScheduler_ExecuteJobDoesNotClobberPeerScheduleUpdate(t *testing.T) {
	_, js := startNATSServer(t)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("CAS_MERGE"),
		WithNATSSubjectPrefix("cas_merge"),
		WithNATSConsumerName("cas-merge-worker"),
		WithNATSSchedulerJobBucket("CAS_MERGE_JOBS"),
		WithNATSSchedulerExecBucket("CAS_MERGE_EXECS"),
		WithReconcilerInterval(0),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(stopCtx)
	}()

	schedule, err := NewIntervalSchedule(200 * time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("merge-job", schedule, nil))

	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// Simulate a peer's UpdateJobSchedule landing while our handler runs:
	// new schedule (1h) and a fresh future NextRun, written directly to KV.
	peerNextRun := time.Now().Add(1 * time.Hour).UTC().Truncate(time.Millisecond)
	peerJV := &natsJobValue{
		ID:             "merge-job",
		ScheduleType:   "interval",
		ScheduleConfig: "1h0m0s",
		Status:         string(JobStatusPending),
		NextRun:        peerNextRun,
		UpdatedAt:      time.Now(),
	}
	data, err := json.Marshal(peerJV)
	require.NoError(t, err)
	_, err = s.jobKV.Put(ctx, "merge-job", data)
	require.NoError(t, err)

	close(releaseHandler)

	// Wait for the post-handler merge to land (LastRun becomes non-zero).
	require.Eventually(t, func() bool {
		entry, err := s.jobKV.Get(ctx, "merge-job")
		if err != nil {
			return false
		}
		var got natsJobValue
		if json.Unmarshal(entry.Value(), &got) != nil {
			return false
		}
		return !got.LastRun.IsZero()
	}, 10*time.Second, 20*time.Millisecond, "post-handler merge never landed")

	entry, err := s.jobKV.Get(ctx, "merge-job")
	require.NoError(t, err)
	var got natsJobValue
	require.NoError(t, json.Unmarshal(entry.Value(), &got))

	assert.Equal(t, "1h0m0s", got.ScheduleConfig,
		"peer's schedule update must survive the post-handler write")
	assert.True(t, got.NextRun.Equal(peerNextRun),
		"peer's NextRun must survive, got %s want %s", got.NextRun, peerNextRun)
	assert.Equal(t, string(JobStatusCompleted), got.Status)
}

// TestNATSScheduler_ExecuteJobDoesNotResurrectRemovedJob simulates a peer
// removing the job while this peer's handler is mid-execution. The
// post-handler write must not recreate the KV row or publish a next tick.
func TestNATSScheduler_ExecuteJobDoesNotResurrectRemovedJob(t *testing.T) {
	_, js := startNATSServer(t)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		if executions.Add(1) == 1 {
			close(handlerStarted)
			<-releaseHandler
		}
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("NO_RESURRECT"),
		WithNATSSubjectPrefix("no_resurrect"),
		WithNATSConsumerName("no-resurrect-worker"),
		WithNATSSchedulerJobBucket("NO_RESURRECT_JOBS"),
		WithNATSSchedulerExecBucket("NO_RESURRECT_EXECS"),
		WithReconcilerInterval(0),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(stopCtx)
	}()

	schedule, err := NewIntervalSchedule(200 * time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("gone-job", schedule, nil))

	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// Simulate a peer's RemoveJob landing while our handler runs.
	require.NoError(t, s.jobKV.Purge(ctx, "gone-job"))

	close(releaseHandler)

	// Wait for post-handler processing to finish (local job map cleaned up).
	require.Eventually(t, func() bool {
		_, err := s.GetJob("gone-job")
		return err != nil
	}, 10*time.Second, 20*time.Millisecond, "job never left the local cache")

	// Give any stray republish a moment to land, then verify the row stayed
	// gone and no further executions happened.
	time.Sleep(500 * time.Millisecond)
	_, err = s.jobKV.Get(ctx, "gone-job")
	assert.ErrorIs(t, err, jetstream.ErrKeyNotFound, "removed job must not be re-created in KV")
	assert.Equal(t, int32(1), executions.Load(), "removed job must not fire again")
}

// TestNATSScheduler_ReconcilerSkipsFreshRunningRow verifies the reconciler
// leaves a row alone while a peer is mid-tick (Status running, fresh
// UpdatedAt), and repairs it once the row is older than the running TTL.
func TestNATSScheduler_ReconcilerSkipsFreshRunningRow(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("RECON_RUN"),
		WithNATSSubjectPrefix("recon_run"),
		WithNATSConsumerName("recon-run-worker"),
		WithNATSSchedulerJobBucket("RECON_RUN_JOBS"),
		WithNATSSchedulerExecBucket("RECON_RUN_EXECS"),
		WithReconcilerInterval(0), // driven manually
		WithReconcilerGracePeriod(0),
		WithReconcilerRunningTTL(1*time.Minute),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	staleNext := time.Now().Add(-10 * time.Second)
	writeRow := func(updatedAt time.Time) {
		jv := &natsJobValue{
			ID:             "run-job",
			ScheduleType:   "interval",
			ScheduleConfig: "1h",
			Status:         string(JobStatusRunning),
			NextRun:        staleNext,
			UpdatedAt:      updatedAt,
		}
		data, err := json.Marshal(jv)
		require.NoError(t, err)
		_, err = s.jobKV.Put(ctx, "run-job", data)
		require.NoError(t, err)
	}

	// Fresh running row (a peer is mid-tick): reconciler must not touch it
	// even though NextRun is stale.
	writeRow(time.Now())
	s.reconcileOnce(ctx)

	entry, err := s.jobKV.Get(ctx, "run-job")
	require.NoError(t, err)
	var got natsJobValue
	require.NoError(t, json.Unmarshal(entry.Value(), &got))
	assert.True(t, got.NextRun.Equal(staleNext),
		"reconciler must skip a fresh running row, but NextRun moved to %s", got.NextRun)

	// Same row but stuck in running beyond the TTL (crashed peer): now the
	// reconciler must repair the chain.
	writeRow(time.Now().Add(-2 * time.Minute))
	s.reconcileOnce(ctx)

	entry, err = s.jobKV.Get(ctx, "run-job")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(entry.Value(), &got))
	assert.True(t, got.NextRun.After(time.Now()),
		"reconciler must advance an abandoned running row, got %s", got.NextRun)
}

// TestNATSScheduler_PreStartJobsArePersistedAndExecuted covers the pre-Start
// AddJob black hole: jobs added before Start() must be persisted to KV,
// published, and actually fire.
func TestNATSScheduler_PreStartJobsArePersistedAndExecuted(t *testing.T) {
	_, js := startNATSServer(t)

	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("PRESTART"),
		WithNATSSubjectPrefix("prestart"),
		WithNATSConsumerName("prestart-worker"),
		WithNATSSchedulerJobBucket("PRESTART_JOBS"),
		WithNATSSchedulerExecBucket("PRESTART_EXECS"),
	).(*natsSchedulerImpl)

	schedule, err := NewIntervalSchedule(200 * time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("early-bird", schedule, map[string]string{"k": "v"}))

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// The job must exist in KV...
	entry, err := s.jobKV.Get(ctx, "early-bird")
	require.NoError(t, err)
	var jv natsJobValue
	require.NoError(t, json.Unmarshal(entry.Value(), &jv))
	assert.Equal(t, "interval", jv.ScheduleType)
	assert.Equal(t, map[string]string{"k": "v"}, jv.Metadata)

	// ...and actually fire.
	require.Eventually(t, func() bool {
		return executions.Load() >= 1
	}, 10*time.Second, 20*time.Millisecond, "pre-Start job never executed")
}

// TestScheduler_PreStartJobsArePersisted is the ticker-scheduler variant:
// jobs added before Start() must be written to storage during Start.
func TestScheduler_PreStartJobsArePersisted(t *testing.T) {
	storage := NewMemoryStorage()
	handler := func(ctx context.Context, event JobEvent) error { return nil }
	s := NewScheduler(storage, handler, NewBasicScheduleCodec())

	schedule, err := NewIntervalSchedule(1 * time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("early-bird", schedule, map[string]string{"k": "v"}))

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	jobData, err := storage.GetJob(ctx, "early-bird")
	require.NoError(t, err, "pre-Start job must be persisted during Start")
	assert.Equal(t, "interval", jobData.ScheduleType)
	assert.Equal(t, map[string]string{"k": "v"}, jobData.Metadata)
}

// TestNATSScheduler_SlowJobDoesNotBlockOtherJobs verifies executions are
// dispatched concurrently: with the serial Consume callback of the old
// implementation, a handler stuck on job A delayed job B indefinitely.
func TestNATSScheduler_SlowJobDoesNotBlockOtherJobs(t *testing.T) {
	_, js := startNATSServer(t)

	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	fastExecuted := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		switch event.ID() {
		case "slow":
			close(slowStarted)
			<-releaseSlow
		case "fast":
			select {
			case <-fastExecuted:
			default:
				close(fastExecuted)
			}
		}
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("CONCUR"),
		WithNATSSubjectPrefix("concur"),
		WithNATSConsumerName("concur-worker"),
		WithNATSSchedulerJobBucket("CONCUR_JOBS"),
		WithNATSSchedulerExecBucket("CONCUR_EXECS"),
		WithReconcilerInterval(0),
		WithMaxConcurrentExecutions(4),
	)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer func() {
		close(releaseSlow)
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(stopCtx)
	}()

	slowSchedule, err := NewOnceSchedule(time.Now().Add(100 * time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, s.AddJob("slow", slowSchedule, nil))

	select {
	case <-slowStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("slow job never started")
	}

	// While the slow handler is blocked, the fast job must still run.
	fastSchedule, err := NewOnceSchedule(time.Now().Add(100 * time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, s.AddJob("fast", fastSchedule, nil))

	select {
	case <-fastExecuted:
	case <-time.After(10 * time.Second):
		t.Fatal("fast job blocked behind slow job — executions are not concurrent")
	}
}

// TestNATSScheduler_StopRespectsContextDeadline verifies Stop gives up
// waiting for a stuck handler when the caller's ctx expires, instead of
// blocking forever.
func TestNATSScheduler_StopRespectsContextDeadline(t *testing.T) {
	_, js := startNATSServer(t)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("STOPCTX"),
		WithNATSSubjectPrefix("stopctx"),
		WithNATSConsumerName("stopctx-worker"),
		WithNATSSchedulerJobBucket("STOPCTX_JOBS"),
		WithNATSSchedulerExecBucket("STOPCTX_EXECS"),
		WithReconcilerInterval(0),
	)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer close(releaseHandler)

	schedule, err := NewOnceSchedule(time.Now().Add(100 * time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, s.AddJob("stuck", schedule, nil))

	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = s.Stop(stopCtx)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 5*time.Second, "Stop must return near the ctx deadline")
}

// TestScheduler_StopRespectsContextDeadline is the ticker-scheduler variant.
func TestScheduler_StopRespectsContextDeadline(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, event JobEvent) error {
		close(handlerStarted)
		<-releaseHandler
		return nil
	}

	s := NewScheduler(NewMemoryStorage(), handler, NewBasicScheduleCodec(),
		WithCheckInterval(10*time.Millisecond))

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer close(releaseHandler)

	schedule, err := NewOnceSchedule(time.Now().Add(20 * time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, s.AddJob("stuck", schedule, nil))

	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = s.Stop(stopCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestScheduler_ExecutionIDsAreUniqueWithinASecond verifies the nanosecond
// executionID: a job firing several times inside one second must produce one
// execution record per firing, not overwrite a single second-keyed record.
func TestScheduler_ExecutionIDsAreUniqueWithinASecond(t *testing.T) {
	storage := NewMemoryStorage()
	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		return nil
	}

	s := NewScheduler(storage, handler, NewBasicScheduleCodec(),
		WithCheckInterval(10*time.Millisecond))

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, err := NewIntervalSchedule(20 * time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("rapid", schedule, nil))

	require.Eventually(t, func() bool {
		return executions.Load() >= 3
	}, 10*time.Second, 10*time.Millisecond)

	records, err := storage.ListExecutions(ctx, "rapid", nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(records), 3,
		"each firing must produce its own execution record")

	seen := make(map[string]bool)
	for _, r := range records {
		assert.False(t, seen[r.ExecutionID], "duplicate executionID %s", r.ExecutionID)
		seen[r.ExecutionID] = true
	}
}

// TestScheduleBuilder exercises the ScheduleBuilder implementation and the
// DailySchedule it produces.
func TestScheduleBuilder(t *testing.T) {
	b := NewScheduleBuilder()

	every, err := b.Every(5 * time.Second)
	require.NoError(t, err)
	assert.IsType(t, &IntervalSchedule{}, every)

	_, err = b.Every(0)
	assert.ErrorIs(t, err, ErrInvalidInterval)

	cronSched, err := b.Cron("*/5 * * * *")
	require.NoError(t, err)
	assert.IsType(t, &CronSchedule{}, cronSched)

	_, err = b.Cron("not a cron")
	assert.Error(t, err)

	daily, err := b.At(10, 30, 0)
	require.NoError(t, err)
	assert.IsType(t, &DailySchedule{}, daily)

	_, err = b.At(24, 0, 0)
	assert.ErrorIs(t, err, ErrInvalidScheduleTime)
	_, err = b.At(0, 60, 0)
	assert.ErrorIs(t, err, ErrInvalidScheduleTime)

	runAt := time.Now().Add(time.Hour)
	once, err := b.Once(runAt)
	require.NoError(t, err)
	assert.IsType(t, &OnceSchedule{}, once)
}

// TestDailySchedule verifies Next semantics and codec round-trip.
func TestDailySchedule(t *testing.T) {
	daily, err := NewDailySchedule(10, 30, 15)
	require.NoError(t, err)

	loc := time.FixedZone("TST", 8*3600)
	before := time.Date(2026, 7, 8, 9, 0, 0, 0, loc)
	next := daily.Next(before)
	assert.Equal(t, time.Date(2026, 7, 8, 10, 30, 15, 0, loc), next,
		"before today's slot → today")

	after := time.Date(2026, 7, 8, 11, 0, 0, 0, loc)
	next = daily.Next(after)
	assert.Equal(t, time.Date(2026, 7, 9, 10, 30, 15, 0, loc), next,
		"after today's slot → tomorrow")

	exact := time.Date(2026, 7, 8, 10, 30, 15, 0, loc)
	next = daily.Next(exact)
	assert.Equal(t, time.Date(2026, 7, 9, 10, 30, 15, 0, loc), next,
		"exactly at the slot → strictly after, so tomorrow")

	// Codec round-trip.
	codec := NewBasicScheduleCodec()
	scheduleType, scheduleConfig, err := codec.Encode(daily)
	require.NoError(t, err)
	assert.Equal(t, "daily", scheduleType)
	assert.Equal(t, "10:30:15", scheduleConfig)

	decoded, err := codec.Decode(scheduleType, scheduleConfig)
	require.NoError(t, err)
	hour, minute, second := decoded.(*DailySchedule).At()
	assert.Equal(t, [3]int{10, 30, 15}, [3]int{hour, minute, second})

	_, err = codec.Decode("daily", "garbage")
	assert.ErrorIs(t, err, ErrInvalidScheduleConfig)
}

// TestGormStorageListExecutionsSortByWhitelist verifies SortBy cannot inject
// SQL and unknown values fall back to the start_time default.
func TestGormStorageListExecutionsSortByWhitelist(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	require.NoError(t, storage.Initialize(ctx))

	base := time.Now().Add(-time.Hour)
	for i, dur := range []time.Duration{3 * time.Second, 1 * time.Second, 2 * time.Second} {
		require.NoError(t, storage.SaveExecution(ctx, &ExecutionRecord{
			JobID:       "job1",
			ExecutionID: strings.Repeat("x", i+1),
			StartTime:   base.Add(time.Duration(i) * time.Minute),
			EndTime:     base.Add(time.Duration(i)*time.Minute + dur),
			Duration:    dur,
			Status:      JobStatusCompleted,
		}))
	}

	// Injection attempt must not reach the ORDER BY clause: the query still
	// succeeds and uses the start_time fallback.
	records, err := storage.ListExecutions(ctx, "job1", &QueryOptions{
		SortBy: "start_time; DROP TABLE scheduler_executions; --",
	})
	require.NoError(t, err)
	require.Len(t, records, 3)
	assert.True(t, records[0].StartTime.Before(records[1].StartTime))

	// The table must still exist.
	records, err = storage.ListExecutions(ctx, "job1", &QueryOptions{SortBy: "duration", SortDesc: true})
	require.NoError(t, err)
	require.Len(t, records, 3)
	assert.Equal(t, 3*time.Second, records[0].Duration)
	assert.Equal(t, 1*time.Second, records[2].Duration)
}

// TestGormStorageCloseDoesNotCloseSharedDB verifies the ownership fix: by
// default Close leaves the injected connection pool usable; WithGormOwnsDB
// opts back into closing it.
func TestGormStorageCloseDoesNotCloseSharedDB(t *testing.T) {
	ctx := context.Background()

	t.Run("default keeps DB open", func(t *testing.T) {
		db := setupTestDB(t)
		storage := NewGormStorage(db)
		require.NoError(t, storage.Initialize(ctx))
		require.NoError(t, storage.Close(ctx))

		sqlDB, err := db.DB()
		require.NoError(t, err)
		assert.NoError(t, sqlDB.Ping(), "shared pool must survive storage.Close")
	})

	t.Run("WithGormOwnsDB closes DB", func(t *testing.T) {
		db := setupTestDB(t)
		storage := NewGormStorage(db, WithGormOwnsDB())
		require.NoError(t, storage.Initialize(ctx))
		require.NoError(t, storage.Close(ctx))

		sqlDB, err := db.DB()
		require.NoError(t, err)
		assert.Error(t, sqlDB.Ping(), "owned pool must be closed by storage.Close")
	})
}

// TestScheduler_PanicAdvancesNextRun verifies a panicking handler does not
// re-fire in a tight loop: the panic path must advance nextRun like a normal
// completion would.
func TestScheduler_PanicAdvancesNextRun(t *testing.T) {
	var executions atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		executions.Add(1)
		panic("boom")
	}

	s := NewScheduler(NewMemoryStorage(), handler, NewBasicScheduleCodec(),
		WithCheckInterval(10*time.Millisecond))

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// First fire almost immediately, subsequent fires an hour apart — so any
	// re-fire we observe can only come from a broken panic path.
	schedule, err := NewStartAtIntervalSchedule(time.Now().Add(30*time.Millisecond), 1*time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("boomer", schedule, nil))

	require.Eventually(t, func() bool {
		return executions.Load() >= 1
	}, 10*time.Second, 10*time.Millisecond)

	// With a 1h interval, any further execution within the observation
	// window means the panic path failed to advance nextRun.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(1), executions.Load(),
		"panicking job must not re-fire every tick")

	job, err := s.GetJob("boomer")
	require.NoError(t, err)
	assert.True(t, job.NextRun().After(time.Now().Add(30*time.Minute)),
		"nextRun must be advanced past the panic, got %s", job.NextRun())
}
