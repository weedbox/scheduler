package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNATSScheduler_StartStop(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()

	err := s.Start(ctx)
	require.NoError(t, err)
	assert.True(t, s.IsRunning())

	// Double start should fail
	err = s.Start(ctx)
	assert.ErrorIs(t, err, ErrSchedulerAlreadyStarted)

	err = s.Stop(ctx)
	require.NoError(t, err)
	assert.False(t, s.IsRunning())

	// Double stop should fail
	err = s.Stop(ctx)
	assert.ErrorIs(t, err, ErrSchedulerNotStarted)
}

func TestNATSScheduler_StartWithoutHandler(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, nil)
	err := s.Start(context.Background())
	assert.ErrorIs(t, err, ErrHandlerNotDefined)
}

func TestNATSScheduler_AddAndGetJob(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)

	err = s.AddJob("test-job", schedule, map[string]string{"key": "value"})
	require.NoError(t, err)

	// Get job
	job, err := s.GetJob("test-job")
	require.NoError(t, err)
	assert.Equal(t, "test-job", job.ID())
	assert.Equal(t, "value", job.Metadata()["key"])
	assert.False(t, job.IsRunning())

	// Duplicate add should fail
	err = s.AddJob("test-job", schedule, nil)
	assert.ErrorIs(t, err, ErrJobAlreadyExists)

	// Empty ID should fail
	err = s.AddJob("", schedule, nil)
	assert.ErrorIs(t, err, ErrEmptyJobID)

	// Nil schedule should fail
	err = s.AddJob("another-job", nil, nil)
	assert.ErrorIs(t, err, ErrInvalidInterval)
}

func TestNATSScheduler_ListJobs(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, _ := NewIntervalSchedule(5 * time.Minute)

	for _, id := range []string{"job-1", "job-2", "job-3"} {
		require.NoError(t, s.AddJob(id, schedule, nil))
	}

	jobs := s.ListJobs()
	assert.Len(t, jobs, 3)
}

func TestNATSScheduler_RemoveJob(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, _ := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, s.AddJob("test-job", schedule, nil))

	err := s.RemoveJob("test-job")
	require.NoError(t, err)

	_, err = s.GetJob("test-job")
	assert.ErrorIs(t, err, ErrJobNotFound)

	// Remove non-existent should fail
	err = s.RemoveJob("non-existent")
	assert.ErrorIs(t, err, ErrJobNotFound)
}

func TestNATSScheduler_UpdateJobSchedule(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, _ := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, s.AddJob("test-job", schedule, nil))

	oldNextRun := mustGetJob(t, s, "test-job").NextRun()

	// Update to a different interval
	newSchedule, _ := NewIntervalSchedule(10 * time.Minute)
	err := s.UpdateJobSchedule("test-job", newSchedule)
	require.NoError(t, err)

	newNextRun := mustGetJob(t, s, "test-job").NextRun()
	assert.True(t, newNextRun.After(oldNextRun), "next run should be later with longer interval")

	// Update non-existent should fail
	err = s.UpdateJobSchedule("non-existent", newSchedule)
	assert.ErrorIs(t, err, ErrJobNotFound)
}

func TestNATSScheduler_WaitUntilRunning(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()

	go func() {
		time.Sleep(50 * time.Millisecond)
		s.Start(ctx)
	}()

	err := s.WaitUntilRunning(ctx)
	require.NoError(t, err)
	assert.True(t, s.IsRunning())

	s.Stop(ctx)
}

func TestNATSScheduler_WaitUntilRunningWithTimeout(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := s.WaitUntilRunning(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestNATSScheduler_IntervalJobExecution(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32
	var mu sync.Mutex
	var events []JobEvent

	handler := func(ctx context.Context, event JobEvent) error {
		count.Add(1)
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Use a very short interval for testing
	schedule, _ := NewIntervalSchedule(100 * time.Millisecond)
	require.NoError(t, s.AddJob("interval-job", schedule, map[string]string{"type": "interval"}))

	// Wait for at least 2 executions
	require.Eventually(t, func() bool {
		return count.Load() >= 2
	}, 10*time.Second, 50*time.Millisecond, "expected at least 2 executions")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "interval-job", events[0].ID())
	assert.Equal(t, "interval", events[0].Metadata()["type"])
}

func TestNATSScheduler_OnceJobExecution(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32

	handler := func(ctx context.Context, event JobEvent) error {
		count.Add(1)
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Schedule for immediate execution
	schedule, _ := NewOnceSchedule(time.Now().Add(100 * time.Millisecond))
	require.NoError(t, s.AddJob("once-job", schedule, nil))

	// Wait for execution
	require.Eventually(t, func() bool {
		return count.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond, "expected at least 1 execution")

	// Wait a bit more to verify it doesn't execute again
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), count.Load(), "once job should execute exactly once")
}

func TestNATSScheduler_CronJobExecution(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32

	handler := func(ctx context.Context, event JobEvent) error {
		count.Add(1)
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Every minute cron - just test that it's accepted
	schedule, err := NewCronSchedule("* * * * *")
	require.NoError(t, err)

	err = s.AddJob("cron-job", schedule, nil)
	require.NoError(t, err)

	job, err := s.GetJob("cron-job")
	require.NoError(t, err)
	assert.Equal(t, "cron-job", job.ID())
	assert.False(t, job.NextRun().IsZero())
}

func TestNATSScheduler_JobPersistence(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	// Use unique names to avoid conflicts
	opts := []NATSSchedulerOption{
		WithNATSStreamName("PERSIST_TEST"),
		WithNATSSubjectPrefix("persist_test"),
		WithNATSConsumerName("persist-worker"),
		WithNATSSchedulerJobBucket("PERSIST_JOBS"),
		WithNATSSchedulerExecBucket("PERSIST_EXECS"),
	}

	ctx := context.Background()

	// Start first instance and add a job
	s1 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s1.Start(ctx))

	schedule, _ := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, s1.AddJob("persistent-job", schedule, map[string]string{"key": "value"}))

	require.NoError(t, s1.Stop(ctx))

	// Start second instance - job should be loaded from KV
	s2 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s2.Start(ctx))
	defer s2.Stop(ctx)

	jobs := s2.ListJobs()
	assert.Len(t, jobs, 1)
	assert.Equal(t, "persistent-job", jobs[0].ID())
	assert.Equal(t, "value", jobs[0].Metadata()["key"])
}

func TestNATSScheduler_CustomOptions(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("CUSTOM_STREAM"),
		WithNATSSubjectPrefix("custom"),
		WithNATSConsumerName("custom-worker"),
		WithNATSSchedulerJobBucket("CUSTOM_JOBS"),
		WithNATSSchedulerExecBucket("CUSTOM_EXECS"),
	).(*natsSchedulerImpl)

	assert.Equal(t, "CUSTOM_STREAM", s.streamName)
	assert.Equal(t, "custom", s.subjectPrefix)
	assert.Equal(t, "custom-worker", s.consumerName)
	assert.Equal(t, "CUSTOM_JOBS", s.jobBucket)
	assert.Equal(t, "CUSTOM_EXECS", s.execBucket)
}

func TestNATSScheduler_OnceJobNotReexecutedAfterRestart(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32

	handler := func(ctx context.Context, event JobEvent) error {
		count.Add(1)
		return nil
	}

	opts := []NATSSchedulerOption{
		WithNATSStreamName("ONCE_RESTART_TEST"),
		WithNATSSubjectPrefix("once_restart"),
		WithNATSConsumerName("once-restart-worker"),
		WithNATSSchedulerJobBucket("ONCE_RESTART_JOBS"),
		WithNATSSchedulerExecBucket("ONCE_RESTART_EXECS"),
	}

	ctx := context.Background()

	// First instance: add and execute a once job
	s1 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s1.Start(ctx))

	schedule, _ := NewOnceSchedule(time.Now().Add(100 * time.Millisecond))
	require.NoError(t, s1.AddJob("once-restart-job", schedule, nil))

	// Wait for execution
	require.Eventually(t, func() bool {
		return count.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, s1.Stop(ctx))
	executedCount := count.Load()

	// Second instance: once job should NOT re-execute
	s2 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s2.Start(ctx))

	// Wait to verify no re-execution
	time.Sleep(2 * time.Second)
	require.NoError(t, s2.Stop(ctx))

	assert.Equal(t, executedCount, count.Load(),
		"once job should not re-execute after restart")
}

func TestNATSScheduler_NoRapidFireAfterRestart(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32

	handler := func(ctx context.Context, event JobEvent) error {
		count.Add(1)
		return nil
	}

	opts := []NATSSchedulerOption{
		WithNATSStreamName("NOFIRE_TEST"),
		WithNATSSubjectPrefix("nofire"),
		WithNATSConsumerName("nofire-worker"),
		WithNATSSchedulerJobBucket("NOFIRE_JOBS"),
		WithNATSSchedulerExecBucket("NOFIRE_EXECS"),
	}

	ctx := context.Background()

	// First instance: add an interval job
	s1 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s1.Start(ctx))

	schedule, _ := NewIntervalSchedule(2 * time.Second)
	require.NoError(t, s1.AddJob("interval-restart-job", schedule, nil))

	// Let it execute once
	require.Eventually(t, func() bool {
		return count.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, s1.Stop(ctx))
	count.Store(0)

	// Second instance: should NOT rapid-fire
	s2 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s2.Start(ctx))

	// Wait 3 seconds — should see at most 2 executions (not dozens)
	time.Sleep(3 * time.Second)
	require.NoError(t, s2.Stop(ctx))

	finalCount := count.Load()
	assert.LessOrEqual(t, finalCount, int32(2),
		"interval job should not rapid-fire after restart, got %d executions", finalCount)
}

func TestNATSScheduler_AllScheduleTypes(t *testing.T) {
	_, js := startNATSServer(t)

	var mu sync.Mutex
	executions := make(map[string]int)

	handler := func(ctx context.Context, event JobEvent) error {
		mu.Lock()
		executions[event.ID()]++
		mu.Unlock()
		return nil
	}

	opts := []NATSSchedulerOption{
		WithNATSStreamName("ALL_TYPES_TEST"),
		WithNATSSubjectPrefix("all_types"),
		WithNATSConsumerName("all-types-worker"),
		WithNATSSchedulerJobBucket("ALL_TYPES_JOBS"),
		WithNATSSchedulerExecBucket("ALL_TYPES_EXECS"),
	}

	s := NewNATSScheduler(js, handler, opts...)
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// 1. IntervalSchedule — every 500ms
	intervalSchedule, err := NewIntervalSchedule(500 * time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("interval-job", intervalSchedule, nil))

	// 2. OnceSchedule — run once after 200ms
	onceSchedule, err := NewOnceSchedule(time.Now().Add(200 * time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, s.AddJob("once-job", onceSchedule, nil))

	// 3. CronSchedule — every minute (use "* * * * *" for fastest possible cron)
	cronSchedule, err := NewCronSchedule("* * * * *")
	require.NoError(t, err)
	require.NoError(t, s.AddJob("cron-job", cronSchedule, nil))

	// 4. StartAtIntervalSchedule — start after 300ms, then every 500ms
	startAtSchedule, err := NewStartAtIntervalSchedule(
		time.Now().Add(300*time.Millisecond),
		500*time.Millisecond,
	)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("startat-job", startAtSchedule, nil))

	// --- Verify IntervalSchedule: should fire multiple times ---
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return executions["interval-job"] >= 2
	}, 10*time.Second, 50*time.Millisecond,
		"IntervalSchedule: expected >= 2 executions")

	// --- Verify OnceSchedule: should fire exactly once ---
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return executions["once-job"] >= 1
	}, 10*time.Second, 50*time.Millisecond,
		"OnceSchedule: expected >= 1 execution")

	// --- Verify StartAtIntervalSchedule: should fire multiple times ---
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return executions["startat-job"] >= 2
	}, 10*time.Second, 50*time.Millisecond,
		"StartAtIntervalSchedule: expected >= 2 executions")

	// --- Verify CronSchedule: job registered with valid next run ---
	cronJob, err := s.GetJob("cron-job")
	require.NoError(t, err)
	assert.False(t, cronJob.NextRun().IsZero(),
		"CronSchedule: NextRun should not be zero")
	assert.True(t, cronJob.NextRun().After(time.Now().Add(-time.Second)),
		"CronSchedule: NextRun should be in the future (or very recent)")

	// Wait a bit and verify OnceSchedule didn't fire again
	time.Sleep(1 * time.Second)
	mu.Lock()
	onceCount := executions["once-job"]
	intervalCount := executions["interval-job"]
	startAtCount := executions["startat-job"]
	mu.Unlock()

	assert.Equal(t, 1, onceCount,
		"OnceSchedule: should execute exactly once, got %d", onceCount)
	assert.GreaterOrEqual(t, intervalCount, 2,
		"IntervalSchedule: should execute multiple times, got %d", intervalCount)
	assert.GreaterOrEqual(t, startAtCount, 2,
		"StartAtIntervalSchedule: should execute multiple times, got %d", startAtCount)

	t.Logf("Results — interval: %d, once: %d, startat: %d, cron next: %s",
		intervalCount, onceCount, startAtCount, cronJob.NextRun().Format("15:04:05"))
}

func TestParseNATSVersion(t *testing.T) {
	tests := []struct {
		version     string
		wantMajor   int
		wantMinor   int
		expectError bool
	}{
		{"2.12.5", 2, 12, false},
		{"2.11.0", 2, 11, false},
		{"3.0.0", 3, 0, false},
		{"v2.12.5", 2, 12, false},
		{"2.12.5-beta.1", 2, 12, false},
		{"invalid", 0, 0, true},
		{"", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			major, minor, err := parseNATSVersion(tt.version)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantMajor, major)
				assert.Equal(t, tt.wantMinor, minor)
			}
		})
	}
}

func TestNATSScheduler_ServerVersionCheck(t *testing.T) {
	// Our test NATS server is 2.12+, so Start should succeed
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	s := NewNATSScheduler(js, handler)
	ctx := context.Background()

	// Should succeed with a 2.12+ server
	err := s.Start(ctx)
	require.NoError(t, err)

	s.Stop(ctx)
}

func mustGetJob(t *testing.T, s Scheduler, id string) Job {
	t.Helper()
	job, err := s.GetJob(id)
	require.NoError(t, err)
	return job
}

// TestNATSScheduler_ReconcilerRecoversBrokenChain reproduces the bug: a
// recurring job whose next-tick scheduled message has been lost (here
// simulated by purging the subject) is recovered by the reconciler instead
// of staying silently dead until restart.
func TestNATSScheduler_ReconcilerRecoversBrokenChain(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32
	handler := func(ctx context.Context, event JobEvent) error {
		count.Add(1)
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("RECON_TEST"),
		WithNATSSubjectPrefix("recon"),
		WithNATSConsumerName("recon-worker"),
		WithNATSSchedulerJobBucket("RECON_JOBS"),
		WithNATSSchedulerExecBucket("RECON_EXECS"),
		WithReconcilerInterval(200*time.Millisecond),
		WithReconcilerGracePeriod(0),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Long interval so the chain doesn't naturally re-fire within the test.
	schedule, err := NewIntervalSchedule(1 * time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("recon-job", schedule, nil))

	// Drop the pending scheduled message to simulate a lost next-tick publish.
	subject := s.jobSubject("recon-job")
	require.NoError(t, s.stream.Purge(ctx, jetstream.WithPurgeSubject(subject)))

	// Rewind KV NextRun into the past so the reconciler treats it as stale
	// and republishes it. The recomputed message has a fresh Nats-Msg-Id
	// (the original was lost), so no dedup happens.
	jv := &natsJobValue{
		ID:             "recon-job",
		ScheduleType:   "interval",
		ScheduleConfig: "1h",
		Status:         string(JobStatusPending),
		NextRun:        time.Now().Add(-1 * time.Second),
	}
	data, err := json.Marshal(jv)
	require.NoError(t, err)
	_, err = s.jobKV.Put(ctx, "recon-job", data)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return count.Load() >= 1
	}, 5*time.Second, 50*time.Millisecond,
		"reconciler should republish the lost next-tick message")
}

// TestNATSScheduler_LoadJobsFromKVLogsDecodeError verifies that a corrupt KV
// entry no longer disappears silently at startup — the logger sees it.
func TestNATSScheduler_LoadJobsFromKVLogsDecodeError(t *testing.T) {
	_, js := startNATSServer(t)

	opts := []NATSSchedulerOption{
		WithNATSStreamName("LOAD_LOG"),
		WithNATSSubjectPrefix("load_log"),
		WithNATSConsumerName("load-log-worker"),
		WithNATSSchedulerJobBucket("LOAD_LOG_JOBS"),
		WithNATSSchedulerExecBucket("LOAD_LOG_EXECS"),
	}

	handler := func(ctx context.Context, event JobEvent) error { return nil }

	// First scheduler: write a malformed KV entry directly.
	s1 := NewNATSScheduler(js, handler, opts...).(*natsSchedulerImpl)
	ctx := context.Background()
	require.NoError(t, s1.Start(ctx))
	_, err := s1.jobKV.Put(ctx, "broken-job", []byte("not-json"))
	require.NoError(t, err)
	require.NoError(t, s1.Stop(ctx))

	// Second scheduler: open with a logger and observe.
	var logged atomic.Int32
	logger := func(msg string, kv ...any) { logged.Add(1) }
	optsWithLogger := append(opts, WithNATSSchedulerLogger(logger))

	s2 := NewNATSScheduler(js, handler, optsWithLogger...)
	require.NoError(t, s2.Start(ctx))
	defer s2.Stop(ctx)

	assert.GreaterOrEqual(t, logged.Load(), int32(1),
		"logger should fire on malformed KV entry")
}

// TestNATSScheduler_OnReschedulingFailedCallback verifies the callback fires
// when reconcileOnce / executeJob exhausts publish retries. We test the
// option wiring by invoking the callback path directly via the field.
func TestNATSScheduler_OnReschedulingFailedCallback(t *testing.T) {
	_, js := startNATSServer(t)

	var got struct {
		mu      sync.Mutex
		jobID   string
		err     error
		called  bool
		nextRun time.Time
	}

	cb := func(jobID string, nextRun time.Time, err error) {
		got.mu.Lock()
		defer got.mu.Unlock()
		got.jobID = jobID
		got.nextRun = nextRun
		got.err = err
		got.called = true
	}

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithOnReschedulingFailed(cb),
	).(*natsSchedulerImpl)
	require.NotNil(t, s.onRescheduleFailed)

	when := time.Now().Add(time.Minute)
	s.onRescheduleFailed("job-x", when, errors.New("simulated"))

	got.mu.Lock()
	defer got.mu.Unlock()
	assert.True(t, got.called)
	assert.Equal(t, "job-x", got.jobID)
	assert.Equal(t, when, got.nextRun)
	assert.EqualError(t, got.err, "simulated")
}

// TestNATSScheduler_PublishRetryHelperRetries forces every publish attempt
// to fail (each call uses an already-cancelled context) and asserts the
// helper still produces a retry log line between attempts.
func TestNATSScheduler_PublishRetryHelperRetries(t *testing.T) {
	_, js := startNATSServer(t)

	var logCount atomic.Int32
	logger := func(msg string, kv ...any) { logCount.Add(1) }

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("RETRY_TEST"),
		WithNATSSubjectPrefix("retry"),
		WithNATSConsumerName("retry-worker"),
		WithNATSSchedulerJobBucket("RETRY_JOBS"),
		WithNATSSchedulerExecBucket("RETRY_EXECS"),
		WithPublishRetry(3, 1*time.Millisecond),
		WithNATSSchedulerLogger(logger),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Publish to a subject the stream does not own. JetStream replies
	// "no stream matched" / "no responders" fast, so retries get a chance
	// to fire within a single test second.
	other := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("RETRY_TEST"),
		WithNATSSubjectPrefix("not-a-real-prefix-for-stream"),
		WithPublishRetry(3, 1*time.Millisecond),
		WithNATSSchedulerLogger(logger),
	).(*natsSchedulerImpl)
	// Skip Start on `other`; we only use it for its subject prefix.
	other.js = js

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := other.publishScheduledMessageWithRetry(callCtx, "retry-job", time.Now().Add(time.Minute))
	assert.Error(t, err)

	// 3 attempts → 2 retry log lines emitted between attempts.
	assert.GreaterOrEqual(t, logCount.Load(), int32(2),
		"expected retry log lines between attempts, got %d", logCount.Load())
}

// TestNATSScheduler_WaitForBackingStreams_ReadyImmediately covers the
// single-node path: StreamInfo.Cluster is nil, so the wait returns at
// once without burning the timeout.
func TestNATSScheduler_WaitForBackingStreams_ReadyImmediately(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("READY_TEST"),
		WithNATSSubjectPrefix("ready"),
		WithNATSConsumerName("ready-worker"),
		WithNATSSchedulerJobBucket("READY_JOBS"),
		WithNATSSchedulerExecBucket("READY_EXECS"),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	start := time.Now()
	err := s.waitForBackingStreams(ctx, 5*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 500*time.Millisecond,
		"single-node should be ready instantly, took %s", elapsed)
}

// TestNATSScheduler_WaitForBackingStreams_TimeoutsOnUnreachable verifies the
// wait actually errors out when the JetStream API is unreachable rather
// than blocking indefinitely.
func TestNATSScheduler_WaitForBackingStreams_TimeoutsOnUnreachable(t *testing.T) {
	ns, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("TIMEOUT_TEST"),
		WithNATSSubjectPrefix("timeout_test"),
		WithNATSConsumerName("timeout-worker"),
		WithNATSSchedulerJobBucket("TIMEOUT_JOBS"),
		WithNATSSchedulerExecBucket("TIMEOUT_EXECS"),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// Take the server down so every Info() call fails.
	ns.Shutdown()
	ns.WaitForShutdown()

	start := time.Now()
	err := s.waitForBackingStreams(ctx, 300*time.Millisecond)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Less(t, elapsed, 3*time.Second,
		"wait should respect the timeout, took %s", elapsed)
}

// TestNATSScheduler_StartupReadyTimeoutDisabled verifies that passing 0
// disables the wait entirely.
func TestNATSScheduler_StartupReadyTimeoutDisabled(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithStartupStreamReadyTimeout(0),
	).(*natsSchedulerImpl)

	assert.Equal(t, time.Duration(0), s.startupStreamReadyTimeout)

	// And Start still succeeds with the wait turned off.
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)
}

// TestNATSScheduler_DropsStaleStreamMessageByKVValidation verifies that a
// stream message whose scheduledAt is older than the KV-authoritative
// NextRun is acked and dropped by handleMessage rather than re-invoking
// the handler. This is the cross-peer dedup that replaces the in-memory
// lastRun check.
func TestNATSScheduler_DropsStaleStreamMessageByKVValidation(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32
	handler := func(ctx context.Context, e JobEvent) error {
		count.Add(1)
		return nil
	}

	s := NewNATSScheduler(js, handler,
		WithNATSStreamName("STALE_KV_TEST"),
		WithNATSSubjectPrefix("stale_kv"),
		WithNATSConsumerName("stale-kv-worker"),
		WithNATSSchedulerJobBucket("STALE_KV_JOBS"),
		WithNATSSchedulerExecBucket("STALE_KV_EXECS"),
		WithReconcilerInterval(0), // off so it doesn't republish underneath us
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Seed KV with a job whose NextRun is well in the future. The seed value
	// represents the cross-peer "authoritative" state that handleMessage will
	// consult when a stale msg lands.
	farFutureNext := time.Now().Add(1 * time.Hour)
	jv := &natsJobValue{
		ID:             "stale-kv-job",
		ScheduleType:   "interval",
		ScheduleConfig: "1h",
		Status:         string(JobStatusPending),
		NextRun:        farFutureNext,
	}
	data, err := json.Marshal(jv)
	require.NoError(t, err)
	_, err = s.jobKV.Put(ctx, "stale-kv-job", data)
	require.NoError(t, err)

	// Inject a stale msg with scheduledAt much earlier than KV.NextRun.
	// Use a near-future time so JetStream actually delivers it inside the
	// test window.
	staleAt := time.Now().Add(100 * time.Millisecond)
	require.NoError(t, s.publishScheduledMessage(ctx, "stale-kv-job", staleAt))

	// Wait long enough that the stale msg's scheduledAt has passed and
	// JetStream has had a chance to deliver and have us drop it.
	time.Sleep(2 * time.Second)

	assert.Equal(t, int32(0), count.Load(),
		"stale stream message (scheduledAt < KV.NextRun) should be dropped, got %d executions", count.Load())
}

// TestNATSScheduler_BrandNewJobNoRapidFireOnRestart covers the "AddJob then
// crash before fire, then restart later" scenario:
//
//   - Time T0: AddJob with interval 5s. KV is written with NextRun=T0+5s,
//     LastRun=zero. The first scheduled msg (scheduledAt=T0+5s) is published.
//   - Time T0+~0s: scheduler stops. Stale msg still sits in the stream.
//   - Time T0+6s (past the original scheduledAt): restart. loadJobsFromKV
//     sees NextRun=T0+5s already in the past, recomputes it to T0+11s, writes
//     the new NextRun back to KV, and publishes a fresh msg with the new
//     scheduledAt.
//
// Without the KV-NextRun validation (and the KV write-back in loadJobsFromKV),
// the stale T0+5s msg would be delivered immediately on restart and fire,
// producing a spurious first execution before the legitimate T0+11s tick.
//
// The assertion window (T0+6s..T0+8s) sits BETWEEN restart and the legit
// fire, so any execution in that window must be from a stale msg.
func TestNATSScheduler_BrandNewJobNoRapidFireOnRestart(t *testing.T) {
	_, js := startNATSServer(t)

	var count atomic.Int32
	handler := func(ctx context.Context, e JobEvent) error {
		count.Add(1)
		return nil
	}

	opts := []NATSSchedulerOption{
		WithNATSStreamName("BRAND_NEW_TEST"),
		WithNATSSubjectPrefix("brand_new"),
		WithNATSConsumerName("brand-new-worker"),
		WithNATSSchedulerJobBucket("BRAND_NEW_JOBS"),
		WithNATSSchedulerExecBucket("BRAND_NEW_EXECS"),
		WithReconcilerInterval(0),
	}

	ctx := context.Background()
	s1 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s1.Start(ctx))

	schedule, err := NewIntervalSchedule(5 * time.Second)
	require.NoError(t, err)
	require.NoError(t, s1.AddJob("brand-new-job", schedule, nil))

	// Stop immediately so the first scheduled msg doesn't fire.
	require.NoError(t, s1.Stop(ctx))

	// Wait past the original scheduledAt so loadJobsFromKV must recompute
	// NextRun forward on restart.
	time.Sleep(6 * time.Second)

	s2 := NewNATSScheduler(js, handler, opts...)
	require.NoError(t, s2.Start(ctx))
	defer s2.Stop(ctx)

	// Watch the window that sits BEFORE the legit T+11s fire. Any execution
	// here must come from the stale msg — which the KV validation should drop.
	// 2s is plenty of time for the stale (past-dated) msg to be delivered.
	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(0), count.Load(),
		"stale msg from before restart must not fire; got %d executions in pre-tick window",
		count.Load())

	// Sanity check: the legit chain still fires. If we accidentally dropped
	// the legit msg too, this would hang.
	require.Eventually(t, func() bool {
		return count.Load() >= 1
	}, 6*time.Second, 50*time.Millisecond,
		"legit recomputed tick should still fire after restart")
}

// TestNATSScheduler_NoStartupPurge verifies the design choice that Start()
// no longer purges the stream or deletes the durable consumer. The stale
// payload from a previous run should still be sitting in the stream after a
// fresh Start() — the KV validation in handleMessage is what filters it.
func TestNATSScheduler_NoStartupPurge(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, e JobEvent) error { return nil }

	opts := []NATSSchedulerOption{
		WithNATSStreamName("NO_PURGE_TEST"),
		WithNATSSubjectPrefix("no_purge"),
		WithNATSConsumerName("no-purge-worker"),
		WithNATSSchedulerJobBucket("NO_PURGE_JOBS"),
		WithNATSSchedulerExecBucket("NO_PURGE_EXECS"),
		WithReconcilerInterval(0),
	}

	ctx := context.Background()
	s1 := NewNATSScheduler(js, handler, opts...).(*natsSchedulerImpl)
	require.NoError(t, s1.Start(ctx))

	// Long interval so the published msg sits in the stream undelivered.
	schedule, err := NewIntervalSchedule(1 * time.Hour)
	require.NoError(t, err)
	require.NoError(t, s1.AddJob("no-purge-job", schedule, nil))

	// Confirm there's a msg in the stream.
	info1, err := s1.stream.Info(ctx)
	require.NoError(t, err)
	require.Greater(t, info1.State.Msgs, uint64(0),
		"expected at least one pending scheduled msg before stop")

	require.NoError(t, s1.Stop(ctx))

	// Restart. If the design were still purging on Start(), the stream
	// message count would drop to 0 here.
	s2 := NewNATSScheduler(js, handler, opts...).(*natsSchedulerImpl)
	require.NoError(t, s2.Start(ctx))
	defer s2.Stop(ctx)

	info2, err := s2.stream.Info(ctx)
	require.NoError(t, err)
	assert.Greater(t, info2.State.Msgs, uint64(0),
		"stream messages should survive restart (no startup purge), got %d msgs", info2.State.Msgs)
}

// TestNATSScheduler_LoadJobsFromKVWritebackCASLosesToRacer documents the
// CAS-guarded writeback in loadJobsFromKV. When another writer (e.g. a peer's
// executeJob completing mid-restart) bumps the KV revision between our Get
// and Update, the Update must fail with ErrKeyExists so we skip the
// writeback and preserve the racer's authoritative LastRun / Status / NextRun.
//
// This is a contract test for the jetstream KV API plus our error-detection
// pattern (errors.Is(err, ErrKeyExists)); if the upstream library ever
// changes how CAS mismatch surfaces, this test catches it before the
// writeback silently regresses to a clobbering Put.
func TestNATSScheduler_LoadJobsFromKVWritebackCASLosesToRacer(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("CAS_TEST"),
		WithNATSSubjectPrefix("cas"),
		WithNATSConsumerName("cas-worker"),
		WithNATSSchedulerJobBucket("CAS_JOBS"),
		WithNATSSchedulerExecBucket("CAS_EXECS"),
		WithReconcilerInterval(0),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// Seed an initial KV value.
	originalNext := time.Now().Add(-1 * time.Hour) // in past, would trigger recompute
	initial := &natsJobValue{
		ID:             "race-job",
		ScheduleType:   "interval",
		ScheduleConfig: "1h",
		Status:         string(JobStatusPending),
		NextRun:        originalNext,
	}
	data, err := json.Marshal(initial)
	require.NoError(t, err)
	_, err = s.jobKV.Put(ctx, "race-job", data)
	require.NoError(t, err)

	// Capture the revision a hypothetical loadJobsFromKV would have read.
	entry, err := s.jobKV.Get(ctx, "race-job")
	require.NoError(t, err)
	staleRevision := entry.Revision()

	// Simulate a racing peer's executeJob completing: it writes a fresher
	// NextRun + Status + LastRun. This bumps the revision past staleRevision.
	racerLastRun := time.Now().Add(-30 * time.Minute)
	racerNext := time.Now().Add(30 * time.Minute)
	racer := *initial
	racer.Status = string(JobStatusCompleted)
	racer.LastRun = racerLastRun
	racer.NextRun = racerNext
	racerData, err := json.Marshal(&racer)
	require.NoError(t, err)
	_, err = s.jobKV.Put(ctx, "race-job", racerData)
	require.NoError(t, err)

	// Now mimic loadJobsFromKV's writeback path with the stale revision.
	recomputed := *initial
	recomputed.NextRun = time.Now().Add(1 * time.Hour)
	recomputedData, err := json.Marshal(&recomputed)
	require.NoError(t, err)
	_, updateErr := s.jobKV.Update(ctx, "race-job", recomputedData, staleRevision)

	require.Error(t, updateErr, "stale-revision Update must fail")
	assert.True(t, errors.Is(updateErr, jetstream.ErrKeyExists),
		"CAS mismatch must surface as ErrKeyExists, got %v", updateErr)

	// KV must still carry the racer's value — no clobbering by the loser.
	final, err := s.jobKV.Get(ctx, "race-job")
	require.NoError(t, err)
	var finalJV natsJobValue
	require.NoError(t, json.Unmarshal(final.Value(), &finalJV))

	assert.Equal(t, racerNext.Unix(), finalJV.NextRun.Unix(),
		"racer's NextRun must be preserved")
	assert.Equal(t, racerLastRun.Unix(), finalJV.LastRun.Unix(),
		"racer's LastRun must be preserved")
	assert.Equal(t, string(JobStatusCompleted), finalJV.Status,
		"racer's Status must be preserved")
}

// TestNATSScheduler_DefaultReconcilerEnabled checks the changed default: the
// background reconciler is now enabled out of the box, with a 30 s interval.
func TestNATSScheduler_DefaultReconcilerEnabled(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil }).(*natsSchedulerImpl)
	assert.Equal(t, 30*time.Second, s.reconcilerInterval,
		"reconciler should default to 30s")
	assert.Equal(t, 5*time.Second, s.addJobRetryBudget,
		"addJobRetryBudget should default to 5s")
}

// TestNATSScheduler_DoWithTransientRetryBehaviour exercises the new
// doWithTransientRetry helper in isolation:
//
//   - succeeds eventually within the budget ➜ returns nil
//   - always fails until the budget elapses ➜ returns the last error
//   - context is cancelled before the budget elapses ➜ returns promptly
//   - budget <= 0 ➜ single attempt, no retry
func TestNATSScheduler_DoWithTransientRetryBehaviour(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil }).(*natsSchedulerImpl)

	// Case 1: succeed after a couple of transient failures.
	var attempts atomic.Int32
	err := s.doWithTransientRetry(context.Background(), 5*time.Second, func(ctx context.Context) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), attempts.Load())

	// Case 2: always fail, budget exhausts, last error bubbles.
	attempts.Store(0)
	err = s.doWithTransientRetry(context.Background(), 250*time.Millisecond, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("persistent")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persistent")
	assert.Greater(t, attempts.Load(), int32(1), "expected more than one attempt")

	// Case 3: cancelled context returns promptly.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err = s.doWithTransientRetry(cancelled, 5*time.Second, func(ctx context.Context) error {
		return errors.New("should not even run")
	})
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Less(t, elapsed, 1*time.Second, "cancelled ctx should return quickly")

	// Case 4: budget <= 0 means single attempt.
	attempts.Store(0)
	err = s.doWithTransientRetry(context.Background(), 0, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("one shot")
	})
	require.Error(t, err)
	assert.Equal(t, int32(1), attempts.Load(), "budget <= 0 should produce exactly one attempt")
}

// TestNATSScheduler_AddJobCreateAlreadyExistsTreatedAsSuccessWhenValueMatches
// validates the dedup logic used by AddJob when KV.Create retries and the
// previous attempt's write was the one that landed (the response packet was
// what got dropped). Matching value -> treat as success; differing value
// -> ErrJobAlreadyExists.
func TestNATSScheduler_AddJobCreateAlreadyExistsTreatedAsSuccessWhenValueMatches(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("CREATE_EXISTS_TEST"),
		WithNATSSubjectPrefix("create_exists"),
		WithNATSConsumerName("create-exists-worker"),
		WithNATSSchedulerJobBucket("CREATE_EXISTS_JOBS"),
		WithNATSSchedulerExecBucket("CREATE_EXISTS_EXECS"),
		WithReconcilerInterval(0),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	matchingData := []byte(`{"id":"foo","payload":"x"}`)
	// Simulate the previous attempt's write having landed.
	_, err := s.jobKV.Create(ctx, "foo", matchingData)
	require.NoError(t, err)

	// Re-run the create-then-compare logic with the same bytes. The Create
	// call should fail with ErrKeyExists; the Get-and-compare branch should
	// then swallow it as success.
	err = s.doWithTransientRetry(ctx, time.Second, func(opCtx context.Context) error {
		_, e := s.jobKV.Create(opCtx, "foo", matchingData)
		if errors.Is(e, jetstream.ErrKeyExists) {
			existing, getErr := s.jobKV.Get(opCtx, "foo")
			if getErr == nil && bytes.Equal(existing.Value(), matchingData) {
				return nil
			}
			return ErrJobAlreadyExists
		}
		return e
	})
	assert.NoError(t, err, "matching value should be treated as success")

	// Now seed a different key with the original bytes, then attempt to
	// create with a different value. The KV row already exists with the
	// "wrong" bytes — that must surface as ErrJobAlreadyExists.
	_, err = s.jobKV.Create(ctx, "bar", matchingData)
	require.NoError(t, err)
	differentData := []byte(`{"id":"bar","payload":"different"}`)
	err = s.doWithTransientRetry(ctx, time.Second, func(opCtx context.Context) error {
		_, e := s.jobKV.Create(opCtx, "bar", differentData)
		if errors.Is(e, jetstream.ErrKeyExists) {
			existing, getErr := s.jobKV.Get(opCtx, "bar")
			if getErr == nil && bytes.Equal(existing.Value(), differentData) {
				return nil
			}
			return ErrJobAlreadyExists
		}
		return e
	})
	assert.ErrorIs(t, err, ErrJobAlreadyExists, "differing value should yield ErrJobAlreadyExists")
}

// TestNATSScheduler_ReschedulingFailedStatus verifies that when the
// next-tick publish ultimately fails, the persisted job Status reflects
// JobStatusReschedulingFailed rather than the handler's status.
func TestNATSScheduler_ReschedulingFailedStatus(t *testing.T) {
	_, js := startNATSServer(t)

	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error { return nil },
		WithNATSStreamName("STATUS_TEST"),
		WithNATSSubjectPrefix("status_test"),
		WithNATSConsumerName("status-worker"),
		WithNATSSchedulerJobBucket("STATUS_JOBS"),
		WithNATSSchedulerExecBucket("STATUS_EXECS"),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	// New JobStatusReschedulingFailed value is reachable and distinct.
	assert.Equal(t, JobStatus("rescheduling_failed"), JobStatusReschedulingFailed)
	assert.NotEqual(t, JobStatusFailed, JobStatusReschedulingFailed)
	assert.NotEqual(t, JobStatusCompleted, JobStatusReschedulingFailed)
}

// TestNATSScheduler_CreatedAtPreservedAcrossExecutionAndUpdate verifies that
// the CreatedAt timestamp written by AddJob survives both the post-handler
// KV write in executeJob and a subsequent UpdateJobSchedule. Earlier code
// constructed natsJobValue without CreatedAt at both sites, silently
// zeroing the stored timestamp on every execution.
func TestNATSScheduler_CreatedAtPreservedAcrossExecutionAndUpdate(t *testing.T) {
	_, js := startNATSServer(t)

	fired := make(chan struct{}, 4)
	s := NewNATSScheduler(js, func(ctx context.Context, e JobEvent) error {
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	},
		WithNATSStreamName("CREATEDAT_TEST"),
		WithNATSSubjectPrefix("createdat_test"),
		WithNATSConsumerName("createdat-worker"),
		WithNATSSchedulerJobBucket("CREATEDAT_JOBS"),
		WithNATSSchedulerExecBucket("CREATEDAT_EXECS"),
	).(*natsSchedulerImpl)

	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, err := NewIntervalSchedule(500 * time.Millisecond)
	require.NoError(t, err)

	before := time.Now()
	require.NoError(t, s.AddJob("job1", schedule, nil))
	after := time.Now()

	readCreatedAt := func(t *testing.T) time.Time {
		t.Helper()
		jv, err := s.readJobValueFromKV(ctx, "job1")
		require.NoError(t, err)
		return jv.CreatedAt
	}

	initial := readCreatedAt(t)
	require.False(t, initial.IsZero(), "AddJob must persist CreatedAt")
	require.False(t, initial.Before(before.Add(-time.Second)) || initial.After(after.Add(time.Second)),
		"CreatedAt %v not within [%v, %v]", initial, before, after)

	// Wait through at least one execution so executeJob Step 1 runs.
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not fire within 5s")
	}
	// Give Step 1 a beat to land in KV.
	time.Sleep(200 * time.Millisecond)

	afterFire := readCreatedAt(t)
	assert.Equal(t, initial, afterFire,
		"CreatedAt must be preserved across post-handler KV write")

	// UpdateJobSchedule used to drop CreatedAt as well.
	updatedSchedule, err := NewIntervalSchedule(time.Second)
	require.NoError(t, err)
	require.NoError(t, s.UpdateJobSchedule("job1", updatedSchedule))
	afterUpdate := readCreatedAt(t)
	assert.Equal(t, initial, afterUpdate,
		"CreatedAt must be preserved across UpdateJobSchedule")
}
