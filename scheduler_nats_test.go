package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
