package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHandlerState struct {
	mu      sync.RWMutex
	handler func(context.Context, JobEvent) error
}

func newTestHandlerState() *testHandlerState {
	return &testHandlerState{}
}

func (s *testHandlerState) Handler() JobHandler {
	return func(ctx context.Context, event JobEvent) error {
		s.mu.RLock()
		fn := s.handler
		s.mu.RUnlock()
		if fn == nil {
			return nil
		}
		return fn(ctx, event)
	}
}

func (s *testHandlerState) SetJobFunc(fn func(context.Context, JobEvent) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = fn
}

func newTestScheduler(storage Storage) (Scheduler, *testHandlerState) {
	state := newTestHandlerState()
	return NewScheduler(storage, state.Handler(), NewBasicScheduleCodec()), state
}

// TestNewScheduler tests creating a new scheduler
func TestNewScheduler(t *testing.T) {
	// Test with storage
	storage := NewMemoryStorage()
	scheduler, _ := newTestScheduler(storage)
	assert.NotNil(t, scheduler)
	assert.False(t, scheduler.IsRunning())

	// Test without storage
	scheduler, _ = newTestScheduler(nil)
	assert.NotNil(t, scheduler)
	assert.False(t, scheduler.IsRunning())
}

// TestSchedulerStartStop tests starting and stopping the scheduler
func TestSchedulerStartStop(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, _ := newTestScheduler(storage)

	// Test start
	err := scheduler.Start(ctx)
	assert.NoError(t, err)
	assert.True(t, scheduler.IsRunning())

	// Test double start
	err = scheduler.Start(ctx)
	assert.Equal(t, ErrSchedulerAlreadyStarted, err)

	// Test stop
	err = scheduler.Stop(ctx)
	assert.NoError(t, err)
	assert.False(t, scheduler.IsRunning())

	// Test double stop
	err = scheduler.Stop(ctx)
	assert.Equal(t, ErrSchedulerNotStarted, err)
}

func TestSchedulerWaitUntilRunning(t *testing.T) {
	ctx := context.Background()
	scheduler, _ := newTestScheduler(nil)

	// Wait without starting should respect context timeout
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := scheduler.WaitUntilRunning(waitCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Start and ensure wait returns immediately
	err = scheduler.Start(ctx)
	require.NoError(t, err)
	err = scheduler.WaitUntilRunning(ctx)
	assert.NoError(t, err)

	err = scheduler.Stop(ctx)
	require.NoError(t, err)

	// Wait while Start is invoked asynchronously
	waitCtx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		errCh <- scheduler.Start(ctx)
	}()

	err = scheduler.WaitUntilRunning(waitCtx)
	assert.NoError(t, err)

	startErr := <-errCh
	assert.NoError(t, startErr)

	require.NoError(t, scheduler.Stop(ctx))
}

func TestJobEventProvidesScheduleInfo(t *testing.T) {
	ctx := context.Background()
	scheduler, state := newTestScheduler(nil)

	schedule, err := NewIntervalSchedule(50 * time.Millisecond)
	require.NoError(t, err)

	eventCh := make(chan JobEvent, 1)
	state.SetJobFunc(func(ctx context.Context, event JobEvent) error {
		select {
		case eventCh <- event:
		default:
		}
		return nil
	})

	require.NoError(t, scheduler.Start(ctx))
	require.NoError(t, scheduler.WaitUntilRunning(ctx))

	metadata := map[string]string{"foo": "bar"}
	require.NoError(t, scheduler.AddJob("job-info", schedule, metadata))

	var event JobEvent
	select {
	case event = <-eventCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for job execution")
	}

	assert.Equal(t, "job-info", event.ID())
	meta := event.Metadata()
	assert.Equal(t, "bar", meta["foo"])
	assert.False(t, event.ScheduledAt().IsZero())
	assert.False(t, event.StartedAt().IsZero())
	assert.False(t, event.StartedAt().Before(event.ScheduledAt()))
	assert.True(t, event.Delay() >= 0)
	assert.True(t, event.LastCompletedAt().IsZero())

	if sched := event.Schedule(); assert.NotNil(t, sched) {
		_, ok := sched.(*IntervalSchedule)
		assert.True(t, ok)
	}

	require.NoError(t, scheduler.Stop(ctx))
}

func TestUpdateJobSchedule(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, _ := newTestScheduler(storage)

	require.NoError(t, scheduler.Start(ctx))
	require.NoError(t, scheduler.WaitUntilRunning(ctx))
	defer scheduler.Stop(ctx)

	initialSchedule, err := NewIntervalSchedule(1 * time.Hour)
	require.NoError(t, err)

	require.NoError(t, scheduler.AddJob("job-update", initialSchedule, map[string]string{"key": "value"}))

	target := time.Now().Add(2 * time.Minute).Round(time.Second)
	newSchedule, err := NewOnceSchedule(target)
	require.NoError(t, err)

	require.NoError(t, scheduler.UpdateJobSchedule("job-update", newSchedule))

	job, err := scheduler.GetJob("job-update")
	require.NoError(t, err)
	assert.WithinDuration(t, target, job.NextRun(), time.Second)

	storedJob, err := storage.GetJob(ctx, "job-update")
	require.NoError(t, err)
	assert.Equal(t, JobStatusPending, storedJob.Status)
	assert.Equal(t, "once", storedJob.ScheduleType)
	assert.Equal(t, target.UTC().Format(time.RFC3339Nano), storedJob.ScheduleConfig)
	assert.WithinDuration(t, target, storedJob.NextRun, time.Second)
}

func TestUpdateJobScheduleValidation(t *testing.T) {
	scheduler, _ := newTestScheduler(nil)

	assert.Equal(t, ErrEmptyJobID, scheduler.UpdateJobSchedule("", nil))
	assert.Equal(t, ErrInvalidInterval, scheduler.UpdateJobSchedule("job", nil))

	interval, err := NewIntervalSchedule(time.Minute)
	require.NoError(t, err)
	assert.Equal(t, ErrJobNotFound, scheduler.UpdateJobSchedule("missing", interval))
}

// TestSchedulerAddJob tests adding jobs to the scheduler
func TestSchedulerAddJob(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule, err := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, err)
	counter := int32(0)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "job1" {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	}

	// Test add job successfully
	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Test add duplicate job
	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.Equal(t, ErrJobAlreadyExists, err)

	// Test add job with empty ID
	err = scheduler.AddJob("", schedule, nil)
	assert.Equal(t, ErrEmptyJobID, err)

	// Test add job with nil schedule
	err = scheduler.AddJob("job2", nil, nil)
	assert.Equal(t, ErrInvalidInterval, err)
}

// TestSchedulerRemoveJob tests removing jobs from the scheduler
func TestSchedulerRemoveJob(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule, err := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, err)

	// Add a job
	state.SetJobFunc(func(context.Context, JobEvent) error { return nil })
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Remove the job
	err = scheduler.RemoveJob("job1")
	assert.NoError(t, err)

	// Test remove non-existent job
	err = scheduler.RemoveJob("nonexistent")
	assert.Equal(t, ErrJobNotFound, err)

	// Test remove with empty ID
	err = scheduler.RemoveJob("")
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestSchedulerGetJob tests retrieving jobs by ID
func TestSchedulerGetJob(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule, err := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, err)

	// Add a job
	state.SetJobFunc(func(context.Context, JobEvent) error { return nil })
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Get the job
	job, err := scheduler.GetJob("job1")
	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "job1", job.ID())

	// Test get non-existent job
	_, err = scheduler.GetJob("nonexistent")
	assert.Equal(t, ErrJobNotFound, err)

	// Test get with empty ID
	_, err = scheduler.GetJob("")
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestSchedulerListJobs tests listing all jobs
func TestSchedulerListJobs(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Test empty list
	jobs := scheduler.ListJobs()
	assert.Empty(t, jobs)

	// Add multiple jobs
	schedule, err := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, err)

	state.SetJobFunc(func(context.Context, JobEvent) error { return nil })
	for i := 1; i <= 3; i++ {
		jobID := string(rune('A' + i - 1))
		err = scheduler.AddJob(jobID, schedule, nil)
		assert.NoError(t, err)
	}

	// List all jobs
	jobs = scheduler.ListJobs()
	assert.Len(t, jobs, 3)
}

// TestSchedulerJobExecution tests that jobs are executed
func TestSchedulerJobExecution(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that runs immediately
	runAt := time.Now().Add(100 * time.Millisecond)
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	counter := int32(0)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "job1" {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for job to execute
	time.Sleep(2 * time.Second)

	// Verify job was executed
	assert.Equal(t, int32(1), atomic.LoadInt32(&counter))

	// Verify job state
	job, err := scheduler.GetJob("job1")
	assert.NoError(t, err)
	assert.False(t, job.IsRunning())
	assert.False(t, job.LastRun().IsZero())
}

// TestSchedulerJobExecutionWithError tests job execution that returns an error
func TestSchedulerJobExecutionWithError(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that fails
	runAt := time.Now().Add(100 * time.Millisecond)
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	expectedErr := errors.New("job failed")
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "job1" {
			return expectedErr
		}
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for job to execute
	time.Sleep(2 * time.Second)

	// Verify execution was recorded with error
	executions, err := storage.ListExecutions(ctx, "job1", nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, executions)
	assert.Equal(t, JobStatusFailed, executions[0].Status)
	assert.Equal(t, expectedErr.Error(), executions[0].Error)
}

// TestSchedulerIntervalExecution tests interval-based job execution
func TestSchedulerIntervalExecution(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that runs every 500ms
	schedule, err := NewIntervalSchedule(500 * time.Millisecond)
	require.NoError(t, err)
	counter := int32(0)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "job1" {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for multiple executions
	time.Sleep(3000 * time.Millisecond)

	// Verify job was executed multiple times (at least 2 times)
	count := atomic.LoadInt32(&counter)
	assert.GreaterOrEqual(t, count, int32(2))
}

// TestSchedulerStoragePersistence tests that job data is persisted to storage
func TestSchedulerStoragePersistence(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)

	runAt := time.Now().Add(100 * time.Millisecond)
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		return nil
	}

	// Add a job
	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for execution
	time.Sleep(2 * time.Second)

	// Verify job data in storage
	jobData, err := storage.GetJob(ctx, "job1")
	assert.NoError(t, err)
	assert.Equal(t, "job1", jobData.ID)

	// Verify execution record in storage
	executions, err := storage.ListExecutions(ctx, "job1", nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, executions)
	assert.Equal(t, "job1", executions[0].JobID)
	assert.Equal(t, JobStatusCompleted, executions[0].Status)

	scheduler.Stop(ctx)
}

// TestSchedulerConcurrentJobExecution tests concurrent job execution
func TestSchedulerConcurrentJobExecution(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create multiple jobs that run concurrently
	var wg sync.WaitGroup
	counter := int32(0)
	handlerMu := sync.RWMutex{}
	jobHandlers := make(map[string]func(context.Context, JobEvent) error)

	state.SetJobFunc(func(ctx context.Context, event JobEvent) error {
		handlerMu.RLock()
		fn := jobHandlers[event.ID()]
		handlerMu.RUnlock()
		if fn == nil {
			return nil
		}
		return fn(ctx, event)
	})

	for i := 0; i < 5; i++ {
		wg.Add(1)
		jobID := string(rune('A' + i))
		// Each job needs its own schedule instance
		runAt := time.Now().Add(100 * time.Millisecond)
		schedule, err := NewOnceSchedule(runAt)
		require.NoError(t, err)
		jobFunc := func(ctx context.Context, event JobEvent) error {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
			atomic.AddInt32(&counter, 1)
			return nil
		}

		handlerMu.Lock()
		jobHandlers[jobID] = jobFunc
		handlerMu.Unlock()
		err = scheduler.AddJob(jobID, schedule, nil)
		assert.NoError(t, err)
	}

	// Wait for all jobs to complete
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// All jobs completed
	case <-time.After(5 * time.Second):
		t.Fatal("Jobs did not complete in time")
	}

	// Verify all jobs were executed
	assert.Equal(t, int32(5), atomic.LoadInt32(&counter))
}

// TestSchedulerRemoveRunningJob tests removing a job while it's running
func TestSchedulerRemoveRunningJob(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that takes some time to execute
	runAt := time.Now().Add(100 * time.Millisecond)
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	started := make(chan bool, 1)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() != "job1" {
			return nil
		}
		started <- true
		time.Sleep(1 * time.Second)
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for job to start
	<-started

	// Try to remove the job while it's running
	done := make(chan bool)
	go func() {
		err := scheduler.RemoveJob("job1")
		assert.NoError(t, err)
		done <- true
	}()

	// Should complete after job finishes
	select {
	case <-done:
		// Job removed successfully
	case <-time.After(3 * time.Second):
		t.Fatal("RemoveJob did not complete in time")
	}

	// Verify job was removed
	_, err = scheduler.GetJob("job1")
	assert.Equal(t, ErrJobNotFound, err)
}

// TestSchedulerGracefulShutdown tests graceful shutdown with running jobs
func TestSchedulerGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)

	// Create a job that takes some time to execute
	runAt := time.Now().Add(100 * time.Millisecond)
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	started := make(chan bool, 1)
	completed := int32(0)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() != "job1" {
			return nil
		}
		started <- true
		time.Sleep(1 * time.Second)
		atomic.StoreInt32(&completed, 1)
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for job to start
	select {
	case <-started:
		// Job started
	case <-time.After(3 * time.Second):
		t.Fatal("Job did not start in time")
	}

	// Stop scheduler (should wait for job to complete)
	err = scheduler.Stop(ctx)
	assert.NoError(t, err)

	// Verify job completed
	assert.Equal(t, int32(1), atomic.LoadInt32(&completed))
}

// TestJobInterface tests the Job interface methods
func TestJobInterface(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule, err := NewIntervalSchedule(1 * time.Hour)
	require.NoError(t, err)
	state.SetJobFunc(func(context.Context, JobEvent) error { return nil })

	metadata := map[string]string{"env": "test"}
	err = scheduler.AddJob("job1", schedule, metadata)
	assert.NoError(t, err)

	job, err := scheduler.GetJob("job1")
	require.NoError(t, err)

	// Test ID
	assert.Equal(t, "job1", job.ID())

	// Test NextRun
	nextRun := job.NextRun()
	assert.False(t, nextRun.IsZero())

	// Test LastRun (should be zero before execution)
	lastRun := job.LastRun()
	assert.True(t, lastRun.IsZero())

	// Test IsRunning (should be false)
	assert.False(t, job.IsRunning())

	// Test Metadata copy
	meta := job.Metadata()
	assert.Equal(t, "test", meta["env"])
	meta["env"] = "mutated"

	// Ensure original metadata is unchanged
	meta2 := job.Metadata()
	assert.Equal(t, "test", meta2["env"])
}

// TestSchedulerWithNilStorage tests scheduler without storage
func TestSchedulerWithNilStorage(t *testing.T) {
	ctx := context.Background()
	scheduler, state := newTestScheduler(nil)

	err := scheduler.Start(ctx)
	assert.NoError(t, err)
	assert.True(t, scheduler.IsRunning())

	schedule, err := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, err)
	counter := int32(0)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "job1" {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	jobs := scheduler.ListJobs()
	assert.Len(t, jobs, 1)

	err = scheduler.Stop(ctx)
	assert.NoError(t, err)
}

// TestSchedulerExecutionRecordDetails tests that execution records contain correct details
func TestSchedulerExecutionRecordDetails(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	runAt := time.Now().Add(100 * time.Millisecond)
	schedule, err := NewOnceSchedule(runAt)
	require.NoError(t, err)
	executionDuration := 100 * time.Millisecond
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "job1" {
			time.Sleep(executionDuration)
		}
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Wait for execution to complete
	time.Sleep(2 * time.Second)

	// Retrieve execution record
	executions, err := storage.ListExecutions(ctx, "job1", nil)
	require.NoError(t, err)
	require.Len(t, executions, 1)

	record := executions[0]
	assert.Equal(t, "job1", record.JobID)
	assert.NotEmpty(t, record.ExecutionID)
	assert.False(t, record.StartTime.IsZero())
	assert.False(t, record.EndTime.IsZero())
	assert.True(t, record.EndTime.After(record.StartTime))
	assert.GreaterOrEqual(t, record.Duration, executionDuration)
	assert.Equal(t, JobStatusCompleted, record.Status)
	assert.Empty(t, record.Error)
}

// TestSchedulerAddJobBeforeStart tests adding jobs before scheduler starts
func TestSchedulerAddJobBeforeStart(t *testing.T) {
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)

	schedule, err := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, err)
	jobFunc := func(ctx context.Context, event JobEvent) error { return nil }

	// Add job before starting scheduler
	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("job1", schedule, nil)
	assert.NoError(t, err)

	// Verify job was added
	jobs := scheduler.ListJobs()
	assert.Len(t, jobs, 1)

	// Start scheduler
	ctx := context.Background()
	err = scheduler.Start(ctx)
	assert.NoError(t, err)

	// Verify job still exists
	job, err := scheduler.GetJob("job1")
	assert.NoError(t, err)
	assert.NotNil(t, job)

	scheduler.Stop(ctx)
}

// TestSchedulerMultipleStartStopCycles tests multiple start/stop cycles
func TestSchedulerMultipleStartStopCycles(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, _ := newTestScheduler(storage)

	for i := 0; i < 3; i++ {
		err := scheduler.Start(ctx)
		assert.NoError(t, err)
		assert.True(t, scheduler.IsRunning())

		err = scheduler.Stop(ctx)
		assert.NoError(t, err)
		assert.False(t, scheduler.IsRunning())
	}
}

// TestNewCronSchedule tests creating a new CronSchedule
func TestNewCronSchedule(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{
			name:       "valid - every friday at 10am",
			expression: "0 10 * * 5",
			wantErr:    false,
		},
		{
			name:       "valid - every day at 2:30pm",
			expression: "30 14 * * *",
			wantErr:    false,
		},
		{
			name:       "valid - first day of month at midnight",
			expression: "0 0 1 * *",
			wantErr:    false,
		},
		{
			name:       "valid - every 5 minutes",
			expression: "*/5 * * * *",
			wantErr:    false,
		},
		{
			name:       "invalid - empty expression",
			expression: "",
			wantErr:    true,
		},
		{
			name:       "invalid - malformed expression",
			expression: "invalid cron",
			wantErr:    true,
		},
		{
			name:       "invalid - too many fields",
			expression: "0 10 * * 5 2024",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, err := NewCronSchedule(tt.expression)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, schedule)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schedule)
				assert.Equal(t, tt.expression, schedule.Expression())
			}
		})
	}
}

// TestCronScheduleNext tests the Next method of CronSchedule
func TestCronScheduleNext(t *testing.T) {
	// Test: Every Friday at 10:00 AM
	schedule, err := NewCronSchedule("0 10 * * 5")
	require.NoError(t, err)

	// Start from a known time (Monday, Jan 1, 2024, 09:00 AM)
	baseTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)

	// First next run should be the next Friday at 10 AM
	nextRun := schedule.Next(baseTime)
	assert.Equal(t, time.Friday, nextRun.Weekday())
	assert.Equal(t, 10, nextRun.Hour())
	assert.Equal(t, 0, nextRun.Minute())

	// Calculate next run from that Friday
	nextRun2 := schedule.Next(nextRun)
	assert.Equal(t, time.Friday, nextRun2.Weekday())
	assert.True(t, nextRun2.After(nextRun))
	// Should be 7 days later
	assert.Equal(t, 7*24*time.Hour, nextRun2.Sub(nextRun))
}

// TestCronScheduleCodec tests encoding and decoding CronSchedule
func TestCronScheduleCodec(t *testing.T) {
	codec := NewBasicScheduleCodec()

	tests := []struct {
		name       string
		expression string
	}{
		{
			name:       "every friday at 10am",
			expression: "0 10 * * 5",
		},
		{
			name:       "every 5 minutes",
			expression: "*/5 * * * *",
		},
		{
			name:       "first day of month",
			expression: "0 0 1 * *",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original schedule
			original, err := NewCronSchedule(tt.expression)
			require.NoError(t, err)

			// Encode
			scheduleType, scheduleConfig, err := codec.Encode(original)
			require.NoError(t, err)
			assert.Equal(t, "cron", scheduleType)
			assert.Equal(t, tt.expression, scheduleConfig)

			// Decode
			decoded, err := codec.Decode(scheduleType, scheduleConfig)
			require.NoError(t, err)
			assert.NotNil(t, decoded)

			// Verify decoded schedule works the same
			cronDecoded, ok := decoded.(*CronSchedule)
			require.True(t, ok)
			assert.Equal(t, tt.expression, cronDecoded.Expression())

			// Verify Next() produces same results
			testTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
			assert.Equal(t, original.Next(testTime), cronDecoded.Next(testTime))
		})
	}
}

// TestSchedulerWithCronSchedule tests scheduler with cron-based job
func TestSchedulerWithCronSchedule(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a cron schedule that runs every minute
	// Note: For testing, we use a short interval
	schedule, err := NewCronSchedule("* * * * *")
	require.NoError(t, err)

	counter := int32(0)
	jobFunc := func(ctx context.Context, event JobEvent) error {
		if event.ID() == "cron-job" {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	}

	state.SetJobFunc(jobFunc)
	err = scheduler.AddJob("cron-job", schedule, map[string]string{"type": "cron"})
	assert.NoError(t, err)

	// Verify job was added
	job, err := scheduler.GetJob("cron-job")
	require.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "cron-job", job.ID())

	// Verify metadata
	metadata := job.Metadata()
	assert.Equal(t, "cron", metadata["type"])

	// Verify next run is set
	assert.False(t, job.NextRun().IsZero())
}

// TestCronSchedulePersistence tests that cron schedules persist correctly
func TestCronSchedulePersistence(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	expression := "0 10 * * 5"
	schedule, err := NewCronSchedule(expression)
	require.NoError(t, err)

	state.SetJobFunc(func(context.Context, JobEvent) error { return nil })
	err = scheduler.AddJob("friday-job", schedule, nil)
	assert.NoError(t, err)

	// Verify job data in storage
	jobData, err := storage.GetJob(ctx, "friday-job")
	require.NoError(t, err)
	assert.Equal(t, "friday-job", jobData.ID)
	assert.Equal(t, "cron", jobData.ScheduleType)
	assert.Equal(t, expression, jobData.ScheduleConfig)
}

// TestUpdateJobScheduleToCron tests updating a job's schedule to a cron schedule
func TestUpdateJobScheduleToCron(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler, state := newTestScheduler(storage)

	require.NoError(t, scheduler.Start(ctx))
	require.NoError(t, scheduler.WaitUntilRunning(ctx))
	defer scheduler.Stop(ctx)

	// Start with an interval schedule
	initialSchedule, err := NewIntervalSchedule(1 * time.Hour)
	require.NoError(t, err)

	state.SetJobFunc(func(context.Context, JobEvent) error { return nil })
	require.NoError(t, scheduler.AddJob("job-update-cron", initialSchedule, nil))

	// Update to a cron schedule
	cronExpression := "0 10 * * 5"
	newSchedule, err := NewCronSchedule(cronExpression)
	require.NoError(t, err)

	require.NoError(t, scheduler.UpdateJobSchedule("job-update-cron", newSchedule))

	// Verify the job was updated
	job, err := scheduler.GetJob("job-update-cron")
	require.NoError(t, err)
	assert.Equal(t, time.Friday, job.NextRun().Weekday())
	assert.Equal(t, 10, job.NextRun().Hour())

	// Verify storage was updated
	storedJob, err := storage.GetJob(ctx, "job-update-cron")
	require.NoError(t, err)
	assert.Equal(t, "cron", storedJob.ScheduleType)
	assert.Equal(t, cronExpression, storedJob.ScheduleConfig)
}
