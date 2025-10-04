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

// Mock schedule implementations for testing

// intervalSchedule runs at fixed intervals
type intervalSchedule struct {
	interval time.Duration
}

func (s *intervalSchedule) Next(t time.Time) time.Time {
	return t.Add(s.interval)
}

// onceSchedule runs only once at a specific time
type onceSchedule struct {
	runTime time.Time
	hasRun  bool
}

func (s *onceSchedule) Next(t time.Time) time.Time {
	if s.hasRun {
		// Return far future time to prevent re-execution
		return time.Now().Add(100 * 365 * 24 * time.Hour)
	}
	s.hasRun = true
	return s.runTime
}

// TestNewScheduler tests creating a new scheduler
func TestNewScheduler(t *testing.T) {
	// Test with storage
	storage := NewMemoryStorage()
	scheduler := NewScheduler(storage)
	assert.NotNil(t, scheduler)
	assert.False(t, scheduler.IsRunning())

	// Test without storage
	scheduler = NewScheduler(nil)
	assert.NotNil(t, scheduler)
	assert.False(t, scheduler.IsRunning())
}

// TestSchedulerStartStop tests starting and stopping the scheduler
func TestSchedulerStartStop(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler := NewScheduler(storage)

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

// TestSchedulerAddJob tests adding jobs to the scheduler
func TestSchedulerAddJob(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule := &intervalSchedule{interval: 1 * time.Second}
	counter := int32(0)
	jobFunc := func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	}

	// Test add job successfully
	err := scheduler.AddJob("job1", schedule, jobFunc)
	assert.NoError(t, err)

	// Test add duplicate job
	err = scheduler.AddJob("job1", schedule, jobFunc)
	assert.Equal(t, ErrJobAlreadyExists, err)

	// Test add job with empty ID
	err = scheduler.AddJob("", schedule, jobFunc)
	assert.Equal(t, ErrEmptyJobID, err)

	// Test add job with nil function
	err = scheduler.AddJob("job2", schedule, nil)
	assert.Equal(t, ErrNilJobFunc, err)

	// Test add job with nil schedule
	err = scheduler.AddJob("job2", nil, jobFunc)
	assert.Equal(t, ErrInvalidInterval, err)
}

// TestSchedulerRemoveJob tests removing jobs from the scheduler
func TestSchedulerRemoveJob(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage()
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule := &intervalSchedule{interval: 1 * time.Second}
	jobFunc := func(ctx context.Context) error {
		return nil
	}

	// Add a job
	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule := &intervalSchedule{interval: 1 * time.Second}
	jobFunc := func(ctx context.Context) error {
		return nil
	}

	// Add a job
	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Test empty list
	jobs := scheduler.ListJobs()
	assert.Empty(t, jobs)

	// Add multiple jobs
	schedule := &intervalSchedule{interval: 1 * time.Second}
	jobFunc := func(ctx context.Context) error {
		return nil
	}

	for i := 1; i <= 3; i++ {
		err := scheduler.AddJob(string(rune('A'+i-1)), schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that runs immediately
	schedule := &onceSchedule{runTime: time.Now()}
	counter := int32(0)
	jobFunc := func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that fails
	schedule := &onceSchedule{runTime: time.Now()}
	expectedErr := errors.New("job failed")
	jobFunc := func(ctx context.Context) error {
		return expectedErr
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that runs every 500ms
	schedule := &intervalSchedule{interval: 500 * time.Millisecond}
	counter := int32(0)
	jobFunc := func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)

	schedule := &onceSchedule{runTime: time.Now()}
	jobFunc := func(ctx context.Context) error {
		return nil
	}

	// Add a job
	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create multiple jobs that run concurrently
	var wg sync.WaitGroup
	counter := int32(0)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		jobID := string(rune('A' + i))
		// Each job needs its own schedule instance
		schedule := &onceSchedule{runTime: time.Now()}
		jobFunc := func(ctx context.Context) error {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
			atomic.AddInt32(&counter, 1)
			return nil
		}

		err := scheduler.AddJob(jobID, schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Create a job that takes some time to execute
	schedule := &onceSchedule{runTime: time.Now()}
	started := make(chan bool, 1)
	jobFunc := func(ctx context.Context) error {
		started <- true
		time.Sleep(1 * time.Second)
		return nil
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)

	// Create a job that takes some time to execute
	schedule := &onceSchedule{runTime: time.Now()}
	started := make(chan bool, 1)
	completed := int32(0)
	jobFunc := func(ctx context.Context) error {
		started <- true
		time.Sleep(1 * time.Second)
		atomic.StoreInt32(&completed, 1)
		return nil
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule := &intervalSchedule{interval: 1 * time.Hour}
	executed := false
	jobFunc := func(ctx context.Context) error {
		executed = true
		return nil
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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

	// Test Execute
	err = job.Execute(ctx)
	assert.NoError(t, err)
	assert.True(t, executed)
}

// TestSchedulerWithNilStorage tests scheduler without storage
func TestSchedulerWithNilStorage(t *testing.T) {
	ctx := context.Background()
	scheduler := NewScheduler(nil)

	err := scheduler.Start(ctx)
	assert.NoError(t, err)
	assert.True(t, scheduler.IsRunning())

	schedule := &intervalSchedule{interval: 1 * time.Second}
	counter := int32(0)
	jobFunc := func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	}

	err = scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	schedule := &onceSchedule{runTime: time.Now()}
	executionDuration := 100 * time.Millisecond
	jobFunc := func(ctx context.Context) error {
		time.Sleep(executionDuration)
		return nil
	}

	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)

	schedule := &intervalSchedule{interval: 1 * time.Second}
	jobFunc := func(ctx context.Context) error {
		return nil
	}

	// Add job before starting scheduler
	err := scheduler.AddJob("job1", schedule, jobFunc)
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
	scheduler := NewScheduler(storage)

	for i := 0; i < 3; i++ {
		err := scheduler.Start(ctx)
		assert.NoError(t, err)
		assert.True(t, scheduler.IsRunning())

		err = scheduler.Stop(ctx)
		assert.NoError(t, err)
		assert.False(t, scheduler.IsRunning())
	}
}
