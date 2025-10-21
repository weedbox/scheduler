package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupTestDB creates an in-memory SQLite database for testing
func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	return db
}

// TestGormStorageInitialize tests the Initialize method
func TestGormStorageInitialize(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)

	// Test first initialization
	err := storage.Initialize(ctx)
	assert.NoError(t, err)
	assert.True(t, storage.initialized)

	// Test double initialization (should not error)
	err = storage.Initialize(ctx)
	assert.NoError(t, err)
}

// TestGormStorageInitializeWithNilDB tests initialization with nil database
func TestGormStorageInitializeWithNilDB(t *testing.T) {
	ctx := context.Background()
	storage := NewGormStorage(nil)

	err := storage.Initialize(ctx)
	assert.Equal(t, ErrStorageConnectionFailed, err)
}

// TestGormStorageClose tests the Close method
func TestGormStorageClose(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)

	// Test close without initialization
	err := storage.Close(ctx)
	assert.Equal(t, ErrStorageNotInitialized, err)

	// Test close after initialization
	err = storage.Initialize(ctx)
	assert.NoError(t, err)

	err = storage.Close(ctx)
	assert.NoError(t, err)
	assert.False(t, storage.initialized)
}

// TestGormStorageSaveJob tests the SaveJob method
func TestGormStorageSaveJob(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()
	job := &JobData{
		ID:             "job1",
		ScheduleType:   "interval",
		ScheduleConfig: "5m",
		Status:         JobStatusPending,
		NextRun:        now.Add(5 * time.Minute),
		LastRun:        time.Time{},
		CreatedAt:      now,
		UpdatedAt:      now,
		Metadata:       map[string]string{"key1": "value1"},
	}

	// Test save job successfully
	err := storage.SaveJob(ctx, job)
	assert.NoError(t, err)

	// Test save duplicate job
	err = storage.SaveJob(ctx, job)
	assert.Equal(t, ErrJobDataAlreadyExists, err)

	// Test save nil job
	err = storage.SaveJob(ctx, nil)
	assert.Equal(t, ErrInvalidJobData, err)

	// Test save job with empty ID
	emptyIDJob := &JobData{ID: ""}
	err = storage.SaveJob(ctx, emptyIDJob)
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageSaveJobWithoutInitialization tests SaveJob without initialization
func TestGormStorageSaveJobWithoutInitialization(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)

	job := &JobData{
		ID:     "job1",
		Status: JobStatusPending,
	}

	err := storage.SaveJob(ctx, job)
	assert.Equal(t, ErrStorageNotInitialized, err)
}

// TestGormStorageUpdateJob tests the UpdateJob method
func TestGormStorageUpdateJob(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()
	job := &JobData{
		ID:        "job1",
		Status:    JobStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
		Metadata:  map[string]string{"key1": "value1"},
	}

	// Save initial job
	err := storage.SaveJob(ctx, job)
	assert.NoError(t, err)

	// Update job
	job.Status = JobStatusRunning
	job.UpdatedAt = now.Add(1 * time.Minute)
	job.Metadata["key2"] = "value2"

	err = storage.UpdateJob(ctx, job)
	assert.NoError(t, err)

	// Verify update
	retrieved, err := storage.GetJob(ctx, "job1")
	assert.NoError(t, err)
	assert.Equal(t, JobStatusRunning, retrieved.Status)
	assert.Equal(t, "value2", retrieved.Metadata["key2"])

	// Test update non-existent job
	nonExistentJob := &JobData{ID: "nonexistent"}
	err = storage.UpdateJob(ctx, nonExistentJob)
	assert.Equal(t, ErrJobDataNotFound, err)

	// Test update with nil job
	err = storage.UpdateJob(ctx, nil)
	assert.Equal(t, ErrInvalidJobData, err)

	// Test update with empty ID
	err = storage.UpdateJob(ctx, &JobData{ID: ""})
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageDeleteJob tests the DeleteJob method
func TestGormStorageDeleteJob(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	job := &JobData{
		ID:     "job1",
		Status: JobStatusPending,
	}

	// Save job
	err := storage.SaveJob(ctx, job)
	assert.NoError(t, err)

	// Delete job
	err = storage.DeleteJob(ctx, "job1")
	assert.NoError(t, err)

	// Verify deletion
	_, err = storage.GetJob(ctx, "job1")
	assert.Equal(t, ErrJobDataNotFound, err)

	// Test delete non-existent job
	err = storage.DeleteJob(ctx, "nonexistent")
	assert.Equal(t, ErrJobDataNotFound, err)

	// Test delete with empty ID
	err = storage.DeleteJob(ctx, "")
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageGetJob tests the GetJob method
func TestGormStorageGetJob(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()
	job := &JobData{
		ID:        "job1",
		Status:    JobStatusPending,
		CreatedAt: now,
		Metadata:  map[string]string{"key1": "value1"},
	}

	// Save job
	err := storage.SaveJob(ctx, job)
	assert.NoError(t, err)

	// Get job
	retrieved, err := storage.GetJob(ctx, "job1")
	assert.NoError(t, err)
	assert.Equal(t, "job1", retrieved.ID)
	assert.Equal(t, JobStatusPending, retrieved.Status)
	assert.Equal(t, "value1", retrieved.Metadata["key1"])

	// Test get non-existent job
	_, err = storage.GetJob(ctx, "nonexistent")
	assert.Equal(t, ErrJobDataNotFound, err)

	// Test get with empty ID
	_, err = storage.GetJob(ctx, "")
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageListJobs tests the ListJobs method
func TestGormStorageListJobs(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	// Test empty list
	jobs, err := storage.ListJobs(ctx)
	assert.NoError(t, err)
	assert.Empty(t, jobs)

	// Add multiple jobs
	for i := 1; i <= 3; i++ {
		job := &JobData{
			ID:     string(rune('A' + i - 1)),
			Status: JobStatusPending,
		}
		err := storage.SaveJob(ctx, job)
		assert.NoError(t, err)
	}

	// List all jobs
	jobs, err = storage.ListJobs(ctx)
	assert.NoError(t, err)
	assert.Len(t, jobs, 3)
}

// TestGormStorageListJobsByStatus tests the ListJobsByStatus method
func TestGormStorageListJobsByStatus(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	// Add jobs with different statuses
	statuses := []JobStatus{JobStatusPending, JobStatusRunning, JobStatusCompleted, JobStatusPending}
	for i, status := range statuses {
		job := &JobData{
			ID:     string(rune('A' + i)),
			Status: status,
		}
		err := storage.SaveJob(ctx, job)
		assert.NoError(t, err)
	}

	// List pending jobs
	pendingJobs, err := storage.ListJobsByStatus(ctx, JobStatusPending)
	assert.NoError(t, err)
	assert.Len(t, pendingJobs, 2)

	// List running jobs
	runningJobs, err := storage.ListJobsByStatus(ctx, JobStatusRunning)
	assert.NoError(t, err)
	assert.Len(t, runningJobs, 1)

	// List failed jobs (none)
	failedJobs, err := storage.ListJobsByStatus(ctx, JobStatusFailed)
	assert.NoError(t, err)
	assert.Empty(t, failedJobs)
}

// TestGormStorageSaveExecution tests the SaveExecution method
func TestGormStorageSaveExecution(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()
	record := &ExecutionRecord{
		JobID:       "job1",
		ExecutionID: "exec1",
		StartTime:   now,
		EndTime:     now.Add(1 * time.Minute),
		Duration:    1 * time.Minute,
		Status:      JobStatusCompleted,
		Metadata:    map[string]string{"key1": "value1"},
	}

	// Save execution
	err := storage.SaveExecution(ctx, record)
	assert.NoError(t, err)

	// Test save nil execution
	err = storage.SaveExecution(ctx, nil)
	assert.Equal(t, ErrInvalidJobData, err)

	// Test save execution with empty execution ID
	err = storage.SaveExecution(ctx, &ExecutionRecord{JobID: "job1"})
	assert.Error(t, err)

	// Test save execution with empty job ID
	err = storage.SaveExecution(ctx, &ExecutionRecord{ExecutionID: "exec2"})
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageGetExecution tests the GetExecution method
func TestGormStorageGetExecution(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()
	record := &ExecutionRecord{
		JobID:       "job1",
		ExecutionID: "exec1",
		StartTime:   now,
		EndTime:     now.Add(1 * time.Minute),
		Duration:    1 * time.Minute,
		Status:      JobStatusCompleted,
	}

	// Save execution
	err := storage.SaveExecution(ctx, record)
	assert.NoError(t, err)

	// Get execution
	retrieved, err := storage.GetExecution(ctx, "exec1")
	assert.NoError(t, err)
	assert.Equal(t, "exec1", retrieved.ExecutionID)
	assert.Equal(t, "job1", retrieved.JobID)
	assert.Equal(t, JobStatusCompleted, retrieved.Status)

	// Test get non-existent execution
	_, err = storage.GetExecution(ctx, "nonexistent")
	assert.Equal(t, ErrExecutionHistoryNotFound, err)

	// Test get with empty ID
	_, err = storage.GetExecution(ctx, "")
	assert.Error(t, err)
}

// TestGormStorageListExecutions tests the ListExecutions method
func TestGormStorageListExecutions(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()

	// Add multiple executions for the same job
	for i := 0; i < 5; i++ {
		record := &ExecutionRecord{
			JobID:       "job1",
			ExecutionID: string(rune('A' + i)),
			StartTime:   now.Add(time.Duration(i) * time.Minute),
			EndTime:     now.Add(time.Duration(i+1) * time.Minute),
			Duration:    1 * time.Minute,
			Status:      JobStatusCompleted,
		}
		err := storage.SaveExecution(ctx, record)
		assert.NoError(t, err)
	}

	// List all executions without filters
	executions, err := storage.ListExecutions(ctx, "job1", nil)
	assert.NoError(t, err)
	assert.Len(t, executions, 5)

	// List executions with limit
	options := &QueryOptions{
		Limit: 3,
	}
	executions, err = storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Len(t, executions, 3)

	// List executions with offset
	options = &QueryOptions{
		Offset: 2,
		Limit:  2,
	}
	executions, err = storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Len(t, executions, 2)

	// Test list for non-existent job
	executions, err = storage.ListExecutions(ctx, "nonexistent", nil)
	assert.NoError(t, err)
	assert.Empty(t, executions)

	// Test with empty job ID
	_, err = storage.ListExecutions(ctx, "", nil)
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageListExecutionsWithTimeFilter tests ListExecutions with time filters
func TestGormStorageListExecutionsWithTimeFilter(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()

	// Add executions at different times
	for i := 0; i < 5; i++ {
		record := &ExecutionRecord{
			JobID:       "job1",
			ExecutionID: string(rune('A' + i)),
			StartTime:   now.Add(time.Duration(i) * time.Hour),
			EndTime:     now.Add(time.Duration(i+1) * time.Hour),
			Duration:    1 * time.Hour,
			Status:      JobStatusCompleted,
		}
		err := storage.SaveExecution(ctx, record)
		assert.NoError(t, err)
	}

	// Filter by start time
	filterTime := now.Add(2 * time.Hour)
	options := &QueryOptions{
		StartTime: &filterTime,
	}
	executions, err := storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Len(t, executions, 3) // Records at 2h, 3h, 4h

	// Filter by end time
	endFilterTime := now.Add(3 * time.Hour)
	options = &QueryOptions{
		EndTime: &endFilterTime,
	}
	executions, err = storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Len(t, executions, 3) // Records at 0h, 1h, 2h
}

// TestGormStorageListExecutionsWithStatusFilter tests ListExecutions with status filter
func TestGormStorageListExecutionsWithStatusFilter(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()
	statuses := []JobStatus{JobStatusCompleted, JobStatusFailed, JobStatusCompleted, JobStatusFailed, JobStatusCompleted}

	// Add executions with different statuses
	for i, status := range statuses {
		record := &ExecutionRecord{
			JobID:       "job1",
			ExecutionID: string(rune('A' + i)),
			StartTime:   now.Add(time.Duration(i) * time.Minute),
			EndTime:     now.Add(time.Duration(i+1) * time.Minute),
			Duration:    1 * time.Minute,
			Status:      status,
		}
		err := storage.SaveExecution(ctx, record)
		assert.NoError(t, err)
	}

	// Filter completed executions
	completedStatus := JobStatusCompleted
	options := &QueryOptions{
		Status: &completedStatus,
	}
	executions, err := storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Len(t, executions, 3)

	// Filter failed executions
	failedStatus := JobStatusFailed
	options = &QueryOptions{
		Status: &failedStatus,
	}
	executions, err = storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Len(t, executions, 2)
}

// TestGormStorageListExecutionsWithSorting tests ListExecutions with sorting
func TestGormStorageListExecutionsWithSorting(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()

	// Add executions with different durations
	durations := []time.Duration{5 * time.Minute, 2 * time.Minute, 8 * time.Minute, 1 * time.Minute}
	for i, duration := range durations {
		record := &ExecutionRecord{
			JobID:       "job1",
			ExecutionID: string(rune('A' + i)),
			StartTime:   now.Add(time.Duration(i) * time.Hour),
			EndTime:     now.Add(time.Duration(i)*time.Hour + duration),
			Duration:    duration,
			Status:      JobStatusCompleted,
		}
		err := storage.SaveExecution(ctx, record)
		assert.NoError(t, err)
	}

	// Sort by duration ascending
	options := &QueryOptions{
		SortBy:   "duration",
		SortDesc: false,
	}
	executions, err := storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Equal(t, 1*time.Minute, executions[0].Duration)
	assert.Equal(t, 8*time.Minute, executions[3].Duration)

	// Sort by duration descending
	options.SortDesc = true
	executions, err = storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Equal(t, 8*time.Minute, executions[0].Duration)
	assert.Equal(t, 1*time.Minute, executions[3].Duration)

	// Sort by start time ascending
	options = &QueryOptions{
		SortBy:   "start_time",
		SortDesc: false,
	}
	executions, err = storage.ListExecutions(ctx, "job1", options)
	assert.NoError(t, err)
	assert.Equal(t, "A", executions[0].ExecutionID)
	assert.Equal(t, "D", executions[3].ExecutionID)
}

// TestGormStorageDeleteExecutions tests the DeleteExecutions method
func TestGormStorageDeleteExecutions(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()

	// Add executions at different times
	for i := 0; i < 5; i++ {
		record := &ExecutionRecord{
			JobID:       "job1",
			ExecutionID: string(rune('A' + i)),
			StartTime:   now.Add(time.Duration(i-2) * time.Hour), // -2h, -1h, 0h, 1h, 2h
			EndTime:     now.Add(time.Duration(i-1) * time.Hour),
			Duration:    1 * time.Hour,
			Status:      JobStatusCompleted,
		}
		err := storage.SaveExecution(ctx, record)
		assert.NoError(t, err)
	}

	// Delete executions before now
	err := storage.DeleteExecutions(ctx, "job1", now)
	assert.NoError(t, err)

	// Verify only future executions remain
	executions, err := storage.ListExecutions(ctx, "job1", nil)
	assert.NoError(t, err)
	assert.Len(t, executions, 2) // Only executions at 1h, 2h remain

	// Verify deleted executions are gone
	_, err = storage.GetExecution(ctx, "A")
	assert.Equal(t, ErrExecutionHistoryNotFound, err)

	// Test delete for non-existent job (should not error)
	err = storage.DeleteExecutions(ctx, "nonexistent", now)
	assert.NoError(t, err)

	// Test with empty job ID
	err = storage.DeleteExecutions(ctx, "", now)
	assert.Equal(t, ErrEmptyJobID, err)
}

// TestGormStorageHealthCheck tests the HealthCheck method
func TestGormStorageHealthCheck(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)

	// Test health check without initialization
	err := storage.HealthCheck(ctx)
	assert.Equal(t, ErrStorageNotInitialized, err)

	// Test health check after initialization
	err = storage.Initialize(ctx)
	assert.NoError(t, err)

	err = storage.HealthCheck(ctx)
	assert.NoError(t, err)
}

// TestGormStorageMetadataEncoding tests that metadata is properly encoded/decoded
func TestGormStorageMetadataEncoding(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()

	// Test job metadata
	job := &JobData{
		ID:        "job1",
		Status:    JobStatusPending,
		CreatedAt: now,
		Metadata:  map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
	}

	err := storage.SaveJob(ctx, job)
	assert.NoError(t, err)

	retrieved, err := storage.GetJob(ctx, "job1")
	assert.NoError(t, err)
	assert.Equal(t, job.Metadata, retrieved.Metadata)

	// Test execution metadata
	record := &ExecutionRecord{
		JobID:       "job1",
		ExecutionID: "exec1",
		StartTime:   now,
		EndTime:     now.Add(1 * time.Minute),
		Duration:    1 * time.Minute,
		Status:      JobStatusCompleted,
		Metadata:    map[string]string{"key1": "value1", "key2": "value2"},
	}

	err = storage.SaveExecution(ctx, record)
	assert.NoError(t, err)

	retrievedExec, err := storage.GetExecution(ctx, "exec1")
	assert.NoError(t, err)
	assert.Equal(t, record.Metadata, retrievedExec.Metadata)
}

// TestGormStorageEmptyMetadata tests handling of empty metadata
func TestGormStorageEmptyMetadata(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)
	storage := NewGormStorage(db)
	storage.Initialize(ctx)

	now := time.Now()

	// Test job with nil metadata
	job := &JobData{
		ID:        "job1",
		Status:    JobStatusPending,
		CreatedAt: now,
		Metadata:  nil,
	}

	err := storage.SaveJob(ctx, job)
	assert.NoError(t, err)

	retrieved, err := storage.GetJob(ctx, "job1")
	assert.NoError(t, err)
	assert.NotNil(t, retrieved.Metadata)
	assert.Empty(t, retrieved.Metadata)
}

// TestGormStorageWithRealScheduler tests GORM storage integration with scheduler
func TestGormStorageWithRealScheduler(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t)

	// Create storage for scheduler
	schedulerStorage := NewGormStorage(db)
	jobFuncs := make(map[string]func(context.Context) error)
	handler := func(ctx context.Context, event JobEvent) error {
		if fn, ok := jobFuncs[event.ID()]; ok && fn != nil {
			return fn(ctx)
		}
		return nil
	}
	scheduler := NewScheduler(schedulerStorage, handler, NewBasicScheduleCodec())
	err := scheduler.Start(ctx)
	assert.NoError(t, err)
	assert.True(t, scheduler.IsRunning())

	// Add a job
	schedule, scheduleErr := NewIntervalSchedule(1 * time.Second)
	require.NoError(t, scheduleErr)
	jobFunc := func(ctx context.Context) error {
		return nil
	}

	jobFuncs["test-job"] = jobFunc
	err = scheduler.AddJob("test-job", schedule, nil)
	assert.NoError(t, err)

	// Wait a bit for execution
	time.Sleep(2 * time.Second)

	// Create a separate storage instance to check persistence (before stopping scheduler)
	checkStorage := NewGormStorage(db)
	err = checkStorage.Initialize(ctx)
	assert.NoError(t, err)

	// Check job was persisted
	jobData, err := checkStorage.GetJob(ctx, "test-job")
	assert.NoError(t, err)
	assert.Equal(t, "test-job", jobData.ID)

	// Check execution was recorded
	executions, err := checkStorage.ListExecutions(ctx, "test-job", nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, executions)

	// Stop scheduler (this will close schedulerStorage)
	err = scheduler.Stop(ctx)
	assert.NoError(t, err)

	// Close the check storage
	checkStorage.Close(ctx)
}
