package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startNATSServer(t *testing.T) (*server.Server, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() {
		nc.Close()
	})

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	return ns, js
}

func TestNATSStorage_Initialize(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()

	err := storage.Initialize(ctx)
	require.NoError(t, err)
	assert.True(t, storage.initialized)

	err = storage.HealthCheck(ctx)
	assert.NoError(t, err)

	err = storage.Close(ctx)
	assert.NoError(t, err)
	assert.False(t, storage.initialized)
}

func TestNATSStorage_SaveAndGetJob(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	now := time.Now().Truncate(time.Millisecond)
	job := &JobData{
		ID:             "test-job",
		ScheduleType:   "interval",
		ScheduleConfig: "5m",
		Status:         JobStatusPending,
		NextRun:        now.Add(5 * time.Minute),
		LastRun:        time.Time{},
		CreatedAt:      now,
		UpdatedAt:      now,
		Metadata:       map[string]string{"key": "value"},
	}

	err := storage.SaveJob(ctx, job)
	require.NoError(t, err)

	// Duplicate save should fail
	err = storage.SaveJob(ctx, job)
	assert.ErrorIs(t, err, ErrJobDataAlreadyExists)

	// Get job
	got, err := storage.GetJob(ctx, "test-job")
	require.NoError(t, err)
	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, job.ScheduleType, got.ScheduleType)
	assert.Equal(t, job.ScheduleConfig, got.ScheduleConfig)
	assert.Equal(t, job.Status, got.Status)
	assert.Equal(t, job.Metadata["key"], got.Metadata["key"])

	// Get non-existent job
	_, err = storage.GetJob(ctx, "non-existent")
	assert.ErrorIs(t, err, ErrJobDataNotFound)
}

func TestNATSStorage_UpdateJob(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	now := time.Now().Truncate(time.Millisecond)
	job := &JobData{
		ID:             "test-job",
		ScheduleType:   "interval",
		ScheduleConfig: "5m",
		Status:         JobStatusPending,
		NextRun:        now.Add(5 * time.Minute),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	require.NoError(t, storage.SaveJob(ctx, job))

	// Update job
	job.Status = JobStatusRunning
	job.UpdatedAt = time.Now().Truncate(time.Millisecond)
	err := storage.UpdateJob(ctx, job)
	require.NoError(t, err)

	got, err := storage.GetJob(ctx, "test-job")
	require.NoError(t, err)
	assert.Equal(t, JobStatusRunning, got.Status)

	// Update non-existent job
	nonExistent := &JobData{ID: "non-existent", Status: JobStatusPending}
	err = storage.UpdateJob(ctx, nonExistent)
	assert.ErrorIs(t, err, ErrJobDataNotFound)
}

func TestNATSStorage_DeleteJob(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	job := &JobData{
		ID:             "test-job",
		ScheduleType:   "interval",
		ScheduleConfig: "5m",
		Status:         JobStatusPending,
	}
	require.NoError(t, storage.SaveJob(ctx, job))

	err := storage.DeleteJob(ctx, "test-job")
	require.NoError(t, err)

	_, err = storage.GetJob(ctx, "test-job")
	assert.ErrorIs(t, err, ErrJobDataNotFound)
}

func TestNATSStorage_ListJobs(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	// Empty list
	jobs, err := storage.ListJobs(ctx)
	require.NoError(t, err)
	assert.Len(t, jobs, 0)

	// Add jobs
	for _, id := range []string{"job-1", "job-2", "job-3"} {
		require.NoError(t, storage.SaveJob(ctx, &JobData{
			ID:             id,
			ScheduleType:   "interval",
			ScheduleConfig: "5m",
			Status:         JobStatusPending,
		}))
	}

	jobs, err = storage.ListJobs(ctx)
	require.NoError(t, err)
	assert.Len(t, jobs, 3)
}

func TestNATSStorage_ListJobsByStatus(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	require.NoError(t, storage.SaveJob(ctx, &JobData{ID: "job-1", Status: JobStatusPending}))
	require.NoError(t, storage.SaveJob(ctx, &JobData{ID: "job-2", Status: JobStatusRunning}))
	require.NoError(t, storage.SaveJob(ctx, &JobData{ID: "job-3", Status: JobStatusPending}))

	pending, err := storage.ListJobsByStatus(ctx, JobStatusPending)
	require.NoError(t, err)
	assert.Len(t, pending, 2)

	running, err := storage.ListJobsByStatus(ctx, JobStatusRunning)
	require.NoError(t, err)
	assert.Len(t, running, 1)
}

func TestNATSStorage_ExecutionRecords(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	now := time.Now().Truncate(time.Millisecond)
	record := &ExecutionRecord{
		JobID:       "test-job",
		ExecutionID: "test-job_20250101120000",
		StartTime:   now,
		EndTime:     now.Add(100 * time.Millisecond),
		Duration:    100 * time.Millisecond,
		Status:      JobStatusCompleted,
		Metadata:    map[string]string{"key": "value"},
	}

	// Save execution
	err := storage.SaveExecution(ctx, record)
	require.NoError(t, err)

	// Get execution
	got, err := storage.GetExecution(ctx, record.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, record.JobID, got.JobID)
	assert.Equal(t, record.ExecutionID, got.ExecutionID)
	assert.Equal(t, record.Status, got.Status)
	assert.Equal(t, record.Duration, got.Duration)

	// Get non-existent execution
	_, err = storage.GetExecution(ctx, "non-existent")
	assert.ErrorIs(t, err, ErrExecutionHistoryNotFound)
}

func TestNATSStorage_ListExecutions(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	now := time.Now().Truncate(time.Millisecond)

	// Save records for two different jobs
	for i := 0; i < 3; i++ {
		require.NoError(t, storage.SaveExecution(ctx, &ExecutionRecord{
			JobID:       "job-1",
			ExecutionID: "job-1_" + now.Add(time.Duration(i)*time.Second).Format("20060102150405"),
			StartTime:   now.Add(time.Duration(i) * time.Second),
			EndTime:     now.Add(time.Duration(i)*time.Second + 50*time.Millisecond),
			Duration:    50 * time.Millisecond,
			Status:      JobStatusCompleted,
		}))
	}
	require.NoError(t, storage.SaveExecution(ctx, &ExecutionRecord{
		JobID:       "job-2",
		ExecutionID: "job-2_" + now.Format("20060102150405"),
		StartTime:   now,
		EndTime:     now.Add(50 * time.Millisecond),
		Duration:    50 * time.Millisecond,
		Status:      JobStatusCompleted,
	}))

	// List for job-1
	records, err := storage.ListExecutions(ctx, "job-1", nil)
	require.NoError(t, err)
	assert.Len(t, records, 3)

	// List for job-2
	records, err = storage.ListExecutions(ctx, "job-2", nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	// List with limit
	records, err = storage.ListExecutions(ctx, "job-1", &QueryOptions{Limit: 2})
	require.NoError(t, err)
	assert.Len(t, records, 2)
}

func TestNATSStorage_DeleteExecutions(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	now := time.Now().Truncate(time.Millisecond)

	for i := 0; i < 3; i++ {
		require.NoError(t, storage.SaveExecution(ctx, &ExecutionRecord{
			JobID:       "job-1",
			ExecutionID: "job-1_exec_" + time.Duration(i).String(),
			StartTime:   now.Add(time.Duration(i) * time.Hour),
			EndTime:     now.Add(time.Duration(i)*time.Hour + time.Minute),
			Duration:    time.Minute,
			Status:      JobStatusCompleted,
		}))
	}

	// Delete executions before now + 1.5 hours (should delete first 2)
	cutoff := now.Add(90 * time.Minute)
	err := storage.DeleteExecutions(ctx, "job-1", cutoff)
	require.NoError(t, err)

	records, err := storage.ListExecutions(ctx, "job-1", nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)
}

func TestNATSStorage_CustomBucketNames(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js,
		WithNATSStorageJobBucket("CUSTOM_JOBS"),
		WithNATSStorageExecBucket("CUSTOM_EXECS"),
	)

	ctx := context.Background()
	require.NoError(t, storage.Initialize(ctx))
	defer storage.Close(ctx)

	assert.Equal(t, "CUSTOM_JOBS", storage.jobBucket)
	assert.Equal(t, "CUSTOM_EXECS", storage.execBucket)

	// Verify it works
	require.NoError(t, storage.SaveJob(ctx, &JobData{ID: "test", Status: JobStatusPending}))
	got, err := storage.GetJob(ctx, "test")
	require.NoError(t, err)
	assert.Equal(t, "test", got.ID)
}

func TestNATSStorage_NotInitialized(t *testing.T) {
	_, js := startNATSServer(t)

	storage := NewNATSStorage(js)
	ctx := context.Background()

	// All operations should fail when not initialized
	err := storage.HealthCheck(ctx)
	assert.ErrorIs(t, err, ErrStorageNotInitialized)

	err = storage.SaveJob(ctx, &JobData{ID: "test"})
	assert.ErrorIs(t, err, ErrStorageNotInitialized)

	_, err = storage.GetJob(ctx, "test")
	assert.ErrorIs(t, err, ErrStorageNotInitialized)

	_, err = storage.ListJobs(ctx)
	assert.ErrorIs(t, err, ErrStorageNotInitialized)
}
