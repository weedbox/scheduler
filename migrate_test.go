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

func TestMigrateStorage_MemoryToMemory(t *testing.T) {
	ctx := context.Background()

	src := NewMemoryStorage()
	require.NoError(t, src.Initialize(ctx))

	dst := NewMemoryStorage()
	require.NoError(t, dst.Initialize(ctx))

	now := time.Now()

	// Seed source with jobs and executions
	job1 := &JobData{
		ID:             "job-1",
		ScheduleType:   "interval",
		ScheduleConfig: `{"interval":"5s"}`,
		Status:         JobStatusPending,
		NextRun:        now.Add(5 * time.Second),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	job2 := &JobData{
		ID:             "job-2",
		ScheduleType:   "cron",
		ScheduleConfig: `{"expression":"* * * * *"}`,
		Status:         JobStatusPending,
		NextRun:        now.Add(time.Minute),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	require.NoError(t, src.SaveJob(ctx, job1))
	require.NoError(t, src.SaveJob(ctx, job2))

	exec1 := &ExecutionRecord{
		JobID:       "job-1",
		ExecutionID: "exec-1",
		StartTime:   now.Add(-10 * time.Second),
		EndTime:     now.Add(-9 * time.Second),
		Duration:    time.Second,
		Status:      JobStatusCompleted,
	}
	exec2 := &ExecutionRecord{
		JobID:       "job-1",
		ExecutionID: "exec-2",
		StartTime:   now.Add(-5 * time.Second),
		EndTime:     now.Add(-4 * time.Second),
		Duration:    time.Second,
		Status:      JobStatusCompleted,
	}
	require.NoError(t, src.SaveExecution(ctx, exec1))
	require.NoError(t, src.SaveExecution(ctx, exec2))

	// Migrate
	result, err := MigrateStorage(ctx, src, dst)
	require.NoError(t, err)
	assert.Equal(t, 2, result.JobsMigrated)
	assert.Equal(t, 0, result.JobsSkipped)
	assert.Equal(t, 2, result.ExecutionsMigrated)
	assert.Equal(t, 0, result.ExecutionsSkipped)

	// Verify destination
	jobs, err := dst.ListJobs(ctx)
	require.NoError(t, err)
	assert.Len(t, jobs, 2)

	execs, err := dst.ListExecutions(ctx, "job-1", nil)
	require.NoError(t, err)
	assert.Len(t, execs, 2)

	// Verify job data integrity
	dstJob, err := dst.GetJob(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "interval", dstJob.ScheduleType)
	assert.Equal(t, `{"interval":"5s"}`, dstJob.ScheduleConfig)
}

func TestMigrateStorage_Idempotent(t *testing.T) {
	ctx := context.Background()

	src := NewMemoryStorage()
	require.NoError(t, src.Initialize(ctx))

	dst := NewMemoryStorage()
	require.NoError(t, dst.Initialize(ctx))

	now := time.Now()
	job := &JobData{
		ID:             "job-1",
		ScheduleType:   "interval",
		ScheduleConfig: `{"interval":"10s"}`,
		Status:         JobStatusPending,
		NextRun:        now.Add(10 * time.Second),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	require.NoError(t, src.SaveJob(ctx, job))

	exec := &ExecutionRecord{
		JobID:       "job-1",
		ExecutionID: "exec-1",
		StartTime:   now,
		EndTime:     now.Add(time.Second),
		Duration:    time.Second,
		Status:      JobStatusCompleted,
	}
	require.NoError(t, src.SaveExecution(ctx, exec))

	// First migration
	result1, err := MigrateStorage(ctx, src, dst)
	require.NoError(t, err)
	assert.Equal(t, 1, result1.JobsMigrated)
	assert.Equal(t, 1, result1.ExecutionsMigrated)

	// Second migration — should skip everything
	result2, err := MigrateStorage(ctx, src, dst)
	require.NoError(t, err)
	assert.Equal(t, 0, result2.JobsMigrated)
	assert.Equal(t, 1, result2.JobsSkipped)
	assert.Equal(t, 0, result2.ExecutionsMigrated)
	assert.Equal(t, 1, result2.ExecutionsSkipped)

	// Destination should still have exactly 1 job and 1 execution
	jobs, err := dst.ListJobs(ctx)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

func TestMigrateStorage_EmptySource(t *testing.T) {
	ctx := context.Background()

	src := NewMemoryStorage()
	require.NoError(t, src.Initialize(ctx))

	dst := NewMemoryStorage()
	require.NoError(t, dst.Initialize(ctx))

	result, err := MigrateStorage(ctx, src, dst)
	require.NoError(t, err)
	assert.Equal(t, 0, result.JobsMigrated)
	assert.Equal(t, 0, result.JobsSkipped)
	assert.Equal(t, 0, result.ExecutionsMigrated)
	assert.Equal(t, 0, result.ExecutionsSkipped)
}

func TestMigrateStorage_GormToNATS(t *testing.T) {
	ctx := context.Background()

	// Setup GORM source
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	gormStorage := NewGormStorage(db)
	require.NoError(t, gormStorage.Initialize(ctx))

	// Setup NATS destination
	_, js := startNATSServer(t)
	natsStorage := NewNATSStorage(js)
	require.NoError(t, natsStorage.Initialize(ctx))

	now := time.Now().Truncate(time.Second)

	// Seed GORM with data
	job := &JobData{
		ID:             "gorm-job-1",
		ScheduleType:   "cron",
		ScheduleConfig: `{"expression":"*/5 * * * *"}`,
		Status:         JobStatusPending,
		NextRun:        now.Add(5 * time.Minute),
		CreatedAt:      now,
		UpdatedAt:      now,
		Metadata:       map[string]string{"source": "gorm"},
	}
	require.NoError(t, gormStorage.SaveJob(ctx, job))

	exec := &ExecutionRecord{
		JobID:       "gorm-job-1",
		ExecutionID: "gorm-exec-1",
		StartTime:   now.Add(-time.Minute),
		EndTime:     now.Add(-59 * time.Second),
		Duration:    time.Second,
		Status:      JobStatusCompleted,
		Metadata:    map[string]string{"result": "ok"},
	}
	require.NoError(t, gormStorage.SaveExecution(ctx, exec))

	// Migrate
	result, err := MigrateStorage(ctx, gormStorage, natsStorage)
	require.NoError(t, err)
	assert.Equal(t, 1, result.JobsMigrated)
	assert.Equal(t, 1, result.ExecutionsMigrated)

	// Verify in NATS
	dstJob, err := natsStorage.GetJob(ctx, "gorm-job-1")
	require.NoError(t, err)
	assert.Equal(t, "cron", dstJob.ScheduleType)
	assert.Equal(t, `{"expression":"*/5 * * * *"}`, dstJob.ScheduleConfig)
	assert.Equal(t, JobStatusPending, dstJob.Status)
	assert.Equal(t, "gorm", dstJob.Metadata["source"])

	dstExec, err := natsStorage.GetExecution(ctx, "gorm-exec-1")
	require.NoError(t, err)
	assert.Equal(t, "gorm-job-1", dstExec.JobID)
	assert.Equal(t, JobStatusCompleted, dstExec.Status)
	assert.Equal(t, "ok", dstExec.Metadata["result"])

	// Test idempotency with GORM→NATS
	result2, err := MigrateStorage(ctx, gormStorage, natsStorage)
	require.NoError(t, err)
	assert.Equal(t, 0, result2.JobsMigrated)
	assert.Equal(t, 1, result2.JobsSkipped)
	assert.Equal(t, 0, result2.ExecutionsMigrated)
	assert.Equal(t, 1, result2.ExecutionsSkipped)
}
