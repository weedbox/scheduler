package scheduler_test

import (
	"context"
	"fmt"
	"time"

	"github.com/Weedbox/scheduler"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// ExampleGormStorage_basic demonstrates basic usage of GORM storage
func ExampleGormStorage_basic() {
	// Create a database connection (SQLite in-memory for this example)
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// Create GORM storage
	storage := scheduler.NewGormStorage(db)

	// Initialize storage
	ctx := context.Background()
	if err := storage.Initialize(ctx); err != nil {
		panic(err)
	}

	// Save a job
	job := &scheduler.JobData{
		ID:             "daily-backup",
		ScheduleType:   "cron",
		ScheduleConfig: "0 0 * * *",
		Status:         scheduler.JobStatusPending,
		NextRun:        time.Now().Add(24 * time.Hour),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		Metadata:       map[string]string{"type": "backup"},
	}

	if err := storage.SaveJob(ctx, job); err != nil {
		panic(err)
	}

	// Retrieve the job
	retrieved, err := storage.GetJob(ctx, "daily-backup")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Job ID: %s, Type: %s\n", retrieved.ID, retrieved.Metadata["type"])

	// Clean up
	storage.Close(ctx)

	// Output:
	// Job ID: daily-backup, Type: backup
}

// ExampleGormStorage_withScheduler demonstrates using GORM storage with scheduler
func ExampleGormStorage_withScheduler() {
	// Create a database connection
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// Create GORM storage and scheduler
	storage := scheduler.NewGormStorage(db)
	sched := scheduler.NewScheduler(storage)

	// Start scheduler (this will initialize storage)
	ctx := context.Background()
	if err := sched.Start(ctx); err != nil {
		panic(err)
	}

	// Note: In a real application, you would add jobs using a Schedule implementation
	// This example demonstrates that the scheduler can work with GORM storage

	// Stop scheduler (this will close storage)
	if err := sched.Stop(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Scheduler started and stopped successfully with GORM storage")

	// Output:
	// Scheduler started and stopped successfully with GORM storage
}

// ExampleGormStorage_executionRecords demonstrates saving and querying execution records
func ExampleGormStorage_executionRecords() {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	storage := scheduler.NewGormStorage(db)
	ctx := context.Background()
	if err := storage.Initialize(ctx); err != nil {
		panic(err)
	}

	// Save execution records
	now := time.Now()
	for i := 0; i < 3; i++ {
		record := &scheduler.ExecutionRecord{
			JobID:       "job1",
			ExecutionID: fmt.Sprintf("exec-%d", i),
			StartTime:   now.Add(time.Duration(i) * time.Hour),
			EndTime:     now.Add(time.Duration(i)*time.Hour + 10*time.Minute),
			Duration:    10 * time.Minute,
			Status:      scheduler.JobStatusCompleted,
		}
		if err := storage.SaveExecution(ctx, record); err != nil {
			panic(err)
		}
	}

	// Query executions with limit
	options := &scheduler.QueryOptions{
		Limit:  2,
		SortBy: "start_time",
	}
	executions, err := storage.ListExecutions(ctx, "job1", options)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Found %d executions\n", len(executions))

	storage.Close(ctx)

	// Output:
	// Found 2 executions
}

// ExampleGormStorage_listJobsByStatus demonstrates querying jobs by status
func ExampleGormStorage_listJobsByStatus() {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	storage := scheduler.NewGormStorage(db)
	ctx := context.Background()
	if err := storage.Initialize(ctx); err != nil {
		panic(err)
	}

	// Create jobs with different statuses
	statuses := []scheduler.JobStatus{
		scheduler.JobStatusPending,
		scheduler.JobStatusRunning,
		scheduler.JobStatusPending,
	}

	for i, status := range statuses {
		job := &scheduler.JobData{
			ID:        fmt.Sprintf("job-%d", i),
			Status:    status,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if err := storage.SaveJob(ctx, job); err != nil {
			panic(err)
		}
	}

	// Query pending jobs
	pendingJobs, err := storage.ListJobsByStatus(ctx, scheduler.JobStatusPending)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Pending jobs: %d\n", len(pendingJobs))

	storage.Close(ctx)

	// Output:
	// Pending jobs: 2
}
