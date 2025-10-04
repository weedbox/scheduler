# Scheduler

A flexible and powerful job scheduler library for Go with pluggable storage backends.

[![Go Version](https://img.shields.io/badge/go-1.23+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Features

- 🚀 **Simple API** - Easy to use interface for scheduling jobs
- 💾 **Pluggable Storage** - Support for multiple storage backends (In-Memory, GORM/SQL)
- ⚡ **High Performance** - Efficient job execution with concurrent support
- 🔄 **Flexible Scheduling** - Support for interval-based, cron-like, and one-time schedules
- 📊 **Execution History** - Track job execution records with rich query capabilities
- 🛡️ **Thread-Safe** - Safe for concurrent use
- 🎯 **Production Ready** - Comprehensive test coverage (87.3%)

## Installation

```bash
go get github.com/Weedbox/scheduler
```

### For GORM Storage Support

```bash
go get gorm.io/gorm
go get gorm.io/driver/sqlite  # or postgres, mysql, etc.
```

## Quick Start

### Basic Usage with In-Memory Storage

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/Weedbox/scheduler"
)

func main() {
    // Create scheduler with in-memory storage
    storage := scheduler.NewMemoryStorage()
    sched := scheduler.NewScheduler(storage)

    // Start the scheduler
    ctx := context.Background()
    if err := sched.Start(ctx); err != nil {
        panic(err)
    }
    defer sched.Stop(ctx)

    // Define a job function
    jobFunc := func(ctx context.Context) error {
        fmt.Println("Job executed at", time.Now())
        return nil
    }

    // Create a schedule (runs every 5 seconds)
    schedule := &IntervalSchedule{interval: 5 * time.Second}

    // Add the job to scheduler
    if err := sched.AddJob("my-job", schedule, jobFunc); err != nil {
        panic(err)
    }

    // Keep running
    time.Sleep(30 * time.Second)
}
```

### Using GORM Storage (Persistent)

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/Weedbox/scheduler"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    // Create database connection
    db, err := gorm.Open(sqlite.Open("scheduler.db"), &gorm.Config{})
    if err != nil {
        panic(err)
    }

    // Create scheduler with GORM storage
    storage := scheduler.NewGormStorage(db)
    sched := scheduler.NewScheduler(storage)

    // Start the scheduler
    ctx := context.Background()
    if err := sched.Start(ctx); err != nil {
        panic(err)
    }
    defer sched.Stop(ctx)

    // Add your jobs...
}
```

## Core Concepts

### Scheduler

The `Scheduler` is the main component that manages job execution.

```go
type Scheduler interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    AddJob(id string, schedule Schedule, fn JobFunc) error
    RemoveJob(id string) error
    GetJob(id string) (Job, error)
    ListJobs() []Job
    IsRunning() bool
}
```

### Job

A `Job` represents a scheduled task with its metadata.

```go
type Job interface {
    ID() string
    Execute(ctx context.Context) error
    NextRun() time.Time
    LastRun() time.Time
    IsRunning() bool
}
```

### Schedule

The `Schedule` interface defines when a job should run.

```go
type Schedule interface {
    Next(t time.Time) time.Time
}
```

### Storage

The `Storage` interface allows you to persist job data and execution history.

```go
type Storage interface {
    Initialize(ctx context.Context) error
    Close(ctx context.Context) error
    SaveJob(ctx context.Context, job *JobData) error
    UpdateJob(ctx context.Context, job *JobData) error
    DeleteJob(ctx context.Context, jobID string) error
    GetJob(ctx context.Context, jobID string) (*JobData, error)
    ListJobs(ctx context.Context) ([]*JobData, error)
    ListJobsByStatus(ctx context.Context, status JobStatus) ([]*JobData, error)
    SaveExecution(ctx context.Context, record *ExecutionRecord) error
    GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error)
    ListExecutions(ctx context.Context, jobID string, options *QueryOptions) ([]*ExecutionRecord, error)
    DeleteExecutions(ctx context.Context, jobID string, before time.Time) error
    HealthCheck(ctx context.Context) error
}
```

## Storage Backends

### In-Memory Storage

Best for testing, development, or when persistence is not required.

```go
storage := scheduler.NewMemoryStorage()
```

**Pros:**
- Fast and simple
- No external dependencies
- Perfect for testing

**Cons:**
- Data lost on restart
- Not suitable for distributed systems

### GORM Storage

Production-ready storage backed by SQL databases.

```go
import (
    "gorm.io/driver/postgres"  // or mysql, sqlite, sqlserver
    "gorm.io/gorm"
)

db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})
storage := scheduler.NewGormStorage(db)
```

**Supported Databases:**
- PostgreSQL
- MySQL
- SQLite
- SQL Server
- Any GORM-supported database

**Pros:**
- Persistent storage
- ACID transactions
- Advanced querying
- Production-ready

**Features:**
- Automatic schema migration
- Indexed fields for performance
- JSON metadata support
- Health check support

## Examples

### Example 1: Interval-Based Schedule

```go
// IntervalSchedule runs a job at fixed intervals
type IntervalSchedule struct {
    interval time.Duration
}

func (s *IntervalSchedule) Next(t time.Time) time.Time {
    return t.Add(s.interval)
}

// Usage
schedule := &IntervalSchedule{interval: 10 * time.Minute}
sched.AddJob("backup-job", schedule, backupFunc)
```

### Example 2: One-Time Schedule

```go
// OnceSchedule runs a job only once at a specific time
type OnceSchedule struct {
    runTime time.Time
    hasRun  bool
}

func (s *OnceSchedule) Next(t time.Time) time.Time {
    if s.hasRun {
        return time.Now().Add(100 * 365 * 24 * time.Hour) // Far future
    }
    s.hasRun = true
    return s.runTime
}

// Usage
runAt := time.Now().Add(1 * time.Hour)
schedule := &OnceSchedule{runTime: runAt}
sched.AddJob("one-time-task", schedule, taskFunc)
```

### Example 3: Query Execution History

```go
ctx := context.Background()

// Get all executions for a job
executions, err := storage.ListExecutions(ctx, "my-job", nil)

// Query with filters
filterTime := time.Now().Add(-24 * time.Hour)
completedStatus := scheduler.JobStatusCompleted

options := &scheduler.QueryOptions{
    StartTime: &filterTime,           // Only after this time
    Status:    &completedStatus,      // Only completed
    SortBy:    "start_time",          // Sort by start time
    SortDesc:  true,                  // Descending order
    Limit:     10,                    // Max 10 records
    Offset:    0,                     // Starting from first
}

executions, err := storage.ListExecutions(ctx, "my-job", options)
if err != nil {
    panic(err)
}

for _, exec := range executions {
    fmt.Printf("Execution %s: Status=%s, Duration=%s\n",
        exec.ExecutionID, exec.Status, exec.Duration)
}
```

### Example 4: Job with Error Handling

```go
jobFunc := func(ctx context.Context) error {
    // Do some work
    if err := doWork(); err != nil {
        // Error will be recorded in execution history
        return fmt.Errorf("work failed: %w", err)
    }
    return nil
}

sched.AddJob("work-job", schedule, jobFunc)

// Later, check for failed executions
failedStatus := scheduler.JobStatusFailed
options := &scheduler.QueryOptions{
    Status: &failedStatus,
}

failedExecutions, _ := storage.ListExecutions(ctx, "work-job", options)
for _, exec := range failedExecutions {
    fmt.Printf("Failed: %s - Error: %s\n", exec.ExecutionID, exec.Error)
}
```

### Example 5: Graceful Shutdown

```go
func main() {
    storage := scheduler.NewMemoryStorage()
    sched := scheduler.NewScheduler(storage)

    ctx := context.Background()
    sched.Start(ctx)

    // Add jobs...
    sched.AddJob("job1", schedule, jobFunc)

    // Handle shutdown signals
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

    <-sigChan
    fmt.Println("Shutting down gracefully...")

    // Stop will wait for running jobs to complete
    if err := sched.Stop(ctx); err != nil {
        fmt.Printf("Error during shutdown: %v\n", err)
    }

    fmt.Println("Shutdown complete")
}
```

### Example 6: Multiple Database Support

```go
// PostgreSQL
import "gorm.io/driver/postgres"

dsn := "host=localhost user=gorm password=gorm dbname=scheduler port=5432"
db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})
storage := scheduler.NewGormStorage(db)

// MySQL
import "gorm.io/driver/mysql"

dsn := "user:pass@tcp(127.0.0.1:3306)/scheduler?charset=utf8mb4"
db, _ := gorm.Open(mysql.Open(dsn), &gorm.Config{})
storage := scheduler.NewGormStorage(db)

// SQLite
import "gorm.io/driver/sqlite"

db, _ := gorm.Open(sqlite.Open("scheduler.db"), &gorm.Config{})
storage := scheduler.NewGormStorage(db)
```

### Example 7: Job Metadata

```go
// Save job with metadata
job := &scheduler.JobData{
    ID:             "report-job",
    ScheduleType:   "cron",
    ScheduleConfig: "0 0 * * *",
    Status:         scheduler.JobStatusPending,
    CreatedAt:      time.Now(),
    UpdatedAt:      time.Now(),
    Metadata: map[string]string{
        "department": "sales",
        "report_type": "monthly",
        "recipients": "team@example.com",
    },
}

storage.SaveJob(ctx, job)

// Retrieve and use metadata
retrieved, _ := storage.GetJob(ctx, "report-job")
department := retrieved.Metadata["department"]
fmt.Printf("Generating %s report for %s\n",
    retrieved.Metadata["report_type"], department)
```

### Example 8: Clean Up Old Executions

```go
// Delete execution records older than 30 days
cutoffTime := time.Now().Add(-30 * 24 * time.Hour)

err := storage.DeleteExecutions(ctx, "my-job", cutoffTime)
if err != nil {
    panic(err)
}

fmt.Println("Old execution records cleaned up")
```

## API Reference

### Scheduler Methods

#### Start
```go
Start(ctx context.Context) error
```
Starts the scheduler and initializes storage.

#### Stop
```go
Stop(ctx context.Context) error
```
Gracefully stops the scheduler, waiting for running jobs to complete.

#### AddJob
```go
AddJob(id string, schedule Schedule, fn JobFunc) error
```
Registers a new job with the scheduler.

**Errors:**
- `ErrEmptyJobID`: Job ID is empty
- `ErrNilJobFunc`: Job function is nil
- `ErrJobAlreadyExists`: Job with this ID already exists
- `ErrInvalidInterval`: Schedule is nil

#### RemoveJob
```go
RemoveJob(id string) error
```
Removes a job from the scheduler. Waits for the job to complete if running.

**Errors:**
- `ErrEmptyJobID`: Job ID is empty
- `ErrJobNotFound`: Job does not exist

#### GetJob
```go
GetJob(id string) (Job, error)
```
Retrieves a job by its ID.

#### ListJobs
```go
ListJobs() []Job
```
Returns all registered jobs.

#### IsRunning
```go
IsRunning() bool
```
Returns whether the scheduler is currently running.

### Storage Methods

#### Initialize
```go
Initialize(ctx context.Context) error
```
Prepares the storage for use (creates tables, etc.).

#### Close
```go
Close(ctx context.Context) error
```
Releases storage resources and closes connections.

#### SaveJob
```go
SaveJob(ctx context.Context, job *JobData) error
```
Persists a new job to storage.

#### UpdateJob
```go
UpdateJob(ctx context.Context, job *JobData) error
```
Updates an existing job in storage.

#### DeleteJob
```go
DeleteJob(ctx context.Context, jobID string) error
```
Removes a job from storage.

#### ListJobsByStatus
```go
ListJobsByStatus(ctx context.Context, status JobStatus) ([]*JobData, error)
```
Returns jobs filtered by status.

**Statuses:**
- `JobStatusPending`: Job is waiting to run
- `JobStatusRunning`: Job is currently executing
- `JobStatusCompleted`: Job completed successfully
- `JobStatusFailed`: Job execution failed
- `JobStatusDisabled`: Job is disabled

#### SaveExecution
```go
SaveExecution(ctx context.Context, record *ExecutionRecord) error
```
Saves a job execution record.

#### ListExecutions
```go
ListExecutions(ctx context.Context, jobID string, options *QueryOptions) ([]*ExecutionRecord, error)
```
Lists execution records with optional filters.

**QueryOptions Fields:**
- `Limit`: Maximum number of records
- `Offset`: Number of records to skip
- `StartTime`: Filter records after this time
- `EndTime`: Filter records before this time
- `Status`: Filter by execution status
- `SortBy`: Field to sort by ("start_time", "duration", "end_time")
- `SortDesc`: Sort in descending order

#### HealthCheck
```go
HealthCheck(ctx context.Context) error
```
Verifies storage connection is healthy.

## Data Structures

### JobData

```go
type JobData struct {
    ID             string
    ScheduleType   string
    ScheduleConfig string
    Status         JobStatus
    NextRun        time.Time
    LastRun        time.Time
    CreatedAt      time.Time
    UpdatedAt      time.Time
    Metadata       map[string]string
}
```

### ExecutionRecord

```go
type ExecutionRecord struct {
    JobID       string
    ExecutionID string
    StartTime   time.Time
    EndTime     time.Time
    Duration    time.Duration
    Status      JobStatus
    Error       string
    Metadata    map[string]string
}
```

### QueryOptions

```go
type QueryOptions struct {
    Limit     int
    Offset    int
    StartTime *time.Time
    EndTime   *time.Time
    Status    *JobStatus
    SortBy    string
    SortDesc  bool
}
```

## Database Schema (GORM)

### scheduler_jobs Table

| Column         | Type      | Index | Description              |
|----------------|-----------|-------|--------------------------|
| id             | string    | PK    | Job identifier           |
| schedule_type  | string    |       | Schedule type            |
| schedule_config| text      |       | Schedule configuration   |
| status         | string    | Yes   | Current job status       |
| next_run       | timestamp | Yes   | Next execution time      |
| last_run       | timestamp |       | Last execution time      |
| created_at     | timestamp | Yes   | Creation time            |
| updated_at     | timestamp | Yes   | Last update time         |
| metadata       | text      |       | JSON-encoded metadata    |

### scheduler_executions Table

| Column       | Type      | Index | Description              |
|--------------|-----------|-------|--------------------------|
| execution_id | string    | PK    | Execution identifier     |
| job_id       | string    | Yes   | Associated job ID        |
| start_time   | timestamp | Yes   | Execution start time     |
| end_time     | timestamp |       | Execution end time       |
| duration     | int64     |       | Duration in nanoseconds  |
| status       | string    | Yes   | Execution status         |
| error        | text      |       | Error message if failed  |
| metadata     | text      |       | JSON-encoded metadata    |

## Error Handling

### Common Errors

```go
var (
    ErrSchedulerNotStarted       = errors.New("scheduler not started")
    ErrSchedulerAlreadyStarted   = errors.New("scheduler already started")
    ErrStorageNotInitialized     = errors.New("storage not initialized")
    ErrJobNotFound               = errors.New("job not found")
    ErrJobAlreadyExists          = errors.New("job already exists")
    ErrEmptyJobID                = errors.New("job ID cannot be empty")
    ErrNilJobFunc                = errors.New("job function cannot be nil")
    ErrInvalidJobData            = errors.New("invalid job data")
    ErrJobDataNotFound           = errors.New("job data not found")
    ErrExecutionHistoryNotFound  = errors.New("execution history not found")
)
```

### Error Handling Example

```go
err := sched.AddJob("my-job", schedule, jobFunc)
if err != nil {
    switch {
    case errors.Is(err, scheduler.ErrJobAlreadyExists):
        fmt.Println("Job already exists, skipping...")
    case errors.Is(err, scheduler.ErrEmptyJobID):
        fmt.Println("Invalid job ID")
    default:
        panic(err)
    }
}
```

## Best Practices

### 1. Always Use Context

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

sched.Start(ctx)
```

### 2. Handle Shutdown Gracefully

```go
defer func() {
    if err := sched.Stop(ctx); err != nil {
        log.Printf("Error stopping scheduler: %v", err)
    }
}()
```

### 3. Use Appropriate Storage

- **Development/Testing**: Use `MemoryStorage`
- **Production**: Use `GormStorage` with PostgreSQL or MySQL

### 4. Monitor Job Execution

```go
// Periodically check for failed jobs
ticker := time.NewTicker(1 * time.Hour)
go func() {
    for range ticker.C {
        checkFailedJobs(storage)
    }
}()
```

### 5. Set Reasonable Intervals

```go
// Good: Reasonable interval
schedule := &IntervalSchedule{interval: 5 * time.Minute}

// Avoid: Too frequent, may cause performance issues
schedule := &IntervalSchedule{interval: 100 * time.Millisecond}
```

### 6. Clean Up Old Executions

```go
// Run cleanup daily
cleanupSchedule := &IntervalSchedule{interval: 24 * time.Hour}
cleanupFunc := func(ctx context.Context) error {
    cutoff := time.Now().Add(-30 * 24 * time.Hour)
    return storage.DeleteExecutions(ctx, "all-jobs", cutoff)
}
sched.AddJob("cleanup", cleanupSchedule, cleanupFunc)
```

## Testing

Run all tests:
```bash
go test -v
```

Run with coverage:
```bash
go test -cover
```

Generate coverage report:
```bash
go test -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Performance Considerations

- **In-Memory Storage**: Fast, suitable for high-frequency jobs
- **GORM Storage**: Optimized with indexes on frequently queried fields
- **Concurrent Execution**: Jobs run in separate goroutines
- **Graceful Shutdown**: Waits for running jobs to complete

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/Weedbox/scheduler).
