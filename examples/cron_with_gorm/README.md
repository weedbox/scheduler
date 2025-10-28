# Cron Schedule with GORM Persistence Example

This example demonstrates how cron jobs are automatically persisted to a database using GORM, and how they continue to run after the program restarts.

## Features

- ✅ **Automatic Persistence** - Jobs are automatically saved to SQLite database
- ✅ **Restore on Restart** - Jobs are loaded from database when program starts
- ✅ **Continuous Execution** - Jobs continue their schedule across restarts
- ✅ **No Manual Save** - Everything is handled automatically by the scheduler

## Running the Example

```bash
# First run - creates jobs
go run main.go

# Stop with Ctrl+C

# Second run - restores jobs from database
go run main.go
```

## What Happens

### First Run
1. Program starts and connects to `scheduler.db` (creates if not exists)
2. Checks if any jobs exist in the database
3. If no jobs found, creates three cron jobs:
   - Weekly report (Every Friday at 10:00 AM)
   - Daily backup (Every day at 2:30 PM)
   - Every minute task (for demonstration)
4. All jobs are automatically saved to the database
5. Scheduler runs until you press Ctrl+C

### Second Run (After Restart)
1. Program starts and connects to `scheduler.db`
2. **Automatically loads** all jobs from the database
3. Jobs continue executing according to their cron schedules
4. No need to recreate jobs - they're restored from the database!

## How It Works

The scheduler automatically handles persistence:

```go
// Create storage with GORM
storage := scheduler.NewGormStorage(db)

// Create scheduler with the storage
sched := scheduler.NewScheduler(storage, handler, scheduler.NewBasicScheduleCodec())

// When you add a job, it's automatically saved to database
sched.AddJob("friday-report", cronSchedule, metadata)

// When you start the scheduler, it automatically loads jobs from database
sched.Start(ctx)
```

### Key Points

1. **Automatic Save** - When you call `AddJob()`, the job is immediately persisted
2. **Automatic Load** - When you call `Start()`, all jobs are loaded from storage
3. **Schedule Codec** - The `BasicScheduleCodec` handles encoding/decoding cron expressions
4. **Database Schema** - GORM automatically creates the required tables

## Database Structure

The scheduler creates two tables:

### scheduler_jobs
- Stores job definitions and their cron schedules
- Includes: id, schedule_type (`"cron"`), schedule_config (`"0 10 * * 5"`)
- Tracks: next_run, last_run, status, metadata

### scheduler_executions
- Stores execution history
- Includes: execution_id, job_id, start_time, end_time, status, error

## Viewing the Database

You can inspect the SQLite database:

```bash
sqlite3 scheduler.db

# View all jobs
SELECT id, schedule_type, schedule_config, next_run FROM scheduler_jobs;

# View execution history
SELECT job_id, start_time, status FROM scheduler_executions ORDER BY start_time DESC LIMIT 10;
```

## Testing Persistence

1. Run the program: `go run main.go`
2. Wait for the minute task to execute once
3. Stop with Ctrl+C
4. Run again: `go run main.go`
5. Notice that jobs are restored and continue executing

## Clean Up

To start fresh, delete the database:

```bash
rm scheduler.db
```
