# Migrate GORM to NATS JetStream Example

This example demonstrates how to migrate scheduler data from a GORM (SQL) storage backend to NATS JetStream KV using `scheduler.MigrateStorage`.

## Prerequisites

- NATS Server 2.12+ with JetStream enabled
- Go 1.24+

### Starting NATS Server

```bash
# Install NATS server (if not already installed)
# macOS
brew install nats-server

# Start with JetStream enabled
nats-server -js
```

## Running the Example

```bash
# Default: SQLite file "scheduler.db", NATS at nats://127.0.0.1:4222
go run main.go

# Or specify custom paths
DB_PATH=my_scheduler.db NATS_URL=nats://localhost:4222 go run main.go
```

## What Happens

1. Opens a SQLite database via GORM (the source storage)
2. Seeds demo data if the database is empty (2 jobs + 3 execution records)
3. Connects to NATS and creates a `NATSStorage` (the destination storage)
4. Calls `scheduler.MigrateStorage(ctx, gormStorage, natsStorage)` to copy all data
5. Lists the migrated jobs in NATS KV to verify
6. Starts a `NewNATSScheduler` — jobs resume automatically from KV data
7. Runs for 15 seconds to show jobs executing, then exits

## How Migration Works

```go
// 1. Set up source (GORM) and destination (NATS) storage
gormStorage := scheduler.NewGormStorage(db)
gormStorage.Initialize(ctx)

natsStorage := scheduler.NewNATSStorage(js)
natsStorage.Initialize(ctx)

// 2. One-line migration
result, err := scheduler.MigrateStorage(ctx, gormStorage, natsStorage)

// 3. Start NATS scheduler — it loads jobs from KV and resumes scheduling
sched := scheduler.NewNATSScheduler(js, handler)
sched.Start(ctx)
```

`MigrateStorage` is:

- **Generic** — works between any two `Storage` implementations (GORM→NATS, Memory→GORM, NATS→NATS, etc.)
- **Idempotent** — safe to run multiple times; existing jobs are skipped, not duplicated
- **Non-destructive** — source data is only read, never modified or deleted

## Migration Result

`MigrateStorage` returns a `*MigrateResult` with counters:

```go
type MigrateResult struct {
    JobsMigrated       int  // jobs successfully copied
    JobsSkipped        int  // jobs already in destination (skipped)
    ExecutionsMigrated int  // execution records copied
    ExecutionsSkipped  int  // execution records already in destination (skipped)
}
```

## KV Bucket Alignment

`NewNATSScheduler` and `NewNATSStorage` both default to these KV bucket names:

| Bucket | Default Name |
|--------|-------------|
| Jobs | `SCHEDULER_JOBS` |
| Executions | `SCHEDULER_EXECUTIONS` |

If you use custom bucket names with the scheduler, pass the same names to `NewNATSStorage`:

```go
natsStorage := scheduler.NewNATSStorage(js,
    scheduler.WithNATSStorageJobBucket("MY_JOBS"),
    scheduler.WithNATSStorageExecBucket("MY_EXECS"),
)

sched := scheduler.NewNATSScheduler(js, handler,
    scheduler.WithNATSSchedulerJobBucket("MY_JOBS"),
    scheduler.WithNATSSchedulerExecBucket("MY_EXECS"),
)
```

## Production Migration Steps

```go
// 1. Stop the old GORM-based scheduler
oldSched.Stop(ctx)

// 2. Initialize both storages
gormStorage := scheduler.NewGormStorage(db)
gormStorage.Initialize(ctx)

natsStorage := scheduler.NewNATSStorage(js)
natsStorage.Initialize(ctx)

// 3. Migrate
result, err := scheduler.MigrateStorage(ctx, gormStorage, natsStorage)
log.Printf("Migrated %d jobs, %d executions", result.JobsMigrated, result.ExecutionsMigrated)

// 4. Switch to NATS scheduler — jobs resume automatically
sched := scheduler.NewNATSScheduler(js, handler)
sched.Start(ctx)
```

## Clean Up

```bash
# Remove the SQLite database (after verifying migration)
rm scheduler.db

# Remove NATS state (if needed)
nats kv rm SCHEDULER_JOBS
nats kv rm SCHEDULER_EXECUTIONS
```
