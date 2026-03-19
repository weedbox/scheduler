# NATS JetStream Scheduler Example

This example demonstrates how to use NATS JetStream as the scheduler backend, leveraging JetStream's native scheduled message delivery for job triggering and KV Store for persistence.

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
# Default: connects to nats://127.0.0.1:4222
go run main.go

# Or specify a custom NATS URL
NATS_URL=nats://localhost:4222 go run main.go
```

## What Happens

### First Run

1. Connects to NATS and creates JetStream resources (stream, KV buckets, consumer)
2. Checks if any jobs exist in the KV Store
3. If no jobs found, creates four demo jobs:
   - **heartbeat** — Interval schedule, runs every 3 seconds
   - **one-time-task** — Once schedule, runs once after 5 seconds
   - **every-minute** — Cron schedule (`* * * * *`), runs every minute
   - **delayed-interval** — StartAt interval, starts after 10 seconds then runs every 5 seconds
4. Jobs begin executing as scheduled messages are delivered by JetStream

### Second Run (After Restart)

1. Connects to NATS and finds existing KV data
2. Automatically restores all jobs from the KV Store
3. Jobs continue executing according to their schedules — no need to recreate them

## How It Works

```go
// Connect to NATS
nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)

// Create NATS scheduler (replaces NewScheduler + Storage)
sched := scheduler.NewNATSScheduler(js, handler,
    scheduler.WithNATSStreamName("EXAMPLE_SCHEDULER"),
    scheduler.WithNATSSubjectPrefix("example.scheduler"),
)

// Start — creates stream, KV buckets, consumer, and loads persisted jobs
sched.Start(ctx)

// Add jobs — persisted to KV, scheduled via JetStream message delivery
sched.AddJob("my-job", schedule, metadata)
```

### Architecture

```
NewNATSScheduler
├── JetStream Stream (AllowMsgSchedules)
│   └── Publishes messages with "Nats-Scheduled-Delivery: @at <time>" header
│       JetStream holds the message until the scheduled time, then delivers it
├── JetStream Consumer (durable, work queue)
│   └── Receives triggered messages → executes JobHandler → schedules next run
└── JetStream KV Store
    ├── EXAMPLE_SCHEDULER_JOBS   — Job metadata (schedule, status, next/last run)
    └── EXAMPLE_SCHEDULER_EXECS  — Execution records (start/end time, duration, errors)
```

### Comparison with DB+Polling Approach

| | DB + Polling (`NewScheduler`) | NATS JetStream (`NewNATSScheduler`) |
|---|---|---|
| Triggering | Polls every second | JetStream delivers at scheduled time |
| Persistence | SQL database (GORM) | JetStream KV Store |
| Distributed | Requires external locking | Built-in via consumer ack mechanism |
| Failover | Manual implementation | Automatic message redelivery |
| Scaling | Single instance | Multiple workers sharing a consumer |

## Distributed Usage

Multiple workers can share the same NATS scheduler. JetStream's consumer ensures each job trigger is delivered to exactly one worker:

```bash
# Terminal 1
go run main.go

# Terminal 2 (connects to same NATS, same consumer group)
go run main.go
```

Jobs are automatically load-balanced across workers. If a worker goes down, pending messages are redelivered to remaining workers.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithNATSStreamName` | `SCHEDULER` | JetStream stream name |
| `WithNATSSubjectPrefix` | `scheduler` | NATS subject prefix for job messages |
| `WithNATSConsumerName` | `scheduler-worker` | Durable consumer name |
| `WithNATSSchedulerJobBucket` | `SCHEDULER_JOBS` | KV bucket for job metadata |
| `WithNATSSchedulerExecBucket` | `SCHEDULER_EXECUTIONS` | KV bucket for execution records |
| `WithNATSSchedulerCodec` | `BasicScheduleCodec` | Schedule encoder/decoder |

## Inspecting State with NATS CLI

```bash
# Install NATS CLI
brew install nats-io/nats-tools/nats

# View stream info
nats stream info EXAMPLE_SCHEDULER

# List KV keys (jobs)
nats kv ls EXAMPLE_SCHEDULER_JOBS

# Get a specific job's data
nats kv get EXAMPLE_SCHEDULER_JOBS heartbeat

# View execution records
nats kv ls EXAMPLE_SCHEDULER_EXECS

# Watch for new messages in real time
nats sub "example.scheduler.>"
```

## Clean Up

To remove all scheduler state from NATS:

```bash
nats stream rm EXAMPLE_SCHEDULER
nats kv rm EXAMPLE_SCHEDULER_JOBS
nats kv rm EXAMPLE_SCHEDULER_EXECS
```
