package scheduler

import (
	"context"
	"errors"
	"time"
)

// Pre-defined errors
var (
	// ErrSchedulerNotStarted indicates the scheduler has not been started
	ErrSchedulerNotStarted = errors.New("scheduler not started")

	// ErrSchedulerAlreadyStarted indicates the scheduler is already running
	ErrSchedulerAlreadyStarted = errors.New("scheduler already started")

	// ErrSchedulerStopped indicates the scheduler has been stopped
	ErrSchedulerStopped = errors.New("scheduler stopped")

	// ErrInvalidInterval indicates an invalid schedule interval
	ErrInvalidInterval = errors.New("invalid schedule interval")

	// ErrInvalidCronExpression indicates an invalid cron expression
	ErrInvalidCronExpression = errors.New("invalid cron expression")

	// ErrInvalidScheduleTime indicates an invalid schedule time
	ErrInvalidScheduleTime = errors.New("invalid schedule time")

	// ErrInvalidScheduleConfig indicates an invalid serialized schedule configuration
	ErrInvalidScheduleConfig = errors.New("invalid schedule configuration")

	// ErrJobNotFound indicates the specified job does not exist
	ErrJobNotFound = errors.New("job not found")

	// ErrJobAlreadyExists indicates a job with the same ID already exists
	ErrJobAlreadyExists = errors.New("job already exists")

	// ErrEmptyJobID indicates the job ID cannot be empty
	ErrEmptyJobID = errors.New("job ID cannot be empty")

	// ErrHandlerNotDefined indicates the scheduler was created without a job handler
	ErrHandlerNotDefined = errors.New("scheduler handler not defined")
)

// Job represents a scheduled job configuration
type Job interface {
	// ID returns the unique identifier of the job
	ID() string

	// NextRun returns the next scheduled run time
	NextRun() time.Time

	// LastRun returns the last execution time
	LastRun() time.Time

	// IsRunning returns whether the job is currently executing
	IsRunning() bool

	// Metadata returns the job metadata
	Metadata() map[string]string
}

// JobEvent represents the context passed to a JobHandler when a job is executed.
type JobEvent struct {
	job           Job
	metadata      map[string]string
	schedule      Schedule
	scheduledAt   time.Time
	startedAt     time.Time
	lastCompleted time.Time
}

// Job returns the underlying Job instance for advanced inspection.
func (e JobEvent) Job() Job {
	return e.job
}

// ID returns the unique identifier of the job associated with this event.
func (e JobEvent) ID() string {
	if e.job == nil {
		return ""
	}
	return e.job.ID()
}

// Name returns the job name (alias for ID) associated with this event.
func (e JobEvent) Name() string {
	return e.ID()
}

// Metadata returns a copy of the job metadata captured at execution time.
func (e JobEvent) Metadata() map[string]string {
	return copyMetadata(e.metadata)
}

// Schedule returns the schedule associated with the job when it executed.
func (e JobEvent) Schedule() Schedule {
	return e.schedule
}

// ScheduledAt returns the time the job was scheduled to execute.
func (e JobEvent) ScheduledAt() time.Time {
	return e.scheduledAt
}

// StartedAt returns the actual time the job handler was invoked.
func (e JobEvent) StartedAt() time.Time {
	return e.startedAt
}

// LastCompletedAt returns the completion time of the previous successful execution, or zero if none.
func (e JobEvent) LastCompletedAt() time.Time {
	return e.lastCompleted
}

// Delay returns the duration between the scheduled time and the handler invocation.
func (e JobEvent) Delay() time.Duration {
	if e.scheduledAt.IsZero() || e.startedAt.IsZero() {
		return 0
	}
	return e.startedAt.Sub(e.scheduledAt)
}

// JobHandler handles job execution events.
type JobHandler func(ctx context.Context, event JobEvent) error

func newJobEvent(job Job, metadata map[string]string, schedule Schedule, scheduledAt, startedAt, lastCompleted time.Time) JobEvent {
	return JobEvent{
		job:           job,
		metadata:      metadata,
		schedule:      schedule,
		scheduledAt:   scheduledAt,
		startedAt:     startedAt,
		lastCompleted: lastCompleted,
	}
}

// Schedule defines the scheduling strategy for a job
type Schedule interface {
	// Next calculates the next run time based on the current time
	Next(t time.Time) time.Time
}

// ScheduleCodec encodes and decodes schedules for persistence
type ScheduleCodec interface {
	// Encode converts a schedule into a serializable representation
	Encode(schedule Schedule) (scheduleType string, scheduleConfig string, err error)

	// Decode reconstructs a schedule from its serialized representation
	Decode(scheduleType string, scheduleConfig string) (Schedule, error)
}

// Scheduler defines the interface for job scheduling operations
type Scheduler interface {
	// Start begins the scheduler execution
	Start(ctx context.Context) error

	// WaitUntilRunning blocks until the scheduler has fully started or the context is done
	WaitUntilRunning(ctx context.Context) error

	// Stop gracefully shuts down the scheduler
	Stop(ctx context.Context) error

	// AddJob registers a new job with the scheduler
	AddJob(id string, schedule Schedule, metadata map[string]string) error

	// UpdateJobSchedule replaces the schedule for an existing job
	UpdateJobSchedule(id string, schedule Schedule) error

	// RemoveJob removes a job from the scheduler
	RemoveJob(id string) error

	// GetJob retrieves a job by its ID
	GetJob(id string) (Job, error)

	// ListJobs returns all registered jobs
	ListJobs() []Job

	// IsRunning returns whether the scheduler is currently running
	IsRunning() bool
}

// ScheduleBuilder provides methods to create common scheduling strategies
type ScheduleBuilder interface {
	// Every creates a schedule that runs at fixed intervals
	Every(duration time.Duration) (Schedule, error)

	// Cron creates a schedule from a cron expression
	Cron(expression string) (Schedule, error)

	// At creates a schedule that runs at a specific time daily
	At(hour, minute, second int) (Schedule, error)

	// Once creates a schedule that runs only once at the specified time
	Once(t time.Time) (Schedule, error)
}
