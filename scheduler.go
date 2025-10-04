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

	// ErrJobNotFound indicates the specified job does not exist
	ErrJobNotFound = errors.New("job not found")

	// ErrJobAlreadyExists indicates a job with the same ID already exists
	ErrJobAlreadyExists = errors.New("job already exists")

	// ErrEmptyJobID indicates the job ID cannot be empty
	ErrEmptyJobID = errors.New("job ID cannot be empty")

	// ErrNilJobFunc indicates the job function cannot be nil
	ErrNilJobFunc = errors.New("job function cannot be nil")
)

// JobFunc represents a function that will be executed by the scheduler
type JobFunc func(ctx context.Context) error

// Job represents a scheduled job configuration
type Job interface {
	// ID returns the unique identifier of the job
	ID() string

	// Execute runs the job function
	Execute(ctx context.Context) error

	// NextRun returns the next scheduled run time
	NextRun() time.Time

	// LastRun returns the last execution time
	LastRun() time.Time

	// IsRunning returns whether the job is currently executing
	IsRunning() bool
}

// Schedule defines the scheduling strategy for a job
type Schedule interface {
	// Next calculates the next run time based on the current time
	Next(t time.Time) time.Time
}

// Scheduler defines the interface for job scheduling operations
type Scheduler interface {
	// Start begins the scheduler execution
	Start(ctx context.Context) error

	// Stop gracefully shuts down the scheduler
	Stop(ctx context.Context) error

	// AddJob registers a new job with the scheduler
	AddJob(id string, schedule Schedule, fn JobFunc) error

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
