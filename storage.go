package scheduler

import (
	"context"
	"errors"
	"time"
)

// Pre-defined storage errors
var (
	// ErrStorageNotInitialized indicates the storage has not been initialized
	ErrStorageNotInitialized = errors.New("storage not initialized")

	// ErrStorageConnectionFailed indicates failed to connect to storage
	ErrStorageConnectionFailed = errors.New("storage connection failed")

	// ErrStorageOperationFailed indicates a storage operation failed
	ErrStorageOperationFailed = errors.New("storage operation failed")

	// ErrJobDataNotFound indicates the job data does not exist in storage
	ErrJobDataNotFound = errors.New("job data not found")

	// ErrJobDataAlreadyExists indicates the job data already exists in storage
	ErrJobDataAlreadyExists = errors.New("job data already exists")

	// ErrInvalidJobData indicates the job data is invalid or corrupted
	ErrInvalidJobData = errors.New("invalid job data")

	// ErrExecutionHistoryNotFound indicates no execution history found
	ErrExecutionHistoryNotFound = errors.New("execution history not found")
)

// JobStatus represents the current status of a job
type JobStatus string

const (
	// JobStatusPending indicates the job is waiting to run
	JobStatusPending JobStatus = "pending"

	// JobStatusRunning indicates the job is currently executing
	JobStatusRunning JobStatus = "running"

	// JobStatusCompleted indicates the job completed successfully
	JobStatusCompleted JobStatus = "completed"

	// JobStatusFailed indicates the job execution failed
	JobStatusFailed JobStatus = "failed"

	// JobStatusDisabled indicates the job is disabled
	JobStatusDisabled JobStatus = "disabled"
)

// JobData represents the persistent data of a scheduled job
type JobData struct {
	// ID is the unique identifier of the job
	ID string

	// ScheduleType indicates the type of schedule (interval, cron, once, etc.)
	ScheduleType string

	// ScheduleConfig stores the schedule configuration as a string
	// For interval: duration string (e.g., "5m", "1h")
	// For cron: cron expression (e.g., "0 0 * * *")
	// For once: timestamp (e.g., "2025-10-02T15:04:05Z")
	ScheduleConfig string

	// Status is the current status of the job
	Status JobStatus

	// NextRun is the next scheduled execution time
	NextRun time.Time

	// LastRun is the last execution time
	LastRun time.Time

	// CreatedAt is the job creation timestamp
	CreatedAt time.Time

	// UpdatedAt is the last update timestamp
	UpdatedAt time.Time

	// Metadata stores additional job metadata as key-value pairs
	Metadata map[string]string
}

// ExecutionRecord represents a single job execution record
type ExecutionRecord struct {
	// JobID is the identifier of the job
	JobID string

	// ExecutionID is the unique identifier of this execution
	ExecutionID string

	// StartTime is when the execution started
	StartTime time.Time

	// EndTime is when the execution completed
	EndTime time.Time

	// Duration is the execution duration
	Duration time.Duration

	// Status is the execution result status
	Status JobStatus

	// Error stores the error message if execution failed
	Error string

	// Metadata stores additional execution metadata
	Metadata map[string]string
}

// QueryOptions defines options for querying execution records
type QueryOptions struct {
	// Limit limits the number of records to return
	Limit int

	// Offset skips the specified number of records
	Offset int

	// StartTime filters records after this time
	StartTime *time.Time

	// EndTime filters records before this time
	EndTime *time.Time

	// Status filters records by status
	Status *JobStatus

	// SortBy specifies the field to sort by (e.g., "start_time", "duration")
	SortBy string

	// SortDesc specifies descending sort order
	SortDesc bool
}

// Storage defines the interface for persistent storage operations
type Storage interface {
	// Initialize prepares the storage for use
	Initialize(ctx context.Context) error

	// Close releases storage resources
	Close(ctx context.Context) error

	// SaveJob persists job data to storage
	SaveJob(ctx context.Context, job *JobData) error

	// UpdateJob updates existing job data in storage
	UpdateJob(ctx context.Context, job *JobData) error

	// DeleteJob removes job data from storage
	DeleteJob(ctx context.Context, jobID string) error

	// GetJob retrieves job data by ID
	GetJob(ctx context.Context, jobID string) (*JobData, error)

	// ListJobs returns all jobs in storage
	ListJobs(ctx context.Context) ([]*JobData, error)

	// ListJobsByStatus returns jobs filtered by status
	ListJobsByStatus(ctx context.Context, status JobStatus) ([]*JobData, error)

	// SaveExecution persists an execution record
	SaveExecution(ctx context.Context, record *ExecutionRecord) error

	// GetExecution retrieves a specific execution record
	GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error)

	// ListExecutions returns execution records for a job with optional filters
	ListExecutions(ctx context.Context, jobID string, options *QueryOptions) ([]*ExecutionRecord, error)

	// DeleteExecutions removes execution records older than the specified time
	DeleteExecutions(ctx context.Context, jobID string, before time.Time) error

	// HealthCheck verifies the storage connection is healthy
	HealthCheck(ctx context.Context) error
}
