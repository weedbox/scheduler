package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryStorage is an in-memory implementation of the Storage interface
type MemoryStorage struct {
	mu            sync.RWMutex
	initialized   bool
	jobs          map[string]*JobData
	executions    map[string]*ExecutionRecord
	jobExecutions map[string][]string // jobID -> []executionID
}

// NewMemoryStorage creates a new in-memory storage instance
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		jobs:          make(map[string]*JobData),
		executions:    make(map[string]*ExecutionRecord),
		jobExecutions: make(map[string][]string),
	}
}

// Initialize prepares the storage for use
func (s *MemoryStorage) Initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initialized {
		return nil
	}

	s.initialized = true
	return nil
}

// Close releases storage resources
func (s *MemoryStorage) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	s.jobs = make(map[string]*JobData)
	s.executions = make(map[string]*ExecutionRecord)
	s.jobExecutions = make(map[string][]string)
	s.initialized = false

	return nil
}

// SaveJob persists job data to storage
func (s *MemoryStorage) SaveJob(ctx context.Context, job *JobData) error {
	if job == nil {
		return ErrInvalidJobData
	}

	if job.ID == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if _, exists := s.jobs[job.ID]; exists {
		return ErrJobDataAlreadyExists
	}

	// Create a copy to avoid external modifications
	jobCopy := *job
	if job.Metadata != nil {
		jobCopy.Metadata = make(map[string]string)
		for k, v := range job.Metadata {
			jobCopy.Metadata[k] = v
		}
	}

	s.jobs[job.ID] = &jobCopy
	return nil
}

// UpdateJob updates existing job data in storage
func (s *MemoryStorage) UpdateJob(ctx context.Context, job *JobData) error {
	if job == nil {
		return ErrInvalidJobData
	}

	if job.ID == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if _, exists := s.jobs[job.ID]; !exists {
		return ErrJobDataNotFound
	}

	// Create a copy to avoid external modifications
	jobCopy := *job
	if job.Metadata != nil {
		jobCopy.Metadata = make(map[string]string)
		for k, v := range job.Metadata {
			jobCopy.Metadata[k] = v
		}
	}

	s.jobs[job.ID] = &jobCopy
	return nil
}

// DeleteJob removes job data from storage
func (s *MemoryStorage) DeleteJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if _, exists := s.jobs[jobID]; !exists {
		return ErrJobDataNotFound
	}

	delete(s.jobs, jobID)
	return nil
}

// GetJob retrieves job data by ID
func (s *MemoryStorage) GetJob(ctx context.Context, jobID string) (*JobData, error) {
	if jobID == "" {
		return nil, ErrEmptyJobID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	job, exists := s.jobs[jobID]
	if !exists {
		return nil, ErrJobDataNotFound
	}

	// Return a copy to prevent external modifications
	jobCopy := *job
	if job.Metadata != nil {
		jobCopy.Metadata = make(map[string]string)
		for k, v := range job.Metadata {
			jobCopy.Metadata[k] = v
		}
	}

	return &jobCopy, nil
}

// ListJobs returns all jobs in storage
func (s *MemoryStorage) ListJobs(ctx context.Context) ([]*JobData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	jobs := make([]*JobData, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobCopy := *job
		if job.Metadata != nil {
			jobCopy.Metadata = make(map[string]string)
			for k, v := range job.Metadata {
				jobCopy.Metadata[k] = v
			}
		}
		jobs = append(jobs, &jobCopy)
	}

	return jobs, nil
}

// ListJobsByStatus returns jobs filtered by status
func (s *MemoryStorage) ListJobsByStatus(ctx context.Context, status JobStatus) ([]*JobData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	jobs := make([]*JobData, 0)
	for _, job := range s.jobs {
		if job.Status == status {
			jobCopy := *job
			if job.Metadata != nil {
				jobCopy.Metadata = make(map[string]string)
				for k, v := range job.Metadata {
					jobCopy.Metadata[k] = v
				}
			}
			jobs = append(jobs, &jobCopy)
		}
	}

	return jobs, nil
}

// SaveExecution persists an execution record
func (s *MemoryStorage) SaveExecution(ctx context.Context, record *ExecutionRecord) error {
	if record == nil {
		return ErrInvalidJobData
	}

	if record.ExecutionID == "" {
		return errors.New("execution ID cannot be empty")
	}

	if record.JobID == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	// Create a copy to avoid external modifications
	recordCopy := *record
	if record.Metadata != nil {
		recordCopy.Metadata = make(map[string]string)
		for k, v := range record.Metadata {
			recordCopy.Metadata[k] = v
		}
	}

	s.executions[record.ExecutionID] = &recordCopy

	// Add to job executions index
	s.jobExecutions[record.JobID] = append(s.jobExecutions[record.JobID], record.ExecutionID)

	return nil
}

// GetExecution retrieves a specific execution record
func (s *MemoryStorage) GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error) {
	if executionID == "" {
		return nil, errors.New("execution ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	record, exists := s.executions[executionID]
	if !exists {
		return nil, ErrExecutionHistoryNotFound
	}

	// Return a copy to prevent external modifications
	recordCopy := *record
	if record.Metadata != nil {
		recordCopy.Metadata = make(map[string]string)
		for k, v := range record.Metadata {
			recordCopy.Metadata[k] = v
		}
	}

	return &recordCopy, nil
}

// ListExecutions returns execution records for a job with optional filters
func (s *MemoryStorage) ListExecutions(ctx context.Context, jobID string, options *QueryOptions) ([]*ExecutionRecord, error) {
	if jobID == "" {
		return nil, ErrEmptyJobID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	executionIDs, exists := s.jobExecutions[jobID]
	if !exists {
		return []*ExecutionRecord{}, nil
	}

	// Collect all execution records for this job
	records := make([]*ExecutionRecord, 0)
	for _, execID := range executionIDs {
		record, exists := s.executions[execID]
		if !exists {
			continue
		}

		// Apply filters if options provided
		if options != nil {
			if options.StartTime != nil && record.StartTime.Before(*options.StartTime) {
				continue
			}
			if options.EndTime != nil && !record.StartTime.Before(*options.EndTime) {
				continue
			}
			if options.Status != nil && record.Status != *options.Status {
				continue
			}
		}

		recordCopy := *record
		if record.Metadata != nil {
			recordCopy.Metadata = make(map[string]string)
			for k, v := range record.Metadata {
				recordCopy.Metadata[k] = v
			}
		}
		records = append(records, &recordCopy)
	}

	// Apply sorting
	if options != nil && options.SortBy != "" {
		s.sortExecutions(records, options.SortBy, options.SortDesc)
	}

	// Apply pagination
	if options != nil {
		offset := options.Offset
		limit := options.Limit

		if offset > len(records) {
			return []*ExecutionRecord{}, nil
		}

		end := len(records)
		if limit > 0 && offset+limit < end {
			end = offset + limit
		}

		records = records[offset:end]
	}

	return records, nil
}

// sortExecutions sorts execution records based on the specified field
func (s *MemoryStorage) sortExecutions(records []*ExecutionRecord, sortBy string, descending bool) {
	// Simple bubble sort implementation for demonstration
	// In production, consider using sort.Slice for better performance
	n := len(records)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			shouldSwap := false

			switch sortBy {
			case "start_time":
				if descending {
					shouldSwap = records[j].StartTime.Before(records[j+1].StartTime)
				} else {
					shouldSwap = records[j].StartTime.After(records[j+1].StartTime)
				}
			case "duration":
				if descending {
					shouldSwap = records[j].Duration < records[j+1].Duration
				} else {
					shouldSwap = records[j].Duration > records[j+1].Duration
				}
			case "end_time":
				if descending {
					shouldSwap = records[j].EndTime.Before(records[j+1].EndTime)
				} else {
					shouldSwap = records[j].EndTime.After(records[j+1].EndTime)
				}
			}

			if shouldSwap {
				records[j], records[j+1] = records[j+1], records[j]
			}
		}
	}
}

// DeleteExecutions removes execution records older than the specified time
func (s *MemoryStorage) DeleteExecutions(ctx context.Context, jobID string, before time.Time) error {
	if jobID == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	executionIDs, exists := s.jobExecutions[jobID]
	if !exists {
		return nil
	}

	// Filter out executions to delete
	remainingIDs := make([]string, 0)
	for _, execID := range executionIDs {
		record, exists := s.executions[execID]
		if !exists {
			continue
		}

		if !record.StartTime.After(before) {
			delete(s.executions, execID)
		} else {
			remainingIDs = append(remainingIDs, execID)
		}
	}

	s.jobExecutions[jobID] = remainingIDs
	return nil
}

// HealthCheck verifies the storage connection is healthy
func (s *MemoryStorage) HealthCheck(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initialized {
		return ErrStorageNotInitialized
	}

	return nil
}
