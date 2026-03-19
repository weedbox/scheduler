package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// natsJobValue is the JSON-serializable representation of JobData for NATS KV storage.
type natsJobValue struct {
	ID             string            `json:"id"`
	ScheduleType   string            `json:"schedule_type"`
	ScheduleConfig string            `json:"schedule_config"`
	Status         string            `json:"status"`
	NextRun        time.Time         `json:"next_run"`
	LastRun        time.Time         `json:"last_run"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// natsExecValue is the JSON-serializable representation of ExecutionRecord for NATS KV storage.
type natsExecValue struct {
	JobID       string            `json:"job_id"`
	ExecutionID string            `json:"execution_id"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	DurationNs  int64             `json:"duration_ns"`
	Status      string            `json:"status"`
	Error       string            `json:"error,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// NATSStorageOption configures a NATSStorage instance.
type NATSStorageOption func(*NATSStorage)

// WithNATSStorageJobBucket sets the KV bucket name for job data.
func WithNATSStorageJobBucket(name string) NATSStorageOption {
	return func(s *NATSStorage) {
		s.jobBucket = name
	}
}

// WithNATSStorageExecBucket sets the KV bucket name for execution records.
func WithNATSStorageExecBucket(name string) NATSStorageOption {
	return func(s *NATSStorage) {
		s.execBucket = name
	}
}

// NATSStorage implements the Storage interface using NATS JetStream KV Store.
type NATSStorage struct {
	js          jetstream.JetStream
	jobKV       jetstream.KeyValue
	execKV      jetstream.KeyValue
	jobBucket   string
	execBucket  string
	initialized bool
}

// NewNATSStorage creates a new NATSStorage instance.
func NewNATSStorage(js jetstream.JetStream, opts ...NATSStorageOption) *NATSStorage {
	s := &NATSStorage{
		js:         js,
		jobBucket:  "SCHEDULER_JOBS",
		execBucket: "SCHEDULER_EXECUTIONS",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Initialize creates the KV buckets and prepares the storage for use.
func (s *NATSStorage) Initialize(ctx context.Context) error {
	var err error
	s.jobKV, err = s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: s.jobBucket,
	})
	if err != nil {
		return fmt.Errorf("%w: failed to create job KV bucket: %v", ErrStorageConnectionFailed, err)
	}

	s.execKV, err = s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: s.execBucket,
	})
	if err != nil {
		return fmt.Errorf("%w: failed to create exec KV bucket: %v", ErrStorageConnectionFailed, err)
	}

	s.initialized = true
	return nil
}

// Close marks the storage as uninitialized.
func (s *NATSStorage) Close(ctx context.Context) error {
	s.initialized = false
	return nil
}

// HealthCheck verifies the storage is initialized and the KV bucket is accessible.
func (s *NATSStorage) HealthCheck(ctx context.Context) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}
	_, err := s.jobKV.Status(ctx)
	return err
}

// SaveJob persists a new job to KV storage.
func (s *NATSStorage) SaveJob(ctx context.Context, job *JobData) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	data, err := json.Marshal(jobDataToNATS(job))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	_, err = s.jobKV.Create(ctx, job.ID, data)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return ErrJobDataAlreadyExists
		}
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

// UpdateJob updates an existing job in KV storage.
func (s *NATSStorage) UpdateJob(ctx context.Context, job *JobData) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	_, err := s.jobKV.Get(ctx, job.ID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return ErrJobDataNotFound
		}
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	data, err := json.Marshal(jobDataToNATS(job))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	_, err = s.jobKV.Put(ctx, job.ID, data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

// DeleteJob removes a job from KV storage.
func (s *NATSStorage) DeleteJob(ctx context.Context, jobID string) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	err := s.jobKV.Purge(ctx, jobID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return ErrJobDataNotFound
		}
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

// GetJob retrieves a job by ID from KV storage.
func (s *NATSStorage) GetJob(ctx context.Context, jobID string) (*JobData, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	entry, err := s.jobKV.Get(ctx, jobID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrJobDataNotFound
		}
		return nil, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	var jv natsJobValue
	if err := json.Unmarshal(entry.Value(), &jv); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJobData, err)
	}

	return natsToJobData(&jv), nil
}

// ListJobs returns all jobs from KV storage.
func (s *NATSStorage) ListJobs(ctx context.Context) ([]*JobData, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	keys, err := s.jobKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []*JobData{}, nil
		}
		return nil, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	jobs := make([]*JobData, 0, len(keys))
	for _, key := range keys {
		entry, err := s.jobKV.Get(ctx, key)
		if err != nil {
			continue
		}
		var jv natsJobValue
		if err := json.Unmarshal(entry.Value(), &jv); err != nil {
			continue
		}
		jobs = append(jobs, natsToJobData(&jv))
	}
	return jobs, nil
}

// ListJobsByStatus returns jobs filtered by status from KV storage.
func (s *NATSStorage) ListJobsByStatus(ctx context.Context, status JobStatus) ([]*JobData, error) {
	jobs, err := s.ListJobs(ctx)
	if err != nil {
		return nil, err
	}

	filtered := make([]*JobData, 0)
	for _, job := range jobs {
		if job.Status == status {
			filtered = append(filtered, job)
		}
	}
	return filtered, nil
}

// SaveExecution persists an execution record to KV storage.
func (s *NATSStorage) SaveExecution(ctx context.Context, record *ExecutionRecord) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	data, err := json.Marshal(execRecordToNATS(record))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	_, err = s.execKV.Put(ctx, record.ExecutionID, data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

// GetExecution retrieves a specific execution record by ID.
func (s *NATSStorage) GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	entry, err := s.execKV.Get(ctx, executionID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrExecutionHistoryNotFound
		}
		return nil, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	var ev natsExecValue
	if err := json.Unmarshal(entry.Value(), &ev); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	return natsToExecRecord(&ev), nil
}

// ListExecutions returns execution records for a job with optional filtering, sorting, and pagination.
func (s *NATSStorage) ListExecutions(ctx context.Context, jobID string, options *QueryOptions) ([]*ExecutionRecord, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	keys, err := s.execKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []*ExecutionRecord{}, nil
		}
		return nil, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	var records []*ExecutionRecord
	for _, key := range keys {
		entry, err := s.execKV.Get(ctx, key)
		if err != nil {
			continue
		}
		var ev natsExecValue
		if err := json.Unmarshal(entry.Value(), &ev); err != nil {
			continue
		}
		if ev.JobID != jobID {
			continue
		}
		records = append(records, natsToExecRecord(&ev))
	}

	if options != nil {
		records = filterNATSExecutions(records, options)
	}

	return records, nil
}

// DeleteExecutions removes execution records for a job that are older than the specified time.
func (s *NATSStorage) DeleteExecutions(ctx context.Context, jobID string, before time.Time) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	keys, err := s.execKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}

	for _, key := range keys {
		entry, err := s.execKV.Get(ctx, key)
		if err != nil {
			continue
		}
		var ev natsExecValue
		if err := json.Unmarshal(entry.Value(), &ev); err != nil {
			continue
		}
		if ev.JobID == jobID && ev.StartTime.Before(before) {
			_ = s.execKV.Purge(ctx, key)
		}
	}
	return nil
}

// Conversion helpers

func jobDataToNATS(job *JobData) *natsJobValue {
	return &natsJobValue{
		ID:             job.ID,
		ScheduleType:   job.ScheduleType,
		ScheduleConfig: job.ScheduleConfig,
		Status:         string(job.Status),
		NextRun:        job.NextRun,
		LastRun:        job.LastRun,
		CreatedAt:      job.CreatedAt,
		UpdatedAt:      job.UpdatedAt,
		Metadata:       copyMetadata(job.Metadata),
	}
}

func natsToJobData(jv *natsJobValue) *JobData {
	return &JobData{
		ID:             jv.ID,
		ScheduleType:   jv.ScheduleType,
		ScheduleConfig: jv.ScheduleConfig,
		Status:         JobStatus(jv.Status),
		NextRun:        jv.NextRun,
		LastRun:        jv.LastRun,
		CreatedAt:      jv.CreatedAt,
		UpdatedAt:      jv.UpdatedAt,
		Metadata:       copyMetadata(jv.Metadata),
	}
}

func execRecordToNATS(record *ExecutionRecord) *natsExecValue {
	return &natsExecValue{
		JobID:       record.JobID,
		ExecutionID: record.ExecutionID,
		StartTime:   record.StartTime,
		EndTime:     record.EndTime,
		DurationNs:  int64(record.Duration),
		Status:      string(record.Status),
		Error:       record.Error,
		Metadata:    copyMetadata(record.Metadata),
	}
}

func natsToExecRecord(ev *natsExecValue) *ExecutionRecord {
	return &ExecutionRecord{
		JobID:       ev.JobID,
		ExecutionID: ev.ExecutionID,
		StartTime:   ev.StartTime,
		EndTime:     ev.EndTime,
		Duration:    time.Duration(ev.DurationNs),
		Status:      JobStatus(ev.Status),
		Error:       ev.Error,
		Metadata:    copyMetadata(ev.Metadata),
	}
}

func filterNATSExecutions(records []*ExecutionRecord, options *QueryOptions) []*ExecutionRecord {
	filtered := make([]*ExecutionRecord, 0, len(records))
	for _, r := range records {
		if options.StartTime != nil && r.StartTime.Before(*options.StartTime) {
			continue
		}
		if options.EndTime != nil && r.StartTime.After(*options.EndTime) {
			continue
		}
		if options.Status != nil && r.Status != *options.Status {
			continue
		}
		filtered = append(filtered, r)
	}

	sortBy := options.SortBy
	if sortBy == "" {
		sortBy = "start_time"
	}
	sort.Slice(filtered, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "duration":
			less = filtered[i].Duration < filtered[j].Duration
		case "end_time":
			less = filtered[i].EndTime.Before(filtered[j].EndTime)
		default:
			less = filtered[i].StartTime.Before(filtered[j].StartTime)
		}
		if options.SortDesc {
			return !less
		}
		return less
	})

	if options.Offset > 0 {
		if options.Offset >= len(filtered) {
			return []*ExecutionRecord{}
		}
		filtered = filtered[options.Offset:]
	}
	if options.Limit > 0 && options.Limit < len(filtered) {
		filtered = filtered[:options.Limit]
	}

	return filtered
}
