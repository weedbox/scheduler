package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

// GormStorage is a GORM-based implementation of the Storage interface
type GormStorage struct {
	db          *gorm.DB
	initialized bool

	// ownsDB controls whether Close() closes the underlying *sql.DB
	// connection pool. The *gorm.DB is injected by the caller and is
	// commonly shared with the rest of the application, so the default is
	// false; opt in with WithGormOwnsDB when the storage is the sole user.
	ownsDB bool
}

// executionSortColumns whitelists ListExecutions SortBy values, mapping each
// to the column it sorts on. Only values present here may reach the ORDER BY
// clause.
var executionSortColumns = map[string]string{
	"start_time": "start_time",
	"end_time":   "end_time",
	"duration":   "duration",
}

// isDuplicateKeyErr recognizes primary-key violations across SQL drivers.
// SaveJob's existence pre-check is check-then-act: two concurrent writers can
// both pass the check, and the loser's Create then fails with a
// driver-specific duplicate-key error that must map to
// ErrJobDataAlreadyExists rather than leak through raw.
func isDuplicateKeyErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || // sqlite
		strings.Contains(msg, "duplicate key value") || // postgres
		strings.Contains(msg, "Duplicate entry") // mysql
}

// GormStorageOption configures a GormStorage created by NewGormStorage.
type GormStorageOption func(*GormStorage)

// WithGormOwnsDB makes Close() close the underlying *sql.DB connection pool.
// Use this only when the *gorm.DB was created exclusively for this storage;
// closing a pool shared with the rest of the application would break every
// other user of that DB (Scheduler.Stop calls Storage.Close).
func WithGormOwnsDB() GormStorageOption {
	return func(s *GormStorage) {
		s.ownsDB = true
	}
}

// JobDataModel represents the database model for JobData
type JobDataModel struct {
	ID             string    `gorm:"primaryKey;size:255"`
	ScheduleType   string    `gorm:"size:50"`
	ScheduleConfig string    `gorm:"type:text"`
	Status         string    `gorm:"size:20;index"`
	NextRun        time.Time `gorm:"index"`
	LastRun        time.Time
	CreatedAt      time.Time `gorm:"index"`
	UpdatedAt      time.Time `gorm:"index"`
	Metadata       string    `gorm:"type:text"` // JSON-encoded map[string]string
}

// TableName specifies the table name for JobDataModel
func (JobDataModel) TableName() string {
	return "scheduler_jobs"
}

// ExecutionRecordModel represents the database model for ExecutionRecord
type ExecutionRecordModel struct {
	ExecutionID string    `gorm:"primaryKey;size:255"`
	JobID       string    `gorm:"size:255;index"`
	StartTime   time.Time `gorm:"index"`
	EndTime     time.Time
	Duration    int64  // nanoseconds
	Status      string `gorm:"size:20;index"`
	Error       string `gorm:"type:text"`
	Metadata    string `gorm:"type:text"` // JSON-encoded map[string]string
}

// TableName specifies the table name for ExecutionRecordModel
func (ExecutionRecordModel) TableName() string {
	return "scheduler_executions"
}

// NewGormStorage creates a new GORM storage instance. By default Close()
// does NOT close the injected *gorm.DB's connection pool (it is usually
// shared with the application); pass WithGormOwnsDB to transfer ownership.
func NewGormStorage(db *gorm.DB, opts ...GormStorageOption) *GormStorage {
	s := &GormStorage{
		db:          db,
		initialized: false,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Initialize prepares the storage for use
func (s *GormStorage) Initialize(ctx context.Context) error {
	if s.initialized {
		return nil
	}

	if s.db == nil {
		return ErrStorageConnectionFailed
	}

	// Auto-migrate the schema (serialized across instances on PostgreSQL)
	err := autoMigrate(ctx, s.db, &JobDataModel{}, &ExecutionRecordModel{})
	if err != nil {
		return err
	}

	s.initialized = true
	return nil
}

// Close releases storage resources. The underlying *sql.DB connection pool
// is closed only when the storage owns it (see WithGormOwnsDB) — by default
// the pool belongs to the caller and stays open.
func (s *GormStorage) Close(ctx context.Context) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if s.ownsDB {
		sqlDB, err := s.db.DB()
		if err != nil {
			return err
		}
		if err := sqlDB.Close(); err != nil {
			return err
		}
	}

	s.initialized = false
	return nil
}

// SaveJob persists job data to storage
func (s *GormStorage) SaveJob(ctx context.Context, job *JobData) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if job == nil {
		return ErrInvalidJobData
	}

	if job.ID == "" {
		return ErrEmptyJobID
	}

	// Check if job already exists
	var count int64
	err := s.db.WithContext(ctx).Model(&JobDataModel{}).Where("id = ?", job.ID).Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return ErrJobDataAlreadyExists
	}

	// Convert to model
	model, err := s.jobDataToModel(job)
	if err != nil {
		return err
	}

	// Save to database
	if err := s.db.WithContext(ctx).Create(model).Error; err != nil {
		if isDuplicateKeyErr(err) {
			return ErrJobDataAlreadyExists
		}
		return err
	}

	return nil
}

// UpdateJob updates existing job data in storage
func (s *GormStorage) UpdateJob(ctx context.Context, job *JobData) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if job == nil {
		return ErrInvalidJobData
	}

	if job.ID == "" {
		return ErrEmptyJobID
	}

	// Check if job exists
	var count int64
	err := s.db.WithContext(ctx).Model(&JobDataModel{}).Where("id = ?", job.ID).Count(&count).Error
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrJobDataNotFound
	}

	// Convert to model
	model, err := s.jobDataToModel(job)
	if err != nil {
		return err
	}

	// Update in database
	if err := s.db.WithContext(ctx).Save(model).Error; err != nil {
		return err
	}

	return nil
}

// DeleteJob removes job data from storage
func (s *GormStorage) DeleteJob(ctx context.Context, jobID string) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if jobID == "" {
		return ErrEmptyJobID
	}

	// Check if job exists
	var count int64
	err := s.db.WithContext(ctx).Model(&JobDataModel{}).Where("id = ?", jobID).Count(&count).Error
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrJobDataNotFound
	}

	// Delete from database
	if err := s.db.WithContext(ctx).Delete(&JobDataModel{}, "id = ?", jobID).Error; err != nil {
		return err
	}

	return nil
}

// GetJob retrieves job data by ID
func (s *GormStorage) GetJob(ctx context.Context, jobID string) (*JobData, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	if jobID == "" {
		return nil, ErrEmptyJobID
	}

	var model JobDataModel
	err := s.db.WithContext(ctx).Where("id = ?", jobID).First(&model).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrJobDataNotFound
		}
		return nil, err
	}

	// Convert to JobData
	job, err := s.modelToJobData(&model)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// ListJobs returns all jobs in storage
func (s *GormStorage) ListJobs(ctx context.Context) ([]*JobData, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	var models []JobDataModel
	err := s.db.WithContext(ctx).Find(&models).Error
	if err != nil {
		return nil, err
	}

	jobs := make([]*JobData, 0, len(models))
	for _, model := range models {
		job, err := s.modelToJobData(&model)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// ListJobsByStatus returns jobs filtered by status
func (s *GormStorage) ListJobsByStatus(ctx context.Context, status JobStatus) ([]*JobData, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	var models []JobDataModel
	err := s.db.WithContext(ctx).Where("status = ?", string(status)).Find(&models).Error
	if err != nil {
		return nil, err
	}

	jobs := make([]*JobData, 0, len(models))
	for _, model := range models {
		job, err := s.modelToJobData(&model)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// SaveExecution persists an execution record
func (s *GormStorage) SaveExecution(ctx context.Context, record *ExecutionRecord) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if record == nil {
		return ErrInvalidJobData
	}

	if record.ExecutionID == "" {
		return errors.New("execution ID cannot be empty")
	}

	if record.JobID == "" {
		return ErrEmptyJobID
	}

	// Convert to model
	model, err := s.executionRecordToModel(record)
	if err != nil {
		return err
	}

	// Save to database
	if err := s.db.WithContext(ctx).Create(model).Error; err != nil {
		return err
	}

	return nil
}

// GetExecution retrieves a specific execution record
func (s *GormStorage) GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	if executionID == "" {
		return nil, errors.New("execution ID cannot be empty")
	}

	var model ExecutionRecordModel
	err := s.db.WithContext(ctx).Where("execution_id = ?", executionID).First(&model).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrExecutionHistoryNotFound
		}
		return nil, err
	}

	// Convert to ExecutionRecord
	record, err := s.modelToExecutionRecord(&model)
	if err != nil {
		return nil, err
	}

	return record, nil
}

// ListExecutions returns execution records for a job with optional filters
func (s *GormStorage) ListExecutions(ctx context.Context, jobID string, options *QueryOptions) ([]*ExecutionRecord, error) {
	if !s.initialized {
		return nil, ErrStorageNotInitialized
	}

	if jobID == "" {
		return nil, ErrEmptyJobID
	}

	// Build query
	query := s.db.WithContext(ctx).Where("job_id = ?", jobID)

	// Apply filters
	if options != nil {
		if options.StartTime != nil {
			query = query.Where("start_time >= ?", *options.StartTime)
		}
		if options.EndTime != nil {
			query = query.Where("start_time < ?", *options.EndTime)
		}
		if options.Status != nil {
			query = query.Where("status = ?", string(*options.Status))
		}
	}

	// Apply sorting — always ordered so results are deterministic and match
	// the other backends. SortBy must be mapped through the whitelist: it
	// lands verbatim in the ORDER BY clause, so an unlisted value would be a
	// SQL injection vector. Unknown or empty values fall back to start_time.
	sortColumn := "start_time"
	sortDesc := false
	if options != nil {
		if column, ok := executionSortColumns[options.SortBy]; ok {
			sortColumn = column
		}
		sortDesc = options.SortDesc
	}
	if sortDesc {
		query = query.Order(sortColumn + " DESC")
	} else {
		query = query.Order(sortColumn + " ASC")
	}

	// Apply pagination
	if options != nil {
		if options.Offset > 0 {
			query = query.Offset(options.Offset)
		}
		if options.Limit > 0 {
			query = query.Limit(options.Limit)
		}
	}

	var models []ExecutionRecordModel
	err := query.Find(&models).Error
	if err != nil {
		return nil, err
	}

	records := make([]*ExecutionRecord, 0, len(models))
	for _, model := range models {
		record, err := s.modelToExecutionRecord(&model)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}

// DeleteExecutions removes execution records older than the specified time
func (s *GormStorage) DeleteExecutions(ctx context.Context, jobID string, before time.Time) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	if jobID == "" {
		return ErrEmptyJobID
	}

	// Delete executions before the specified time
	err := s.db.WithContext(ctx).
		Where("job_id = ? AND start_time <= ?", jobID, before).
		Delete(&ExecutionRecordModel{}).Error
	if err != nil {
		return err
	}

	return nil
}

// HealthCheck verifies the storage connection is healthy
func (s *GormStorage) HealthCheck(ctx context.Context) error {
	if !s.initialized {
		return ErrStorageNotInitialized
	}

	// Get underlying SQL database
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}

	// Ping the database
	if err := sqlDB.PingContext(ctx); err != nil {
		return err
	}

	return nil
}

// Helper methods for conversion

func (s *GormStorage) jobDataToModel(job *JobData) (*JobDataModel, error) {
	model := &JobDataModel{
		ID:             job.ID,
		ScheduleType:   job.ScheduleType,
		ScheduleConfig: job.ScheduleConfig,
		Status:         string(job.Status),
		NextRun:        job.NextRun,
		LastRun:        job.LastRun,
		CreatedAt:      job.CreatedAt,
		UpdatedAt:      job.UpdatedAt,
	}

	// Encode metadata as JSON
	if job.Metadata != nil {
		metadataJSON, err := json.Marshal(job.Metadata)
		if err != nil {
			return nil, err
		}
		model.Metadata = string(metadataJSON)
	}

	return model, nil
}

func (s *GormStorage) modelToJobData(model *JobDataModel) (*JobData, error) {
	job := &JobData{
		ID:             model.ID,
		ScheduleType:   model.ScheduleType,
		ScheduleConfig: model.ScheduleConfig,
		Status:         JobStatus(model.Status),
		NextRun:        model.NextRun,
		LastRun:        model.LastRun,
		CreatedAt:      model.CreatedAt,
		UpdatedAt:      model.UpdatedAt,
	}

	// Decode metadata from JSON
	if model.Metadata != "" {
		var metadata map[string]string
		if err := json.Unmarshal([]byte(model.Metadata), &metadata); err != nil {
			return nil, err
		}
		job.Metadata = metadata
	} else {
		job.Metadata = make(map[string]string)
	}

	return job, nil
}

func (s *GormStorage) executionRecordToModel(record *ExecutionRecord) (*ExecutionRecordModel, error) {
	model := &ExecutionRecordModel{
		ExecutionID: record.ExecutionID,
		JobID:       record.JobID,
		StartTime:   record.StartTime,
		EndTime:     record.EndTime,
		Duration:    int64(record.Duration),
		Status:      string(record.Status),
		Error:       record.Error,
	}

	// Encode metadata as JSON
	if record.Metadata != nil {
		metadataJSON, err := json.Marshal(record.Metadata)
		if err != nil {
			return nil, err
		}
		model.Metadata = string(metadataJSON)
	}

	return model, nil
}

func (s *GormStorage) modelToExecutionRecord(model *ExecutionRecordModel) (*ExecutionRecord, error) {
	record := &ExecutionRecord{
		ExecutionID: model.ExecutionID,
		JobID:       model.JobID,
		StartTime:   model.StartTime,
		EndTime:     model.EndTime,
		Duration:    time.Duration(model.Duration),
		Status:      JobStatus(model.Status),
		Error:       model.Error,
	}

	// Decode metadata from JSON
	if model.Metadata != "" {
		var metadata map[string]string
		if err := json.Unmarshal([]byte(model.Metadata), &metadata); err != nil {
			return nil, err
		}
		record.Metadata = metadata
	} else {
		record.Metadata = make(map[string]string)
	}

	return record, nil
}
