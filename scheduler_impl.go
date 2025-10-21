package scheduler

import (
	"context"
	"sync"
	"time"
)

// schedulerImpl is the concrete implementation of the Scheduler interface
type schedulerImpl struct {
	mu       sync.RWMutex
	running  bool
	jobs     map[string]*jobImpl
	storage  Storage
	handler  JobHandler
	codec    ScheduleCodec
	ctx      context.Context
	cancel   context.CancelFunc
	stopCh   chan struct{}
	ticker   *time.Ticker
	interval time.Duration
}

// NewScheduler creates a new Scheduler instance
func NewScheduler(storage Storage, handler JobHandler, codec ScheduleCodec) Scheduler {
	return &schedulerImpl{
		jobs:     make(map[string]*jobImpl),
		storage:  storage,
		handler:  handler,
		codec:    codec,
		interval: time.Second, // Default check interval
	}
}

// Start begins the scheduler execution
func (s *schedulerImpl) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrSchedulerAlreadyStarted
	}

	if s.handler == nil {
		return ErrHandlerNotDefined
	}

	// Initialize storage if provided
	if s.storage != nil {
		if err := s.storage.Initialize(ctx); err != nil {
			return err
		}

		// Load jobs from storage
		if err := s.loadJobsFromStorage(ctx); err != nil {
			return err
		}
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopCh = make(chan struct{})
	s.running = true
	s.ticker = time.NewTicker(s.interval)

	// Start scheduler loop
	go s.run()

	return nil
}

// Stop gracefully shuts down the scheduler
func (s *schedulerImpl) Stop(ctx context.Context) error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerNotStarted
	}

	// Signal stop
	if s.cancel != nil {
		s.cancel()
	}

	if s.ticker != nil {
		s.ticker.Stop()
	}

	s.mu.Unlock()

	// Wait for scheduler loop to finish
	<-s.stopCh

	// Wait for all running jobs to complete
	s.waitForJobs()

	s.mu.Lock()
	// Close storage if provided
	if s.storage != nil {
		if err := s.storage.Close(ctx); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	s.running = false
	s.mu.Unlock()
	return nil
}

// AddJob registers a new job with the scheduler
func (s *schedulerImpl) AddJob(id string, schedule Schedule, metadata map[string]string) error {
	if id == "" {
		return ErrEmptyJobID
	}

	if schedule == nil {
		return ErrInvalidInterval
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; exists {
		return ErrJobAlreadyExists
	}

	var scheduleType string
	var scheduleConfig string
	var err error

	if s.codec != nil {
		scheduleType, scheduleConfig, err = s.codec.Encode(schedule)
		if err != nil {
			return err
		}
	}

	now := time.Now()
	job := &jobImpl{
		id:       id,
		schedule: schedule,
		metadata: copyMetadata(metadata),
		nextRun:  schedule.Next(now),
		lastRun:  time.Time{},
		running:  false,
	}

	s.jobs[id] = job

	// Persist to storage if available
	if s.storage != nil && s.running {
		jobData := &JobData{
			ID:             id,
			Status:         JobStatusPending,
			NextRun:        job.nextRun,
			LastRun:        job.lastRun,
			CreatedAt:      now,
			UpdatedAt:      now,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Metadata:       copyMetadata(metadata),
		}

		if err := s.storage.SaveJob(s.ctx, jobData); err != nil {
			delete(s.jobs, id)
			return err
		}
	}

	return nil
}

// RemoveJob removes a job from the scheduler
func (s *schedulerImpl) RemoveJob(id string) error {
	if id == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[id]
	if !exists {
		return ErrJobNotFound
	}

	// Wait for job to complete if running
	for job.IsRunning() {
		s.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		s.mu.Lock()
	}

	delete(s.jobs, id)

	// Delete from storage if available
	if s.storage != nil && s.running {
		if err := s.storage.DeleteJob(s.ctx, id); err != nil {
			return err
		}
	}

	return nil
}

// GetJob retrieves a job by its ID
func (s *schedulerImpl) GetJob(id string) (Job, error) {
	if id == "" {
		return nil, ErrEmptyJobID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[id]
	if !exists {
		return nil, ErrJobNotFound
	}

	return job, nil
}

// ListJobs returns all registered jobs
func (s *schedulerImpl) ListJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}

	return jobs
}

// IsRunning returns whether the scheduler is currently running
func (s *schedulerImpl) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.running
}

// run is the main scheduler loop
func (s *schedulerImpl) run() {
	defer close(s.stopCh)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.ticker.C:
			s.checkAndRunJobs()
		}
	}
}

// checkAndRunJobs checks for jobs that need to run and executes them
func (s *schedulerImpl) checkAndRunJobs() {
	now := time.Now()

	s.mu.RLock()
	jobsToRun := make([]*jobImpl, 0)

	for _, job := range s.jobs {
		if !job.IsRunning() && !job.nextRun.After(now) {
			jobsToRun = append(jobsToRun, job)
		}
	}
	s.mu.RUnlock()

	// Execute jobs outside the lock
	for _, job := range jobsToRun {
		go s.executeJob(job)
	}
}

// executeJob executes a single job
func (s *schedulerImpl) executeJob(job *jobImpl) {
	job.mu.Lock()
	if job.running {
		job.mu.Unlock()
		return
	}
	job.running = true
	job.mu.Unlock()

	startTime := time.Now()
	executionID := job.id + "_" + startTime.Format("20060102150405")
	scheduleType, scheduleConfig, hasSchedule := s.encodeSchedule(job.schedule)

	job.mu.RLock()
	initialNextRun := job.nextRun
	initialMetadata := copyMetadata(job.metadata)
	job.mu.RUnlock()

	// Update storage status to running
	if s.storage != nil {
		jobData := &JobData{
			ID:        job.id,
			Status:    JobStatusRunning,
			NextRun:   initialNextRun,
			LastRun:   startTime,
			Metadata:  initialMetadata,
			UpdatedAt: startTime,
		}
		if hasSchedule {
			jobData.ScheduleType = scheduleType
			jobData.ScheduleConfig = scheduleConfig
		}
		s.storage.UpdateJob(s.ctx, jobData)
	}

	// Execute job function
	var err error
	if s.handler != nil {
		event := newJobEvent(job, initialMetadata)
		err = s.handler(s.ctx, event)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Update job state
	job.mu.Lock()
	job.lastRun = startTime
	job.nextRun = job.schedule.Next(endTime)
	job.running = false
	job.mu.Unlock()

	// Determine final status
	status := JobStatusCompleted
	errorMsg := ""
	if err != nil {
		status = JobStatusFailed
		errorMsg = err.Error()
	}

	// Save execution record
	if s.storage != nil {
		record := &ExecutionRecord{
			JobID:       job.id,
			ExecutionID: executionID,
			StartTime:   startTime,
			EndTime:     endTime,
			Duration:    duration,
			Status:      status,
			Error:       errorMsg,
			Metadata:    copyMetadata(job.metadata),
		}
		s.storage.SaveExecution(s.ctx, record)

		job.mu.RLock()
		updatedNextRun := job.nextRun
		updatedMetadata := copyMetadata(job.metadata)
		lastRun := job.lastRun
		job.mu.RUnlock()

		// Update job status
		jobData := &JobData{
			ID:        job.id,
			Status:    status,
			NextRun:   updatedNextRun,
			LastRun:   lastRun,
			Metadata:  updatedMetadata,
			UpdatedAt: endTime,
		}
		if hasSchedule {
			jobData.ScheduleType = scheduleType
			jobData.ScheduleConfig = scheduleConfig
		}
		s.storage.UpdateJob(s.ctx, jobData)
	}
}

// loadJobsFromStorage loads jobs from storage
func (s *schedulerImpl) loadJobsFromStorage(ctx context.Context) error {
	jobDataList, err := s.storage.ListJobs(ctx)
	if err != nil {
		return err
	}

	// Note: This requires a schedule codec to reconstruct job schedules.
	if s.codec != nil {
		now := time.Now()
		for _, jobData := range jobDataList {
			if _, exists := s.jobs[jobData.ID]; exists {
				continue
			}

			schedule, err := s.codec.Decode(jobData.ScheduleType, jobData.ScheduleConfig)
			if err != nil {
				continue
			}

			nextRun := jobData.NextRun
			if nextRun.IsZero() {
				nextRun = schedule.Next(now)
			}

			job := &jobImpl{
				id:       jobData.ID,
				schedule: schedule,
				metadata: copyMetadata(jobData.Metadata),
				nextRun:  nextRun,
				lastRun:  jobData.LastRun,
				running:  false,
			}

			s.jobs[jobData.ID] = job
		}
	}

	return nil
}

// waitForJobs waits for all running jobs to complete
func (s *schedulerImpl) waitForJobs() {
	for {
		s.mu.RLock()
		hasRunning := false
		for _, job := range s.jobs {
			if job.IsRunning() {
				hasRunning = true
				break
			}
		}
		s.mu.RUnlock()

		if !hasRunning {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// jobImpl is the concrete implementation of the Job interface
type jobImpl struct {
	mu       sync.RWMutex
	id       string
	schedule Schedule
	metadata map[string]string
	nextRun  time.Time
	lastRun  time.Time
	running  bool
}

// ID returns the unique identifier of the job
func (j *jobImpl) ID() string {
	return j.id
}

// NextRun returns the next scheduled run time
func (j *jobImpl) NextRun() time.Time {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return j.nextRun
}

// LastRun returns the last execution time
func (j *jobImpl) LastRun() time.Time {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return j.lastRun
}

// IsRunning returns whether the job is currently executing
func (j *jobImpl) IsRunning() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return j.running
}

// Metadata returns the job metadata
func (j *jobImpl) Metadata() map[string]string {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return copyMetadata(j.metadata)
}

func copyMetadata(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}

	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (s *schedulerImpl) encodeSchedule(schedule Schedule) (string, string, bool) {
	if s.codec == nil || schedule == nil {
		return "", "", false
	}

	scheduleType, scheduleConfig, err := s.codec.Encode(schedule)
	if err != nil {
		return "", "", false
	}

	return scheduleType, scheduleConfig, true
}
