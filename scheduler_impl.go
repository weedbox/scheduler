package scheduler

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

// SchedulerLogger receives structured error events from the ticker-based
// scheduler. Same shape as NATSSchedulerLogger.
type SchedulerLogger func(msg string, keysAndValues ...any)

// SchedulerOption configures a Scheduler created by NewScheduler.
type SchedulerOption func(*schedulerImpl)

// WithCheckInterval sets how often the scheduler polls for due jobs.
// Defaults to one second; the interval bounds how late a job can fire.
func WithCheckInterval(interval time.Duration) SchedulerOption {
	return func(s *schedulerImpl) {
		if interval > 0 {
			s.interval = interval
		}
	}
}

// WithSchedulerLogger installs a logger for background errors (storage
// write failures, handler panics) that previously vanished silently.
func WithSchedulerLogger(logger SchedulerLogger) SchedulerOption {
	return func(s *schedulerImpl) {
		s.logger = logger
	}
}

// schedulerImpl is the concrete implementation of the Scheduler interface
type schedulerImpl struct {
	mu         sync.RWMutex
	running    bool
	jobs       map[string]*jobImpl
	storage    Storage
	handler    JobHandler
	codec      ScheduleCodec
	ctx        context.Context
	cancel     context.CancelFunc
	stopCh     chan struct{}
	startReady chan struct{}
	ticker     *time.Ticker
	interval   time.Duration
	logger     SchedulerLogger
}

// NewScheduler creates a new Scheduler instance.
//
// This ticker-based scheduler has no cross-instance coordination: multiple
// instances sharing the same storage (e.g. one GORM database) will each fire
// every job independently. For multi-instance deployments use
// NewNATSScheduler, or ensure only one instance runs (leader election is the
// caller's responsibility).
func NewScheduler(storage Storage, handler JobHandler, codec ScheduleCodec, opts ...SchedulerOption) Scheduler {
	s := &schedulerImpl{
		jobs:       make(map[string]*jobImpl),
		storage:    storage,
		handler:    handler,
		codec:      codec,
		interval:   time.Second, // Default check interval
		startReady: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// logError forwards a structured error event to the configured logger.
// Safe to call from any goroutine; no-op when no logger is installed.
func (s *schedulerImpl) logError(msg string, keysAndValues ...any) {
	if s.logger == nil {
		return
	}
	defer func() {
		// A panicking logger must not crash a scheduler goroutine.
		_ = recover()
	}()
	s.logger(msg, keysAndValues...)
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

		// Jobs added via AddJob before Start existed only in memory: AddJob
		// skips the storage write when the scheduler is not yet running.
		// Persist them now, or a restart would silently lose them.
		if err := s.persistPreStartJobs(ctx); err != nil {
			return err
		}
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopCh = make(chan struct{})
	s.running = true
	s.ticker = time.NewTicker(s.interval)

	ready := s.startReady
	// Start scheduler loop
	go s.run()

	close(ready)

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

	// Wait for scheduler loop to finish, bounded by the caller's ctx.
	var stopErr error
	select {
	case <-s.stopCh:
		// Wait for all running jobs to complete
		stopErr = s.waitForJobs(ctx)
	case <-ctx.Done():
		stopErr = ctx.Err()
	}

	s.mu.Lock()
	// Close storage if provided
	if s.storage != nil {
		if err := s.storage.Close(ctx); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	s.running = false
	s.startReady = make(chan struct{})
	s.mu.Unlock()
	return stopErr
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
	nextRun := schedule.Next(now)
	if onceSchedule, ok := schedule.(*OnceSchedule); ok {
		runAt := onceSchedule.RunAt()
		if runAt.After(now) {
			nextRun = runAt
		} else {
			nextRun = now
		}
	}
	job := &jobImpl{
		id:        id,
		schedule:  schedule,
		metadata:  copyMetadata(metadata),
		nextRun:   nextRun,
		lastRun:   time.Time{},
		createdAt: now,
		running:   false,
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

// UpdateJobSchedule replaces the schedule of an existing job.
func (s *schedulerImpl) UpdateJobSchedule(id string, schedule Schedule) error {
	if id == "" {
		return ErrEmptyJobID
	}

	if schedule == nil {
		return ErrInvalidInterval
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
	nextRun := schedule.Next(now)
	if onceSchedule, ok := schedule.(*OnceSchedule); ok {
		runAt := onceSchedule.RunAt()
		if runAt.After(now) {
			nextRun = runAt
		} else {
			nextRun = now
		}
	}

	s.mu.Lock()
	job, exists := s.jobs[id]
	if !exists {
		s.mu.Unlock()
		return ErrJobNotFound
	}

	for job.IsRunning() {
		s.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		s.mu.Lock()
		job, exists = s.jobs[id]
		if !exists {
			s.mu.Unlock()
			return ErrJobNotFound
		}
	}

	var (
		oldSchedule = job.schedule
		oldNextRun  = job.nextRun
	)

	job.mu.Lock()
	job.schedule = schedule
	job.nextRun = nextRun
	metadata := copyMetadata(job.metadata)
	lastRun := job.lastRun
	job.mu.Unlock()
	s.mu.Unlock()

	if s.storage != nil && s.running {
		jobData := &JobData{
			ID:             id,
			Status:         JobStatusPending,
			NextRun:        nextRun,
			LastRun:        lastRun,
			Metadata:       metadata,
			UpdatedAt:      now,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
		}

		if err := s.storage.UpdateJob(s.ctx, jobData); err != nil {
			job.mu.Lock()
			job.schedule = oldSchedule
			job.nextRun = oldNextRun
			job.mu.Unlock()
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

// WaitUntilRunning blocks until the scheduler has completed its Start sequence or the context is done.
func (s *schedulerImpl) WaitUntilRunning(ctx context.Context) error {
	s.mu.RLock()
	if s.running {
		s.mu.RUnlock()
		return nil
	}
	startReady := s.startReady
	s.mu.RUnlock()

	if startReady == nil {
		return ErrSchedulerNotStarted
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-startReady:
		return nil
	}
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
	// Panic recovery: ensure job.running is reset so the job won't be stuck
	// forever. The schedule must also be advanced — a panicking handler skips
	// the normal lastRun/nextRun update below, and leaving nextRun in the
	// past would make the ticker re-fire the job every check interval in a
	// tight loop.
	defer func() {
		if r := recover(); r != nil {
			s.logError("panic during job execution", "job_id", job.id, "panic", r)
			now := time.Now()
			job.mu.Lock()
			job.lastRun = now
			job.nextRun = job.schedule.Next(now)
			job.running = false
			job.mu.Unlock()
		}
	}()

	job.mu.Lock()
	if job.running {
		job.mu.Unlock()
		return
	}
	job.running = true
	job.mu.Unlock()

	startTime := time.Now()
	// Nanosecond precision: a second-resolution timestamp would collide for
	// jobs firing more than once per second, silently overwriting execution
	// records.
	executionID := job.id + "_" + strconv.FormatInt(startTime.UnixNano(), 10)
	scheduleType, scheduleConfig, hasSchedule := s.encodeSchedule(job.schedule)

	job.mu.RLock()
	initialNextRun := job.nextRun
	initialMetadata := copyMetadata(job.metadata)
	lastCompleted := job.lastRun
	currentSchedule := job.schedule
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
		if err := s.storage.UpdateJob(s.ctx, jobData); err != nil {
			s.logError("failed to persist running status", "job_id", job.id, "err", err)
		}
	}

	// Execute job function
	var err error
	if s.handler != nil {
		event := newJobEvent(job, initialMetadata, currentSchedule, initialNextRun, startTime, lastCompleted)
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
		if err := s.storage.SaveExecution(s.ctx, record); err != nil {
			s.logError("failed to persist execution record",
				"job_id", job.id, "execution_id", executionID, "err", err)
		}

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
		if err := s.storage.UpdateJob(s.ctx, jobData); err != nil {
			s.logError("failed to persist job state", "job_id", job.id, "err", err)
		}
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
				id:        jobData.ID,
				schedule:  schedule,
				metadata:  copyMetadata(jobData.Metadata),
				nextRun:   nextRun,
				lastRun:   jobData.LastRun,
				createdAt: jobData.CreatedAt,
				running:   false,
			}

			s.jobs[jobData.ID] = job
		}
	}

	return nil
}

// persistPreStartJobs saves jobs registered via AddJob before Start() to
// storage. Called from Start with s.mu held, after loadJobsFromStorage has
// merged persisted jobs into s.jobs; any job already present in storage is
// left untouched (the in-memory copy shadows it for this process).
func (s *schedulerImpl) persistPreStartJobs(ctx context.Context) error {
	now := time.Now()
	for id, job := range s.jobs {
		if _, err := s.storage.GetJob(ctx, id); err == nil {
			continue
		} else if !errors.Is(err, ErrJobDataNotFound) {
			return err
		}

		var scheduleType, scheduleConfig string
		if s.codec != nil {
			var err error
			scheduleType, scheduleConfig, err = s.codec.Encode(job.schedule)
			if err != nil {
				return err
			}
		}

		jobData := &JobData{
			ID:             id,
			Status:         JobStatusPending,
			NextRun:        job.nextRun,
			LastRun:        job.lastRun,
			CreatedAt:      job.createdAt,
			UpdatedAt:      now,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Metadata:       copyMetadata(job.metadata),
		}
		if err := s.storage.SaveJob(ctx, jobData); err != nil && !errors.Is(err, ErrJobDataAlreadyExists) {
			return err
		}
	}
	return nil
}

// waitForJobs waits for all running jobs to complete, bounded by ctx.
func (s *schedulerImpl) waitForJobs(ctx context.Context) error {
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
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// jobImpl is the concrete implementation of the Job interface
type jobImpl struct {
	mu        sync.RWMutex
	id        string
	schedule  Schedule
	metadata  map[string]string
	nextRun   time.Time
	lastRun   time.Time
	createdAt time.Time
	running   bool
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
