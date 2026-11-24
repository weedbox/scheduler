package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"
)

const (
	// defaultPostgresPollInterval bounds how long an idle instance waits
	// before re-checking the jobs table for due work. Cross-instance
	// visibility of AddJob/UpdateJobSchedule is bounded by this interval.
	defaultPostgresPollInterval = 1 * time.Second

	// defaultPostgresLeaseDuration is how long a claimed job stays owned by
	// an instance without a heartbeat. A crashed instance's jobs become
	// claimable again once the lease expires. Aligned with the NATS
	// scheduler's reconciler running-TTL default.
	defaultPostgresLeaseDuration = 5 * time.Minute

	// defaultPostgresMaxConcurrentExecutions caps in-flight handlers per
	// instance. The slot is reserved before claiming, so surplus due jobs
	// stay claimable by other instances instead of queueing locally.
	defaultPostgresMaxConcurrentExecutions = 32

	// postgresCompletionRetryAttempts bounds write-back retries after the
	// handler finishes. If all attempts fail the row stays running until
	// the lease expires, at which point another instance re-claims it.
	postgresCompletionRetryAttempts = 3
	postgresCompletionRetryBackoff  = 200 * time.Millisecond

	// minPostgresClaimWait is the floor for idle sleeps so a claim race
	// between instances cannot degenerate into a busy loop.
	minPostgresClaimWait = 20 * time.Millisecond

	// minPostgresHeartbeatInterval is the floor for lease heartbeats.
	minPostgresHeartbeatInterval = 1 * time.Second

	// postgresHeartbeatTimeout bounds each lease-extension round-trip so a
	// hung connection cannot block the heartbeat goroutine (and with it the
	// executor's shutdown drain) indefinitely.
	postgresHeartbeatTimeout = 10 * time.Second
)

// postgresJobModel mirrors JobDataModel on the same scheduler_jobs table and
// adds the claim-coordination columns. AutoMigrate only adds the two extra
// columns, so a table created by GormStorage upgrades in place and single-node
// GORM deployments can migrate to multi-instance without moving data.
type postgresJobModel struct {
	ID             string    `gorm:"primaryKey;size:255"`
	ScheduleType   string    `gorm:"size:50"`
	ScheduleConfig string    `gorm:"type:text"`
	Status         string    `gorm:"size:20;index"`
	NextRun        time.Time `gorm:"index"`
	LastRun        time.Time
	CreatedAt      time.Time `gorm:"index"`
	UpdatedAt      time.Time `gorm:"index"`
	Metadata       string    `gorm:"type:text"` // JSON-encoded map[string]string

	// LeaseExpiresAt is set while an instance owns the job. An expired
	// lease means the owner crashed or stalled; the job is claimable again.
	LeaseExpiresAt *time.Time `gorm:"index"`

	// ClaimSeq is a fencing token incremented by every claim and every
	// UpdateJobSchedule. Heartbeats and completion write-backs are guarded
	// by it, so a stale owner can never clobber a newer claim or update.
	// Because ClaimSeq restarts at zero when a job is deleted and re-created
	// under the same ID, the guards also match on CreatedAt, which is unique
	// per row incarnation — closing the ABA window.
	ClaimSeq int64 `gorm:"not null;default:0"`
}

// TableName shares the table with JobDataModel (GormStorage).
func (postgresJobModel) TableName() string {
	return JobDataModel{}.TableName()
}

// PostgresSchedulerOption customizes the PostgreSQL-backed scheduler.
type PostgresSchedulerOption func(*postgresSchedulerImpl)

// WithPostgresPollInterval sets the idle poll interval (default 1s). It
// bounds the latency for picking up jobs added or updated by other instances.
func WithPostgresPollInterval(interval time.Duration) PostgresSchedulerOption {
	return func(s *postgresSchedulerImpl) {
		if interval > 0 {
			s.pollInterval = interval
		}
	}
}

// WithPostgresLeaseDuration sets how long a claim stays valid without a
// heartbeat (default 5m). Handlers are protected by automatic heartbeats
// while they run, so this mainly controls how quickly the jobs of a crashed
// instance are taken over — and therefore also the double-execution window:
// a job re-claimed after a lease expiry may run again even if the original
// handler eventually finished. Keep handlers idempotent.
func WithPostgresLeaseDuration(d time.Duration) PostgresSchedulerOption {
	return func(s *postgresSchedulerImpl) {
		if d > 0 {
			s.leaseDuration = d
		}
	}
}

// WithPostgresMaxConcurrentExecutions caps concurrently running handlers per
// instance (default 32). Slots are reserved before claiming, so an instance
// at capacity leaves due jobs for its peers instead of hoarding them.
func WithPostgresMaxConcurrentExecutions(n int) PostgresSchedulerOption {
	return func(s *postgresSchedulerImpl) {
		if n > 0 {
			s.maxConcurrent = n
		}
	}
}

// WithPostgresSchedulerLogger installs a logger for background errors that
// have no caller to return to (claim failures, lost leases, write-back
// failures). Same shape as SchedulerLogger for the ticker scheduler.
func WithPostgresSchedulerLogger(logger SchedulerLogger) PostgresSchedulerOption {
	return func(s *postgresSchedulerImpl) {
		s.logger = logger
	}
}

// WithPostgresExecRecordTTL prunes execution records older than ttl after
// each execution (default 0 = keep forever, matching GormStorage behavior).
func WithPostgresExecRecordTTL(ttl time.Duration) PostgresSchedulerOption {
	return func(s *postgresSchedulerImpl) {
		if ttl > 0 {
			s.execRecordTTL = ttl
		}
	}
}

// postgresSchedulerImpl is a multi-instance scheduler backed by a shared
// PostgreSQL database. Any number of instances point at the same database;
// due jobs are claimed with a single FOR UPDATE SKIP LOCKED round-trip, so
// exactly one instance runs each tick without leader election. Claims are
// leased and heartbeated; jobs owned by a crashed instance become claimable
// again when the lease expires. Delivery is at-least-once (a lease takeover
// can re-run a tick whose completion was never written), so handlers should
// be idempotent — the same contract as the NATS scheduler.
//
// The database clock is the single time authority: due-time comparisons and
// lease arithmetic run on now() inside the claim SQL, and next_run values
// are computed from a fetched database timestamp, so clock skew between
// instances cannot shift or duplicate ticks.
type postgresSchedulerImpl struct {
	mu      sync.RWMutex
	db      *gorm.DB
	storage *GormStorage
	handler JobHandler
	codec   ScheduleCodec
	logger  SchedulerLogger

	pollInterval  time.Duration
	leaseDuration time.Duration
	maxConcurrent int
	execRecordTTL time.Duration

	running    bool
	ctx        context.Context
	cancel     context.CancelFunc
	stopCh     chan struct{}
	wakeCh     chan struct{}
	startReady chan struct{}
	execWG     sync.WaitGroup
	execSem    chan struct{}

	// preStartJobs holds jobs registered via AddJob before Start; they are
	// persisted during Start, mirroring the other scheduler implementations.
	preStartJobs map[string]*jobImpl
}

// NewPostgresScheduler creates a scheduler that coordinates multiple
// instances through a shared PostgreSQL database using FOR UPDATE SKIP LOCKED
// claims (PostgreSQL 9.5+). It reuses the scheduler_jobs/scheduler_executions
// tables of GormStorage, adding lease_expires_at and claim_seq columns on
// Start, so an existing single-node GORM deployment can switch backends
// without a data migration.
//
// The db handle must point at PostgreSQL; the claim query uses PostgreSQL
// syntax and will fail on SQLite or MySQL. The scheduler never closes the
// injected *gorm.DB.
//
// If codec is nil, NewBasicScheduleCodec() is used.
func NewPostgresScheduler(db *gorm.DB, handler JobHandler, codec ScheduleCodec, opts ...PostgresSchedulerOption) Scheduler {
	if codec == nil {
		codec = NewBasicScheduleCodec()
	}

	s := &postgresSchedulerImpl{
		db:            db,
		storage:       NewGormStorage(db),
		handler:       handler,
		codec:         codec,
		pollInterval:  defaultPostgresPollInterval,
		leaseDuration: defaultPostgresLeaseDuration,
		maxConcurrent: defaultPostgresMaxConcurrentExecutions,
		wakeCh:        make(chan struct{}, 1),
		startReady:    make(chan struct{}),
		preStartJobs:  make(map[string]*jobImpl),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// dbNow returns the database clock, the single time authority shared by all
// instances. Using it instead of time.Now() keeps schedules immune to clock
// skew between application hosts.
func (s *postgresSchedulerImpl) dbNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := s.db.WithContext(ctx).Raw("SELECT now()").Row().Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

// logError forwards a structured error event to the configured logger.
// A panicking logger must not take down the scheduler.
func (s *postgresSchedulerImpl) logError(msg string, keysAndValues ...any) {
	if s.logger == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	s.logger(msg, keysAndValues...)
}

// Start begins the scheduler execution
func (s *postgresSchedulerImpl) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrSchedulerAlreadyStarted
	}

	if s.handler == nil {
		return ErrHandlerNotDefined
	}

	if s.db == nil {
		return ErrStorageNotInitialized
	}

	if err := s.storage.Initialize(ctx); err != nil {
		return err
	}

	// Add the claim-coordination columns to the shared jobs table.
	if err := autoMigrate(ctx, s.db, &postgresJobModel{}); err != nil {
		return err
	}

	if err := s.persistPreStartJobs(ctx); err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopCh = make(chan struct{})
	s.execSem = make(chan struct{}, s.maxConcurrent)
	s.running = true

	ready := s.startReady
	go s.run()
	close(ready)

	return nil
}

// persistPreStartJobs saves jobs registered via AddJob before Start. Called
// with s.mu held. A job already present in the shared table (e.g. created by
// another instance) is left untouched.
func (s *postgresSchedulerImpl) persistPreStartJobs(ctx context.Context) error {
	now := time.Now()
	// Recompute next_run against the database clock so pre-start jobs are
	// due relative to the shared time authority, not this host's clock.
	dbNow, err := s.dbNow(ctx)
	if err != nil {
		s.logError("failed to fetch database time; using local clock", "err", err)
		dbNow = now
	}
	for id, job := range s.preStartJobs {
		scheduleType, scheduleConfig, err := s.codec.Encode(job.schedule)
		if err != nil {
			return err
		}

		jobData := &JobData{
			ID:             id,
			Status:         JobStatusPending,
			NextRun:        initialNextRun(job.schedule, dbNow),
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
	s.preStartJobs = make(map[string]*jobImpl)
	return nil
}

// Stop gracefully shuts down the scheduler
func (s *postgresSchedulerImpl) Stop(ctx context.Context) error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerNotStarted
	}

	if s.cancel != nil {
		s.cancel()
	}
	stopCh := s.stopCh
	s.mu.Unlock()

	// Wait for the claim loop to exit, bounded by the caller's ctx.
	var stopErr error
	select {
	case <-stopCh:
		stopErr = s.waitForExecutions(ctx)
	case <-ctx.Done():
		stopErr = ctx.Err()
	}

	s.mu.Lock()
	if err := s.storage.Close(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	s.running = false
	s.startReady = make(chan struct{})
	s.mu.Unlock()

	return stopErr
}

// waitForExecutions waits for all in-flight handlers to complete, bounded by ctx.
func (s *postgresSchedulerImpl) waitForExecutions(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.execWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsRunning returns whether the scheduler is currently running
func (s *postgresSchedulerImpl) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.running
}

// WaitUntilRunning blocks until the scheduler has completed its Start sequence or the context is done.
func (s *postgresSchedulerImpl) WaitUntilRunning(ctx context.Context) error {
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

// AddJob registers a new job with the scheduler
func (s *postgresSchedulerImpl) AddJob(id string, schedule Schedule, metadata map[string]string) error {
	if id == "" {
		return ErrEmptyJobID
	}

	if schedule == nil {
		return ErrInvalidInterval
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	nextRun := initialNextRun(schedule, now)

	if !s.running {
		if _, exists := s.preStartJobs[id]; exists {
			return ErrJobAlreadyExists
		}
		s.preStartJobs[id] = &jobImpl{
			id:        id,
			schedule:  schedule,
			metadata:  copyMetadata(metadata),
			nextRun:   nextRun,
			createdAt: now,
		}
		return nil
	}

	scheduleType, scheduleConfig, err := s.codec.Encode(schedule)
	if err != nil {
		return err
	}

	// Recompute against the database clock so the first fire is not
	// shifted by local clock skew.
	if dbNow, err := s.dbNow(s.ctx); err == nil {
		nextRun = initialNextRun(schedule, dbNow)
	} else {
		s.logError("failed to fetch database time; using local clock", "job_id", id, "err", err)
	}

	jobData := &JobData{
		ID:             id,
		Status:         JobStatusPending,
		NextRun:        nextRun,
		CreatedAt:      now,
		UpdatedAt:      now,
		ScheduleType:   scheduleType,
		ScheduleConfig: scheduleConfig,
		Metadata:       copyMetadata(metadata),
	}
	if err := s.storage.SaveJob(s.ctx, jobData); err != nil {
		if errors.Is(err, ErrJobDataAlreadyExists) {
			return ErrJobAlreadyExists
		}
		return err
	}

	s.wake()
	return nil
}

// UpdateJobSchedule replaces the schedule of an existing job. The update
// bumps the claim fencing token, so an instance currently executing the job
// under the old schedule cannot advance the chain on completion — the update
// owns the next tick, mirroring the NATS scheduler's merge semantics.
func (s *postgresSchedulerImpl) UpdateJobSchedule(id string, schedule Schedule) error {
	if id == "" {
		return ErrEmptyJobID
	}

	if schedule == nil {
		return ErrInvalidInterval
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	nextRun := initialNextRun(schedule, now)

	if !s.running {
		job, exists := s.preStartJobs[id]
		if !exists {
			return ErrJobNotFound
		}
		job.mu.Lock()
		job.schedule = schedule
		job.nextRun = nextRun
		job.mu.Unlock()
		return nil
	}

	scheduleType, scheduleConfig, err := s.codec.Encode(schedule)
	if err != nil {
		return err
	}

	// Recompute against the database clock so the rescheduled fire is not
	// shifted by local clock skew.
	if dbNow, err := s.dbNow(s.ctx); err == nil {
		nextRun = initialNextRun(schedule, dbNow)
	} else {
		s.logError("failed to fetch database time; using local clock", "job_id", id, "err", err)
	}

	res := s.db.WithContext(s.ctx).Exec(fmt.Sprintf(`
		UPDATE %s SET
			schedule_type = ?,
			schedule_config = ?,
			next_run = ?,
			status = ?,
			lease_expires_at = NULL,
			claim_seq = claim_seq + 1,
			updated_at = now()
		WHERE id = ?`, postgresJobModel{}.TableName()),
		scheduleType, scheduleConfig, nextRun.UTC(), string(JobStatusPending), id)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrJobNotFound
	}

	s.wake()
	return nil
}

// RemoveJob removes a job from the scheduler
func (s *postgresSchedulerImpl) RemoveJob(id string) error {
	if id == "" {
		return ErrEmptyJobID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		if _, exists := s.preStartJobs[id]; !exists {
			return ErrJobNotFound
		}
		delete(s.preStartJobs, id)
		return nil
	}

	if err := s.storage.DeleteJob(s.ctx, id); err != nil {
		if errors.Is(err, ErrJobDataNotFound) {
			return ErrJobNotFound
		}
		return err
	}
	return nil
}

// GetJob retrieves a job by its ID
func (s *postgresSchedulerImpl) GetJob(id string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.running {
		job, exists := s.preStartJobs[id]
		if !exists {
			return nil, ErrJobNotFound
		}
		return job, nil
	}

	jobData, err := s.storage.GetJob(s.ctx, id)
	if err != nil {
		if errors.Is(err, ErrJobDataNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}

	return s.jobFromData(jobData), nil
}

// HasJob reports whether a job with this ID exists in shared storage.
//
// Unlike the NATS backend, this one's GetJob already reads storage rather than
// a cache, so HasJob is the same question minus the decode. Before Start there
// is no shared storage yet and the pre-start set is the only thing to answer
// from.
func (s *postgresSchedulerImpl) HasJob(id string) (bool, error) {
	if id == "" {
		return false, ErrEmptyJobID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.running {
		_, exists := s.preStartJobs[id]
		return exists, nil
	}

	if _, err := s.storage.GetJob(s.ctx, id); err != nil {
		if errors.Is(err, ErrJobDataNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ListJobs returns all registered jobs
func (s *postgresSchedulerImpl) ListJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.running {
		jobs := make([]Job, 0, len(s.preStartJobs))
		for _, job := range s.preStartJobs {
			jobs = append(jobs, job)
		}
		return jobs
	}

	jobDataList, err := s.storage.ListJobs(s.ctx)
	if err != nil {
		s.logError("failed to list jobs", "err", err)
		return nil
	}

	jobs := make([]Job, 0, len(jobDataList))
	for _, jobData := range jobDataList {
		jobs = append(jobs, s.jobFromData(jobData))
	}
	return jobs
}

// jobFromData builds a point-in-time Job snapshot from a persisted row.
func (s *postgresSchedulerImpl) jobFromData(jobData *JobData) Job {
	schedule, err := s.codec.Decode(jobData.ScheduleType, jobData.ScheduleConfig)
	if err != nil {
		schedule = nil
	}
	return &jobImpl{
		id:        jobData.ID,
		schedule:  schedule,
		metadata:  copyMetadata(jobData.Metadata),
		nextRun:   jobData.NextRun,
		lastRun:   jobData.LastRun,
		createdAt: jobData.CreatedAt,
		running:   jobData.Status == JobStatusRunning,
	}
}

// wake nudges the claim loop out of its idle sleep after a local mutation.
// Remote mutations are picked up within pollInterval.
func (s *postgresSchedulerImpl) wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// run is the claim loop: reserve an execution slot, claim one due job, and
// dispatch it. When nothing is claimable it sleeps until the earliest
// next_run, bounded by pollInterval.
func (s *postgresSchedulerImpl) run() {
	defer close(s.stopCh)

	for {
		select {
		case s.execSem <- struct{}{}:
		case <-s.ctx.Done():
			return
		}

		claimed, err := s.claimDueJob()
		if err != nil {
			<-s.execSem
			if s.ctx.Err() != nil {
				return
			}
			s.logError("failed to claim due job", "err", err)
			if !s.sleep(s.pollInterval) {
				return
			}
			continue
		}

		if claimed == nil {
			<-s.execSem
			if !s.sleep(s.nextWait()) {
				return
			}
			continue
		}

		s.execWG.Add(1)
		go func(m *postgresJobModel) {
			defer s.execWG.Done()
			defer func() { <-s.execSem }()
			s.executeClaimedJob(m)
		}(claimed)
	}
}

// claimDueJob atomically claims one due job with a single round-trip. SKIP
// LOCKED lets concurrent instances claim disjoint jobs without contention;
// an expired lease on a running row means its owner crashed, so the row is
// claimable again (takeover).
func (s *postgresSchedulerImpl) claimDueJob() (*postgresJobModel, error) {
	tbl := postgresJobModel{}.TableName()

	// All time comparisons and the lease arithmetic run on the database
	// clock (now()), so instance clock skew cannot affect claiming.
	var m postgresJobModel
	res := s.db.WithContext(s.ctx).Raw(fmt.Sprintf(`
		UPDATE %s SET
			status = ?,
			lease_expires_at = now() + (? * interval '1 millisecond'),
			claim_seq = claim_seq + 1,
			updated_at = now()
		WHERE id = (
			SELECT id FROM %s
			WHERE next_run <= now()
			  AND status <> ?
			  AND (status <> ? OR lease_expires_at IS NULL OR lease_expires_at <= now())
			ORDER BY next_run
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING *`, tbl, tbl),
		string(JobStatusRunning), s.leaseDuration.Milliseconds(),
		string(JobStatusDisabled), string(JobStatusRunning),
	).Scan(&m)

	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, nil
	}
	return &m, nil
}

// nextWait returns how long to sleep when nothing was claimable: until the
// earliest claimable next_run, bounded by pollInterval (which also bounds the
// latency of noticing jobs created by other instances and lease expirations).
func (s *postgresSchedulerImpl) nextWait() time.Duration {
	// Fetch now() alongside next_run so the wait is a difference of two
	// database timestamps, independent of the local clock.
	var next, dbNow time.Time
	row := s.db.WithContext(s.ctx).Raw(fmt.Sprintf(`
		SELECT next_run, now() FROM %s
		WHERE status <> ?
		  AND (status <> ? OR lease_expires_at IS NULL OR lease_expires_at <= now())
		ORDER BY next_run
		LIMIT 1`, postgresJobModel{}.TableName()),
		string(JobStatusDisabled), string(JobStatusRunning),
	).Row()
	if err := row.Scan(&next, &dbNow); err != nil {
		// Includes sql.ErrNoRows (empty table): just poll.
		return s.pollInterval
	}

	wait := next.Sub(dbNow)
	if wait > s.pollInterval {
		wait = s.pollInterval
	}
	return wait
}

// sleep waits for d, the next wake signal, or shutdown. Returns false when
// the scheduler is stopping.
func (s *postgresSchedulerImpl) sleep(d time.Duration) bool {
	if d < minPostgresClaimWait {
		d = minPostgresClaimWait
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-s.ctx.Done():
		return false
	case <-s.wakeCh:
		return true
	case <-timer.C:
		return true
	}
}

// executeClaimedJob runs the handler for a claimed job and writes the result
// back, guarded by the fencing token captured at claim time.
func (s *postgresSchedulerImpl) executeClaimedJob(m *postgresJobModel) {
	schedule, err := s.codec.Decode(m.ScheduleType, m.ScheduleConfig)
	if err != nil {
		s.logError("failed to decode schedule for claimed job; parking it",
			"job_id", m.ID, "schedule_type", m.ScheduleType, "err", err)
		s.parkUndecodableJob(m)
		return
	}

	scheduledAt := m.NextRun
	startedAt := time.Now()
	metadata := decodePostgresMetadata(m.Metadata)

	snapshot := &jobImpl{
		id:        m.ID,
		schedule:  schedule,
		metadata:  copyMetadata(metadata),
		nextRun:   m.NextRun,
		lastRun:   m.LastRun,
		createdAt: m.CreatedAt,
		running:   true,
	}

	// Heartbeat keeps the lease alive for handlers that outlive it.
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go s.heartbeat(m, hbStop, hbDone)

	event := newJobEvent(snapshot, metadata, schedule, scheduledAt, startedAt, m.LastRun)

	var execErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("panic during job execution: %v", r)
				s.logError("panic during job execution", "job_id", m.ID, "panic", r)
			}
		}()
		execErr = s.handler(s.ctx, event)
	}()

	close(hbStop)
	<-hbDone

	endTime := time.Now()
	status := JobStatusCompleted
	errMsg := ""
	if execErr != nil {
		status = JobStatusFailed
		errMsg = execErr.Error()
	}

	// The write-back must survive Stop() cancelling s.ctx while the handler
	// was finishing, so it runs on a fresh bounded context.
	postCtx, cancel := context.WithTimeout(context.Background(), defaultPostHandlerTimeout)
	defer cancel()

	// Advance from completion time: ticks missed while every instance was
	// down collapse into the single execution above (same catch-up policy
	// as the NATS reconciler). Next() is computed from the database clock;
	// if it cannot be fetched, the local clock is close enough as fallback.
	completedAt, err := s.dbNow(postCtx)
	if err != nil {
		s.logError("failed to fetch database time; falling back to local clock",
			"job_id", m.ID, "err", err)
		completedAt = endTime
	}
	s.completeClaimedJob(postCtx, m, status, schedule.Next(completedAt), startedAt)

	record := &ExecutionRecord{
		JobID:       m.ID,
		ExecutionID: fmt.Sprintf("%s_%d", m.ID, startedAt.UnixNano()),
		StartTime:   startedAt,
		EndTime:     endTime,
		Duration:    endTime.Sub(startedAt),
		Status:      status,
		Error:       errMsg,
		Metadata:    metadata,
	}
	if err := s.storage.SaveExecution(postCtx, record); err != nil {
		s.logError("failed to persist execution record",
			"job_id", m.ID, "execution_id", record.ExecutionID, "err", err)
	}

	if s.execRecordTTL > 0 {
		if err := s.storage.DeleteExecutions(postCtx, m.ID, endTime.Add(-s.execRecordTTL)); err != nil {
			s.logError("failed to prune execution records", "job_id", m.ID, "err", err)
		}
	}
}

// heartbeat periodically extends the lease while the handler runs, guarded by
// the fencing token and the row's created_at (see completeClaimedJob). A
// failed guard means the claim was superseded (lease takeover, schedule
// update, removal, or removal plus re-creation); the handler keeps running
// but its completion write-back will be discarded by the same guard.
func (s *postgresSchedulerImpl) heartbeat(m *postgresJobModel, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	interval := s.leaseDuration / 3
	if interval < minPostgresHeartbeatInterval {
		interval = minPostgresHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			res := s.extendLease(m)
			if res.Error != nil {
				s.logError("failed to extend lease", "job_id", m.ID, "err", res.Error)
				continue
			}
			if res.RowsAffected == 0 {
				s.logError("lease lost during execution; another instance may re-run this job",
					"job_id", m.ID)
				return
			}
		}
	}
}

// extendLease performs one bounded lease-extension round-trip.
func (s *postgresSchedulerImpl) extendLease(m *postgresJobModel) *gorm.DB {
	ctx, cancel := context.WithTimeout(context.Background(), postgresHeartbeatTimeout)
	defer cancel()

	return s.db.WithContext(ctx).Exec(fmt.Sprintf(`
		UPDATE %s SET lease_expires_at = now() + (? * interval '1 millisecond'), updated_at = now()
		WHERE id = ? AND status = ? AND claim_seq = ? AND created_at = ?`, postgresJobModel{}.TableName()),
		s.leaseDuration.Milliseconds(), m.ID, string(JobStatusRunning), m.ClaimSeq, m.CreatedAt)
}

// completeClaimedJob writes the execution outcome and the advanced next_run,
// guarded by the fencing token. RowsAffected == 0 means the claim was
// superseded while the handler ran — the job was removed (must not be
// resurrected), its schedule was updated (the updater owns the next tick), or
// the lease expired and another instance took over — so the result is
// discarded. The guard also matches created_at: claim_seq restarts at zero
// when a job is deleted and re-created under the same ID, so the sequence
// alone could collide with a stale owner's token (ABA); created_at is unique
// per row incarnation and comes from RETURNING * at claim time, so comparing
// it against the stored value is exact. If every attempt errors, the row
// stays running until its lease expires and another instance re-claims it
// (at-least-once).
func (s *postgresSchedulerImpl) completeClaimedJob(ctx context.Context, m *postgresJobModel, status JobStatus, nextRun, lastRun time.Time) {
	var lastErr error
	for attempt := 0; attempt < postgresCompletionRetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(postgresCompletionRetryBackoff)
		}

		res := s.db.WithContext(ctx).Exec(fmt.Sprintf(`
			UPDATE %s SET
				status = ?,
				next_run = ?,
				last_run = ?,
				lease_expires_at = NULL,
				updated_at = now()
			WHERE id = ? AND status = ? AND claim_seq = ? AND created_at = ?`, postgresJobModel{}.TableName()),
			string(status), nextRun.UTC(), lastRun.UTC(),
			m.ID, string(JobStatusRunning), m.ClaimSeq, m.CreatedAt)
		if res.Error != nil {
			lastErr = res.Error
			continue
		}
		if res.RowsAffected == 0 {
			s.logError("claim superseded during execution; discarding write-back",
				"job_id", m.ID, "claim_seq", m.ClaimSeq)
		}
		return
	}

	s.logError("failed to write back job result; lease expiry will recover it",
		"job_id", m.ID, "err", lastErr)
}

// parkUndecodableJob marks a job whose schedule cannot be decoded as failed
// and pushes next_run far into the future so it is not re-claimed in a hot
// loop. A subsequent UpdateJobSchedule resets next_run and revives it.
func (s *postgresSchedulerImpl) parkUndecodableJob(m *postgresJobModel) {
	// At the far-future scale, clock skew is irrelevant; local time is fine.
	res := s.db.Exec(fmt.Sprintf(`
		UPDATE %s SET
			status = ?,
			next_run = ?,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = ? AND status = ? AND claim_seq = ? AND created_at = ?`, postgresJobModel{}.TableName()),
		string(JobStatusFailed), farFutureTime(time.Now().UTC()),
		m.ID, string(JobStatusRunning), m.ClaimSeq, m.CreatedAt)
	if res.Error != nil {
		s.logError("failed to park undecodable job", "job_id", m.ID, "err", res.Error)
	}
}

// initialNextRun computes the first next_run for a newly added or updated
// schedule. A OnceSchedule whose run time already passed fires as soon as
// possible instead of being pushed to the far future by Next().
func initialNextRun(schedule Schedule, now time.Time) time.Time {
	if once, ok := schedule.(*OnceSchedule); ok {
		if runAt := once.RunAt(); runAt.After(now) {
			return runAt
		}
		return now
	}
	return schedule.Next(now)
}

// decodePostgresMetadata decodes the JSON metadata column of a claimed row.
func decodePostgresMetadata(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	var metadata map[string]string
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return nil
	}
	return metadata
}
