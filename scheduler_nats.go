package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultNATSStreamName    = "SCHEDULER"
	defaultNATSSubjectPrefix = "scheduler"
	defaultNATSConsumerName  = "scheduler-worker"
	defaultNATSJobBucket     = "SCHEDULER_JOBS"
	defaultNATSExecBucket    = "SCHEDULER_EXECUTIONS"

	natsScheduledDeliveryHeader = "Nats-Scheduled-Delivery"

	// natsMinMajor and natsMinMinor define the minimum NATS server version
	// required for AllowMsgSchedules support.
	natsMinMajor = 2
	natsMinMinor = 12
)

// ErrNATSServerTooOld indicates the connected NATS server does not support
// scheduled message delivery (requires NATS 2.12+).
var ErrNATSServerTooOld = errors.New("NATS server does not support scheduled message delivery (requires 2.12+)")

// scheduleMessage is the payload for scheduled NATS messages.
type scheduleMessage struct {
	JobID       string    `json:"job_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// NATSSchedulerOption configures a natsSchedulerImpl instance.
type NATSSchedulerOption func(*natsSchedulerImpl)

// WithNATSStreamName sets the JetStream stream name.
func WithNATSStreamName(name string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.streamName = name
	}
}

// WithNATSSubjectPrefix sets the NATS subject prefix for job messages.
func WithNATSSubjectPrefix(prefix string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.subjectPrefix = prefix
	}
}

// WithNATSConsumerName sets the durable consumer name.
func WithNATSConsumerName(name string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.consumerName = name
	}
}

// WithNATSSchedulerJobBucket sets the KV bucket name for job data.
func WithNATSSchedulerJobBucket(name string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.jobBucket = name
	}
}

// WithNATSSchedulerExecBucket sets the KV bucket name for execution records.
func WithNATSSchedulerExecBucket(name string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.execBucket = name
	}
}

// WithNATSSchedulerCodec sets the schedule codec for encoding/decoding schedules.
func WithNATSSchedulerCodec(codec ScheduleCodec) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.codec = codec
	}
}

// natsSchedulerImpl implements the Scheduler interface using NATS JetStream.
// It uses JetStream scheduled message delivery (AllowMsgSchedules, NATS 2.12+)
// for triggering jobs, and JetStream KV Store for persisting job metadata
// and execution records.
type natsSchedulerImpl struct {
	mu         sync.RWMutex
	running    bool
	jobs       map[string]*jobImpl
	handler    JobHandler
	codec      ScheduleCodec
	js         jetstream.JetStream
	stream     jetstream.Stream
	consumeCtx jetstream.ConsumeContext
	jobKV      jetstream.KeyValue
	execKV     jetstream.KeyValue
	ctx        context.Context
	cancel     context.CancelFunc
	startReady chan struct{}

	streamName    string
	subjectPrefix string
	consumerName  string
	jobBucket     string
	execBucket    string
}

// NewNATSScheduler creates a new Scheduler backed by NATS JetStream.
// Requires NATS Server 2.12+ with JetStream enabled and AllowMsgSchedules support.
//
// The scheduler uses:
//   - A JetStream stream with scheduled delivery for triggering jobs at their scheduled times
//   - A JetStream KV Store for persisting job metadata and execution records
//   - A durable consumer for reliable message consumption with automatic failover
//
// Example:
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//	js, _ := jetstream.New(nc)
//	s := scheduler.NewNATSScheduler(js, handler)
//	s.Start(ctx)
func NewNATSScheduler(js jetstream.JetStream, handler JobHandler, opts ...NATSSchedulerOption) Scheduler {
	s := &natsSchedulerImpl{
		jobs:          make(map[string]*jobImpl),
		handler:       handler,
		codec:         NewBasicScheduleCodec(),
		js:            js,
		startReady:    make(chan struct{}),
		streamName:    defaultNATSStreamName,
		subjectPrefix: defaultNATSSubjectPrefix,
		consumerName:  defaultNATSConsumerName,
		jobBucket:     defaultNATSJobBucket,
		execBucket:    defaultNATSExecBucket,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start initializes NATS resources (stream, KV buckets, consumer),
// loads persisted jobs, and begins consuming scheduled messages.
func (s *natsSchedulerImpl) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrSchedulerAlreadyStarted
	}
	if s.handler == nil {
		return ErrHandlerNotDefined
	}

	// Check NATS server version supports scheduled message delivery
	if err := checkNATSServerVersion(s.js); err != nil {
		return err
	}

	var err error

	// Create KV buckets for job metadata and execution records
	s.jobKV, err = s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: s.jobBucket,
	})
	if err != nil {
		return fmt.Errorf("failed to create job KV bucket: %w", err)
	}

	s.execKV, err = s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: s.execBucket,
	})
	if err != nil {
		return fmt.Errorf("failed to create exec KV bucket: %w", err)
	}

	// Create stream with scheduled delivery support
	s.stream, err = s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              s.streamName,
		Subjects:          []string{s.subjectPrefix + ".>"},
		AllowMsgSchedules: true,
		Retention:         jetstream.WorkQueuePolicy,
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	// Verify the server actually applied AllowMsgSchedules.
	// Older servers silently ignore unknown fields instead of returning an error.
	if info := s.stream.CachedInfo(); info != nil && !info.Config.AllowMsgSchedules {
		return fmt.Errorf("%w: server %s accepted the stream but did not enable AllowMsgSchedules",
			ErrNATSServerTooOld, s.js.Conn().ConnectedServerVersion())
	}

	// Purge stale scheduled messages from previous runs to avoid duplicates
	_ = s.stream.Purge(ctx)

	// Delete old consumer to clear any pending ack state from previous runs
	_ = s.stream.DeleteConsumer(ctx, s.consumerName)

	// Load persisted jobs and reschedule them
	if err := s.loadJobsFromKV(ctx); err != nil {
		return fmt.Errorf("failed to load jobs: %w", err)
	}

	// Create fresh consumer for receiving scheduled messages
	consumer, err := s.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   s.consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   5 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true

	// Start consuming scheduled messages
	s.consumeCtx, err = consumer.Consume(s.handleMessage)
	if err != nil {
		s.running = false
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	ready := s.startReady
	close(ready)

	return nil
}

// Stop gracefully shuts down the scheduler by stopping the consumer
// and waiting for all running jobs to complete.
func (s *natsSchedulerImpl) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerNotStarted
	}

	// Stop consuming new messages
	if s.consumeCtx != nil {
		s.consumeCtx.Stop()
	}

	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()

	// Wait for all running jobs to complete
	s.waitForJobs()

	s.mu.Lock()
	s.running = false
	s.startReady = make(chan struct{})
	s.mu.Unlock()

	return nil
}

// AddJob registers a new job, persists it to KV, and publishes a scheduled message.
func (s *natsSchedulerImpl) AddJob(id string, schedule Schedule, metadata map[string]string) error {
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

	scheduleType, scheduleConfig, err := s.codec.Encode(schedule)
	if err != nil {
		return err
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
		id:       id,
		schedule: schedule,
		metadata: copyMetadata(metadata),
		nextRun:  nextRun,
		lastRun:  time.Time{},
		running:  false,
	}
	s.jobs[id] = job

	if s.running {
		// Persist job metadata to KV
		jv := &natsJobValue{
			ID:             id,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Status:         string(JobStatusPending),
			NextRun:        nextRun,
			CreatedAt:      now,
			UpdatedAt:      now,
			Metadata:       copyMetadata(metadata),
		}
		data, err := json.Marshal(jv)
		if err != nil {
			delete(s.jobs, id)
			return err
		}
		if _, err = s.jobKV.Create(s.ctx, id, data); err != nil {
			delete(s.jobs, id)
			return fmt.Errorf("failed to save job to KV: %w", err)
		}

		// Publish scheduled message for the first execution
		if err := s.publishScheduledMessage(s.ctx, id, nextRun); err != nil {
			_ = s.jobKV.Purge(s.ctx, id)
			delete(s.jobs, id)
			return fmt.Errorf("failed to publish scheduled message: %w", err)
		}
	}

	return nil
}

// UpdateJobSchedule replaces the schedule for an existing job.
// It purges old scheduled messages and publishes a new one with the updated schedule.
func (s *natsSchedulerImpl) UpdateJobSchedule(id string, schedule Schedule) error {
	if id == "" {
		return ErrEmptyJobID
	}
	if schedule == nil {
		return ErrInvalidInterval
	}

	scheduleType, scheduleConfig, err := s.codec.Encode(schedule)
	if err != nil {
		return err
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

	// Wait for job to finish if running
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

	oldSchedule := job.schedule
	oldNextRun := job.nextRun

	job.mu.Lock()
	job.schedule = schedule
	job.nextRun = nextRun
	metadata := copyMetadata(job.metadata)
	lastRun := job.lastRun
	job.mu.Unlock()
	s.mu.Unlock()

	if s.running {
		// Update KV
		jv := &natsJobValue{
			ID:             id,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Status:         string(JobStatusPending),
			NextRun:        nextRun,
			LastRun:        lastRun,
			UpdatedAt:      now,
			Metadata:       metadata,
		}
		data, err := json.Marshal(jv)
		if err != nil {
			job.mu.Lock()
			job.schedule = oldSchedule
			job.nextRun = oldNextRun
			job.mu.Unlock()
			return err
		}
		if _, err = s.jobKV.Put(s.ctx, id, data); err != nil {
			job.mu.Lock()
			job.schedule = oldSchedule
			job.nextRun = oldNextRun
			job.mu.Unlock()
			return err
		}

		// Purge old scheduled messages and publish new one
		subject := s.jobSubject(id)
		_ = s.stream.Purge(s.ctx, jetstream.WithPurgeSubject(subject))
		if err := s.publishScheduledMessage(s.ctx, id, nextRun); err != nil {
			return fmt.Errorf("failed to publish scheduled message: %w", err)
		}
	}

	return nil
}

// RemoveJob removes a job from the scheduler, purges its scheduled messages, and deletes it from KV.
func (s *natsSchedulerImpl) RemoveJob(id string) error {
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

	if s.running {
		// Purge scheduled messages for this job
		subject := s.jobSubject(id)
		_ = s.stream.Purge(s.ctx, jetstream.WithPurgeSubject(subject))

		// Delete job metadata from KV
		_ = s.jobKV.Purge(s.ctx, id)
	}

	return nil
}

// GetJob retrieves a job by its ID.
func (s *natsSchedulerImpl) GetJob(id string) (Job, error) {
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

// ListJobs returns all registered jobs.
func (s *natsSchedulerImpl) ListJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// IsRunning returns whether the scheduler is currently running.
func (s *natsSchedulerImpl) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// WaitUntilRunning blocks until the scheduler has fully started or the context is done.
func (s *natsSchedulerImpl) WaitUntilRunning(ctx context.Context) error {
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

// handleMessage processes a scheduled message delivered by JetStream.
func (s *natsSchedulerImpl) handleMessage(msg jetstream.Msg) {
	var schedMsg scheduleMessage
	if err := json.Unmarshal(msg.Data(), &schedMsg); err != nil {
		_ = msg.Term()
		return
	}

	jobID := schedMsg.JobID

	s.mu.RLock()
	job, exists := s.jobs[jobID]
	running := s.running
	s.mu.RUnlock()

	if !exists || !running {
		// Job was removed or scheduler is stopping; discard message
		_ = msg.Ack()
		return
	}

	// Skip already-completed one-time schedules
	job.mu.RLock()
	currentSchedule := job.schedule
	lastRun := job.lastRun
	job.mu.RUnlock()
	if onceSchedule, ok := currentSchedule.(*OnceSchedule); ok {
		if !lastRun.IsZero() || !onceSchedule.RunAt().After(time.Now()) {
			_ = msg.Ack()
			return
		}
	}

	// Guard: if the scheduled time has not arrived yet, NAK with delay.
	// This serves as a fallback when the NATS server does not support
	// AllowMsgSchedules or the Nats-Scheduled-Delivery header is ignored.
	waitTime := time.Until(schedMsg.ScheduledAt)
	if waitTime > time.Second {
		delay := waitTime
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		_ = msg.NakWithDelay(delay)
		return
	}

	if job.IsRunning() {
		// Job is still executing from a previous trigger; retry later
		_ = msg.NakWithDelay(time.Second)
		return
	}

	// Periodically extend ack deadline for long-running jobs
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = msg.InProgress()
			}
		}
	}()

	s.executeJob(job, schedMsg.ScheduledAt)
	close(done)
	_ = msg.Ack()
}

// executeJob executes a single job and schedules the next run for recurring schedules.
func (s *natsSchedulerImpl) executeJob(job *jobImpl, scheduledAt time.Time) {
	defer func() {
		if r := recover(); r != nil {
			job.mu.Lock()
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
	executionID := job.id + "_" + startTime.Format("20060102150405")

	job.mu.RLock()
	metadata := copyMetadata(job.metadata)
	lastCompleted := job.lastRun
	currentSchedule := job.schedule
	job.mu.RUnlock()

	// Execute handler
	var handlerErr error
	if s.handler != nil {
		event := newJobEvent(job, metadata, currentSchedule, scheduledAt, startTime, lastCompleted)
		handlerErr = s.handler(s.ctx, event)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Update job state
	job.mu.Lock()
	job.lastRun = startTime
	nextRun := job.schedule.Next(endTime)
	job.nextRun = nextRun
	job.running = false
	job.mu.Unlock()

	status := JobStatusCompleted
	errorMsg := ""
	if handlerErr != nil {
		status = JobStatusFailed
		errorMsg = handlerErr.Error()
	}

	// Save execution record to KV (best-effort)
	if s.execKV != nil {
		record := &natsExecValue{
			JobID:       job.id,
			ExecutionID: executionID,
			StartTime:   startTime,
			EndTime:     endTime,
			DurationNs:  int64(duration),
			Status:      string(status),
			Error:       errorMsg,
			Metadata:    copyMetadata(metadata),
		}
		data, _ := json.Marshal(record)
		_, _ = s.execKV.Put(s.ctx, executionID, data)
	}

	// Update job state in KV (best-effort)
	if s.jobKV != nil {
		scheduleType, scheduleConfig, _ := s.codec.Encode(currentSchedule)
		jv := &natsJobValue{
			ID:             job.id,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Status:         string(status),
			NextRun:        nextRun,
			LastRun:        startTime,
			UpdatedAt:      endTime,
			Metadata:       copyMetadata(metadata),
		}
		data, _ := json.Marshal(jv)
		_, _ = s.jobKV.Put(s.ctx, job.id, data)
	}

	// Schedule next execution (skip for one-time schedules)
	if _, ok := currentSchedule.(*OnceSchedule); !ok {
		_ = s.publishScheduledMessage(s.ctx, job.id, nextRun)
	}
}

// publishScheduledMessage publishes a NATS message with scheduled delivery header.
func (s *natsSchedulerImpl) publishScheduledMessage(ctx context.Context, jobID string, scheduledAt time.Time) error {
	subject := s.jobSubject(jobID)
	payload, err := json.Marshal(scheduleMessage{
		JobID:       jobID,
		ScheduledAt: scheduledAt,
	})
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	msg.Header.Set(natsScheduledDeliveryHeader, "@at "+scheduledAt.UTC().Format(time.RFC3339))

	_, err = s.js.PublishMsg(ctx, msg)
	return err
}

// jobSubject returns the NATS subject for a given job ID.
func (s *natsSchedulerImpl) jobSubject(jobID string) string {
	return s.subjectPrefix + ".jobs." + jobID
}

// loadJobsFromKV loads persisted jobs from KV store and publishes scheduled messages for them.
func (s *natsSchedulerImpl) loadJobsFromKV(ctx context.Context) error {
	keys, err := s.jobKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return err
	}

	now := time.Now()
	for _, key := range keys {
		entry, err := s.jobKV.Get(ctx, key)
		if err != nil {
			continue
		}

		var jv natsJobValue
		if err := json.Unmarshal(entry.Value(), &jv); err != nil {
			continue
		}

		schedule, err := s.codec.Decode(jv.ScheduleType, jv.ScheduleConfig)
		if err != nil {
			continue
		}

		// Skip completed one-time schedules: if the run time has passed,
		// the job was already executed and should not be rescheduled.
		if onceSchedule, ok := schedule.(*OnceSchedule); ok {
			if !onceSchedule.RunAt().After(now) {
				// Still load into memory for GetJob/ListJobs but don't schedule
				s.jobs[jv.ID] = &jobImpl{
					id:       jv.ID,
					schedule: schedule,
					metadata: copyMetadata(jv.Metadata),
					nextRun:  onceSchedule.RunAt(),
					lastRun:  jv.LastRun,
				}
				continue
			}
		}

		nextRun := jv.NextRun
		if nextRun.Before(now) {
			nextRun = schedule.Next(now)
		}

		job := &jobImpl{
			id:       jv.ID,
			schedule: schedule,
			metadata: copyMetadata(jv.Metadata),
			nextRun:  nextRun,
			lastRun:  jv.LastRun,
		}
		s.jobs[jv.ID] = job

		_ = s.publishScheduledMessage(ctx, jv.ID, nextRun)
	}

	return nil
}

// checkNATSServerVersion verifies that the connected NATS server supports
// scheduled message delivery (AllowMsgSchedules, requires NATS 2.12+).
func checkNATSServerVersion(js jetstream.JetStream) error {
	version := js.Conn().ConnectedServerVersion()
	if version == "" {
		return fmt.Errorf("%w: unable to determine server version", ErrNATSServerTooOld)
	}

	major, minor, err := parseNATSVersion(version)
	if err != nil {
		return fmt.Errorf("%w: unable to parse server version %q: %v", ErrNATSServerTooOld, version, err)
	}

	if major < natsMinMajor || (major == natsMinMajor && minor < natsMinMinor) {
		return fmt.Errorf("%w: connected to %s, requires %d.%d+",
			ErrNATSServerTooOld, version, natsMinMajor, natsMinMinor)
	}

	return nil
}

// parseNATSVersion parses a NATS server version string (e.g. "2.12.5") into major and minor numbers.
func parseNATSVersion(version string) (major, minor int, err error) {
	// Strip leading "v" if present
	version = strings.TrimPrefix(version, "v")

	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("unexpected version format: %s", version)
	}

	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	return major, minor, nil
}

// waitForJobs waits for all running jobs to complete.
func (s *natsSchedulerImpl) waitForJobs() {
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
