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

	// defaultStreamDuplicatesWindow bounds how long the stream tracks
	// Nats-Msg-Id values for deduplication. With multiple peers running
	// loadJobsFromKV at startup (or republishing after a leadership change),
	// each peer would otherwise produce its own copy of the same scheduled
	// message; a wide window keeps the second-and-later copies suppressed
	// even when peer restarts are spread out over hours.
	defaultStreamDuplicatesWindow = 24 * time.Hour

	// defaultPublishAttempts is the number of times executeJob retries the
	// next-tick publishScheduledMessage call before giving up. The first
	// attempt counts, so 3 = 1 initial + 2 retries.
	defaultPublishAttempts = 3

	// defaultPublishRetryBackoff is the wait before the second publish
	// attempt; it doubles between each subsequent attempt.
	defaultPublishRetryBackoff = time.Second

	// defaultPostHandlerTimeout bounds the total time executeJob spends on
	// post-handler work (KV updates + next-tick publish, including retries).
	// Wider than the previous 10 s so the retry budget fits.
	defaultPostHandlerTimeout = 60 * time.Second

	// defaultStartupStreamReadyTimeout bounds how long Start() waits for the
	// SCHEDULER stream and the KV backing streams to have a raft leader
	// before continuing to loadJobsFromKV. In multi-node JetStream clusters
	// CreateOrUpdateStream / CreateOrUpdateKeyValue return as soon as the
	// metaleader has accepted the config, so the asset's own raft group may
	// still be electing a leader — any publish/KV call issued in that
	// window returns nats: no responders available for request.
	defaultStartupStreamReadyTimeout = 30 * time.Second
)

// ErrNATSServerTooOld indicates the connected NATS server does not support
// scheduled message delivery (requires NATS 2.12+).
var ErrNATSServerTooOld = errors.New("NATS server does not support scheduled message delivery (requires 2.12+)")

// scheduleMessage is the payload for scheduled NATS messages.
type scheduleMessage struct {
	JobID       string    `json:"job_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// NATSSchedulerLogger is a structured-log callback used to surface errors
// from best-effort operations (KV writes, next-tick publishes, reconciler
// scans). Arguments after msg are key/value pairs in the slog style.
//
// The scheduler invokes the logger from background goroutines; the callback
// must be safe to call concurrently.
type NATSSchedulerLogger func(msg string, keysAndValues ...any)

// RescheduleFailureFunc is invoked when executeJob has run the handler
// successfully (or with error) but failed to enqueue the next-tick scheduled
// message even after retries. It is the hook callers use to plug in a
// dead-letter or out-of-band reconciler. nextRun is the scheduled time of
// the publish that failed.
//
// Invoked from a background goroutine; the callback must be safe to call
// concurrently and should not block for long.
type RescheduleFailureFunc func(jobID string, nextRun time.Time, err error)

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

// WithSkipStartupCleanup controls whether Start() purges the scheduled-message
// stream and deletes the existing durable consumer.
//
// The default (false) preserves the original v0.0.5 behaviour and is safe for
// single-instance deployments: stale messages from the previous run are
// dropped and a fresh consumer is created.
//
// In multi-instance deployments where several processes share the same NATS
// JetStream cluster (and therefore the same SCHEDULER stream / KV buckets /
// durable consumer), the default is destructive: a peer's Start() wipes the
// scheduled messages a healthy peer has already published, and deletes the
// durable consumer the healthy peer is consuming from. Pass true to skip both
// operations and rely on the per-message Nats-Msg-Id (combined with the
// stream's Duplicates window) to suppress the duplicate publishes that
// loadJobsFromKV would otherwise produce.
func WithSkipStartupCleanup(skip bool) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.skipStartupCleanup = skip
	}
}

// WithStreamDuplicatesWindow overrides the duration the stream tracks
// Nats-Msg-Id values for deduplication. Larger values catch republishes that
// arrive long after the original publish (e.g. a peer that restarts hours
// later), at the cost of more server-side state.
func WithStreamDuplicatesWindow(window time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.duplicatesWindow = window
	}
}

// WithNATSSchedulerLogger installs a logger called when the scheduler hits a
// non-fatal error in a best-effort path (KV puts, next-tick publishes, and
// the reconciler). Without this option, those errors are dropped silently.
func WithNATSSchedulerLogger(logger NATSSchedulerLogger) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.logger = logger
	}
}

// WithOnReschedulingFailed installs a callback invoked when executeJob fails
// to enqueue the next-tick scheduled message after exhausting retries.
// Callers can use this to record a dead-letter, page on-call, or trigger an
// external reconciler. Without this option, the failure is logged (if a
// logger is configured) but otherwise silently absorbed.
func WithOnReschedulingFailed(fn RescheduleFailureFunc) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.onRescheduleFailed = fn
	}
}

// WithPublishRetry configures how many times executeJob retries the
// next-tick publishScheduledMessage call when it fails, and the backoff
// between attempts (doubling between each attempt). attempts < 1 is treated
// as 1 (no retry).
func WithPublishRetry(attempts int, initialBackoff time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		if attempts < 1 {
			attempts = 1
		}
		if initialBackoff <= 0 {
			initialBackoff = defaultPublishRetryBackoff
		}
		s.publishAttempts = attempts
		s.publishRetryBackoff = initialBackoff
	}
}

// WithReconcilerInterval enables a background goroutine that periodically
// scans the job KV and republishes the scheduled message for any recurring
// job whose NextRun is more than the supplied grace period in the past.
//
// Combined with the stream's Duplicates window and the deterministic
// Nats-Msg-Id used by publishScheduledMessage, republishing is safe even
// when the original scheduled message is still alive in the stream: the
// duplicate is silently suppressed.
//
// interval <= 0 disables the reconciler (the default). A typical setting is
// one full schedule interval (e.g. 1m for a minute-granularity cron).
func WithReconcilerInterval(interval time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.reconcilerInterval = interval
	}
}

// WithStartupStreamReadyTimeout bounds how long Start() waits for the
// SCHEDULER stream and KV backing streams to have a raft leader before
// proceeding to load persisted jobs.
//
// In multi-node JetStream clusters, CreateOrUpdateStream and
// CreateOrUpdateKeyValue return as soon as the metaleader has accepted the
// config — the asset's own raft group may still be electing a leader, so a
// publishScheduledMessage or KV Keys/Get call issued in that window returns
// nats: no responders available for request. Without this wait,
// loadJobsFromKV would silently or noisily drop the first startup publish
// for every persisted job until the chain naturally re-fires (it doesn't,
// because the chain is the publish we just lost).
//
// timeout <= 0 disables the wait. Single-node JetStream (no Cluster info)
// is treated as ready immediately.
func WithStartupStreamReadyTimeout(timeout time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.startupStreamReadyTimeout = timeout
	}
}

// WithReconcilerGracePeriod sets the lag tolerance the reconciler applies
// before treating a job as stuck. A job is republished only if its KV
// NextRun is older than now - gracePeriod. The default is 30 s, large enough
// to skip jobs that are mid-tick yet small enough to recover quickly.
func WithReconcilerGracePeriod(gracePeriod time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		if gracePeriod < 0 {
			gracePeriod = 0
		}
		s.reconcilerGrace = gracePeriod
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

	skipStartupCleanup bool
	duplicatesWindow   time.Duration

	logger                    NATSSchedulerLogger
	onRescheduleFailed        RescheduleFailureFunc
	publishAttempts           int
	publishRetryBackoff       time.Duration
	postHandlerTimeout        time.Duration
	startupStreamReadyTimeout time.Duration

	reconcilerInterval time.Duration
	reconcilerGrace    time.Duration
	reconcilerWG       sync.WaitGroup
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
		jobs:                      make(map[string]*jobImpl),
		handler:                   handler,
		codec:                     NewBasicScheduleCodec(),
		js:                        js,
		startReady:                make(chan struct{}),
		streamName:                defaultNATSStreamName,
		subjectPrefix:             defaultNATSSubjectPrefix,
		consumerName:              defaultNATSConsumerName,
		jobBucket:                 defaultNATSJobBucket,
		execBucket:                defaultNATSExecBucket,
		duplicatesWindow:          defaultStreamDuplicatesWindow,
		publishAttempts:           defaultPublishAttempts,
		publishRetryBackoff:       defaultPublishRetryBackoff,
		postHandlerTimeout:        defaultPostHandlerTimeout,
		startupStreamReadyTimeout: defaultStartupStreamReadyTimeout,
		reconcilerGrace:           30 * time.Second,
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

	// Create stream with scheduled delivery support. The Duplicates window
	// pairs with the per-message Nats-Msg-Id set by publishScheduledMessage
	// to suppress duplicate publishes — most importantly the ones produced
	// when several peers each run loadJobsFromKV at startup.
	s.stream, err = s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              s.streamName,
		Subjects:          []string{s.subjectPrefix + ".>"},
		AllowMsgSchedules: true,
		Retention:         jetstream.WorkQueuePolicy,
		Duplicates:        s.duplicatesWindow,
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

	// Wait for the SCHEDULER stream and KV backing streams to have a raft
	// leader before any publish or KV operation. Without this, multi-node
	// clusters return nats: no responders available for request during the
	// election window — silently dropping the first startup publish for
	// every persisted job until a peer restart.
	if s.startupStreamReadyTimeout > 0 {
		if err := s.waitForBackingStreams(ctx, s.startupStreamReadyTimeout); err != nil {
			return fmt.Errorf("backing streams not ready: %w", err)
		}
	}

	if !s.skipStartupCleanup {
		// Single-instance behaviour: clear stale state from a previous run.
		// Skipped when WithSkipStartupCleanup(true) is set so a starting peer
		// does not wipe the scheduled messages a healthy peer just published
		// and does not delete the durable consumer that healthy peer is
		// consuming from.
		_ = s.stream.Purge(ctx)
		_ = s.stream.DeleteConsumer(ctx, s.consumerName)
	}

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

	// Start the optional background reconciler. It runs until s.ctx is
	// cancelled by Stop(), and Stop() waits for it via s.reconcilerWG.
	if s.reconcilerInterval > 0 {
		s.reconcilerWG.Add(1)
		go s.runReconciler(s.ctx, s.reconcilerInterval)
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

	// Wait for the reconciler goroutine to exit before declaring stopped, so
	// it does not race with a subsequent Start() on the same instance.
	s.reconcilerWG.Wait()

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

	if !running {
		_ = msg.Ack()
		return
	}

	if !exists {
		// In multi-instance deployments the durable consumer fans messages
		// out across peers; this peer may receive a message for a job that
		// another peer added after our Start(). Lazy-load from KV (the
		// shared source of truth) before discarding.
		loaded, err := s.loadJobFromKV(s.ctx, jobID)
		if err != nil {
			// Genuinely unknown job (removed or never persisted).
			_ = msg.Ack()
			return
		}
		s.mu.Lock()
		if existing, ok := s.jobs[jobID]; ok {
			job = existing
		} else {
			s.jobs[jobID] = loaded
			job = loaded
		}
		s.mu.Unlock()
	}

	// Skip already-executed one-time schedules
	job.mu.RLock()
	currentSchedule := job.schedule
	lastRun := job.lastRun
	job.mu.RUnlock()
	if _, ok := currentSchedule.(*OnceSchedule); ok && !lastRun.IsZero() {
		_ = msg.Ack()
		return
	}

	// Skip messages whose scheduledAt is at or before the last execution.
	// Covers two scenarios:
	//   1. A peer was offline long enough for stale messages to accumulate
	//      in the stream; on restart they would otherwise re-fire with past
	//      scheduledAt values.
	//   2. A duplicate publish that slipped past the Duplicates window (e.g.
	//      a peer restart spread wider than WithStreamDuplicatesWindow).
	// lastRun is a per-peer view and may lag the KV truth across pods, so
	// this catches the common cases without claiming exactness — the
	// authoritative cross-pod ordering still relies on JetStream's
	// at-most-once delivery per durable consumer.
	if !lastRun.IsZero() && !schedMsg.ScheduledAt.After(lastRun) {
		_ = msg.Ack()
		return
	}

	// Guard: if the scheduled time has not arrived yet, NAK with delay.
	// This serves as a fallback when the NATS server does not support
	// AllowMsgSchedules or the Nats-Scheduled-Delivery header is ignored.
	// Using a 100ms tolerance to account for minor clock drift and delivery latency.
	waitTime := time.Until(schedMsg.ScheduledAt)
	if waitTime > 100*time.Millisecond {
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

	// Update job state. Note: job.running stays true until the post-handler
	// section below finishes. waitForJobs() (called from Stop) only checks
	// job.IsRunning(), so resetting running here would let Stop return —
	// and the caller may then close the NATS connection — before the
	// next-tick publish completes, breaking the recurring chain.
	job.mu.Lock()
	job.lastRun = startTime
	nextRun := job.schedule.Next(endTime)
	job.nextRun = nextRun
	job.mu.Unlock()

	status := JobStatusCompleted
	errorMsg := ""
	if handlerErr != nil {
		status = JobStatusFailed
		errorMsg = handlerErr.Error()
	}

	// Post-handler updates and the next-tick publish must outlive Stop()'s
	// context cancellation. If we used s.ctx here, a SIGTERM mid-tick would
	// silently fail to enqueue the next scheduled message and break the
	// recurring chain until a peer restart triggers loadJobsFromKV.
	postCtx, postCancel := context.WithTimeout(context.Background(), s.postHandlerTimeout)
	defer postCancel()

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
		if _, err := s.execKV.Put(postCtx, executionID, data); err != nil {
			s.logError("failed to persist execution record",
				"job_id", job.id, "execution_id", executionID, "err", err)
		}
	}

	// Schedule next execution (skip for one-time schedules) before the
	// final KV state write so the persisted Status accurately reflects
	// whether the next-tick publish succeeded.
	publishErr := error(nil)
	if _, ok := currentSchedule.(*OnceSchedule); !ok {
		publishErr = s.publishScheduledMessageWithRetry(postCtx, job.id, nextRun)
		if publishErr != nil {
			s.logError("failed to publish next-tick scheduled message",
				"job_id", job.id, "next_run", nextRun, "err", publishErr)
			if s.onRescheduleFailed != nil {
				go s.onRescheduleFailed(job.id, nextRun, publishErr)
			}
		}
	}

	// Update job state in KV (best-effort)
	if s.jobKV != nil {
		// JobStatusReschedulingFailed is reserved for the "handler succeeded
		// but we couldn't enqueue the next tick" case; when the handler
		// itself failed, the handler error remains the primary signal.
		finalStatus := status
		if publishErr != nil && handlerErr == nil {
			finalStatus = JobStatusReschedulingFailed
		}
		scheduleType, scheduleConfig, _ := s.codec.Encode(currentSchedule)
		jv := &natsJobValue{
			ID:             job.id,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Status:         string(finalStatus),
			NextRun:        nextRun,
			LastRun:        startTime,
			UpdatedAt:      endTime,
			Metadata:       copyMetadata(metadata),
		}
		data, _ := json.Marshal(jv)
		if _, err := s.jobKV.Put(postCtx, job.id, data); err != nil {
			s.logError("failed to persist job state",
				"job_id", job.id, "next_run", nextRun, "err", err)
		}
	}

	// Mark the job as no longer running only after all post-handler work is
	// done. This is what makes waitForJobs() (and therefore Stop()) wait for
	// the next-tick publish to complete.
	job.mu.Lock()
	job.running = false
	job.mu.Unlock()
}

// waitForBackingStreams blocks until the SCHEDULER stream and both KV
// backing streams (KV_<jobBucket>, KV_<execBucket>) report a raft leader,
// or the timeout elapses. Single-node JetStream (where StreamInfo.Cluster
// is nil) is treated as ready immediately.
//
// The poll catches three failure modes for a freshly-created asset in a
// multi-node cluster:
//   - stream.Info / js.Stream returns nats: no responders (asset stream not
//     yet hosted on any peer that responds);
//   - StreamInfo.Cluster.Leader == "" (raft group still electing);
//   - the call itself errors with a transient JetStream API error.
//
// In each case the probe is retried with bounded exponential backoff.
func (s *natsSchedulerImpl) waitForBackingStreams(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	type target struct {
		label string
		info  func(context.Context) (*jetstream.StreamInfo, error)
	}
	targets := []target{
		{
			label: s.streamName,
			info:  func(c context.Context) (*jetstream.StreamInfo, error) { return s.stream.Info(c) },
		},
	}
	if s.jobKV != nil {
		jobStreamName := "KV_" + s.jobBucket
		targets = append(targets, target{
			label: jobStreamName,
			info: func(c context.Context) (*jetstream.StreamInfo, error) {
				st, err := s.js.Stream(c, jobStreamName)
				if err != nil {
					return nil, err
				}
				return st.Info(c)
			},
		})
	}
	if s.execKV != nil {
		execStreamName := "KV_" + s.execBucket
		targets = append(targets, target{
			label: execStreamName,
			info: func(c context.Context) (*jetstream.StreamInfo, error) {
				st, err := s.js.Stream(c, execStreamName)
				if err != nil {
					return nil, err
				}
				return st.Info(c)
			},
		})
	}

	for _, t := range targets {
		backoff := 50 * time.Millisecond
		for {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("%s: no raft leader after %s", t.label, timeout)
			}
			// Cap each probe so a hung JetStream API request can't blow
			// past the overall deadline. 2 s is enough for a healthy
			// cluster and short enough that the retry loop stays responsive.
			callTimeout := 2 * time.Second
			if callTimeout > remaining {
				callTimeout = remaining
			}
			infoCtx, infoCancel := context.WithTimeout(ctx, callTimeout)
			info, err := t.info(infoCtx)
			infoCancel()
			if err == nil && (info.Cluster == nil || info.Cluster.Leader != "") {
				break
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 500*time.Millisecond {
				backoff *= 2
			}
		}
	}
	return nil
}

// runReconciler periodically scans the job KV and republishes the scheduled
// message for any recurring job whose KV NextRun is older than the grace
// period. Safe to run concurrently with executeJob and other peers: the
// per-message Nats-Msg-Id (jobID, scheduledAt) combined with the stream's
// Duplicates window suppresses the republish when the original scheduled
// message is still alive. When the chain is genuinely broken (the next-tick
// publish failed silently), the republish restores it.
func (s *natsSchedulerImpl) runReconciler(ctx context.Context, interval time.Duration) {
	defer s.reconcilerWG.Done()

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileOnce(ctx)
		}
	}
}

// reconcileOnce performs a single reconciliation sweep over the KV. Exposed
// for tests; production callers should use runReconciler.
func (s *natsSchedulerImpl) reconcileOnce(ctx context.Context) {
	if s.jobKV == nil {
		return
	}
	if err := ctx.Err(); err != nil {
		return
	}
	keys, err := s.jobKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return
		}
		// Suppress the log line when the only reason Keys failed is that
		// Stop() cancelled the context — that's expected, not an incident.
		if ctx.Err() == nil {
			s.logError("reconciler: list KV keys failed", "err", err)
		}
		return
	}

	grace := s.reconcilerGrace
	if grace < 0 {
		grace = 0
	}
	cutoff := time.Now().Add(-grace)

	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return
		}
		entry, err := s.jobKV.Get(ctx, key)
		if err != nil {
			s.logError("reconciler: read KV entry failed", "key", key, "err", err)
			continue
		}
		var jv natsJobValue
		if err := json.Unmarshal(entry.Value(), &jv); err != nil {
			s.logError("reconciler: decode KV entry failed", "key", key, "err", err)
			continue
		}
		schedule, err := s.codec.Decode(jv.ScheduleType, jv.ScheduleConfig)
		if err != nil {
			s.logError("reconciler: decode schedule failed",
				"job_id", jv.ID, "schedule_type", jv.ScheduleType, "err", err)
			continue
		}
		// One-time schedules don't form a chain; nothing to repair.
		if _, ok := schedule.(*OnceSchedule); ok {
			continue
		}
		// Skip jobs we know are mid-tick locally — executeJob will publish
		// the next-tick message itself.
		s.mu.RLock()
		local, hasLocal := s.jobs[jv.ID]
		s.mu.RUnlock()
		if hasLocal && local.IsRunning() {
			continue
		}
		// NextRun in the future (or only marginally in the past) means the
		// chain is intact — leave it alone.
		if jv.NextRun.IsZero() || jv.NextRun.After(cutoff) {
			continue
		}

		// Republish with the EXACT KV NextRun so the deterministic Msg-Id
		// matches the original publish. If that original is still alive in
		// the stream (i.e. the chain was actually fine and KV is just lagging
		// our peer), the Duplicates window suppresses this republish.
		if err := s.publishScheduledMessage(ctx, jv.ID, jv.NextRun); err != nil {
			// Same shutdown-noise suppression as the Keys() path above.
			if ctx.Err() == nil {
				s.logError("reconciler: republish failed",
					"job_id", jv.ID, "next_run", jv.NextRun, "err", err)
			}
			continue
		}
		s.logError("reconciler: republished stale next-tick",
			"job_id", jv.ID, "next_run", jv.NextRun)
	}
}

// logError forwards a structured error event to the configured logger.
// Safe to call from any goroutine; no-op when no logger is installed.
func (s *natsSchedulerImpl) logError(msg string, keysAndValues ...any) {
	if s.logger == nil {
		return
	}
	defer func() {
		// A panicking logger must not crash a scheduler goroutine.
		_ = recover()
	}()
	s.logger(msg, keysAndValues...)
}

// publishScheduledMessageWithRetry retries publishScheduledMessage with
// bounded exponential backoff. The first attempt counts toward the budget,
// so attempts = 3 means 1 initial + 2 retries.
func (s *natsSchedulerImpl) publishScheduledMessageWithRetry(ctx context.Context, jobID string, scheduledAt time.Time) error {
	attempts := s.publishAttempts
	if attempts < 1 {
		attempts = 1
	}
	backoff := s.publishRetryBackoff
	if backoff <= 0 {
		backoff = defaultPublishRetryBackoff
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return lastErr
		}
		lastErr = s.publishScheduledMessage(ctx, jobID, scheduledAt)
		if lastErr == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		s.logError("publishScheduledMessage attempt failed; retrying",
			"job_id", jobID, "attempt", i+1, "next_run", scheduledAt, "err", lastErr)
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return lastErr
}

// publishScheduledMessage publishes a NATS message with scheduled delivery header.
//
// The message carries a deterministic Nats-Msg-Id derived from (jobID,
// scheduledAt). Combined with the stream's Duplicates window, this prevents
// duplicate executions when multiple peers republish the same scheduled
// message concurrently — for example two pods both running loadJobsFromKV
// after a cluster cold-start.
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
	msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("%s-%d", jobID, scheduledAt.UnixNano()))

	_, err = s.js.PublishMsg(ctx, msg)
	return err
}

// jobSubject returns the NATS subject for a given job ID.
func (s *natsSchedulerImpl) jobSubject(jobID string) string {
	return s.subjectPrefix + ".jobs." + jobID
}

// loadJobFromKV reads a single job entry from the KV bucket and rebuilds an
// in-memory jobImpl from it. Used by handleMessage to recover a job that this
// peer has not seen because another peer added it after our Start().
func (s *natsSchedulerImpl) loadJobFromKV(ctx context.Context, id string) (*jobImpl, error) {
	if s.jobKV == nil {
		return nil, ErrJobNotFound
	}
	entry, err := s.jobKV.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	var jv natsJobValue
	if err := json.Unmarshal(entry.Value(), &jv); err != nil {
		return nil, err
	}
	schedule, err := s.codec.Decode(jv.ScheduleType, jv.ScheduleConfig)
	if err != nil {
		return nil, err
	}
	return &jobImpl{
		id:       jv.ID,
		schedule: schedule,
		metadata: copyMetadata(jv.Metadata),
		nextRun:  jv.NextRun,
		lastRun:  jv.LastRun,
	}, nil
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
			s.logError("startup: failed to read job KV entry",
				"key", key, "err", err)
			continue
		}

		var jv natsJobValue
		if err := json.Unmarshal(entry.Value(), &jv); err != nil {
			s.logError("startup: failed to decode job KV entry",
				"key", key, "err", err)
			continue
		}

		schedule, err := s.codec.Decode(jv.ScheduleType, jv.ScheduleConfig)
		if err != nil {
			s.logError("startup: failed to decode schedule",
				"job_id", jv.ID, "schedule_type", jv.ScheduleType, "err", err)
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

		if err := s.publishScheduledMessage(ctx, jv.ID, nextRun); err != nil {
			s.logError("startup: failed to publish scheduled message",
				"job_id", jv.ID, "next_run", nextRun, "err", err)
		}
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
