package scheduler

import (
	"bytes"
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

	// defaultReconcilerInterval is how often the background reconciler scans
	// the job KV for chains whose next-tick publish was lost. Enabled by
	// default so a single dropped publish does not silently kill a recurring
	// job — the reconciler will republish within this interval.
	defaultReconcilerInterval = 30 * time.Second

	// defaultAddJobRetryBudget bounds how long AddJob / UpdateJobSchedule
	// will keep retrying their KV write and publish through a transient
	// cluster hiccup (typically a raft leader re-election finishes well
	// inside this budget). Zero disables the retry.
	defaultAddJobRetryBudget = 5 * time.Second

	// defaultStartPhaseTimeout caps each individual NATS API call inside
	// Start() (KV bucket creation, stream creation, consumer creation,
	// initial Consume subscribe). Without this, a single hung JetStream
	// API request would consume the caller's entire Start ctx — typical
	// callers pass several minutes, so the failure surfaced 5+ minutes
	// after the real problem with no indication of which phase blocked.
	// Each phase logs entry/exit timing through the installed logger and
	// its error is wrapped with the phase name. Zero falls back to using
	// the caller's ctx directly (legacy behaviour).
	defaultStartPhaseTimeout = 30 * time.Second

	// defaultJetStreamReadyTimeout bounds how long Start() waits for the
	// JetStream metaleader to be available before issuing any JetStream API
	// call. Brand-new multi-node deployments race scheduler startup against
	// the cluster's metaleader raft election: until a metaleader is elected
	// the JetStream API does not reliably return NoResponders, so the very
	// first AccountInfo / CreateOrUpdateKeyValue call hangs for the caller's
	// entire ctx deadline (nats.go documents this on AccountInfo: "For
	// clustered topologies, AccountInfo will time out").
	//
	// The wait probes AccountInfo with a 2 s per-call cap and short
	// exponential backoff; on a healthy cluster it returns almost
	// instantly, on a still-electing cluster it returns the moment the
	// metaleader is up.
	defaultJetStreamReadyTimeout = 30 * time.Second

	// defaultLoadJobsConcurrency is the number of worker goroutines
	// loadJobsFromKV uses to parallelize KV reads and async scheduled-message
	// publishes at startup. With this many in-flight KV.Get round-trips and
	// the JetStream client's default 4000-deep async publish pipeline,
	// startup of an N-job deployment drops from O(N * RTT) to roughly
	// O((N / concurrency) * RTT) for the KV reads, with the publishes
	// effectively overlapped behind them.
	defaultLoadJobsConcurrency = 32

	// defaultLoadJobsAsyncPublishTimeout bounds how long loadJobsFromKV
	// waits for in-flight async PublishMsgAsync calls to be acked by the
	// stream leader before declaring the load complete. The reconciler
	// catches any straggler that misses this window.
	defaultLoadJobsAsyncPublishTimeout = 30 * time.Second
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

// WithReconcilerInterval overrides how often the background reconciler scans
// the job KV and republishes the scheduled message for any recurring job
// whose NextRun is more than the supplied grace period in the past.
//
// Combined with the stream's Duplicates window and the deterministic
// Nats-Msg-Id used by publishScheduledMessage, republishing is safe even
// when the original scheduled message is still alive in the stream: the
// duplicate is silently suppressed.
//
// The reconciler is enabled by default (defaultReconcilerInterval). Passing
// interval <= 0 disables it.
func WithReconcilerInterval(interval time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.reconcilerInterval = interval
	}
}

// WithAddJobRetryBudget bounds how long AddJob and UpdateJobSchedule will
// keep retrying their KV write and scheduled-message publish through a
// transient cluster hiccup (a raft leader re-election typically finishes
// well inside the default 5 s budget). A truly unavailable cluster will
// exhaust the budget and bubble the error back to the caller.
//
// Setting budget <= 0 disables the retry entirely, restoring the previous
// "fail fast on the first transient error" behaviour.
func WithAddJobRetryBudget(budget time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		if budget < 0 {
			budget = 0
		}
		s.addJobRetryBudget = budget
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

// WithJetStreamReadyTimeout bounds how long Start() waits for the
// JetStream metaleader to be reachable before issuing the first KV /
// stream / consumer create.
//
// This is the fix for the multi-instance brand-new-deployment hang.
// nats.go's AccountInfo documents:
//
//	"For clustered topologies, AccountInfo will time out."
//
// Every JetStream API the scheduler uses (CreateOrUpdateKeyValue,
// CreateOrUpdateStream, CreateOrUpdateConsumer) issues an AccountInfo
// internally. When several scheduler instances boot against a freshly-
// started 3-node NATS cluster, the metaleader may still be electing —
// the API request neither succeeds nor fails fast, it hangs for the
// caller's ctx deadline. Without this wait, Start() can hang for
// minutes (typical caller ctx is much longer than 30 s).
//
// timeout <= 0 disables the wait (legacy behaviour). Default 30 s
// comfortably covers a metaleader election; the moment the metaleader
// answers, the wait returns.
func WithJetStreamReadyTimeout(timeout time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.jetStreamReadyTimeout = timeout
	}
}

// WithStartPhaseTimeout caps each individual NATS API call inside Start
// (KV bucket creation, stream creation, backing-stream wait, persisted-job
// load, consumer creation, initial Consume subscribe). Without this cap a
// single hung JetStream API request consumes the caller's entire Start
// context — typical callers pass several minutes, so the hang surfaces 5+
// minutes later with no indication of which phase blocked.
//
// Each phase logs entry, exit, and elapsed time through the installed
// logger, and on failure the returned error is wrapped with the phase name.
//
// timeout <= 0 falls back to using the caller's ctx directly (legacy
// behaviour, no per-phase bound).
func WithStartPhaseTimeout(timeout time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.startPhaseTimeout = timeout
	}
}

// WithLoadJobsConcurrency sets the number of worker goroutines used by
// loadJobsFromKV during Start() to parallelize KV reads and async
// scheduled-message publishes for persisted jobs.
//
// Each worker does a sync jobKV.Get + (optional) jobKV.Update + a fire-and-
// forget PublishMsgAsync for one job before pulling the next key. The
// JetStream client itself pipelines the asyncs behind the scenes, so the
// publish stage is effectively free; this option controls how many KV reads
// run in parallel.
//
// Higher values cut startup time on deployments with hundreds or thousands
// of persisted jobs (O(N*RTT) → O(N/concurrency * RTT)). Values below 1 are
// treated as 1 (no parallelism). The default is 32, comfortable for most
// clusters; raise it only if you have measured KV.Get latency dominating
// Start() time.
//
// Note on error visibility: per-message publish ack errors at startup are
// not logged by the scheduler (the reconciler is the safety net — any job
// whose scheduled message never landed will be republished within the
// reconciler interval). If you want per-message visibility, install
// jetstream.WithPublishAsyncErrHandler on the JetStream client you pass to
// NewNATSScheduler — that handler fires for every async publish whose ack
// fails or times out.
func WithLoadJobsConcurrency(n int) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		if n < 1 {
			n = 1
		}
		s.loadJobsConcurrency = n
	}
}

// WithLoadJobsAsyncPublishTimeout bounds how long loadJobsFromKV waits at
// the end of Start() for outstanding async PublishMsgAsync calls to be
// acked. A timeout here does not break correctness — the background
// reconciler will republish any persisted job whose next-tick message was
// lost or never acked — but it is logged so operators can spot a cluster
// that is slow to ack startup publishes.
//
// timeout <= 0 disables the wait (workers exit and Start() returns as soon
// as the last PublishMsgAsync has been enqueued).
func WithLoadJobsAsyncPublishTimeout(timeout time.Duration) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.loadJobsAsyncPublishTimeout = timeout
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

	duplicatesWindow time.Duration

	// addJobRetryBudget bounds how long AddJob / UpdateJobSchedule will
	// keep retrying their KV write and publish through a transient cluster
	// hiccup (typically a raft leader re-election). Zero disables retry.
	addJobRetryBudget time.Duration

	logger                    NATSSchedulerLogger
	onRescheduleFailed        RescheduleFailureFunc
	publishAttempts           int
	publishRetryBackoff       time.Duration
	postHandlerTimeout        time.Duration
	jetStreamReadyTimeout     time.Duration
	startupStreamReadyTimeout time.Duration
	startPhaseTimeout         time.Duration

	reconcilerInterval time.Duration
	reconcilerGrace    time.Duration
	reconcilerWG       sync.WaitGroup

	// loadJobsConcurrency caps how many worker goroutines loadJobsFromKV
	// uses to parallelize KV reads at Start(). Async PublishMsgAsync calls
	// fired by workers pipeline through the JetStream client's own buffer.
	loadJobsConcurrency         int
	loadJobsAsyncPublishTimeout time.Duration

	// once serializes the multi-process first-deploy race on
	// CreateOrUpdateKeyValue / CreateOrUpdateStream /
	// CreateOrUpdateConsumer inside Start. Defaults to a built-in
	// JetStream-KV-backed implementation; WithOnce overrides for callers
	// that already operate a distributed-lock service.
	once           OnceFunc
	onceLockBucket string
	onceKey        string
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
		jetStreamReadyTimeout:     defaultJetStreamReadyTimeout,
		startupStreamReadyTimeout: defaultStartupStreamReadyTimeout,
		startPhaseTimeout:         defaultStartPhaseTimeout,
		reconcilerInterval:        defaultReconcilerInterval,
		reconcilerGrace:           30 * time.Second,
		addJobRetryBudget:         defaultAddJobRetryBudget,
		onceLockBucket:            defaultOnceLockBucket,
		onceKey:                   defaultSchedulerOnceKey,

		loadJobsConcurrency:         defaultLoadJobsConcurrency,
		loadJobsAsyncPublishTimeout: defaultLoadJobsAsyncPublishTimeout,
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

	// Wait for the JetStream metaleader before any JS API call. In a
	// freshly-started multi-node cluster the metaleader raft is still
	// electing; nats.go's AccountInfo (which every CreateOrUpdate* path
	// calls internally) does not return NoResponders in that window — it
	// just hangs until the caller's ctx deadline. Without this gate, a
	// brand-new 3-node deployment can leave some instances stuck for the
	// full Start ctx (typically minutes) while others succeed by luck of
	// timing.
	if s.jetStreamReadyTimeout > 0 {
		if err := s.startPhaseUnbounded(ctx, "wait-jetstream-ready", func(c context.Context) error {
			return waitForJetStreamReady(c, s.js, s.jetStreamReadyTimeout)
		}); err != nil {
			return err
		}
	}

	// Resolve the Once primitive used to serialize first-deploy
	// CreateOrUpdate* races. WithOnce wins if the caller installed one;
	// otherwise build the JetStream-KV-backed default lazily here so the
	// allocation is paid only by callers that actually Start the
	// scheduler.
	if s.once == nil {
		s.once = newOnceStore(s.js, s.onceLockBucket).Do
	}

	// Single Once block keyed "scheduler.init" wraps every first-deploy
	// CreateOrUpdate*. Concurrent peers serialize through it; the
	// sentinel fast-paths every subsequent boot. Lookups run outside —
	// they're plain metadata reads that cannot hang on the reply-path
	// race, and keeping them separate preserves per-step error context.
	if err := s.startPhase(ctx, "provision", func(c context.Context) error {
		return s.once(c, s.onceKey, func(c context.Context) error {
			if _, e := s.js.CreateOrUpdateKeyValue(c, jetstream.KeyValueConfig{
				Bucket: s.jobBucket,
			}); e != nil {
				return fmt.Errorf("create job KV: %w", e)
			}
			if _, e := s.js.CreateOrUpdateKeyValue(c, jetstream.KeyValueConfig{
				Bucket: s.execBucket,
			}); e != nil {
				return fmt.Errorf("create exec KV: %w", e)
			}
			if _, e := s.js.CreateOrUpdateStream(c, jetstream.StreamConfig{
				Name:              s.streamName,
				Subjects:          []string{s.subjectPrefix + ".>"},
				AllowMsgSchedules: true,
				Retention:         jetstream.WorkQueuePolicy,
				Duplicates:        s.duplicatesWindow,
			}); e != nil {
				return fmt.Errorf("create stream: %w", e)
			}
			st, e := s.js.Stream(c, s.streamName)
			if e != nil {
				return fmt.Errorf("lookup stream for consumer create: %w", e)
			}
			if _, e := st.CreateOrUpdateConsumer(c, jetstream.ConsumerConfig{
				Durable:   s.consumerName,
				AckPolicy: jetstream.AckExplicitPolicy,
				AckWait:   5 * time.Minute,
			}); e != nil {
				return fmt.Errorf("create consumer: %w", e)
			}
			return nil
		})
	}); err != nil {
		return fmt.Errorf("failed to provision: %w", err)
	}

	jobKV, err := s.js.KeyValue(ctx, s.jobBucket)
	if err != nil {
		return fmt.Errorf("failed to lookup job KV bucket: %w", err)
	}
	s.jobKV = jobKV
	execKV, err := s.js.KeyValue(ctx, s.execBucket)
	if err != nil {
		return fmt.Errorf("failed to lookup exec KV bucket: %w", err)
	}
	s.execKV = execKV
	st, err := s.js.Stream(ctx, s.streamName)
	if err != nil {
		return fmt.Errorf("failed to lookup stream: %w", err)
	}
	s.stream = st

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
		if err := s.startPhaseUnbounded(ctx, "wait-backing-streams", func(c context.Context) error {
			return s.waitForBackingStreams(c, s.startupStreamReadyTimeout)
		}); err != nil {
			return fmt.Errorf("backing streams not ready: %w", err)
		}
	}

	// No startup purge or consumer delete. Stale messages left behind from a
	// previous run are filtered by the KV validation in handleMessage (a msg
	// whose ScheduledAt is <= KV.LastRun or < KV.NextRun is acked and
	// dropped). This keeps single- and multi-instance deployments on a
	// single code path: multi-instance must not wipe a healthy peer's
	// already-published scheduled messages, and single-instance no longer
	// needs to.

	// Load persisted jobs and reschedule them
	if err := s.startPhase(ctx, "load-jobs-from-kv", func(c context.Context) error {
		return s.loadJobsFromKV(c)
	}); err != nil {
		return fmt.Errorf("failed to load jobs: %w", err)
	}

	// Consumer was provisioned inside the merged Once block above;
	// here we just look it up. Lookup is a plain metadata read that
	// cannot hang on the multi-pod reply-path race.
	var consumer jetstream.Consumer
	if err := s.startPhase(ctx, "lookup-consumer", func(c context.Context) error {
		cons, e := s.stream.Consumer(c, s.consumerName)
		if e != nil {
			return e
		}
		consumer = cons
		return nil
	}); err != nil {
		return fmt.Errorf("failed to lookup consumer: %w", err)
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true

	// Start consuming scheduled messages. Consume() itself is non-blocking
	// in nats.go/jetstream — it sets up the pull subscription via a
	// fire-and-forget PublishRequest — so we wrap it for the diagnostic
	// log only, not because we expect it to hang.
	if err := s.startPhase(ctx, "start-consume", func(c context.Context) error {
		cc, e := consumer.Consume(s.handleMessage)
		if e != nil {
			return e
		}
		s.consumeCtx = cc
		return nil
	}); err != nil {
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
		id:        id,
		schedule:  schedule,
		metadata:  copyMetadata(metadata),
		nextRun:   nextRun,
		lastRun:   time.Time{},
		createdAt: now,
		running:   false,
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

		// KV.Create is a request-reply call. A transient cluster condition
		// (raft leader flip, very brief partition) can fail either before
		// the server processed the request — safe to retry — or after, in
		// which case our retry observes ErrKeyExists for our own already-
		// landed write. Get-and-compare disambiguates: matching value means
		// our previous attempt's ack was the thing that got dropped.
		createErr := s.doWithTransientRetry(s.ctx, s.addJobRetryBudget, func(ctx context.Context) error {
			_, e := s.jobKV.Create(ctx, id, data)
			if errors.Is(e, jetstream.ErrKeyExists) {
				existing, getErr := s.jobKV.Get(ctx, id)
				if getErr == nil && bytes.Equal(existing.Value(), data) {
					return nil
				}
				return ErrJobAlreadyExists
			}
			return e
		})
		if createErr != nil {
			delete(s.jobs, id)
			if errors.Is(createErr, ErrJobAlreadyExists) {
				return ErrJobAlreadyExists
			}
			return fmt.Errorf("failed to save job to KV: %w", createErr)
		}

		// Publish scheduled message for the first execution
		if err := s.doWithTransientRetry(s.ctx, s.addJobRetryBudget, func(ctx context.Context) error {
			return s.publishScheduledMessage(ctx, id, nextRun)
		}); err != nil {
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
	createdAt := job.createdAt
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
			CreatedAt:      createdAt,
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
		if err := s.doWithTransientRetry(s.ctx, s.addJobRetryBudget, func(ctx context.Context) error {
			_, e := s.jobKV.Put(ctx, id, data)
			return e
		}); err != nil {
			job.mu.Lock()
			job.schedule = oldSchedule
			job.nextRun = oldNextRun
			job.mu.Unlock()
			return err
		}

		// Purge old scheduled messages and publish new one. Purge is
		// best-effort — even if it fails the per-message dedup window plus
		// the KV validation in handleMessage will suppress the previous
		// schedule's pending messages.
		subject := s.jobSubject(id)
		_ = s.doWithTransientRetry(s.ctx, s.addJobRetryBudget, func(ctx context.Context) error {
			return s.stream.Purge(ctx, jetstream.WithPurgeSubject(subject))
		})
		if err := s.doWithTransientRetry(s.ctx, s.addJobRetryBudget, func(ctx context.Context) error {
			return s.publishScheduledMessage(ctx, id, nextRun)
		}); err != nil {
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
		// This peer is stopping. Ack would delete the message from the
		// WorkQueue and nobody else would ever process it — the chain
		// would die until the reconciler advances NextRun on a future
		// sweep. Nak so another peer (multi-pod) can take it; in a
		// single-pod deployment the reconciler still heals via the
		// CAS-advance path within one sweep.
		_ = msg.Nak()
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

	job.mu.RLock()
	currentSchedule := job.schedule
	lastRun := job.lastRun
	job.mu.RUnlock()
	if _, ok := currentSchedule.(*OnceSchedule); ok && !lastRun.IsZero() {
		_ = msg.Ack()
		return
	}

	// KV is the cross-peer source of truth for schedule state. The in-memory
	// job cache on this peer may lag the KV (e.g. peer B sees a redelivery
	// for a job peer A finished and acked, or the stream still holds a stale
	// message left over from a previous run on this same peer). Both cases
	// look like "valid scheduled message" locally, so the only safe filter
	// is reading the authoritative KV state for the (jobID, scheduledAt)
	// being processed.
	//
	// Two conditions drop the message as superseded:
	//   - ScheduledAt is at or before KV.LastRun — already-executed delivery
	//     (typical after a peer crashed mid-handler and AckWait expired)
	//   - ScheduledAt is before KV.NextRun — earlier-tick delivery that has
	//     been recomputed past it (typical after restart where loadJobsFromKV
	//     moved NextRun forward)
	//
	// KV-read failure is non-fatal: we fall through to handling the message
	// normally. This preserves the previous behaviour during a brief KV-side
	// outage rather than silently dropping every delivery.
	if kvJV, kvErr := s.readJobValueFromKV(s.ctx, jobID); kvErr == nil {
		if !kvJV.LastRun.IsZero() && !schedMsg.ScheduledAt.After(kvJV.LastRun) {
			_ = msg.Ack()
			return
		}
		if !kvJV.NextRun.IsZero() && schedMsg.ScheduledAt.Before(kvJV.NextRun) {
			_ = msg.Ack()
			return
		}
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

	// Update in-memory job state. job.running stays true until the entire
	// post-handler section below finishes — see comment on the final
	// job.running = false below for why that matters for Stop().
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

	// Post-handler work must outlive Stop()'s context cancellation. If we
	// used s.ctx here, a SIGTERM mid-tick would silently fail to enqueue
	// the next scheduled message and break the recurring chain until the
	// reconciler picks it up on the next sweep.
	postCtx, postCancel := context.WithTimeout(context.Background(), s.postHandlerTimeout)
	defer postCancel()

	// Step 1: Persist the authoritative post-handler KV state IMMEDIATELY.
	// LastRun is set to the message's scheduledAt (not startTime) so that
	// any redelivery of this same (jobID, scheduledAt) — most commonly
	// after a peer crash and AckWait expiry — is identifiable by the next
	// handler invocation as already-executed and dropped by handleMessage's
	// KV validation. This compresses the "handler done but LastRun not yet
	// persisted" window to a single KV.Put RTT.
	if s.jobKV != nil {
		scheduleType, scheduleConfig, _ := s.codec.Encode(currentSchedule)
		jv := &natsJobValue{
			ID:             job.id,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Status:         string(status),
			NextRun:        nextRun,
			LastRun:        scheduledAt,
			CreatedAt:      job.createdAt,
			UpdatedAt:      endTime,
			Metadata:       copyMetadata(metadata),
		}
		data, _ := json.Marshal(jv)
		if _, err := s.jobKV.Put(postCtx, job.id, data); err != nil {
			s.logError("failed to persist job state",
				"job_id", job.id, "next_run", nextRun, "err", err)
		}
	}

	// Step 2: Save execution record to KV (best-effort).
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

	// Step 3: Publish the next-tick scheduled message (recurring schedules
	// only). If this fails the reconciler will republish on its next sweep,
	// so a transient publish failure no longer kills the chain silently.
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

	// Step 4: If the publish failed despite a successful handler, rewrite
	// the KV row so the persisted Status reflects "handler succeeded but
	// next-tick publish never landed". JobStatusReschedulingFailed is the
	// observability hook for reconciler dashboards / dead-letter handling.
	// When the handler itself failed, the handler error stays the primary
	// signal — we don't overwrite it with a reschedule status.
	if s.jobKV != nil && publishErr != nil && handlerErr == nil {
		scheduleType, scheduleConfig, _ := s.codec.Encode(currentSchedule)
		jv := &natsJobValue{
			ID:             job.id,
			ScheduleType:   scheduleType,
			ScheduleConfig: scheduleConfig,
			Status:         string(JobStatusReschedulingFailed),
			NextRun:        nextRun,
			LastRun:        scheduledAt,
			CreatedAt:      job.createdAt,
			UpdatedAt:      time.Now(),
			Metadata:       copyMetadata(metadata),
		}
		data, _ := json.Marshal(jv)
		if _, err := s.jobKV.Put(postCtx, job.id, data); err != nil {
			s.logError("failed to persist rescheduling-failed status",
				"job_id", job.id, "next_run", nextRun, "err", err)
		}
	}

	// Mark the job as no longer running only after all post-handler work is
	// done. waitForJobs() (called from Stop) only checks job.IsRunning(),
	// so resetting running earlier would let Stop return — and the caller
	// may then close the NATS connection — before the next-tick publish
	// completes.
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

// runReconciler periodically scans the job KV and resurrects any recurring
// job whose KV NextRun is older than the grace period. For each stale row it
// recomputes a fresh NextRun from the schedule, writes it back via a CAS
// (revision-guarded) update, and publishes a scheduled message at the new
// NextRun. Because the new scheduledAt yields a different deterministic
// Nats-Msg-Id, the republish bypasses the stream's Duplicates window — so
// even if a previous tick was acked-without-rescheduling (handler panic,
// silent drop, exhausted publish retries) the chain recovers within one
// sweep. The CAS guard ensures at most one writer advances NextRun even
// when multiple peers race; the losers skip and let the winner own the tick.
// Any in-flight stream message with the now-stale scheduledAt is then
// self-cleaned by handleMessage's KV validation (scheduledAt < KV.NextRun
// → ack-drop).
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

		// Advance NextRun by recomputing from the schedule against now.
		// Republishing the same KV.NextRun would collide on Nats-Msg-Id and
		// be silently suppressed by the Duplicates window — the very case
		// that left chains dead before. A fresh scheduledAt gives a fresh
		// Msg-Id and actually lands.
		now := time.Now()
		freshNext := schedule.Next(now)
		if freshNext.IsZero() || !freshNext.After(now) {
			// Defensive: schedule has nothing more to deliver (e.g. a
			// pathological cron). Leave the row for human inspection
			// rather than republishing forever.
			continue
		}
		staleNext := jv.NextRun
		jv.NextRun = freshNext
		jv.UpdatedAt = now
		data, mErr := json.Marshal(&jv)
		if mErr != nil {
			s.logError("reconciler: marshal advanced KV value failed",
				"job_id", jv.ID, "err", mErr)
			continue
		}
		// CAS via the revision we just read. If another peer (or this
		// peer's own executeJob) wrote a fresher value between our Get
		// and Update, the Update returns ErrKeyExists and we silently
		// skip — the racer already established a NextRun at least as
		// recent as ours, so the chain is alive.
		if _, err := s.jobKV.Update(ctx, key, data, entry.Revision()); err != nil {
			if !errors.Is(err, jetstream.ErrKeyExists) && ctx.Err() == nil {
				s.logError("reconciler: CAS update KV failed",
					"job_id", jv.ID, "next_run", freshNext, "err", err)
			}
			continue
		}
		if err := s.publishScheduledMessage(ctx, jv.ID, freshNext); err != nil {
			// Same shutdown-noise suppression as the Keys() path above.
			// KV is already advanced; the next reconciler sweep will
			// retry the publish if this one failed transiently.
			if ctx.Err() == nil {
				s.logError("reconciler: publish advanced next-tick failed",
					"job_id", jv.ID, "next_run", freshNext, "err", err)
			}
			continue
		}
		s.logError("reconciler: advanced stale next-tick",
			"job_id", jv.ID, "stale_next_run", staleNext, "fresh_next_run", freshNext)
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

// waitForJetStreamReady probes the JetStream metaleader with bounded
// AccountInfo requests until one succeeds, the overall timeout elapses, or
// ctx is cancelled.
//
// This is the fix for the multi-instance brand-new-deployment hang. nats.go
// documents on AccountInfo: "For clustered topologies, AccountInfo will
// time out." Every JetStream API used by NATSScheduler.Start /
// NATSStorage.Initialize (CreateOrUpdateKeyValue, CreateOrUpdateStream,
// CreateOrUpdateConsumer) calls AccountInfo internally — so until the
// metaleader is elected, the very first KV/stream create hangs for the
// caller's whole ctx deadline (commonly minutes).
//
// Each probe is capped at 2 s so a server that swallows the request without
// reply cannot consume the overall budget. Backoff doubles 100 ms → 500 ms.
// ErrJetStreamNotEnabled and ErrJetStreamNotEnabledForAccount are terminal
// (the cluster will not become ready by retrying) and returned immediately.
//
// Shared by NATSScheduler.Start and NATSStorage.Initialize so both gate any
// JetStream API call behind the same metaleader-ready check.
func waitForJetStreamReady(ctx context.Context, js jetstream.JetStream, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("jetstream not ready: %w (last probe: %v)", err, lastErr)
			}
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if lastErr != nil {
				return fmt.Errorf("jetstream metaleader not ready after %s: %w", timeout, lastErr)
			}
			return fmt.Errorf("jetstream metaleader not ready after %s", timeout)
		}
		callTimeout := 2 * time.Second
		if callTimeout > remaining {
			callTimeout = remaining
		}
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		_, err := js.AccountInfo(callCtx)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, jetstream.ErrJetStreamNotEnabled) ||
			errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount) {
			return err
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("jetstream not ready: %w (last probe: %v)", ctx.Err(), lastErr)
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

// startPhase runs a single Start-time NATS API call under a bounded
// sub-context (s.startPhaseTimeout), logging entry and exit through
// s.logger. The wrapped op receives a context whose deadline is the sooner
// of ctx and now + s.startPhaseTimeout. On failure the returned error is
// prefixed with the phase name so the caller can identify which API call
// blocked.
//
// When s.startPhaseTimeout <= 0 the helper uses ctx directly — legacy
// behaviour with no per-phase bound. The phase entry/exit logs still fire,
// which is useful for diagnosing multi-instance startup hangs.
//
// Use this for unbounded NATS API calls (CreateOrUpdate*, Consume).
// For phases that already manage their own timeout (waitForJetStreamReady,
// waitForBackingStreams) use startPhaseUnbounded so the outer cap doesn't
// silently override the user-tunable inner budget.
func (s *natsSchedulerImpl) startPhase(ctx context.Context, name string, op func(context.Context) error) error {
	s.logError("start: phase begin", "phase", name)
	start := time.Now()

	var callCtx context.Context
	var cancel context.CancelFunc
	if s.startPhaseTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, s.startPhaseTimeout)
	} else {
		callCtx, cancel = ctx, func() {}
	}
	defer cancel()

	err := op(callCtx)
	elapsed := time.Since(start)
	if err != nil {
		s.logError("start: phase failed", "phase", name, "elapsed", elapsed, "err", err)
		return fmt.Errorf("%s: %w", name, err)
	}
	s.logError("start: phase ok", "phase", name, "elapsed", elapsed)
	return nil
}

// startPhaseUnbounded is the variant of startPhase for phases that manage
// their own timeout internally (waitForJetStreamReady,
// waitForBackingStreams). It logs entry, exit, and elapsed time but does
// NOT wrap ctx with s.startPhaseTimeout — that would silently truncate the
// user-tunable inner budget when the two limits differ.
func (s *natsSchedulerImpl) startPhaseUnbounded(ctx context.Context, name string, op func(context.Context) error) error {
	s.logError("start: phase begin", "phase", name)
	start := time.Now()
	err := op(ctx)
	elapsed := time.Since(start)
	if err != nil {
		s.logError("start: phase failed", "phase", name, "elapsed", elapsed, "err", err)
		return fmt.Errorf("%s: %w", name, err)
	}
	s.logError("start: phase ok", "phase", name, "elapsed", elapsed)
	return nil
}

// doWithTransientRetry runs op until it succeeds, the budget elapses, or
// ctx is cancelled. Backoff doubles each attempt (100ms, 200ms, 400ms, 800ms)
// and is capped at 1s. All errors are retried; the caller is responsible for
// choosing a budget that matches "transient hiccup" rather than "real failure"
// (the default 5 s comfortably covers a JetStream raft re-election).
//
// budget <= 0 falls through to a single attempt — same behaviour as before
// the retry helper existed.
func (s *natsSchedulerImpl) doWithTransientRetry(ctx context.Context, budget time.Duration, op func(context.Context) error) error {
	if budget <= 0 {
		return op(ctx)
	}
	deadline := time.Now().Add(budget)
	backoff := 100 * time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		// Cap each attempt so a hung call cannot blow past the overall budget.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if lastErr != nil {
				return lastErr
			}
			return context.DeadlineExceeded
		}
		callCtx, cancel := context.WithTimeout(ctx, remaining)
		err := op(callCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't bother sleeping if there's no time left for another attempt.
		if time.Until(deadline) <= 0 {
			return lastErr
		}

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
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

// publishScheduledMessageAsync is the fire-and-forget counterpart used by
// loadJobsFromKV at startup. It enqueues the scheduled message via the
// JetStream client's async publish pipeline and returns immediately; the
// ack arrives later on the PubAckFuture, which we ignore here.
//
// Correctness is preserved by the same deterministic Nats-Msg-Id used by
// the sync path (the stream's Duplicates window suppresses any
// double-publish), and by the background reconciler, which will republish
// any job whose ack never arrived.
func (s *natsSchedulerImpl) publishScheduledMessageAsync(jobID string, scheduledAt time.Time) error {
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

	_, err = s.js.PublishMsgAsync(msg)
	return err
}

// jobSubject returns the NATS subject for a given job ID.
func (s *natsSchedulerImpl) jobSubject(jobID string) string {
	return s.subjectPrefix + ".jobs." + jobID
}

// readJobValueFromKV fetches and JSON-decodes the natsJobValue for a job id.
// Used by handleMessage to validate scheduledAt against the authoritative KV
// LastRun/NextRun without paying for schedule decoding.
func (s *natsSchedulerImpl) readJobValueFromKV(ctx context.Context, id string) (*natsJobValue, error) {
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
	return &jv, nil
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
		id:        jv.ID,
		schedule:  schedule,
		metadata:  copyMetadata(jv.Metadata),
		nextRun:   jv.NextRun,
		lastRun:   jv.LastRun,
		createdAt: jv.CreatedAt,
	}, nil
}

// loadJobsFromKV loads persisted jobs from KV store and publishes scheduled
// messages for them.
//
// Hot-path optimisation: instead of a single goroutine doing
// Get → decode → Update → sync-Publish per key, we spawn a small worker pool
// to parallelize the KV round-trips and fire each scheduled message via the
// JetStream client's async publish pipeline. For N persisted jobs this
// brings Start() from O(N * RTT) down to roughly O((N / concurrency) * RTT)
// for the reads, with the publishes effectively overlapped behind them.
//
// Concurrency control:
//   - Worker count is min(s.loadJobsConcurrency, len(keys)); for small key
//     sets we never spawn more workers than there is work.
//   - Workers run per-key processing concurrently. They never touch s.jobs
//     directly; results flow through jobCh to a single collector goroutine
//     that owns the map writes. This avoids needing a separate lock — Start
//     already holds s.mu exclusively for the duration of loadJobsFromKV.
//   - Async publishes are throttled by the JetStream client's built-in
//     max-pending buffer (PublishMsgAsync blocks once that fills up), so
//     no separate semaphore is needed for that stage.
//   - After workers finish, we wait up to loadJobsAsyncPublishTimeout for
//     PublishAsyncComplete to fire. A timeout is non-fatal: the reconciler
//     republishes any straggler that never acked.
func (s *natsSchedulerImpl) loadJobsFromKV(ctx context.Context) error {
	keys, err := s.jobKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	now := time.Now()

	concurrency := s.loadJobsConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(keys) {
		concurrency = len(keys)
	}

	keyCh := make(chan string, concurrency)
	jobCh := make(chan *jobImpl, concurrency)

	var workerWG sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for key := range keyCh {
				if job := s.loadAndPublishOneJob(ctx, key, now); job != nil {
					jobCh <- job
				}
			}
		}()
	}

	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		for job := range jobCh {
			s.jobs[job.id] = job
		}
	}()

	var feedErr error
feedLoop:
	for _, key := range keys {
		select {
		case <-ctx.Done():
			feedErr = ctx.Err()
			break feedLoop
		case keyCh <- key:
		}
	}
	close(keyCh)
	workerWG.Wait()
	close(jobCh)
	<-collectDone

	if feedErr != nil {
		return feedErr
	}

	// Drain in-flight async publishes. A timeout is non-fatal — the
	// background reconciler will republish any job whose ack never arrived,
	// dedupe header on the message keeps it safe.
	if s.loadJobsAsyncPublishTimeout > 0 {
		select {
		case <-s.js.PublishAsyncComplete():
		case <-time.After(s.loadJobsAsyncPublishTimeout):
			s.logError("startup: timed out waiting for async publish drain",
				"timeout", s.loadJobsAsyncPublishTimeout,
				"pending", s.js.PublishAsyncPending())
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// loadAndPublishOneJob handles a single KV key during startup: read the
// entry, decode the schedule, recompute NextRun if it's already past, write
// the recomputed value back to KV under CAS, and fire-and-forget the next
// scheduled message via PublishMsgAsync. Returns the jobImpl to install
// into s.jobs, or nil if the entry was unreadable / undecodable.
func (s *natsSchedulerImpl) loadAndPublishOneJob(ctx context.Context, key string, now time.Time) *jobImpl {
	entry, err := s.jobKV.Get(ctx, key)
	if err != nil {
		s.logError("startup: failed to read job KV entry",
			"key", key, "err", err)
		return nil
	}

	var jv natsJobValue
	if err := json.Unmarshal(entry.Value(), &jv); err != nil {
		s.logError("startup: failed to decode job KV entry",
			"key", key, "err", err)
		return nil
	}

	schedule, err := s.codec.Decode(jv.ScheduleType, jv.ScheduleConfig)
	if err != nil {
		s.logError("startup: failed to decode schedule",
			"job_id", jv.ID, "schedule_type", jv.ScheduleType, "err", err)
		return nil
	}

	// Skip completed one-time schedules: if the run time has passed,
	// the job was already executed and should not be rescheduled.
	if onceSchedule, ok := schedule.(*OnceSchedule); ok {
		if !onceSchedule.RunAt().After(now) {
			return &jobImpl{
				id:        jv.ID,
				schedule:  schedule,
				metadata:  copyMetadata(jv.Metadata),
				nextRun:   onceSchedule.RunAt(),
				lastRun:   jv.LastRun,
				createdAt: jv.CreatedAt,
			}
		}
	}

	nextRun := jv.NextRun
	recomputed := false
	if nextRun.Before(now) {
		nextRun = schedule.Next(now)
		recomputed = true
	}

	job := &jobImpl{
		id:        jv.ID,
		schedule:  schedule,
		metadata:  copyMetadata(jv.Metadata),
		nextRun:   nextRun,
		lastRun:   jv.LastRun,
		createdAt: jv.CreatedAt,
	}

	// Persist the recomputed NextRun back to KV so handleMessage's
	// KV-based stale-msg check (scheduledAt < KV.NextRun) can reject
	// any stream-resident message left over from the previous run.
	//
	// CAS-guarded with the revision we read above: if another peer (or
	// this peer's own handler completing concurrently) wrote a fresher
	// value between our Get and Update, the Update returns ErrKeyExists
	// (same JSErrCodeStreamWrongLastSequence wrapper Create uses) and
	// we skip silently — the racer already established a NextRun at
	// least as recent as ours, which is exactly what the stale-msg
	// guard needs. Falling back to Put would clobber LastRun/Status
	// written by the racer.
	if recomputed {
		jv.NextRun = nextRun
		jv.UpdatedAt = now
		if data, mErr := json.Marshal(&jv); mErr == nil {
			if _, pErr := s.jobKV.Update(ctx, jv.ID, data, entry.Revision()); pErr != nil {
				if !errors.Is(pErr, jetstream.ErrKeyExists) {
					s.logError("startup: failed to persist recomputed next-run to KV",
						"job_id", jv.ID, "next_run", nextRun, "err", pErr)
				}
			}
		} else {
			s.logError("startup: failed to marshal recomputed job value",
				"job_id", jv.ID, "err", mErr)
		}
	}

	if err := s.publishScheduledMessageAsync(jv.ID, nextRun); err != nil {
		s.logError("startup: failed to enqueue async scheduled message",
			"job_id", jv.ID, "next_run", nextRun, "err", err)
	}
	return job
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
