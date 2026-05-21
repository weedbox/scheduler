package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Replica defaults applied when the caller does not set cfg.Replicas
// explicitly. Targets a standard 3-node JetStream cluster so newly created
// streams / KV buckets are highly available by default. On a deployment
// that cannot sustain three replicas (typically a single-node cluster
// returning err_code 10074 "insufficient resources") the convergent
// helpers transparently fall back to one replica; if the cluster later
// grows, EnsureReplicaScale opportunistically promotes the existing
// resource back toward the default.
//
// Existing streams keep whatever replica count they were originally
// created with — these constants only affect resources created (or
// scaled) through the convergent helpers.
const (
	defaultReplicas  = 3
	fallbackReplicas = 1
)

const (
	convergentBaseBackoff = 200 * time.Millisecond
	convergentMaxBackoff  = 2 * time.Second
)

// EnsureKVConvergent provisions a JetStream KV bucket and blocks until the
// underlying KV_<bucket> stream is ready for reads/writes, or ctx is done.
//
// Designed to be safe under multi-process cold start without external
// coordination:
//
//   - The NATS server's meta-leader serializes CreateOrUpdateKeyValue by
//     bucket name, so concurrent callers either all match the same config
//     (idempotent — every caller succeeds) or one peer wins and the others
//     surface "bucket already exists" (recoverable, loops back to verify
//     ready).
//   - Transient cold-start errors (no responders, leader-flap, "no response
//     from stream", etc.) are absorbed by the retry loop.
//   - "Insufficient peers" demotes cfg.Replicas to 1 and retries, which
//     lets a single-node deployment provision the same bucket without the
//     caller having to know the cluster topology. Once the bucket is
//     ready EnsureReplicaScale runs a best-effort promotion toward the
//     original Replicas, which makes a cluster that later grows from
//     1-node to 3-node self-heal on the next call.
//
// ctx is the only deadline. Aborts immediately on ctx cancellation. Returns
// the resolved KeyValue handle on success.
//
// logger may be nil; otherwise it receives structured-log lines on
// non-trivial transitions (insufficient-peers fallback, scale-up result,
// final replica scale failure).
func EnsureKVConvergent(ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig, logger NATSSchedulerLogger) (jetstream.KeyValue, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("EnsureKVConvergent: bucket required")
	}

	desiredReplicas := cfg.Replicas
	if desiredReplicas <= 0 {
		desiredReplicas = defaultReplicas
		cfg.Replicas = defaultReplicas
	}

	backoff := convergentBaseBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		kv, err := js.CreateOrUpdateKeyValue(ctx, cfg)
		switch {
		case err == nil:
			if isJetStreamKVReady(ctx, js, cfg.Bucket) {
				EnsureReplicaScale(ctx, js, "KV_"+cfg.Bucket, desiredReplicas, logger)
				return kv, nil
			}
			// Bucket exists but underlying stream not yet publishable.
			// Loop back to retry the readiness check.

		case isInsufficientPeers(err) && cfg.Replicas > fallbackReplicas:
			logEvent(logger, "convergent: KV create rejected at default replica count, falling back to single replica",
				"bucket", cfg.Bucket,
				"attempted_replicas", cfg.Replicas,
				"err", err)
			cfg.Replicas = fallbackReplicas

		case isAlreadyInUse(err) || isTransientJetStreamError(err):
			// peer is mid-create or burst-induced transient — verify ready.

		default:
			return nil, fmt.Errorf("create-or-update kv bucket %s: %w", cfg.Bucket, err)
		}

		if err := sleepBackoff(ctx, &backoff); err != nil {
			return nil, err
		}
	}
}

// EnsureStreamConvergent provisions a JetStream stream and blocks until it
// is publishable (leader elected, replicas current), or ctx is done.
//
// Same multi-process-safe semantics as EnsureKVConvergent: server-side
// meta-leader serialization by stream name, transient retry, insufficient-
// peers fallback to single replica, post-ready opportunistic promotion via
// EnsureReplicaScale.
//
// Returns the resolved Stream handle.
func EnsureStreamConvergent(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, logger NATSSchedulerLogger) (jetstream.Stream, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("EnsureStreamConvergent: stream name required")
	}

	desiredReplicas := cfg.Replicas
	if desiredReplicas <= 0 {
		desiredReplicas = defaultReplicas
		cfg.Replicas = defaultReplicas
	}

	backoff := convergentBaseBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		stream, err := js.CreateOrUpdateStream(ctx, cfg)
		switch {
		case err == nil:
			if isStreamReady(ctx, stream) {
				EnsureReplicaScale(ctx, js, cfg.Name, desiredReplicas, logger)
				return stream, nil
			}

		case isInsufficientPeers(err) && cfg.Replicas > fallbackReplicas:
			logEvent(logger, "convergent: stream create rejected at default replica count, falling back to single replica",
				"stream", cfg.Name,
				"attempted_replicas", cfg.Replicas,
				"err", err)
			cfg.Replicas = fallbackReplicas

		case isAlreadyInUse(err) || isTransientJetStreamError(err):

		default:
			return nil, fmt.Errorf("create-or-update stream %s: %w", cfg.Name, err)
		}

		if err := sleepBackoff(ctx, &backoff); err != nil {
			return nil, err
		}
	}
}

// EnsureConsumerConvergent provisions a durable consumer on the given
// stream, retrying through transient cold-start errors. Consumers inherit
// the parent stream's replica count, so there is no replica fallback here —
// the call either succeeds, surfaces a permanent config error, or loops
// against transient failures until ctx is done.
func EnsureConsumerConvergent(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig, logger NATSSchedulerLogger) (jetstream.Consumer, error) {
	if stream == nil {
		return nil, fmt.Errorf("EnsureConsumerConvergent: stream handle required")
	}

	backoff := convergentBaseBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
		switch {
		case err == nil:
			return consumer, nil

		case isAlreadyInUse(err) || isTransientJetStreamError(err):

		default:
			return nil, fmt.Errorf("create-or-update consumer %s: %w", cfg.Durable, err)
		}

		if err := sleepBackoff(ctx, &backoff); err != nil {
			return nil, err
		}
	}
}

// EnsureReplicaScale opportunistically upgrades an existing stream's
// replica count toward `desired` when the cluster has grown since the
// stream was originally created (e.g., a previously single-node deployment
// scaled out to three nodes). Best-effort: any failure is logged at warn
// level and swallowed so it cannot block the caller's startup — the next
// call will retry.
//
// Skips silently when: ctx already cancelled, desired ≤ 1 (nothing to scale
// to), the stream has no Cluster info (single-node server), current replica
// count already ≥ desired, or the server reports insufficient peers (the
// cluster is still too small).
//
// Multi-process safe: the server's meta-leader serializes UpdateStream by
// name and the call is idempotent for matching cfg. If two peers race the
// scale-up, the second is a no-op.
func EnsureReplicaScale(ctx context.Context, js jetstream.JetStream, streamName string, desired int, logger NATSSchedulerLogger) {
	if desired <= 1 {
		return
	}
	if ctx.Err() != nil {
		return
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return
	}
	info, err := stream.Info(ctx)
	if err != nil || info == nil {
		return
	}
	if info.Cluster == nil {
		return
	}
	current := 1 + len(info.Cluster.Replicas)
	if current >= desired {
		return
	}

	cfg := info.Config
	cfg.Replicas = desired
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		if isInsufficientPeers(err) || ctx.Err() != nil {
			return
		}
		logEvent(logger, "convergent: failed to scale stream replicas; will retry on next start",
			"stream", streamName,
			"from", current,
			"to", desired,
			"err", err)
		return
	}
	logEvent(logger, "convergent: scaled stream replicas",
		"stream", streamName,
		"from", current,
		"to", desired)
}

// isStreamReady reports whether a JetStream stream can accept publishes
// right now. For a clustered stream this means an elected leader and all
// replica peers caught up and online. For a non-clustered (single-node)
// stream a non-nil StreamInfo is sufficient.
func isStreamReady(ctx context.Context, stream jetstream.Stream) bool {
	if stream == nil {
		return false
	}
	info, err := stream.Info(ctx)
	if err != nil || info == nil {
		return false
	}
	if info.Cluster == nil {
		return true
	}
	if info.Cluster.Leader == "" {
		return false
	}
	for _, r := range info.Cluster.Replicas {
		if r == nil || r.Offline || !r.Current {
			return false
		}
	}
	return true
}

// isJetStreamKVReady verifies the KV_<bucket> stream that backs a KV
// bucket is publishable. Wraps isStreamReady against the underlying stream
// lookup; absorbs not-found / transient errors as "not ready yet" so the
// outer loop can retry.
func isJetStreamKVReady(ctx context.Context, js jetstream.JetStream, bucket string) bool {
	stream, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return false
	}
	return isStreamReady(ctx, stream)
}

// isAlreadyInUse reports whether err indicates a concurrent peer has
// already created the stream / bucket / consumer with the same name. Not
// transient (the server-side state is stable) but recoverable — the
// caller's job is to fall through to a readiness verification on the
// existing resource.
func isAlreadyInUse(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "stream name already in use"),
		strings.Contains(s, "stream already exists"),
		strings.Contains(s, "bucket already exists"),
		strings.Contains(s, "consumer already exists"),
		strings.Contains(s, "name already in use"):
		return true
	}
	return false
}

// isInsufficientPeers reports whether err indicates the cluster lacks
// enough peers to satisfy the requested replica count (e.g., Replicas=3
// against a single-node deployment). err_code 10074 ("insufficient
// resources") is the canonical code; substring matches cover both the
// classic and new-API wrapped error shapes.
func isInsufficientPeers(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "no suitable peers"),
		strings.Contains(s, "insufficient peers"),
		strings.Contains(s, "insufficient resources"),
		strings.Contains(s, "10074"):
		return true
	}
	return false
}

// isTransientJetStreamError reports whether err is a cold-start /
// flapping-cluster error worth retrying rather than surfacing to the
// caller. Covers both the new jetstream sentinels and the wrapped error
// strings the server returns through the API.
//
// Deliberately excludes "insufficient peers" / "no suitable peers" /
// err_code 10074 — those are placement failures, handled separately by
// the replica-fallback path so the retry budget isn't burned on a state
// the cluster cannot recover from.
func isTransientJetStreamError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrNoStreamResponse) {
		return true
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "no responders"),
		strings.Contains(s, "no response from stream"),
		strings.Contains(s, "context deadline exceeded"),
		strings.Contains(s, "leadership transferred"),
		strings.Contains(s, "JetStream system temporarily unavailable"),
		strings.Contains(s, "system temporarily unavailable"),
		strings.Contains(s, "stream is offline"),
		strings.Contains(s, "stream not currently available"):
		return true
	}
	return false
}

// sleepBackoff sleeps for the current backoff window, doubles it (capped
// at convergentMaxBackoff), and returns ctx.Err() if ctx is done.
func sleepBackoff(ctx context.Context, backoff *time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(*backoff):
	}
	if *backoff < convergentMaxBackoff {
		*backoff *= 2
		if *backoff > convergentMaxBackoff {
			*backoff = convergentMaxBackoff
		}
	}
	return nil
}

// logEvent emits a structured log line through the caller's logger, if
// installed. No-op when logger is nil.
func logEvent(logger NATSSchedulerLogger, msg string, kv ...any) {
	if logger == nil {
		return
	}
	logger(msg, kv...)
}
