package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// defaultOnceLockBucket is the JetStream KV bucket the built-in Once
	// uses to coordinate first-time provisioning across processes.
	defaultOnceLockBucket = "SCHEDULER_LOCKS"

	// onceLeaseTTL bounds how long a leader's lock entry survives without a
	// heartbeat. A crashed leader's lock auto-expires within this window so
	// a follower can take over and re-run fn. The same TTL applies to the
	// "done" sentinel — once it expires, a much later restart pays one
	// idempotent re-run of fn, which is the intended cost of keeping the
	// implementation to a single bucket.
	onceLeaseTTL = 30 * time.Second

	// onceHeartbeatInterval is how often the leader refreshes its lock
	// entry while fn is running. Three beats per TTL is enough headroom for
	// scheduling jitter on the leader without flooding the bucket.
	onceHeartbeatInterval = 10 * time.Second

	// onceBootstrapTimeout caps the single CreateOrUpdateKeyValue call that
	// brings the lock bucket itself into existence. This is the only step
	// Once cannot serialize (there is no lock bucket yet to lock against).
	// nats.go's new jetstream API has no internal per-call timeout, so
	// concurrent first-time callers that lose the server's reply
	// coalescing would otherwise hang to the caller's ctx deadline.
	// Bounded recovery: on timeout we fall back to a plain KeyValue
	// lookup, which sees the bucket the winning peer just created.
	onceBootstrapTimeout = 5 * time.Second

	// onceLookupTimeout caps each fallback js.KeyValue lookup so a single
	// unreachable metadata read can't consume the caller's whole ctx.
	onceLookupTimeout = 5 * time.Second

	// onceWatchPollFallback is how often a waiting follower polls the
	// lock/sentinel state in case kv.Watch silently drops an event (a
	// belt-and-suspenders backstop, not the primary signal).
	onceWatchPollFallback = 2 * time.Second

	onceLockKeyPrefix = "lock."
	onceDoneKeyPrefix = "done."

	// defaultSchedulerOnceKey / defaultStorageOnceKey are the keys this
	// package hands to its OnceFunc — one Once block per component
	// wraps that component's full first-deploy provisioning (all the
	// CreateOrUpdate* calls together; they're idempotent so combining
	// them under one key is safe).
	//
	// The built-in onceStore uses a private lock bucket so the keys are
	// cosmetic by default; they matter when a caller injects a shared
	// distributed-lock substrate via WithOnce, in which case these keep
	// scheduler's lock entries from colliding with other modules'.
	// Override via WithOnceKey / WithNATSStorageOnceKey.
	//
	// Scheduler and Storage MUST use distinct keys: each side's fn
	// provisions a different resource set (Storage: 2 KVs; Scheduler:
	// 2 KVs + stream + consumer). Sharing the same key under one
	// OnceFunc would let the first runner write the sentinel and the
	// second silently skip its own setup. Sharing the OnceFunc itself
	// (so both sides serialize through the same lock substrate) is
	// fine, and recommended in cluster deployments.
	defaultSchedulerOnceKey = "scheduler.init"
	defaultStorageOnceKey   = "scheduler.storage-init"
)

// OnceFunc serializes execution of fn across processes that share the same
// distributed-coordination substrate. The contract:
//
//   - Exactly one caller (per key, per fn-success cycle) runs fn.
//   - Concurrent callers block until that fn returns, then return nil.
//   - Subsequent callers (after fn has succeeded) return nil without
//     running fn.
//
// Implementations may treat the "fn has succeeded" memory as bounded
// (e.g. expiring after some TTL); callers must therefore make fn
// idempotent so a delayed late-joiner that re-runs fn does not corrupt
// state. CreateOrUpdateKeyValue / CreateOrUpdateStream are idempotent in
// this sense, which is why Once is the right primitive for serializing
// scheduler bootstrap.
//
// Callers can inject a custom OnceFunc via WithOnce to share a lock
// substrate with other modules; otherwise the scheduler uses its own
// built-in JetStream-KV-backed implementation.
type OnceFunc func(ctx context.Context, key string, fn func(context.Context) error) error

// WithOnce installs a caller-provided distributed-once implementation. By
// default the scheduler uses an internal JetStream-KV-backed Once. Pass a
// custom OnceFunc when you already have a distributed lock service (e.g.
// the one your platform's other modules use) and want the scheduler to
// share it so all first-deploy provisioning serializes through a single
// substrate.
func WithOnce(fn OnceFunc) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.once = fn
	}
}

// WithOnceLockBucket overrides the JetStream KV bucket name used by the
// built-in Once. Has no effect when WithOnce has installed a custom
// implementation. Defaults to SCHEDULER_LOCKS.
func WithOnceLockBucket(name string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.onceLockBucket = name
	}
}

// WithOnceKey overrides the key handed to OnceFunc when serializing the
// scheduler's first-deploy provisioning. Defaults to "scheduler.init".
// Override when sharing a distributed-lock substrate with other modules
// and your global naming scheme expects a different value.
func WithOnceKey(key string) NATSSchedulerOption {
	return func(s *natsSchedulerImpl) {
		s.onceKey = key
	}
}

// onceStore is the built-in JetStream-KV-backed implementation of
// OnceFunc. It uses a single lock bucket with a short TTL: lock entries
// auto-release when their lease expires (covering crashed leaders);
// "done" sentinels share the same TTL on the assumption that any pod
// joining long after the original Once cycle can safely re-run fn
// (idempotent CreateOrUpdate calls). This keeps the implementation to one
// bucket rather than two (lock-with-TTL + sentinel-without).
type onceStore struct {
	js     jetstream.JetStream
	bucket string
	nodeID string

	initMu sync.Mutex
	kv     jetstream.KeyValue
}

func newOnceStore(js jetstream.JetStream, bucket string) *onceStore {
	if bucket == "" {
		bucket = defaultOnceLockBucket
	}
	return &onceStore{
		js:     js,
		bucket: bucket,
		nodeID: randomNodeID(),
	}
}

// Do implements OnceFunc against the lock bucket. See OnceFunc for the
// contract. The retry loop covers two recoverable cases: (a) a previous
// leader crashed before writing the sentinel — the lock entry's TTL
// expires and we re-acquire; (b) a previous leader's fn returned an
// error and it released the lock without writing a sentinel — we
// re-acquire and rerun. Genuine fn errors propagate to the calling
// leader (the one that just ran fn), not to followers.
func (o *onceStore) Do(ctx context.Context, key string, fn func(context.Context) error) error {
	kv, err := o.ensureBucket(ctx)
	if err != nil {
		return err
	}

	lockKey := onceLockKeyPrefix + key
	doneKey := onceDoneKeyPrefix + key

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Sentinel fast path: someone has already finished fn. Most pods
		// after the very first deploy reach this branch and return
		// immediately without touching the lock entry.
		if _, err := kv.Get(ctx, doneKey); err == nil {
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("once: read sentinel %s: %w", doneKey, err)
		}

		// Try to acquire the lock. kv.Create is CAS-like: it succeeds only
		// when the key does not currently exist (or has TTL'd out).
		leaseBytes, err := encodeLease(o.nodeID)
		if err != nil {
			return err
		}
		rev, err := kv.Create(ctx, lockKey, leaseBytes)
		if err == nil {
			return o.runAsLeader(ctx, kv, key, rev, fn)
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return fmt.Errorf("once: acquire lock %s: %w", lockKey, err)
		}

		// Lock held by another peer. Wait until it releases (delete event,
		// TTL expiry, or sentinel appears), then loop back to recheck.
		if waitErr := o.waitForRelease(ctx, kv, lockKey, doneKey); waitErr != nil {
			return waitErr
		}
	}
}

// runAsLeader runs fn while extending the lock lease via a background
// heartbeat. On success it writes a permanent (TTL'd, see package docs)
// sentinel and releases the lock; on failure it releases the lock so a
// follower can retry.
func (o *onceStore) runAsLeader(parent context.Context, kv jetstream.KeyValue, key string, initialRev uint64, fn func(context.Context) error) error {
	lockKey := onceLockKeyPrefix + key
	doneKey := onceDoneKeyPrefix + key

	hbCtx, cancelHB := context.WithCancel(parent)
	defer cancelHB()

	var (
		hbMu   sync.Mutex
		hbRev  = initialRev
		hbDone = make(chan struct{})
	)
	go func() {
		defer close(hbDone)
		ticker := time.NewTicker(onceHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				lease, err := encodeLease(o.nodeID)
				if err != nil {
					return
				}
				hbMu.Lock()
				rev := hbRev
				hbMu.Unlock()
				// Update under hbCtx so cancelHB() unblocks any in-flight
				// heartbeat instead of waiting for the request to complete.
				newRev, err := kv.Update(hbCtx, lockKey, lease, rev)
				if err != nil {
					// Lost the lease (TTL expired, another peer grabbed it, or
					// transient transport error). Stop heartbeating; the main
					// goroutine will detect via the sentinel/release on its own
					// next acquire cycle.
					return
				}
				hbMu.Lock()
				hbRev = newRev
				hbMu.Unlock()
			}
		}
	}()

	fnErr := fn(parent)

	cancelHB()
	<-hbDone

	hbMu.Lock()
	finalRev := hbRev
	hbMu.Unlock()

	if fnErr != nil {
		// Release the lock so the next caller can retry. CAS-Purge with the
		// last revision we owned so we never delete a lock entry another peer
		// took over after our lease expired. Followers observe the delete
		// event on their watcher either way.
		_ = kv.Purge(parent, lockKey, jetstream.LastRevision(finalRev))
		return fnErr
	}

	// Sentinel must land before the lock release so a follower that wakes
	// from the watcher delete event observes "done" on its re-check.
	if _, putErr := kv.Put(parent, doneKey, []byte(o.nodeID)); putErr != nil {
		// fn succeeded but we could not record it. Don't release the lock —
		// let it TTL out, which makes the next caller re-run fn. Safe
		// because fn is idempotent by contract.
		return fmt.Errorf("once: write sentinel %s: %w", doneKey, putErr)
	}
	_ = kv.Purge(parent, lockKey, jetstream.LastRevision(finalRev))
	return nil
}

// waitForRelease blocks until the lock entry disappears (purge, delete,
// or TTL) or the sentinel appears. Uses kv.Watch as the primary signal
// with a polling backstop in case the watcher misses an event.
func (o *onceStore) waitForRelease(ctx context.Context, kv jetstream.KeyValue, lockKey, doneKey string) error {
	watcher, err := kv.Watch(ctx, lockKey, jetstream.UpdatesOnly())
	if err != nil {
		return o.pollUntilFree(ctx, kv, lockKey, doneKey)
	}
	defer watcher.Stop()

	poll := time.NewTicker(onceWatchPollFallback)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			if released, err := o.checkReleased(ctx, kv, lockKey, doneKey); err != nil {
				return err
			} else if released {
				return nil
			}
		case ev, ok := <-watcher.Updates():
			if !ok {
				return nil
			}
			if ev == nil {
				continue
			}
			switch ev.Operation() {
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				return nil
			}
			// KeyValuePut (heartbeat or re-acquire) — keep waiting.
		}
	}
}

func (o *onceStore) pollUntilFree(ctx context.Context, kv jetstream.KeyValue, lockKey, doneKey string) error {
	ticker := time.NewTicker(onceWatchPollFallback)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if released, err := o.checkReleased(ctx, kv, lockKey, doneKey); err != nil {
				return err
			} else if released {
				return nil
			}
		}
	}
}

func (o *onceStore) checkReleased(ctx context.Context, kv jetstream.KeyValue, lockKey, doneKey string) (bool, error) {
	if _, err := kv.Get(ctx, doneKey); err == nil {
		return true, nil
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, fmt.Errorf("once: poll sentinel %s: %w", doneKey, err)
	}
	if _, err := kv.Get(ctx, lockKey); errors.Is(err, jetstream.ErrKeyNotFound) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("once: poll lock %s: %w", lockKey, err)
	}
	return false, nil
}

// ensureBucket lazy-creates the lock bucket. This single
// CreateOrUpdateKeyValue is the only call Once cannot itself serialize —
// there is no lock bucket yet to lock against. We bound the call with
// onceBootstrapTimeout so a concurrent first-deploy peer that loses the
// server's reply coalescing recovers via the lookup fallback instead of
// hanging to the caller's ctx deadline.
func (o *onceStore) ensureBucket(parent context.Context) (jetstream.KeyValue, error) {
	o.initMu.Lock()
	defer o.initMu.Unlock()
	if o.kv != nil {
		return o.kv, nil
	}

	createCtx, cancel := context.WithTimeout(parent, onceBootstrapTimeout)
	kv, createErr := o.js.CreateOrUpdateKeyValue(createCtx, jetstream.KeyValueConfig{
		Bucket:      o.bucket,
		TTL:         onceLeaseTTL,
		Description: "scheduler distributed-once coordination",
	})
	cancel()
	if createErr == nil {
		o.kv = kv
		return kv, nil
	}

	// Concurrent first-create likely lost the reply race. Try a plain
	// lookup — by now the winning peer's create has propagated.
	lookupCtx, cancel := context.WithTimeout(parent, onceLookupTimeout)
	kv, lookupErr := o.js.KeyValue(lookupCtx, o.bucket)
	cancel()
	if lookupErr == nil {
		o.kv = kv
		return kv, nil
	}
	return nil, fmt.Errorf("once: bootstrap lock bucket %s: %w (lookup also failed: %v)", o.bucket, createErr, lookupErr)
}

type onceLease struct {
	NodeID      string    `json:"node_id"`
	AcquiredAt  time.Time `json:"acquired_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

func encodeLease(nodeID string) ([]byte, error) {
	now := time.Now().UTC()
	return json.Marshal(onceLease{
		NodeID:      nodeID,
		AcquiredAt:  now,
		HeartbeatAt: now,
	})
}

func randomNodeID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely unlikely; fall back to a time-based identifier so
		// node IDs remain reasonably unique even if /dev/urandom is
		// pathological.
		return fmt.Sprintf("nodeid-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
