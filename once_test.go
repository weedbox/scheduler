package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOnce_RunsExactlyOnce: many concurrent Do calls on the same key
// must run fn exactly once. This is the bedrock guarantee scheduler
// Start relies on to avoid the multi-pod first-deploy
// CreateOrUpdateKeyValue reply-path race.
func TestOnce_RunsExactlyOnce(t *testing.T) {
	_, js := startNATSServer(t)
	store := newOnceStore(js, "TEST_ONCE_RUNS")

	const callers = 16
	var (
		runs    atomic.Int32
		wg      sync.WaitGroup
		results = make([]error, callers)
	)
	wg.Add(callers)
	for i := range callers {
		go func(idx int) {
			defer wg.Done()
			results[idx] = store.Do(context.Background(), "provision", func(ctx context.Context) error {
				runs.Add(1)
				// Hold the lock long enough that followers actually wait.
				time.Sleep(150 * time.Millisecond)
				return nil
			})
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		assert.NoErrorf(t, err, "caller %d failed", i)
	}
	assert.Equal(t, int32(1), runs.Load(), "fn must run exactly once across all callers")
}

// TestOnce_PropagatesLeaderError: the leader's fn error reaches the
// leader's caller, and a subsequent Do retries (no sentinel was written).
func TestOnce_PropagatesLeaderError(t *testing.T) {
	_, js := startNATSServer(t)
	store := newOnceStore(js, "TEST_ONCE_ERR")

	sentinelErr := errors.New("provision failed")
	var runs atomic.Int32

	// First Do: fn errors.
	err := store.Do(context.Background(), "k", func(ctx context.Context) error {
		runs.Add(1)
		return sentinelErr
	})
	assert.ErrorIs(t, err, sentinelErr)

	// Second Do: should retry, see no sentinel, and run fn again.
	err = store.Do(context.Background(), "k", func(ctx context.Context) error {
		runs.Add(1)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(2), runs.Load())

	// Third Do: sentinel now exists, fn must not run again.
	err = store.Do(context.Background(), "k", func(ctx context.Context) error {
		runs.Add(1)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(2), runs.Load())
}

// TestOnce_SentinelFastPath: after fn succeeds, later callers must
// short-circuit on the sentinel without re-running fn or holding the
// lock. This is the steady-state behaviour every subsequent boot relies
// on.
func TestOnce_SentinelFastPath(t *testing.T) {
	_, js := startNATSServer(t)
	store := newOnceStore(js, "TEST_ONCE_FAST")

	var runs atomic.Int32
	for range 5 {
		err := store.Do(context.Background(), "k", func(ctx context.Context) error {
			runs.Add(1)
			return nil
		})
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), runs.Load())
}

// TestOnce_ContextCanceled: a caller's ctx cancellation propagates out
// of Do without leaking goroutines or leaving the lock held.
func TestOnce_ContextCanceled(t *testing.T) {
	_, js := startNATSServer(t)
	store := newOnceStore(js, "TEST_ONCE_CTX")

	// First Do acquires the lock and runs a slow fn.
	released := make(chan struct{})
	go func() {
		defer close(released)
		_ = store.Do(context.Background(), "k", func(ctx context.Context) error {
			time.Sleep(300 * time.Millisecond)
			return nil
		})
	}()

	// Give the leader a chance to acquire.
	time.Sleep(50 * time.Millisecond)

	// Second Do races with a short ctx that will expire before the
	// leader releases.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := store.Do(ctx, "k", func(ctx context.Context) error {
		t.Fatal("follower's fn must not run")
		return nil
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	<-released
}

// TestNATSScheduler_ConcurrentStartSerializedByOnce: this is the
// end-to-end check that proves Once protects Start. Three schedulers
// share one JetStream context (simulating three pods on the same
// cluster) and call Start concurrently. All three must succeed.
// Without the Once wrappers in scheduler_nats.go, this exercises the
// same first-deploy race that hangs production pods.
func TestNATSScheduler_ConcurrentStartSerializedByOnce(t *testing.T) {
	_, js := startNATSServer(t)

	const peers = 3
	handler := func(ctx context.Context, event JobEvent) error { return nil }

	schedulers := make([]Scheduler, peers)
	for i := range peers {
		schedulers[i] = NewNATSScheduler(js, handler)
	}

	var wg sync.WaitGroup
	errs := make([]error, peers)
	wg.Add(peers)
	for i, s := range schedulers {
		go func(idx int, s Scheduler) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			errs[idx] = s.Start(ctx)
		}(i, s)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoErrorf(t, err, "scheduler %d Start failed", i)
	}
	for _, s := range schedulers {
		assert.True(t, s.IsRunning())
		_ = s.Stop(context.Background())
	}
}

// TestNATSScheduler_WithOnceOverridesBuiltIn: WithOnce must replace the
// built-in implementation. The recorder must see exactly one Once
// invocation — Start now wraps every CreateOrUpdate* in a single
// provision block keyed defaultSchedulerOnceKey.
func TestNATSScheduler_WithOnceOverridesBuiltIn(t *testing.T) {
	_, js := startNATSServer(t)

	var (
		mu   sync.Mutex
		keys []string
	)
	recorder := func(ctx context.Context, key string, fn func(context.Context) error) error {
		mu.Lock()
		keys = append(keys, key)
		mu.Unlock()
		return fn(ctx)
	}

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	s := NewNATSScheduler(js, handler, WithOnce(recorder))
	require.NoError(t, s.Start(context.Background()))
	defer s.Stop(context.Background())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{defaultSchedulerOnceKey}, keys,
		"Start should invoke Once exactly once under the default scheduler key")
}

// TestNATSScheduler_WithOnceLockBucketIsHonoured verifies the default
// Once implementation actually creates the configured lock bucket,
// rather than the default one. A custom name should land in JetStream.
func TestNATSScheduler_WithOnceLockBucketIsHonoured(t *testing.T) {
	_, js := startNATSServer(t)

	const customBucket = "MY_CUSTOM_LOCKS"
	handler := func(ctx context.Context, event JobEvent) error { return nil }
	s := NewNATSScheduler(js, handler, WithOnceLockBucket(customBucket))
	require.NoError(t, s.Start(context.Background()))
	defer s.Stop(context.Background())

	kv, err := js.KeyValue(context.Background(), customBucket)
	require.NoError(t, err, "custom lock bucket should exist after Start")
	_ = kv

	// Default bucket name must NOT have been created when an override is
	// in effect.
	_, err = js.KeyValue(context.Background(), defaultOnceLockBucket)
	assert.ErrorIs(t, err, jetstream.ErrBucketNotFound,
		"default lock bucket should not be created when WithOnceLockBucket is set")
}

// TestNATSStorage_ConcurrentInitializeSerializedByOnce: three NATSStorage
// instances Initialize() concurrently on the same JetStream — same race
// the NATSScheduler.Start fix targets, but on the storage path that
// plasma runs before Start. All three must succeed; the underlying
// CreateOrUpdate must run at most once per bucket (sentinel fast path).
func TestNATSStorage_ConcurrentInitializeSerializedByOnce(t *testing.T) {
	_, js := startNATSServer(t)

	const peers = 3
	var (
		mu       sync.Mutex
		fnByKey  = map[string]int{}
		recorder = func(ctx context.Context, key string, fn func(context.Context) error) error {
			if err := fn(ctx); err != nil {
				return err
			}
			mu.Lock()
			fnByKey[key]++
			mu.Unlock()
			return nil
		}
	)

	storages := make([]*NATSStorage, peers)
	for i := range peers {
		storages[i] = NewNATSStorage(js, WithNATSStorageOnce(recorder))
	}

	var wg sync.WaitGroup
	errs := make([]error, peers)
	wg.Add(peers)
	for i, s := range storages {
		go func(idx int, s *NATSStorage) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			errs[idx] = s.Initialize(ctx)
		}(i, s)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoErrorf(t, err, "storage %d Initialize failed", i)
	}

	mu.Lock()
	defer mu.Unlock()
	// Initialize flows through the recorded OnceFunc under one key
	// covering both KV buckets.
	assert.Contains(t, fnByKey, defaultStorageOnceKey,
		"Initialize should invoke the OnceFunc under defaultStorageOnceKey")
}

// TestNATSStorage_WithOnceOverridesBuiltIn: the recorder must see every
// Initialize-time lock key, with the same naming convention NATSScheduler
// uses so callers can share one OnceFunc across both code paths and have
// the SCHEDULER_JOBS / SCHEDULER_EXECUTIONS sentinels coalesce.
func TestNATSStorage_WithOnceOverridesBuiltIn(t *testing.T) {
	_, js := startNATSServer(t)

	var (
		mu   sync.Mutex
		keys []string
	)
	recorder := func(ctx context.Context, key string, fn func(context.Context) error) error {
		mu.Lock()
		keys = append(keys, key)
		mu.Unlock()
		return fn(ctx)
	}

	storage := NewNATSStorage(js, WithNATSStorageOnce(recorder))
	require.NoError(t, storage.Initialize(context.Background()))
	defer storage.Close(context.Background())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{defaultStorageOnceKey}, keys,
		"Initialize should invoke Once exactly once under the default storage key")
}

// TestNATSStorage_WithOnceLockBucketIsHonoured: same as the scheduler
// counterpart — a custom lock bucket name actually lands in JetStream
// and the default bucket is not created.
func TestNATSStorage_WithOnceLockBucketIsHonoured(t *testing.T) {
	_, js := startNATSServer(t)

	const customBucket = "MY_STORAGE_LOCKS"
	storage := NewNATSStorage(js, WithNATSStorageOnceLockBucket(customBucket))
	require.NoError(t, storage.Initialize(context.Background()))
	defer storage.Close(context.Background())

	_, err := js.KeyValue(context.Background(), customBucket)
	require.NoError(t, err, "custom lock bucket should exist after Initialize")

	_, err = js.KeyValue(context.Background(), defaultOnceLockBucket)
	assert.ErrorIs(t, err, jetstream.ErrBucketNotFound,
		"default lock bucket should not be created when WithNATSStorageOnceLockBucket is set")
}

// TestNATSStorage_SharedOnceDistinctKeys: when NATSStorage and
// NATSScheduler share one OnceFunc, they must use distinct keys —
// each side's fn provisions a different resource set (Storage: 2 KVs;
// Scheduler: 2 KVs + stream + consumer), so coalescing under one key
// would silently skip one side's setup. This test verifies the
// recommended configuration: shared OnceFunc, default per-component
// keys, each fn runs exactly once.
func TestNATSStorage_SharedOnceDistinctKeys(t *testing.T) {
	_, js := startNATSServer(t)

	var (
		mu     sync.Mutex
		runs   = map[string]int{}
		shared = func(ctx context.Context, key string, fn func(context.Context) error) error {
			mu.Lock()
			already := runs[key] > 0
			mu.Unlock()
			if already {
				return nil
			}
			if err := fn(ctx); err != nil {
				return err
			}
			mu.Lock()
			runs[key]++
			mu.Unlock()
			return nil
		}
	)

	storage := NewNATSStorage(js, WithNATSStorageOnce(shared))
	require.NoError(t, storage.Initialize(context.Background()))
	defer storage.Close(context.Background())

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	s := NewNATSScheduler(js, handler, WithOnce(shared))
	require.NoError(t, s.Start(context.Background()))
	defer s.Stop(context.Background())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, runs[defaultStorageOnceKey],
		"storage provisioning runs exactly once under defaultStorageOnceKey")
	assert.Equal(t, 1, runs[defaultSchedulerOnceKey],
		"scheduler provisioning runs exactly once under defaultSchedulerOnceKey")
}
