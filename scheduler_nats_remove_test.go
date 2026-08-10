package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RemoveJob is a cluster-wide operation, but s.jobs is a per-process cache:
// seeded from KV at Start, then filled in only as messages arrive. A job added
// by a peer after this instance started is therefore absent locally while fully
// alive in KV — and used to make RemoveJob return ErrJobNotFound without
// touching KV at all, so the job kept firing on its owner and the caller was
// told there had been nothing to remove.
func TestNATSScheduler_RemoveJobNotInLocalCachePurgesKV(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	// The peer that will never learn about the job: started first, so
	// loadJobsFromKV sees an empty bucket and nothing later populates it.
	remover := NewNATSScheduler(js, handler)
	require.NoError(t, remover.Start(ctx))
	defer remover.Stop(ctx)

	owner := NewNATSScheduler(js, handler)
	require.NoError(t, owner.Start(ctx))
	defer owner.Stop(ctx)

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)
	require.NoError(t, owner.AddJob("cross-instance-job", schedule, nil))

	// Precondition: the remover genuinely does not know this job. Without this
	// the test would pass on the old code for the wrong reason.
	_, err = remover.GetJob("cross-instance-job")
	require.ErrorIs(t, err, ErrJobNotFound, "remover must not have the job cached")

	require.NoError(t, remover.RemoveJob("cross-instance-job"),
		"a job that exists in KV must be removable from any instance")

	kv := remover.(*natsSchedulerImpl).jobKV
	_, err = kv.Get(ctx, "cross-instance-job")
	assert.ErrorIs(t, err, jetstream.ErrKeyNotFound,
		"purging the KV row is what actually stops the job on its owning peer")
}

// The other half of the same branch, and the reason it consults KV rather than
// simply assuming a cache miss means "somewhere else": a job that exists
// nowhere must still report ErrJobNotFound, or every caller loses the ability
// to tell a stale reference from a live one. Pins the same contract as
// TestNATSScheduler_RemoveJob's non-existent case, from the KV side.
func TestNATSScheduler_RemoveJobAbsentFromKVStillNotFound(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	s := NewNATSScheduler(js, handler)
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	assert.ErrorIs(t, s.RemoveJob("never-existed"), ErrJobNotFound)
	assert.ErrorIs(t, s.RemoveJob(""), ErrEmptyJobID)
}

// A KV purge that fails leaves the job runnable on every peer, so reporting
// success is how a caller comes to delete its own last reference to live work.
// Deleting the bucket underneath is a stand-in for the real failures (KV
// unreachable, permissions, bucket recreated elsewhere); what matters is that
// the error reaches the caller instead of being dropped on the floor.
func TestNATSScheduler_RemoveJobSurfacesKVPurgeFailure(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	s := NewNATSScheduler(js, handler)
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("doomed-job", schedule, nil))

	require.NoError(t, js.DeleteKeyValue(ctx, defaultNATSJobBucket))

	assert.Error(t, s.RemoveJob("doomed-job"),
		"a removal whose KV purge failed has not removed anything")
}

// A stopped scheduler has no KV to consult, so it answers from the cache it
// still holds — unchanged behaviour, pinned because the new KV path must not
// creep into a state where s.jobKV is unusable.
func TestNATSScheduler_RemoveJobWhileStoppedUsesLocalCache(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	s := NewNATSScheduler(js, handler)
	require.NoError(t, s.Start(ctx))

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("cached-job", schedule, nil))
	require.NoError(t, s.Stop(ctx))

	assert.NoError(t, s.RemoveJob("cached-job"), "still cached locally")
	assert.ErrorIs(t, s.RemoveJob("cached-job"), ErrJobNotFound, "and now it is not")
}
