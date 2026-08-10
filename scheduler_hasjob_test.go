package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The reason HasJob exists: GetJob answers from a per-process cache, so a peer
// that never loaded a job reports it absent while it is registered and firing.
// A caller acting on that answer registers a replacement under a fresh ID —
// a different KV key, so nothing collides — and two live chains run one job.
func TestNATSScheduler_HasJobSeesJobsRegisteredByAnotherInstance(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	// Started first, so loadJobsFromKV finds an empty bucket and nothing
	// afterwards populates its cache.
	observer := NewNATSScheduler(js, handler)
	require.NoError(t, observer.Start(ctx))
	defer observer.Stop(ctx)

	owner := NewNATSScheduler(js, handler)
	require.NoError(t, owner.Start(ctx))
	defer owner.Stop(ctx)

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)
	require.NoError(t, owner.AddJob("peer-job", schedule, nil))

	// The precondition this whole method is a response to.
	_, err = observer.GetJob("peer-job")
	require.ErrorIs(t, err, ErrJobNotFound, "GetJob is cache-scoped")

	has, err := observer.HasJob("peer-job")
	require.NoError(t, err)
	assert.True(t, has, "KV is shared, so the job exists as far as any peer is concerned")
}

// False must mean absent everywhere, or the method is useless for deciding
// whether to register: a caller that cannot trust "false" cannot act on it.
func TestNATSScheduler_HasJobFalseForUnknownJob(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	s := NewNATSScheduler(js, handler)
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	has, err := s.HasJob("never-existed")
	require.NoError(t, err)
	assert.False(t, has)

	_, err = s.HasJob("")
	assert.ErrorIs(t, err, ErrEmptyJobID)
}

// A removed job must read as absent — otherwise the disable/delete paths that
// now depend on RemoveJob converging KV would still see a ghost.
func TestNATSScheduler_HasJobFalseAfterRemoval(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	s := NewNATSScheduler(js, handler)
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("doomed", schedule, nil))
	require.NoError(t, s.RemoveJob("doomed"))

	has, err := s.HasJob("doomed")
	require.NoError(t, err)
	assert.False(t, has)
}

// An unreachable store must surface as an error rather than as false. Folding
// it into the bool would hand callers a confident "nothing is scheduled" at
// exactly the moment the scheduler cannot tell — the answer most likely to
// cause a duplicate registration.
func TestNATSScheduler_HasJobSurfacesKVFailure(t *testing.T) {
	_, js := startNATSServer(t)

	handler := func(ctx context.Context, event JobEvent) error { return nil }
	ctx := context.Background()

	s := NewNATSScheduler(js, handler)
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	require.NoError(t, js.DeleteKeyValue(ctx, defaultNATSJobBucket))

	has, err := s.HasJob("anything")
	assert.Error(t, err, "cannot tell is not no")
	assert.False(t, has)
}

// The in-memory scheduler has no peers, so its cache is authoritative — but it
// still has to honour the contract the interface promises.
func TestMemoryScheduler_HasJob(t *testing.T) {
	handler := func(ctx context.Context, event JobEvent) error { return nil }
	s := NewScheduler(NewMemoryStorage(), handler, NewBasicScheduleCodec())
	ctx := context.Background()
	require.NoError(t, s.Start(ctx))
	defer s.Stop(ctx)

	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)
	require.NoError(t, s.AddJob("local-job", schedule, nil))

	has, err := s.HasJob("local-job")
	require.NoError(t, err)
	assert.True(t, has)

	has, err = s.HasJob("other-job")
	require.NoError(t, err)
	assert.False(t, has)

	_, err = s.HasJob("")
	assert.ErrorIs(t, err, ErrEmptyJobID)
}
