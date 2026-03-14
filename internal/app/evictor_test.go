package app_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvictor_EvictsAfterAge(t *testing.T) {
	cfg := makeConfig(t)
	meta := newMemMeta()
	ctx := context.Background()

	b0 := newMemBackend("tier0")
	b1 := newMemBackend("tier1")
	backends := map[string]domain.Backend{"tier0": b0, "tier1": b1}
	log := zaptest.NewLogger(t)

	lookup := &fakeTierLookup{backends: backends}

	replCfg := app.ReplicatorConfig{Workers: 1, MaxRetries: 0, RetryInterval: time.Millisecond, Verify: "none"}
	repl := app.NewReplicator(replCfg, meta, lookup, log)
	repl.Start()
	defer repl.Stop()

	evictCfg := app.EvictorConfig{
		CheckInterval:     20 * time.Millisecond,
		CapacityThreshold: 0.9,
		CapacityHeadroom:  0.7,
	}

	// Use a tiny capacity-agnostic TierCapacity stub.
	capacity := &fakeCapacity{}

	evictor := app.NewEvictor(evictCfg, meta, cfg.Policy, repl, lookup, capacity, log)
	evictor.Start()
	defer evictor.Stop()

	// Seed a file that is synced on tier0, with tier1 verified, arrived > 24h ago.
	relPath := "recordings/cam1/old.mp4"
	b0.store[relPath] = []byte("content")
	b1.store[relPath] = []byte("content")

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     relPath,
		CurrentTier: "tier0",
		State:       domain.StateSynced,
		Size:        7,
	}))
	arrivedAt := time.Now().Add(-25 * time.Hour)
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  "tier0",
		ArrivedAt: arrivedAt,
		Verified:  true,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  "tier1",
		ArrivedAt: arrivedAt,
		Verified:  true,
	}))

	// Evictor should move file to tier1 after a few ticks.
	require.Eventually(t, func() bool {
		f, err := meta.GetFile(ctx, relPath)
		return err == nil && f.CurrentTier == "tier1"
	}, 2*time.Second, 30*time.Millisecond, "file should be evicted to tier1")

	// tier0 backend should no longer have the file.
	b0.mu.Lock()
	_, exists := b0.store[relPath]
	b0.mu.Unlock()
	assert.False(t, exists, "file should be removed from tier0 backend")
}

func TestEvictor_DoesNotEvictPinned(t *testing.T) {
	cfg := makeConfig(t)
	meta := newMemMeta()
	ctx := context.Background()

	b0 := newMemBackend("tier0")
	backends := map[string]domain.Backend{"tier0": b0, "tier1": newMemBackend("tier1")}
	log := zaptest.NewLogger(t)
	lookup := &fakeTierLookup{backends: backends}

	replCfg := app.ReplicatorConfig{Workers: 1, MaxRetries: 0, RetryInterval: time.Millisecond, Verify: "none"}
	repl := app.NewReplicator(replCfg, meta, lookup, log)
	repl.Start()
	defer repl.Stop()

	evictCfg := app.EvictorConfig{CheckInterval: 20 * time.Millisecond, CapacityThreshold: 0.9, CapacityHeadroom: 0.7}
	evictor := app.NewEvictor(evictCfg, meta, cfg.Policy, repl, lookup, &fakeCapacity{}, log)
	evictor.Start()
	defer evictor.Stop()

	relPath := "exports/my-export.mp4" // pin_tier = tier0
	b0.store[relPath] = []byte("pinned")
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     relPath,
		CurrentTier: "tier0",
		State:       domain.StateSynced,
		Size:        6,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  "tier0",
		ArrivedAt: time.Now().Add(-100 * time.Hour), // very old
		Verified:  true,
	}))

	// Wait several evictor ticks.
	time.Sleep(100 * time.Millisecond)

	f, err := meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier, "pinned file should stay on tier0")
}

// fakeCapacity reports zero usage, unlimited capacity.
type fakeCapacity struct{}

func (f *fakeCapacity) UsedBytes(_ context.Context, _ string) (int64, error) { return 0, nil }
func (f *fakeCapacity) CapacityBytes(_ string) (int64, bool)                 { return 0, true }
