package app_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── PromoteToHot tests ────────────────────────────────────────────────────────

func TestPromoteToHot_FileOnHotTier_NoOp(t *testing.T) {
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	// File is already on tier0.
	backends["tier0"].store["recordings/clip.mp4"] = []byte("data")
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/clip.mp4",
		CurrentTier: "tier0",
		State:       domain.StateSynced,
		Size:        4,
	}))

	backend, localPath, err := ts.PromoteToHot(ctx, "recordings/clip.mp4")
	require.NoError(t, err)
	assert.NotNil(t, backend)
	// memBackend.LocalPath returns ("", false) so localPath is empty — that is
	// correct for our mem stub. The important assertion is no error and the
	// file is still on tier0.
	_ = localPath

	f, err := meta.GetFile(ctx, "recordings/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
}

func TestPromoteToHot_FileOnColdTier_CopiedToHot(t *testing.T) {
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	data := []byte("cold tier content")
	backends["tier1"].store["recordings/clip.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/clip.mp4",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
	}))

	backend, _, err := ts.PromoteToHot(ctx, "recordings/clip.mp4")
	require.NoError(t, err)
	assert.NotNil(t, backend)

	// Content must be on tier0 now.
	assert.Equal(t, data, backends["tier0"].store["recordings/clip.mp4"],
		"promoted content should be on tier0")

	// Metadata must reflect tier0 and StateWriting (caller is about to write).
	f, err := meta.GetFile(ctx, "recordings/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
	assert.Equal(t, domain.StateWriting, f.State,
		"state should be writing so subsequent OnWriteComplete updates correctly")
}

func TestPromoteToHot_FileNotFound(t *testing.T) {
	ts, _, _ := makeTierService(t)
	_, _, err := ts.PromoteToHot(context.Background(), "nonexistent/file.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestPromoteToHot_TierFileTierRecordAdded(t *testing.T) {
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	data := []byte("promote me")
	backends["tier1"].store["clips/x.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "clips/x.mp4",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
	}))
	// Seed the tier1 tier record.
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   "clips/x.mp4",
		TierName:  "tier1",
		ArrivedAt: time.Now().Add(-48 * time.Hour),
		Verified:  true,
	}))

	_, _, err := ts.PromoteToHot(ctx, "clips/x.mp4")
	require.NoError(t, err)

	// tier0 tier record should have been added.
	tiers, err := meta.GetFileTiers(ctx, "clips/x.mp4")
	require.NoError(t, err)
	tierNames := make([]string, len(tiers))
	for i, ft := range tiers {
		tierNames[i] = ft.TierName
	}
	assert.Contains(t, tierNames, "tier0",
		"tier0 entry should be recorded after promotion")
	assert.Contains(t, tierNames, "tier1",
		"tier1 entry should still be present — cold copy survives until eviction")
}

func TestPromoteToHot_ColdTierCopyPreserved(t *testing.T) {
	// Promotion copies to tier0 but must NOT delete the tier1 copy.
	// The normal eviction path handles cold-copy cleanup after re-replication.
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	data := []byte("preserve cold copy")
	backends["tier1"].store["recordings/keep.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/keep.mp4",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
	}))

	_, _, err := ts.PromoteToHot(ctx, "recordings/keep.mp4")
	require.NoError(t, err)

	// tier1 backend still has the file.
	assert.Equal(t, data, backends["tier1"].store["recordings/keep.mp4"],
		"tier1 copy should be preserved — eviction will clean it up later")
}

// ── WriteGuard integration with PromoteToHot ─────────────────────────────────

func TestPromoteToHot_WriteGuardIsClearedOnDelete(t *testing.T) {
	// After delete, the WriteGuard entry should be forgotten so a re-created
	// file with the same path doesn't inherit the old quiescence window.
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	backends["tier0"].store["recordings/cam1/clip.mp4"] = []byte("d")
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/cam1/clip.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:  "recordings/cam1/clip.mp4",
		TierName: "tier0",
		Verified: true,
	}))

	// Signal write-open then delete.
	ts.Guard().Open("recordings/cam1/clip.mp4")
	require.NoError(t, ts.OnDelete(ctx, "recordings/cam1/clip.mp4"))

	// Guard entry should be forgotten — path is not write-active after delete.
	active, _ := ts.Guard().IsWriteActive("recordings/cam1/clip.mp4")
	assert.False(t, active, "WriteGuard entry should be cleared by OnDelete")
}

// ── Replicator respects promotion path ────────────────────────────────────────

func TestReplicator_DoesNotCopyWhileWriteActive(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := []byte("write in progress")
	src.store["recordings/active.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/active.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	guard := app.NewWriteGuard(0)
	guard.Open("recordings/active.mp4") // simulate open write handle

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    1,
		RetryInterval: 20 * time.Millisecond,
		Verify:        "none",
		WriteGuard:    guard,
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "recordings/active.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Give the worker time to process.
	time.Sleep(60 * time.Millisecond)

	// File should NOT have been copied while the handle is open.
	dst.mu.Lock()
	_, copied := dst.store["recordings/active.mp4"]
	dst.mu.Unlock()
	assert.False(t, copied, "file must not be copied while write handle is open")

	// Now close the handle.
	guard.Close("recordings/active.mp4")

	// The job should be re-enqueued and eventually complete.
	require.Eventually(t, func() bool {
		dst.mu.Lock()
		_, ok := dst.store["recordings/active.mp4"]
		dst.mu.Unlock()
		return ok
	}, 3*time.Second, 30*time.Millisecond, "file should be copied once handle is released")
}

func TestReplicator_AbortsCopyOnStaleness(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")

	originalData := []byte("version 1")
	src.store["clip.mp4"] = originalData

	originalMtime := time.Now().Add(-10 * time.Second)
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "clip.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(originalData)),
		ModTime:     originalMtime,
		Digest:      "somedigest", // non-empty triggers staleness check
	}))

	// Simulate the file being modified after enqueue: update src.store to
	// have different content and size (the Stat call in process() will
	// see the new size via Stat() on the memBackend).
	// We do this by modifying meta to record original state but leaving the
	// backend file larger — process() will call Stat and see size mismatch.
	src.store["clip.mp4"] = []byte("version 2 — longer content")

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    3,
		RetryInterval: 20 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "clip.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Give worker time to process.
	time.Sleep(80 * time.Millisecond)

	// Stale copy must NOT have been written to dst.
	dst.mu.Lock()
	_, copied := dst.store["clip.mp4"]
	dst.mu.Unlock()
	assert.False(t, copied, "stale copy must be aborted, not written to destination")

	// The job should have been dropped silently (not retried as an error).
	copied2, failed2, _ := r.Metrics()
	assert.Equal(t, int64(0), failed2, "staleness abort should not count as a failure")
	_ = copied2
}

// ── HottestTierName ───────────────────────────────────────────────────────────

func TestTierService_HottestTierName(t *testing.T) {
	ts, _, _ := makeTierService(t)
	assert.Equal(t, "tier0", ts.HottestTierName())
}

// ── makeTierService helper now used by multiple test files ────────────────────
// (declared in tier_service_test.go — verified it doesn't conflict)

func newTierServiceForPromotion(t *testing.T) (*app.TierService, *memMeta, map[string]*memBackend) {
	t.Helper()
	cfg := makeConfig(t)
	meta := newMemMeta()
	b0 := newMemBackend("tier0")
	b1 := newMemBackend("tier1")
	backends := map[string]domain.Backend{"tier0": b0, "tier1": b1}
	log := zaptest.NewLogger(t)
	ts := app.NewTierService(cfg, meta, backends, log)
	return ts, meta, map[string]*memBackend{"tier0": b0, "tier1": b1}
}

// Verify PromoteToHot with a more realistic sequence:
// write → evict → write-open (triggers promotion) → write complete → replicate
func TestPromoteToHot_FullLifecycle(t *testing.T) {
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	// 1. File written to tier0.
	relPath := "recordings/cam1/clip.mp4"
	data := bytes.Repeat([]byte("original content"), 64)
	backends["tier0"].store[relPath] = data
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     relPath,
		CurrentTier: "tier0",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
		Digest:      "abc123",
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  "tier0",
		ArrivedAt: time.Now().Add(-48 * time.Hour),
		Verified:  true,
	}))

	// 2. Simulate eviction: file moves to tier1, tier0 copy deleted.
	backends["tier1"].store[relPath] = data
	delete(backends["tier0"].store, relPath)
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     relPath,
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
		Digest:      "abc123",
	}))

	f, err := meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier1", f.CurrentTier, "file should be on tier1 after eviction")
	assert.Empty(t, backends["tier0"].store[relPath], "tier0 should have no copy")

	// 3. Application opens file for writing (simulates O_RDWR).
	//    PromoteToHot should bring it back to tier0.
	_, _, err = ts.PromoteToHot(ctx, relPath)
	require.NoError(t, err)

	f, err = meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier, "after write-open, file should be on tier0")
	assert.Equal(t, domain.StateWriting, f.State, "state should be writing")
	assert.Equal(t, data, backends["tier0"].store[relPath], "tier0 should have the content")
	assert.Equal(t, data, backends["tier1"].store[relPath], "tier1 copy preserved")

	// 4. Write completes: OnWriteComplete updates digest and re-enqueues.
	newSize := int64(len(data) + 10)
	// (In real FUSE, the write handle modifies the file; here we just call OnWriteComplete)
	err = ts.OnWriteComplete(ctx, relPath, "tier0", newSize, time.Now())
	require.NoError(t, err)

	f, err = meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
	assert.Equal(t, domain.StateLocal, f.State, "should be local awaiting replication")
	assert.Equal(t, newSize, f.Size)
}

// Verify the Replicator's Metrics method still works.
func TestReplicator_Metrics_ZeroInitial(t *testing.T) {
	r := app.NewReplicator(
		app.ReplicatorConfig{Workers: 1, MaxRetries: 0, RetryInterval: time.Second, Verify: "none"},
		newMemMeta(),
		&fakeTierLookup{backends: map[string]domain.Backend{}},
		zaptest.NewLogger(t),
	)
	r.Start()
	defer r.Stop()
	copied, failed, depth := r.Metrics()
	assert.Equal(t, int64(0), copied)
	assert.Equal(t, int64(0), failed)
	assert.Equal(t, int64(0), depth)
}

// Ensure the io.Reader passed to PromoteToHot Put is fully consumed.
func TestPromoteToHot_ReaderFullyConsumed(t *testing.T) {
	// If Put doesn't fully drain the reader from Get, the underlying connection
	// (for SMB/S3) would be left in a broken state. For memBackend this means
	// the stored bytes arrive intact.
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	data := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 256)
	backends["tier1"].store["exports/dump.bin"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "exports/dump.bin",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
	}))

	_, _, err := ts.PromoteToHot(ctx, "exports/dump.bin")
	require.NoError(t, err)

	promoted := backends["tier0"].store["exports/dump.bin"]
	assert.Equal(t, data, promoted, "all bytes must arrive on tier0 intact")

	// memBackend.Get returns a bytes.Reader; verify it would be fully consumed
	// by checking the promoted size.
	assert.Len(t, promoted, len(data))
}

// Helper: verify PromoteToHot returns an error when the source backend fails.
type failingGetBackend struct {
	*memBackend
}

func (f *failingGetBackend) Get(_ context.Context, _ string) (io.ReadCloser, int64, error) {
	return nil, 0, fmt.Errorf("simulated backend read failure")
}

func TestPromoteToHot_SourceReadFailure(t *testing.T) {
	cfg := makeConfig(t)
	meta := newMemMeta()
	ctx := context.Background()

	badSrc := &failingGetBackend{newMemBackend("tier1")}
	b0 := newMemBackend("tier0")
	backends := map[string]domain.Backend{"tier0": b0, "tier1": badSrc}
	log := zaptest.NewLogger(t)
	ts := app.NewTierService(cfg, meta, backends, log)

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/fail.mp4",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
	}))

	_, _, err := ts.PromoteToHot(ctx, "recordings/fail.mp4")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "promote")
	// tier0 should not have a partial copy.
	_, hasPartial := b0.store["recordings/fail.mp4"]
	assert.False(t, hasPartial, "no partial copy on tier0 after failed promotion")
}
