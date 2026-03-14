package app_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/digest"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTierLookup struct {
	backends map[string]domain.Backend
}

func (f *fakeTierLookup) BackendFor(name string) (domain.Backend, error) {
	b, ok := f.backends[name]
	if !ok {
		return nil, domain.ErrTierNotFound
	}
	return b, nil
}

func TestReplicator_CopiesFile(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := bytes.Repeat([]byte("replicate"), 100)
	src.store["recordings/test.mp4"] = data

	// Seed metadata.
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/test.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	log := zaptest.NewLogger(t)
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    1,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "recordings/test.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Wait for copy.
	require.Eventually(t, func() bool {
		dst.mu.Lock()
		_, ok := dst.store["recordings/test.mp4"]
		dst.mu.Unlock()
		return ok
	}, 3*time.Second, 50*time.Millisecond, "file should arrive on tier1")

	// Verify metadata updated.
	require.Eventually(t, func() bool {
		ok, _ := meta.IsTierVerified(ctx, "recordings/test.mp4", "tier1")
		return ok
	}, 3*time.Second, 50*time.Millisecond, "tier1 should be marked verified")

	f, err := meta.GetFile(ctx, "recordings/test.mp4")
	require.NoError(t, err)
	assert.Equal(t, domain.StateSynced, f.State)

	// Content must match.
	dst.mu.Lock()
	got := dst.store["recordings/test.mp4"]
	dst.mu.Unlock()
	assert.Equal(t, data, got)
}

func TestReplicator_RetriesOnError(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := &failingBackend{inner: newMemBackend("tier1"), failN: 2}
	data := []byte("retry data")
	src.store["a.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "a.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	log := zaptest.NewLogger(t)
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    3,
		RetryInterval: 20 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "a.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Should eventually succeed after retries.
	require.Eventually(t, func() bool {
		ok, _ := meta.IsTierVerified(ctx, "a.mp4", "tier1")
		return ok
	}, 5*time.Second, 50*time.Millisecond, "should succeed after retries")
}

func TestReplicator_Metrics(t *testing.T) {
	r := app.NewReplicator(app.ReplicatorConfig{Workers: 1, MaxRetries: 0, RetryInterval: time.Second, Verify: "none"},
		newMemMeta(), &fakeTierLookup{backends: map[string]domain.Backend{}}, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	copied, failed, depth := r.Metrics()
	assert.Equal(t, int64(0), copied)
	assert.Equal(t, int64(0), failed)
	assert.Equal(t, int64(0), depth)
}

func TestReplicator_StalenessNoFalsePositive(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := []byte("staleness test data")

	// Use Put to populate both store and modTimes consistently.
	require.NoError(t, src.Put(ctx, "test.mp4", bytes.NewReader(data), int64(len(data))))

	// Read back the modTime the backend recorded, and use it in metadata.
	src.mu.Lock()
	backendMtime := src.modTimes["test.mp4"]
	src.mu.Unlock()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
		ModTime:     backendMtime,
		Digest:      "deadbeef",
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	log := zaptest.NewLogger(t)
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    1,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "test.mp4", FromTier: "tier0", ToTier: "tier1"})

	require.Eventually(t, func() bool {
		dst.mu.Lock()
		_, ok := dst.store["test.mp4"]
		dst.mu.Unlock()
		return ok
	}, 3*time.Second, 50*time.Millisecond,
		"file should arrive on tier1 without false staleness abort")
}

func TestReplicator_DigestVerifyRemote(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := bytes.Repeat([]byte("digest-verify"), 200)
	src.mu.Lock()
	src.store["video.mp4"] = data
	src.mu.Unlock()

	d, err := digest.Compute(bytes.NewReader(data))
	require.NoError(t, err)

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "video.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
		Digest:      d,
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    1,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "digest",
	}
	r := app.NewReplicator(cfg, meta, lookup, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "video.mp4", FromTier: "tier0", ToTier: "tier1"})

	require.Eventually(t, func() bool {
		ok, _ := meta.IsTierVerified(ctx, "video.mp4", "tier1")
		return ok
	}, 3*time.Second, 50*time.Millisecond, "remote digest verify should succeed")

	dst.mu.Lock()
	got := dst.store["video.mp4"]
	dst.mu.Unlock()
	assert.Equal(t, data, got)
}

// failingBackend fails the first N Put calls, then succeeds.
type failingBackend struct {
	inner *memBackend
	failN int
	callN int
}

func (f *failingBackend) Scheme() string                    { return f.inner.Scheme() }
func (f *failingBackend) URI(p string) string               { return f.inner.URI(p) }
func (f *failingBackend) LocalPath(p string) (string, bool) { return f.inner.LocalPath(p) }
func (f *failingBackend) Stat(ctx context.Context, p string) (*domain.FileInfo, error) {
	return f.inner.Stat(ctx, p)
}
func (f *failingBackend) Delete(ctx context.Context, p string) error { return f.inner.Delete(ctx, p) }
func (f *failingBackend) List(ctx context.Context, p string) ([]domain.FileInfo, error) {
	return f.inner.List(ctx, p)
}
func (f *failingBackend) Get(ctx context.Context, p string) (io.ReadCloser, int64, error) {
	return f.inner.Get(ctx, p)
}
func (f *failingBackend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error {
	f.callN++
	if f.callN <= f.failN {
		io.Copy(io.Discard, r) //nolint:errcheck
		return domain.ErrBackendFailure
	}
	return f.inner.Put(ctx, relPath, r, size)
}

// truncatingBackend wraps a memBackend but stores only the first half of the data.
type truncatingBackend struct {
	inner *memBackend
}

func (tb *truncatingBackend) Scheme() string                    { return tb.inner.Scheme() }
func (tb *truncatingBackend) URI(p string) string               { return tb.inner.URI(p) }
func (tb *truncatingBackend) LocalPath(p string) (string, bool) { return tb.inner.LocalPath(p) }
func (tb *truncatingBackend) Stat(ctx context.Context, p string) (*domain.FileInfo, error) {
	return tb.inner.Stat(ctx, p)
}
func (tb *truncatingBackend) Delete(ctx context.Context, p string) error {
	return tb.inner.Delete(ctx, p)
}
func (tb *truncatingBackend) List(ctx context.Context, p string) ([]domain.FileInfo, error) {
	return tb.inner.List(ctx, p)
}
func (tb *truncatingBackend) Get(ctx context.Context, p string) (io.ReadCloser, int64, error) {
	return tb.inner.Get(ctx, p)
}
func (tb *truncatingBackend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	half := data[:len(data)/2]
	return tb.inner.Put(ctx, relPath, bytes.NewReader(half), int64(len(half)))
}

// ── Queue full test ──────────────────────────────────────────────────────────

func TestReplicator_QueueFullDropsJob(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	log := zap.New(core)

	meta := newMemMeta()
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{}}
	cfg := app.ReplicatorConfig{
		Workers:       0, // no workers to drain the queue
		MaxRetries:    0,
		RetryInterval: time.Second,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	// Do not call Start() — no workers means queue stays full.

	// Fill the 4096-element queue.
	for i := 0; i < 4096; i++ {
		r.Enqueue(app.CopyJob{RelPath: "fill.mp4", FromTier: "tier0", ToTier: "tier1"})
	}

	_, _, depth := r.Metrics()
	assert.Equal(t, int64(4096), depth, "queue should be full at 4096")

	// Enqueue one more — should be dropped.
	r.Enqueue(app.CopyJob{RelPath: "dropped.mp4", FromTier: "tier0", ToTier: "tier1"})

	_, _, depth = r.Metrics()
	assert.Equal(t, int64(4096), depth, "depth should remain 4096 after drop")

	// Verify "queue full" warning was logged.
	found := false
	for _, entry := range logs.All() {
		if entry.Level == zapcore.WarnLevel && entry.Message == "replication queue full, dropping job" {
			found = true
			break
		}
	}
	assert.True(t, found, "should log a 'queue full' warning")
}

// ── Permanent failure after max retries ──────────────────────────────────────

func TestReplicator_PermanentFailureAfterMaxRetries(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	// failN > MaxRetries means it never succeeds.
	dst := &failingBackend{inner: newMemBackend("tier1"), failN: 100}
	data := []byte("doomed data")
	src.store["doomed.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "doomed.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    2,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "doomed.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Wait until all retries are exhausted: initial attempt + 2 retries = 3 failures.
	require.Eventually(t, func() bool {
		_, failed, _ := r.Metrics()
		return failed >= 3
	}, 5*time.Second, 20*time.Millisecond, "should record at least 3 failures")

	// Destination should NOT have the file.
	dst.inner.mu.Lock()
	_, hasFile := dst.inner.store["doomed.mp4"]
	dst.inner.mu.Unlock()
	assert.False(t, hasFile, "file should not be on destination after permanent failure")

	copied, _, _ := r.Metrics()
	assert.Equal(t, int64(0), copied, "no successful copies should be recorded")
}

// ── Digest mismatch test ─────────────────────────────────────────────────────

func TestReplicator_DigestMismatch(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := bytes.Repeat([]byte("digest-data"), 100)
	src.store["mismatch.mp4"] = data

	// Seed metadata with a WRONG digest.
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "mismatch.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
		Digest:      "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    0,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "digest",
	}
	r := app.NewReplicator(cfg, meta, lookup, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "mismatch.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Wait for the failure.
	require.Eventually(t, func() bool {
		_, failed, _ := r.Metrics()
		return failed > 0
	}, 3*time.Second, 20*time.Millisecond, "should record a failure for digest mismatch")

	copied, _, _ := r.Metrics()
	assert.Equal(t, int64(0), copied, "no copies should succeed with wrong digest")

	// The mismatched file should have been deleted from the destination.
	dst.mu.Lock()
	_, hasFile := dst.store["mismatch.mp4"]
	dst.mu.Unlock()
	assert.False(t, hasFile, "mismatched file should be deleted from destination")
}

// ── Verify size success ──────────────────────────────────────────────────────

func TestReplicator_VerifySize_Success(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := bytes.Repeat([]byte("size-ok"), 50)
	src.store["sized.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "sized.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    1,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "size",
	}
	r := app.NewReplicator(cfg, meta, lookup, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "sized.mp4", FromTier: "tier0", ToTier: "tier1"})

	require.Eventually(t, func() bool {
		ok, _ := meta.IsTierVerified(ctx, "sized.mp4", "tier1")
		return ok
	}, 3*time.Second, 50*time.Millisecond, "size-verified copy should succeed")

	copied, failed, _ := r.Metrics()
	assert.Equal(t, int64(1), copied)
	assert.Equal(t, int64(0), failed)

	dst.mu.Lock()
	got := dst.store["sized.mp4"]
	dst.mu.Unlock()
	assert.Equal(t, data, got)
}

// ── Verify size mismatch ─────────────────────────────────────────────────────

func TestReplicator_VerifySize_Mismatch(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := &truncatingBackend{inner: newMemBackend("tier1")}
	data := bytes.Repeat([]byte("truncate-me"), 100)
	src.store["trunc.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "trunc.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    0,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "size",
	}
	r := app.NewReplicator(cfg, meta, lookup, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "trunc.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Wait for the failure.
	require.Eventually(t, func() bool {
		_, failed, _ := r.Metrics()
		return failed > 0
	}, 3*time.Second, 20*time.Millisecond, "should fail due to size mismatch")

	copied, _, _ := r.Metrics()
	assert.Equal(t, int64(0), copied, "no successful copies expected")

	// The truncated file should have been deleted from destination.
	dst.inner.mu.Lock()
	_, hasFile := dst.inner.store["trunc.mp4"]
	dst.inner.mu.Unlock()
	assert.False(t, hasFile, "truncated file should be deleted from destination")
}

// ── Enhanced Metrics test: verify non-zero after a copy ──────────────────────

func TestReplicator_Metrics_NonZeroAfterCopy(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := []byte("metrics data")
	src.store["metrics.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "metrics.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    1,
		RetryInterval: 10 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, zaptest.NewLogger(t))
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "metrics.mp4", FromTier: "tier0", ToTier: "tier1"})

	require.Eventually(t, func() bool {
		copied, _, _ := r.Metrics()
		return copied > 0
	}, 3*time.Second, 50*time.Millisecond, "copied metric should be non-zero after success")

	copied, failed, _ := r.Metrics()
	assert.Equal(t, int64(1), copied)
	assert.Equal(t, int64(0), failed)
}
