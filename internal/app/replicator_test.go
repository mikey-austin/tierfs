package app_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/app"
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

// failingBackend fails the first N Put calls, then succeeds.
type failingBackend struct {
	inner   *memBackend
	failN   int
	callN   int
}

func (f *failingBackend) Scheme() string { return f.inner.Scheme() }
func (f *failingBackend) URI(p string) string { return f.inner.URI(p) }
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
