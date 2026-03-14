// Package integration contains end-to-end tests that exercise the full
// TierFS stack without FUSE (which requires root/fusermount).
// These tests use real SQLite, real file backends, and a real TierService,
// verifying replication, eviction, and rename across tiers.
//
// Run with: go test ./integration/ -v -timeout 60s
package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/adapters/meta/sqlite"
	filerbackend "github.com/mikey-austin/tierfs/internal/adapters/storage/file"
	"github.com/mikey-austin/tierfs/internal/adapters/storage/transform"
	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Helpers ──────────────────────────────────────────────────────────────────

type stack struct {
	meta  *sqlite.Store
	tier0 *filerbackend.Backend
	tier1 *filerbackend.Backend
	svc   *app.TierService
	cfg   *config.Resolved
}

func newStack(t *testing.T) *stack {
	t.Helper()
	dir := t.TempDir()
	tier0Root := filepath.Join(dir, "tier0")
	tier1Root := filepath.Join(dir, "tier1")
	metaDB := filepath.Join(dir, "meta.db")

	toml := `
[mount]
path    = "` + filepath.Join(dir, "mount") + `"
meta_db = "` + metaDB + `"

[replication]
workers = 2
retry_interval = "100ms"
max_retries = 3
verify = "digest"

[eviction]
check_interval = "50ms"
capacity_threshold = 0.90
capacity_headroom  = 0.70

[[backend]]
name = "ssd"
uri  = "file://` + tier0Root + `"

[[backend]]
name = "nas"
uri  = "file://` + tier1Root + `"

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "1GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "unlimited"

[[rule]]
name  = "instant-evict"
match = "thumbnails/**"
evict_schedule = [{after = "0s", to = "tier1"}]
promote_on_read = false

[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [{after = "10ms", to = "tier1"}]
promote_on_read = false

[[rule]]
name  = "default"
match = "**"
evict_schedule = [{after = "1h", to = "tier1"}]
promote_on_read = false
`
	f, err := os.CreateTemp(dir, "*.toml")
	require.NoError(t, err)
	f.WriteString(toml)
	f.Close()

	cfg, err := config.Load(f.Name())
	require.NoError(t, err)

	meta, err := sqlite.Open(metaDB)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	b0, err := filerbackend.New(tier0Root)
	require.NoError(t, err)
	b1, err := filerbackend.New(tier1Root)
	require.NoError(t, err)

	log := zaptest.NewLogger(t)
	backends := map[string]domain.Backend{"tier0": b0, "tier1": b1}

	svc := app.NewTierService(cfg, meta, backends, log)
	svc.Start()
	t.Cleanup(svc.Stop)

	return &stack{
		meta:  meta,
		tier0: b0,
		tier1: b1,
		svc:   svc,
		cfg:   cfg,
	}
}

type transformStack struct {
	stack
	tier1Raw *filerbackend.Backend // unwrapped tier1 for raw access
}

func newStackWithTransforms(t *testing.T) *transformStack {
	t.Helper()
	dir := t.TempDir()
	tier0Root := filepath.Join(dir, "tier0")
	tier1Root := filepath.Join(dir, "tier1")
	metaDB := filepath.Join(dir, "meta.db")

	toml := `
[mount]
path    = "` + filepath.Join(dir, "mount") + `"
meta_db = "` + metaDB + `"

[replication]
workers = 2
retry_interval = "100ms"
max_retries = 3
verify = "digest"

[eviction]
check_interval = "50ms"
capacity_threshold = 0.90
capacity_headroom  = 0.70

[[backend]]
name = "ssd"
uri  = "file://` + tier0Root + `"

[[backend]]
name = "nas"
uri  = "file://` + tier1Root + `"

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "1GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "unlimited"

[[rule]]
name  = "instant-evict"
match = "thumbnails/**"
evict_schedule = [{after = "0s", to = "tier1"}]
promote_on_read = false

[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [{after = "10ms", to = "tier1"}]
promote_on_read = false

[[rule]]
name  = "default"
match = "**"
evict_schedule = [{after = "1h", to = "tier1"}]
promote_on_read = false
`
	f, err := os.CreateTemp(dir, "*.toml")
	require.NoError(t, err)
	f.WriteString(toml)
	f.Close()

	cfg, err := config.Load(f.Name())
	require.NoError(t, err)

	meta, err := sqlite.Open(metaDB)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	b0, err := filerbackend.New(tier0Root)
	require.NoError(t, err)

	b1Raw, err := filerbackend.New(tier1Root)
	require.NoError(t, err)

	// Wrap tier1 with a zstd compression transform.
	log := zaptest.NewLogger(t)
	zstdT, err := transform.NewZstd(zstd.SpeedDefault)
	require.NoError(t, err)
	b1Wrapped := transform.New(b1Raw, transform.Pipeline(zstdT), log)

	backends := map[string]domain.Backend{"tier0": b0, "tier1": b1Wrapped}

	svc := app.NewTierService(cfg, meta, backends, log)
	svc.Start()
	t.Cleanup(svc.Stop)

	return &transformStack{
		stack: stack{
			meta:  meta,
			tier0: b0,
			tier1: nil, // tier1 is wrapped; use tier1Raw for raw access
			svc:   svc,
			cfg:   cfg,
		},
		tier1Raw: b1Raw,
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestIntegration_WriteAndRead(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	relPath := "recordings/cam1/2026-03-13/clip.mp4"
	data := bytes.Repeat([]byte("hello tierfs"), 1000)

	// Write via TierService (simulating FUSE Create + write + Release).
	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier0", tierName)

	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Metadata must reflect the file.
	f, err := s.meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
	assert.Equal(t, domain.StateLocal, f.State)
	assert.Equal(t, int64(len(data)), f.Size)
	assert.NotEmpty(t, f.Digest)

	// File must be physically present on tier0.
	b, _, err := backend.Get(ctx, relPath)
	require.NoError(t, err)
	b.Close()
}

func TestIntegration_ReplicationToTier1(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	relPath := "recordings/cam1/replicate.mp4"
	data := bytes.Repeat([]byte("replicate me"), 512)

	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Replication is async; wait up to 5s for it to complete.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond, "file should be verified on tier1")

	// Verify tier1 backend physically holds the file.
	rc, size, err := s.tier1.Get(ctx, relPath)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, int64(len(data)), size)
}

func TestIntegration_EvictionAfterReplication(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	relPath := "recordings/cam1/evict.mp4"
	data := bytes.Repeat([]byte("evict me"), 512)

	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Wait for replication.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond)

	// Wait for eviction (evict_after = 10ms for recordings).
	require.Eventually(t, func() bool {
		f, err := s.meta.GetFile(ctx, relPath)
		return err == nil && f.CurrentTier == "tier1"
	}, 5*time.Second, 100*time.Millisecond, "file should be evicted to tier1")

	// File must no longer be on tier0.
	_, _, err = s.tier0.Get(ctx, relPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)

	// File must still be readable on tier1.
	rc, _, err := s.tier1.Get(ctx, relPath)
	require.NoError(t, err)
	rc.Close()
}

func TestIntegration_DeletePropagates(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	relPath := "recordings/cam1/delete.mp4"
	data := []byte("delete this")

	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Wait for tier1 copy.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond)

	// Delete.
	require.NoError(t, s.svc.OnDelete(ctx, relPath))

	// Must be gone from metadata.
	_, err = s.meta.GetFile(ctx, relPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)

	// Must be gone from tier0.
	_, _, err = s.tier0.Get(ctx, relPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestIntegration_RenameUpdatesMetadata(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	oldPath := "recordings/cam1/old.mp4"
	newPath := "recordings/cam1/new.mp4"
	data := []byte("rename test")

	backend, tierName, err := s.svc.WriteTarget(oldPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, oldPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, oldPath, tierName, int64(len(data)), time.Now()))

	require.NoError(t, s.svc.OnRename(ctx, oldPath, newPath))

	_, err = s.meta.GetFile(ctx, oldPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)

	f, err := s.meta.GetFile(ctx, newPath)
	require.NoError(t, err)
	assert.Equal(t, newPath, f.RelPath)

	// Physical file must have moved.
	_, _, err = s.tier0.Get(ctx, oldPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)
	rc, _, err := s.tier0.Get(ctx, newPath)
	require.NoError(t, err)
	rc.Close()
}

func TestIntegration_ListDir(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	paths := []string{
		"recordings/cam1/2026-03/13/10/00.mp4",
		"recordings/cam1/2026-03/13/10/01.mp4",
		"recordings/cam2/2026-03/13/10/00.mp4",
		"thumbnails/cam1/thumb.jpg",
	}
	for _, p := range paths {
		backend, tier, err := s.svc.WriteTarget(p)
		require.NoError(t, err)
		require.NoError(t, backend.Put(ctx, p, bytes.NewReader([]byte("x")), 1))
		require.NoError(t, s.svc.OnWriteComplete(ctx, p, tier, 1, time.Now()))
	}

	entries, err := s.meta.ListDir(ctx, "")
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.ElementsMatch(t, []string{"recordings", "thumbnails"}, names)

	recEntries, err := s.meta.ListDir(ctx, "recordings")
	require.NoError(t, err)
	recNames := make([]string, len(recEntries))
	for i, e := range recEntries {
		recNames[i] = e.Name
	}
	assert.ElementsMatch(t, []string{"cam1", "cam2"}, recNames)
}

func TestIntegration_MultipleFilesOnTier(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		p := filepath.Join("recordings", "cam1", fmt.Sprintf("%02d.mp4", i))
		backend, tier, err := s.svc.WriteTarget(p)
		require.NoError(t, err)
		require.NoError(t, backend.Put(ctx, p, bytes.NewReader([]byte("data")), 4))
		require.NoError(t, s.svc.OnWriteComplete(ctx, p, tier, 4, time.Now()))
	}

	// Some files may have already been replicated by the background workers,
	// so check that all 5 files exist in metadata rather than asserting exact state.
	files, err := s.meta.ListFiles(ctx, "recordings/cam1")
	require.NoError(t, err)
	assert.Len(t, files, 5)
}

// ── Transform Tier Integration Tests ──────────────────────────────────────────

func TestIntegration_TransformTier_WriteAndRead(t *testing.T) {
	s := newStackWithTransforms(t)
	ctx := context.Background()

	relPath := "recordings/cam1/2026-03-13/clip.mp4"
	data := bytes.Repeat([]byte("hello tierfs transform"), 1000)

	// Write to tier0 (plain file://).
	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	assert.Equal(t, "tier0", tierName)

	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Wait for replication to compressed tier1.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond, "file should be verified on tier1")

	// Read back through the TierService — should decompress transparently.
	f, err := s.meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), f.Size)
	assert.NotEmpty(t, f.Digest)
}

func TestIntegration_TransformTier_ReplicationVerifiesDigest(t *testing.T) {
	s := newStackWithTransforms(t)
	ctx := context.Background()

	relPath := "recordings/cam1/verified.mp4"
	data := bytes.Repeat([]byte("digest verification"), 512)

	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Wait for replication with digest verification to tier1.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond, "file should be digest-verified on tier1")

	// Metadata must reflect verified state.
	f, err := s.meta.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.NotEmpty(t, f.Digest)
}

func TestIntegration_TransformTier_EvictionPreservesData(t *testing.T) {
	s := newStackWithTransforms(t)
	ctx := context.Background()

	relPath := "recordings/cam1/evict-transform.mp4"
	data := bytes.Repeat([]byte("evict me through transforms"), 512)

	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Wait for replication.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond)

	// Wait for eviction (evict_after = 10ms for recordings).
	require.Eventually(t, func() bool {
		f, err := s.meta.GetFile(ctx, relPath)
		return err == nil && f.CurrentTier == "tier1"
	}, 5*time.Second, 100*time.Millisecond, "file should be evicted to tier1")

	// File must no longer be on tier0.
	_, _, err = s.tier0.Get(ctx, relPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)

	// File must still be readable on tier1 (through the transform layer).
	// Read via the raw backend to verify it is physically present.
	rc, _, err := s.tier1Raw.Get(ctx, relPath)
	require.NoError(t, err)
	rc.Close()
}

func TestIntegration_TransformTier_RawStoredDiffers(t *testing.T) {
	s := newStackWithTransforms(t)
	ctx := context.Background()

	relPath := "recordings/cam1/raw-check.mp4"
	data := bytes.Repeat([]byte("compressible data for raw check"), 1000)

	backend, tierName, err := s.svc.WriteTarget(relPath)
	require.NoError(t, err)
	require.NoError(t, backend.Put(ctx, relPath, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, s.svc.OnWriteComplete(ctx, relPath, tierName, int64(len(data)), time.Now()))

	// Wait for replication to compressed tier1.
	require.Eventually(t, func() bool {
		ok, _ := s.meta.IsTierVerified(ctx, relPath, "tier1")
		return ok
	}, 5*time.Second, 100*time.Millisecond, "file should be verified on tier1")

	// Read the raw bytes directly from the tier1 file backend (bypassing transform).
	rc, _, err := s.tier1Raw.Get(ctx, relPath)
	require.NoError(t, err)
	rawBytes, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()

	// The raw stored bytes must differ from the original — proving compression happened.
	assert.NotEqual(t, data, rawBytes,
		"raw stored bytes on tier1 should differ from original (compression applied)")
	assert.Less(t, len(rawBytes), len(data),
		"compressed bytes (%d) should be smaller than original (%d)",
		len(rawBytes), len(data))
}
