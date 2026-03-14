package fuse

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/adapters/storage/file"
)

// makeHandleTestService builds a TierService for handle tests using a real
// file.Backend for tier0 and returns the components needed.
func makeHandleTestService(t *testing.T) (*app.TierService, *memMeta, *file.Backend) {
	t.Helper()
	tier0Dir := t.TempDir()
	tier1Dir := t.TempDir()

	fb, err := file.New(tier0Dir)
	require.NoError(t, err)

	mb := newMemBackend("tier1")
	meta := newMemMeta()

	cfg := makeTestConfig(t, tier0Dir, tier1Dir)
	log := zaptest.NewLogger(t)

	backends := map[string]domain.Backend{
		"tier0": fb,
		"tier1": mb,
	}

	svc := app.NewTierService(cfg, meta, backends, log)
	return svc, meta, fb
}

// createTempFile is a helper that creates a temp file and writes initial data.
func createTempFile(t *testing.T, data []byte) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "handle-test-*")
	require.NoError(t, err)
	if len(data) > 0 {
		_, err = f.Write(data)
		require.NoError(t, err)
	}
	return f
}

// ── WriteHandle: write and read data back ────────────────────────────────────

func TestWriteHandle_WriteAndRead(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	// Seed metadata so OnWriteComplete can find the file.
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/write-read.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	// Create a file on the file backend.
	f, err := fb.CreateFile("test/write-read.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/write-read.txt", "tier0", svc, log, "", nil)

	payload := []byte("hello write handle")
	n, ws := wh.Write(payload, 0)
	require.Equal(t, gofuse.OK, ws)
	assert.Equal(t, uint32(len(payload)), n)

	// Read returns EIO because the underlying file is opened write-only.
	buf := make([]byte, 100)
	_, rs := wh.Read(buf, 0)
	require.Equal(t, gofuse.EIO, rs)

	wh.Release()
}

// ── WriteHandle: write at offset ─────────────────────────────────────────────

func TestWriteHandle_WriteAtOffset(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/offset.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/offset.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/offset.txt", "tier0", svc, log, "", nil)

	// Write at offset 0.
	_, ws := wh.Write([]byte("AAAA"), 0)
	require.Equal(t, gofuse.OK, ws)

	// Write at offset 4.
	_, ws = wh.Write([]byte("BBBB"), 4)
	require.Equal(t, gofuse.OK, ws)

	// Read returns EIO because the underlying file is opened write-only.
	buf := make([]byte, 8)
	_, rs := wh.Read(buf, 0)
	require.Equal(t, gofuse.EIO, rs)

	wh.Release()
}

// ── WriteHandle: Fsync returns OK ────────────────────────────────────────────

func TestWriteHandle_Fsync(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/fsync.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/fsync.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/fsync.txt", "tier0", svc, log, "", nil)
	defer wh.Release()

	_, ws := wh.Write([]byte("sync me"), 0)
	require.Equal(t, gofuse.OK, ws)

	status := wh.Fsync(0)
	assert.Equal(t, gofuse.OK, status)
}

// ── WriteHandle: Flush returns OK ────────────────────────────────────────────

func TestWriteHandle_Flush(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/flush.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/flush.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/flush.txt", "tier0", svc, log, "", nil)
	defer wh.Release()

	_, ws := wh.Write([]byte("flush me"), 0)
	require.Equal(t, gofuse.OK, ws)

	status := wh.Flush()
	assert.Equal(t, gofuse.OK, status)
}

// ── WriteHandle: Release closes file, calls Guard.Close and OnWriteComplete ──

func TestWriteHandle_Release_ClosesAndFinalizes(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/release.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/release.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/release.txt", "tier0", svc, log, "", nil)

	// Guard should show this file as write-active.
	active, _ := svc.Guard().IsWriteActive("test/release.txt")
	assert.True(t, active, "guard should be write-active while handle is open")

	payload := []byte("release test content")
	_, ws := wh.Write(payload, 0)
	require.Equal(t, gofuse.OK, ws)

	wh.Release()

	// OnWriteComplete should have been called, moving state to StateLocal.
	require.Eventually(t, func() bool {
		mf, err := meta.GetFile(ctx, "test/release.txt")
		return err == nil && mf.State == domain.StateLocal
	}, 2*time.Second, 50*time.Millisecond)

	// Size should be updated.
	mf, err := meta.GetFile(ctx, "test/release.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len(payload)), mf.Size)
}

// ── WriteHandle: GetAttr returns correct size after writes ───────────────────

func TestWriteHandle_GetAttr(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/getattr.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/getattr.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/getattr.txt", "tier0", svc, log, "", nil)
	defer wh.Release()

	// Write some data.
	data := []byte("getattr test payload 1234567890")
	_, ws := wh.Write(data, 0)
	require.Equal(t, gofuse.OK, ws)

	// Force flush to ensure data is on disk for stat.
	wh.Flush()

	var attr gofuse.Attr
	status := wh.GetAttr(&attr)
	require.Equal(t, gofuse.OK, status)
	assert.Equal(t, uint64(len(data)), attr.Size)
	assert.Equal(t, uint32(0o100644), attr.Mode) // S_IFREG | 0o644
}

// ── WriteHandle: pushStage failure preserves stage file ──────────────────────

func TestWriteHandle_PushStageFailure(t *testing.T) {
	// Build a custom setup with a failingBackend for the stage push path.
	tier0Dir := t.TempDir()
	tier1Dir := t.TempDir()

	fb, err := file.New(tier0Dir)
	require.NoError(t, err)

	failBackend := newMemBackend("tier1-fail")
	failBackend.putErr = fmt.Errorf("simulated put failure")

	meta := newMemMeta()
	cfg := makeTestConfig(t, tier0Dir, tier1Dir)
	log := zaptest.NewLogger(t)

	backends := map[string]domain.Backend{
		"tier0": fb,
		"tier1": failBackend,
	}

	svc := app.NewTierService(cfg, meta, backends, log)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/push-fail.txt",
		CurrentTier: "tier1",
		State:       domain.StateWriting,
	}))

	// Create a local stage file to simulate writing to a remote backend.
	stageDir := t.TempDir()
	stagePath := stageDir + "/push-fail-stage"
	stageFile, err := os.Create(stagePath)
	require.NoError(t, err)

	payload := []byte("this data should be preserved")
	_, err = stageFile.Write(payload)
	require.NoError(t, err)

	wh := newWriteHandle(stageFile, "test/push-fail.txt", "tier1", svc, log, stagePath, failBackend)

	// Guard should be write-active.
	active, _ := svc.Guard().IsWriteActive("test/push-fail.txt")
	assert.True(t, active, "guard should be write-active while handle is open")

	// Release triggers pushStage in a goroutine.
	wh.Release()

	// Wait for the goroutine to finish and verify the stage file is preserved.
	require.Eventually(t, func() bool {
		// The push will fail, so the stage file should still exist.
		_, err := os.Stat(stagePath)
		return err == nil
	}, 2*time.Second, 50*time.Millisecond,
		"stage file should be preserved after push failure")

	// Verify stage file contents are intact.
	preserved, err := os.ReadFile(stagePath)
	require.NoError(t, err)
	assert.Equal(t, payload, preserved)

	// OnWriteComplete should NOT have been called (state should still be writing).
	mf, err := meta.GetFile(ctx, "test/push-fail.txt")
	require.NoError(t, err)
	assert.Equal(t, domain.StateWriting, mf.State,
		"state should remain writing when push fails")
}

// ── WriteHandle: pushStage success deletes stage and calls OnWriteComplete ───

func TestWriteHandle_PushStageSuccess(t *testing.T) {
	tier0Dir := t.TempDir()
	tier1Dir := t.TempDir()

	fb, err := file.New(tier0Dir)
	require.NoError(t, err)

	remoteBackend := newMemBackend("tier1")
	meta := newMemMeta()
	cfg := makeTestConfig(t, tier0Dir, tier1Dir)
	log := zaptest.NewLogger(t)

	backends := map[string]domain.Backend{
		"tier0": fb,
		"tier1": remoteBackend,
	}

	svc := app.NewTierService(cfg, meta, backends, log)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/push-ok.txt",
		CurrentTier: "tier1",
		State:       domain.StateWriting,
	}))

	// Create a local stage file.
	stageDir := t.TempDir()
	stagePath := stageDir + "/push-ok-stage"
	stageFile, err := os.Create(stagePath)
	require.NoError(t, err)

	payload := []byte("data to push to remote")
	_, err = stageFile.Write(payload)
	require.NoError(t, err)

	wh := newWriteHandle(stageFile, "test/push-ok.txt", "tier1", svc, log, stagePath, remoteBackend)
	wh.Release()

	// Wait for async push to complete.
	require.Eventually(t, func() bool {
		// Stage file should be cleaned up on success.
		_, err := os.Stat(stagePath)
		return os.IsNotExist(err)
	}, 2*time.Second, 50*time.Millisecond,
		"stage file should be removed after successful push")

	// Data should be in the remote backend.
	remoteBackend.mu.Lock()
	data, ok := remoteBackend.store["test/push-ok.txt"]
	remoteBackend.mu.Unlock()
	assert.True(t, ok, "data should exist in remote backend")
	assert.Equal(t, payload, data)

	// OnWriteComplete should have been called.
	require.Eventually(t, func() bool {
		mf, err := meta.GetFile(ctx, "test/push-ok.txt")
		return err == nil && mf.State == domain.StateLocal
	}, 2*time.Second, 50*time.Millisecond,
		"OnWriteComplete should set state to local")
}

// ── WriteHandle: multiple writes accumulate correctly ────────────────────────

func TestWriteHandle_MultipleWrites(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/multi.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/multi.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/multi.txt", "tier0", svc, log, "", nil)

	// Write in multiple chunks.
	for i := 0; i < 10; i++ {
		chunk := []byte(fmt.Sprintf("chunk%d\n", i))
		_, ws := wh.Write(chunk, int64(i*8))
		require.Equal(t, gofuse.OK, ws)
	}

	// GetAttr should reflect accumulated size.
	wh.Flush()
	var attr gofuse.Attr
	status := wh.GetAttr(&attr)
	require.Equal(t, gofuse.OK, status)
	assert.True(t, attr.Size > 0, "size should be non-zero after multiple writes")

	wh.Release()
}

// ── WriteHandle: Read beyond EOF returns empty ───────────────────────────────

func TestWriteHandle_ReadBeyondEOF(t *testing.T) {
	svc, meta, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/eof.txt",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
	}))

	f, err := fb.CreateFile("test/eof.txt", 0o644)
	require.NoError(t, err)

	wh := newWriteHandle(f, "test/eof.txt", "tier0", svc, log, "", nil)
	defer wh.Release()

	_, ws := wh.Write([]byte("short"), 0)
	require.Equal(t, gofuse.OK, ws)

	// Read returns EIO because the underlying file is opened write-only.
	buf := make([]byte, 100)
	_, rs := wh.Read(buf, 1000)
	require.Equal(t, gofuse.EIO, rs)
}

// ── writeHandle created via newWriteHandle registers with Guard ──────────────

func TestWriteHandle_GuardRegistration(t *testing.T) {
	svc, _, fb := makeHandleTestService(t)
	log := zaptest.NewLogger(t)

	f, err := fb.CreateFile("test/guard.txt", 0o644)
	require.NoError(t, err)

	active, _ := svc.Guard().IsWriteActive("test/guard.txt")
	assert.False(t, active, "should not be write-active before handle created")

	wh := newWriteHandle(f, "test/guard.txt", "tier0", svc, log, "", nil)
	active, _ = svc.Guard().IsWriteActive("test/guard.txt")
	assert.True(t, active, "should be write-active while handle is open")

	wh.Release()
	// After release, the quiescence window may still be active, but the
	// open handle count should be zero. Use Snapshot to verify.
	snap := svc.Guard().Snapshot()
	if entry, ok := snap["test/guard.txt"]; ok {
		assert.Equal(t, 0, entry.OpenCount, "no open handles after Release")
	}
}

// makeTestConfig is defined in fs_test.go (same package). We use it here too
// since both files are in package fuse.
//
// We also reuse the memBackend, memMeta, etc. stubs from fs_test.go.

// ── Ensure pushStage reads from the correct stage file ───────────────────────

func TestWriteHandle_PushStage_ReadsCorrectFile(t *testing.T) {
	tier0Dir := t.TempDir()
	tier1Dir := t.TempDir()

	fb, err := file.New(tier0Dir)
	require.NoError(t, err)

	captureBackend := &capturingBackend{memBackend: newMemBackend("tier1")}
	meta := newMemMeta()
	cfg := makeTestConfig(t, tier0Dir, tier1Dir)
	log := zaptest.NewLogger(t)

	backends := map[string]domain.Backend{
		"tier0": fb,
		"tier1": captureBackend,
	}

	svc := app.NewTierService(cfg, meta, backends, log)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "test/capture.txt",
		CurrentTier: "tier1",
		State:       domain.StateWriting,
	}))

	stageDir := t.TempDir()
	stagePath := stageDir + "/capture-stage"
	stageFile, err := os.Create(stagePath)
	require.NoError(t, err)

	expected := []byte("specific stage content")
	_, err = stageFile.Write(expected)
	require.NoError(t, err)

	wh := newWriteHandle(stageFile, "test/capture.txt", "tier1", svc, log, stagePath, captureBackend)
	wh.Release()

	// Wait for the push.
	require.Eventually(t, func() bool {
		captureBackend.mu.Lock()
		defer captureBackend.mu.Unlock()
		_, ok := captureBackend.store["test/capture.txt"]
		return ok
	}, 2*time.Second, 50*time.Millisecond)

	captureBackend.mu.Lock()
	assert.Equal(t, expected, captureBackend.store["test/capture.txt"])
	captureBackend.mu.Unlock()
}

// capturingBackend wraps memBackend to capture Put data for verification.
type capturingBackend struct {
	*memBackend
}

func (c *capturingBackend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error {
	return c.memBackend.Put(ctx, relPath, r, size)
}

func (c *capturingBackend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) {
	return c.memBackend.Get(ctx, relPath)
}

func (c *capturingBackend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) {
	return c.memBackend.Stat(ctx, relPath)
}

func (c *capturingBackend) Delete(ctx context.Context, relPath string) error {
	return c.memBackend.Delete(ctx, relPath)
}

func (c *capturingBackend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) {
	return c.memBackend.List(ctx, prefix)
}

func (c *capturingBackend) Scheme() string                    { return c.memBackend.Scheme() }
func (c *capturingBackend) URI(p string) string               { return c.memBackend.URI(p) }
func (c *capturingBackend) LocalPath(p string) (string, bool) { return c.memBackend.LocalPath(p) }
