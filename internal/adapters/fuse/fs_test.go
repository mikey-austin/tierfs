package fuse

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/adapters/storage/file"
	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/domain"
)

// ── In-memory stubs ──────────────────────────────────────────────────────────

type memBackend struct {
	mu       sync.Mutex
	name     string
	store    map[string][]byte
	modTimes map[string]time.Time

	// putErr can be set to simulate backend failures.
	putErr error
}

func newMemBackend(name string) *memBackend {
	return &memBackend{
		name:     name,
		store:    make(map[string][]byte),
		modTimes: make(map[string]time.Time),
	}
}

func (m *memBackend) Scheme() string                    { return "mem" }
func (m *memBackend) URI(p string) string               { return "mem://" + m.name + "/" + p }
func (m *memBackend) LocalPath(_ string) (string, bool) { return "", false }

func (m *memBackend) Put(_ context.Context, relPath string, r io.Reader, _ int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return m.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.store[relPath] = data
	m.modTimes[relPath] = time.Now()
	return nil
}

func (m *memBackend) Get(_ context.Context, relPath string) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	d, ok := m.store[relPath]
	m.mu.Unlock()
	if !ok {
		return nil, 0, domain.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(d)), int64(len(d)), nil
}

func (m *memBackend) Stat(_ context.Context, relPath string) (*domain.FileInfo, error) {
	m.mu.Lock()
	d, ok := m.store[relPath]
	m.mu.Unlock()
	if !ok {
		return nil, domain.ErrNotExist
	}
	return &domain.FileInfo{RelPath: relPath, Size: int64(len(d)), ModTime: m.modTimes[relPath]}, nil
}

func (m *memBackend) Delete(_ context.Context, relPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.store[relPath]; !ok {
		return domain.ErrNotExist
	}
	delete(m.store, relPath)
	delete(m.modTimes, relPath)
	return nil
}

func (m *memBackend) List(_ context.Context, _ string) ([]domain.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.FileInfo, 0, len(m.store))
	for k, v := range m.store {
		out = append(out, domain.FileInfo{RelPath: k, Size: int64(len(v))})
	}
	return out, nil
}

// memMeta is an in-memory MetadataStore with a working ListDir implementation.
type memMeta struct {
	mu        sync.Mutex
	files     map[string]domain.File
	fileTiers map[string][]domain.FileTier
}

func newMemMeta() *memMeta {
	return &memMeta{
		files:     make(map[string]domain.File),
		fileTiers: make(map[string][]domain.FileTier),
	}
}

func (m *memMeta) UpsertFile(_ context.Context, f domain.File) error {
	m.mu.Lock()
	m.files[f.RelPath] = f
	m.mu.Unlock()
	return nil
}

func (m *memMeta) GetFile(_ context.Context, relPath string) (*domain.File, error) {
	m.mu.Lock()
	f, ok := m.files[relPath]
	m.mu.Unlock()
	if !ok {
		return nil, domain.ErrNotExist
	}
	return &f, nil
}

func (m *memMeta) DeleteFile(_ context.Context, relPath string) error {
	m.mu.Lock()
	delete(m.files, relPath)
	delete(m.fileTiers, relPath)
	m.mu.Unlock()
	return nil
}

func (m *memMeta) ListFiles(_ context.Context, _ string) ([]domain.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.File, 0, len(m.files))
	for _, f := range m.files {
		out = append(out, f)
	}
	return out, nil
}

func (m *memMeta) AddFileTier(_ context.Context, ft domain.FileTier) error {
	m.mu.Lock()
	m.fileTiers[ft.RelPath] = append(m.fileTiers[ft.RelPath], ft)
	m.mu.Unlock()
	return nil
}

func (m *memMeta) GetFileTiers(_ context.Context, relPath string) ([]domain.FileTier, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fileTiers[relPath], nil
}

func (m *memMeta) MarkTierVerified(_ context.Context, relPath, tierName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, ft := range m.fileTiers[relPath] {
		if ft.TierName == tierName {
			m.fileTiers[relPath][i].Verified = true
		}
	}
	return nil
}

func (m *memMeta) RemoveFileTier(_ context.Context, relPath, tierName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tiers := m.fileTiers[relPath]
	out := tiers[:0]
	for _, ft := range tiers {
		if ft.TierName != tierName {
			out = append(out, ft)
		}
	}
	m.fileTiers[relPath] = out
	return nil
}

func (m *memMeta) TierArrivedAt(_ context.Context, relPath, tierName string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ft := range m.fileTiers[relPath] {
		if ft.TierName == tierName {
			return ft.ArrivedAt, nil
		}
	}
	return time.Time{}, domain.ErrNotExist
}

func (m *memMeta) IsTierVerified(_ context.Context, relPath, tierName string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ft := range m.fileTiers[relPath] {
		if ft.TierName == tierName {
			return ft.Verified, nil
		}
	}
	return false, nil
}

func (m *memMeta) FilesOnTier(_ context.Context, tierName string) ([]domain.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.File
	for _, f := range m.files {
		if f.CurrentTier == tierName && f.State == domain.StateSynced {
			out = append(out, f)
		}
	}
	return out, nil
}

func (m *memMeta) FilesAwaitingReplication(_ context.Context) ([]domain.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.File
	for _, f := range m.files {
		if f.State == domain.StateLocal {
			out = append(out, f)
		}
	}
	return out, nil
}

func (m *memMeta) EvictionCandidates(_ context.Context, tierName string, olderThan time.Time) ([]domain.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.File
	for _, f := range m.files {
		if f.CurrentTier != tierName || f.State != domain.StateSynced {
			continue
		}
		for _, ft := range m.fileTiers[f.RelPath] {
			if ft.TierName == tierName && ft.ArrivedAt.Before(olderThan) {
				out = append(out, f)
				break
			}
		}
	}
	return out, nil
}

func (m *memMeta) OldestAwaitingReplication(_ context.Context) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var oldest time.Time
	for _, f := range m.files {
		if f.State == domain.StateLocal {
			if oldest.IsZero() || f.ModTime.Before(oldest) {
				oldest = f.ModTime
			}
		}
	}
	return oldest, nil
}

// ListDir derives directory entries from stored files. It synthesises
// virtual directories for intermediate path components.
func (m *memMeta) ListDir(_ context.Context, dirPath string) ([]domain.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[string]bool)
	var out []domain.FileInfo

	prefix := dirPath
	if prefix != "" {
		prefix += "/"
	}

	for relPath, f := range m.files {
		if prefix != "" && !strings.HasPrefix(relPath, prefix) {
			continue
		}
		// If prefix is "", we want root-level entries.
		rest := strings.TrimPrefix(relPath, prefix)
		parts := strings.SplitN(rest, "/", 2)
		childName := parts[0]
		if seen[childName] {
			continue
		}
		seen[childName] = true

		if len(parts) == 1 {
			// Direct child file.
			out = append(out, domain.FileInfo{
				RelPath: relPath,
				Name:    childName,
				Size:    f.Size,
				ModTime: f.ModTime,
				IsDir:   false,
			})
		} else {
			// Intermediate directory.
			dirRel := dirPath
			if dirRel != "" {
				dirRel += "/"
			}
			dirRel += childName
			out = append(out, domain.FileInfo{
				RelPath: dirRel,
				Name:    childName,
				IsDir:   true,
			})
		}
	}
	return out, nil
}

func (m *memMeta) Close() error { return nil }

// ── Helpers ──────────────────────────────────────────────────────────────────

func makeTestConfig(t *testing.T, tier0Dir, tier1Dir string) *config.Resolved {
	t.Helper()
	tomlContent := `
[mount]
path    = "/mnt/test"
meta_db = "/tmp/test.db"

[replication]
workers = 1
verify  = "none"
sweep_interval = "1h"

[eviction]
check_interval = "1h"

[[backend]]
name = "ssd"
uri  = "file://` + tier0Dir + `"

[[backend]]
name = "nas"
uri  = "file://` + tier1Dir + `"

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "10GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "unlimited"

[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [{after = "24h", to = "tier1"}]
promote_on_read = false

[[rule]]
name  = "default"
match = "**"
evict_schedule = [{after = "48h", to = "tier1"}]
promote_on_read = false
`
	tmp, err := os.CreateTemp(t.TempDir(), "*.toml")
	require.NoError(t, err)
	_, err = tmp.WriteString(tomlContent)
	require.NoError(t, err)
	tmp.Close()

	cfg, err := config.Load(tmp.Name())
	require.NoError(t, err)
	return cfg
}

// makeFuseFS creates a TierFS backed by a real file.Backend for tier0 and
// a memBackend for tier1. Returns the TierFS, the memMeta, the file.Backend
// for tier0, the memBackend for tier1, and the stager.
func makeFuseFS(t *testing.T) (*TierFS, *memMeta, *file.Backend, *memBackend, *app.Stager) {
	t.Helper()
	tier0Dir := t.TempDir()
	tier1Dir := t.TempDir()
	stageDir := t.TempDir()

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

	svc := app.NewTierService(cfg, meta, backends, nil, 0, log)
	stager := app.NewStager(stageDir, log)
	fs := New(svc, meta, stager, log, nil)
	return fs, meta, fb, mb, stager
}

// seedFile creates a file on tier0 (file backend) and registers metadata.
func seedFile(t *testing.T, fs *TierFS, meta *memMeta, fb *file.Backend, relPath string, data []byte) {
	t.Helper()
	ctx := context.Background()

	// Write to the file backend.
	err := fb.Put(ctx, relPath, bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     relPath,
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
		ModTime:     now,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  "tier0",
		ArrivedAt: now,
		Verified:  true,
	}))
}

// seedRemoteFile puts a file on the memBackend (tier1) only and registers metadata.
func seedRemoteFile(t *testing.T, meta *memMeta, mb *memBackend, relPath string, data []byte) {
	t.Helper()
	ctx := context.Background()

	mb.mu.Lock()
	mb.store[relPath] = data
	mb.modTimes[relPath] = time.Now()
	mb.mu.Unlock()

	now := time.Now()
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     relPath,
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
		ModTime:     now,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  "tier1",
		ArrivedAt: now,
		Verified:  true,
	}))
}

// ── GetAttr tests ────────────────────────────────────────────────────────────

func TestGetAttr_Root(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	attr, status := fs.GetAttr("", nil)
	require.Equal(t, gofuse.OK, status)
	assert.Equal(t, uint32(gofuse.S_IFDIR|0o755), attr.Mode)
	assert.Equal(t, uint64(1), attr.Ino)
	assert.Equal(t, uint32(2), attr.Nlink)
}

func TestGetAttr_KnownFile(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	data := []byte("hello world")
	seedFile(t, fs, meta, fb, "recordings/cam1/clip.mp4", data)

	attr, status := fs.GetAttr("recordings/cam1/clip.mp4", nil)
	require.Equal(t, gofuse.OK, status)
	assert.Equal(t, uint32(gofuse.S_IFREG|0o644), attr.Mode)
	assert.Equal(t, uint64(len(data)), attr.Size)
	assert.NotZero(t, attr.Mtime)
	assert.Equal(t, uint32(1), attr.Nlink)
}

func TestGetAttr_VirtualDirectory(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/cam1/clip.mp4", []byte("data"))

	attr, status := fs.GetAttr("recordings", nil)
	require.Equal(t, gofuse.OK, status)
	assert.Equal(t, uint32(gofuse.S_IFDIR|0o755), attr.Mode)
	assert.Equal(t, uint32(2), attr.Nlink)

	attr, status = fs.GetAttr("recordings/cam1", nil)
	require.Equal(t, gofuse.OK, status)
	assert.Equal(t, uint32(gofuse.S_IFDIR|0o755), attr.Mode)
}

func TestGetAttr_NonExistent(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	_, status := fs.GetAttr("does/not/exist.txt", nil)
	assert.Equal(t, gofuse.ENOENT, status)
}

func TestGetAttr_InodeStability(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/stable.mp4", []byte("data"))

	attr1, s1 := fs.GetAttr("recordings/stable.mp4", nil)
	require.Equal(t, gofuse.OK, s1)
	attr2, s2 := fs.GetAttr("recordings/stable.mp4", nil)
	require.Equal(t, gofuse.OK, s2)

	assert.Equal(t, attr1.Ino, attr2.Ino, "same path should yield the same inode")
	assert.NotEqual(t, uint64(0), attr1.Ino)
}

// ── OpenDir/ListDir tests ────────────────────────────────────────────────────

func TestOpenDir_Root(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/cam1/clip.mp4", []byte("data"))
	seedFile(t, fs, meta, fb, "topfile.txt", []byte("top"))

	entries, status := fs.OpenDir("", nil)
	require.Equal(t, gofuse.OK, status)

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
	}
	assert.True(t, names["recordings"], "should list recordings subdir")
	assert.True(t, names["topfile.txt"], "should list top-level file")
}

func TestOpenDir_Subdirectory(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/cam1/clip1.mp4", []byte("d1"))
	seedFile(t, fs, meta, fb, "recordings/cam1/clip2.mp4", []byte("d2"))
	seedFile(t, fs, meta, fb, "recordings/cam2/clip3.mp4", []byte("d3"))

	entries, status := fs.OpenDir("recordings", nil)
	require.Equal(t, gofuse.OK, status)

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
	}
	assert.True(t, names["cam1"], "should list cam1")
	assert.True(t, names["cam2"], "should list cam2")
	assert.Len(t, entries, 2, "only immediate children")
}

func TestOpenDir_Empty(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	entries, status := fs.OpenDir("", nil)
	require.Equal(t, gofuse.OK, status)
	assert.Empty(t, entries)
}

// ── Create tests ─────────────────────────────────────────────────────────────

func TestCreate_LocalBackend(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)

	fh, status := fs.Create("recordings/new.mp4", 0, 0o644, nil)
	require.Equal(t, gofuse.OK, status)
	require.NotNil(t, fh)

	// Write some data.
	n, ws := fh.Write([]byte("test data"), 0)
	require.Equal(t, gofuse.OK, ws)
	assert.Equal(t, uint32(9), n)

	// Release the handle to finalize.
	fh.Release()

	// Metadata should be present.
	ctx := context.Background()
	require.Eventually(t, func() bool {
		f, err := meta.GetFile(ctx, "recordings/new.mp4")
		return err == nil && f.State != ""
	}, 2*time.Second, 50*time.Millisecond)

	// File should exist on the local backend.
	_, err := fb.Stat(ctx, "recordings/new.mp4")
	assert.NoError(t, err)

	// WriteGuard should no longer show the file as write-active.
	active, _ := fs.svc.Guard().IsWriteActive("recordings/new.mp4")
	assert.False(t, active, "guard should not be write-active after Release")
}

// ── Open (read) tests ────────────────────────────────────────────────────────

func TestOpen_LocalFile_ReadOnly(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/read.mp4", []byte("read data"))

	fh, status := fs.Open("recordings/read.mp4", uint32(os.O_RDONLY), nil)
	require.Equal(t, gofuse.OK, status)
	require.NotNil(t, fh)
	fh.Release()
}

func TestOpen_NonExistent(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	_, status := fs.Open("does/not/exist.mp4", uint32(os.O_RDONLY), nil)
	assert.Equal(t, gofuse.ENOENT, status)
}

func TestOpen_RemoteFile_StagesLocally(t *testing.T) {
	fs, meta, _, mb, stager := makeFuseFS(t)
	data := []byte("remote content")
	seedRemoteFile(t, meta, mb, "recordings/remote.mp4", data)

	fh, status := fs.Open("recordings/remote.mp4", uint32(os.O_RDONLY), nil)
	require.Equal(t, gofuse.OK, status)
	require.NotNil(t, fh)
	defer fh.Release()

	// Verify the staged file exists.
	stagePath := stager.StagePath("recordings/remote.mp4")
	_, err := os.Stat(stagePath)
	assert.NoError(t, err, "staged file should exist after open")
}

// ── Open (write with promotion) tests ────────────────────────────────────────

func TestOpen_WriteLocal_NoPromotion(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/hot.mp4", []byte("hot data"))

	// Open for writing (O_WRONLY = 1).
	fh, status := fs.Open("recordings/hot.mp4", 1, nil)
	require.Equal(t, gofuse.OK, status)
	require.NotNil(t, fh)
	fh.Release()
}

func TestOpen_WriteCold_PromotesToHot(t *testing.T) {
	fs, meta, fb, mb, _ := makeFuseFS(t)
	coldData := []byte("cold data to promote")
	seedRemoteFile(t, meta, mb, "recordings/cold.mp4", coldData)

	// Open for writing — should promote to tier0.
	fh, status := fs.Open("recordings/cold.mp4", 1, nil)
	require.Equal(t, gofuse.OK, status)
	require.NotNil(t, fh)
	fh.Release()

	// After promotion, file should exist on the local (tier0) backend.
	ctx := context.Background()
	require.Eventually(t, func() bool {
		_, err := fb.Stat(ctx, "recordings/cold.mp4")
		return err == nil
	}, 2*time.Second, 50*time.Millisecond)

	f, err := meta.GetFile(ctx, "recordings/cold.mp4")
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
}

// ── Unlink tests ─────────────────────────────────────────────────────────────

func TestUnlink_Success(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/delete-me.mp4", []byte("bye"))

	status := fs.Unlink("recordings/delete-me.mp4", nil)
	assert.Equal(t, gofuse.OK, status)

	ctx := context.Background()
	_, err := meta.GetFile(ctx, "recordings/delete-me.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestUnlink_NonExistent(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	status := fs.Unlink("nope.mp4", nil)
	assert.Equal(t, gofuse.OK, status)
}

// ── Rename tests ─────────────────────────────────────────────────────────────

func TestRename_Success(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/old.mp4", []byte("rename me"))

	status := fs.Rename("recordings/old.mp4", "recordings/new.mp4", nil)
	assert.Equal(t, gofuse.OK, status)

	ctx := context.Background()
	_, err := meta.GetFile(ctx, "recordings/old.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)

	f, err := meta.GetFile(ctx, "recordings/new.mp4")
	require.NoError(t, err)
	assert.Equal(t, "recordings/new.mp4", f.RelPath)
}

func TestRename_NonExistent(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	status := fs.Rename("nope.mp4", "also-nope.mp4", nil)
	assert.Equal(t, gofuse.ENOENT, status)
}

// ── Utimens tests ────────────────────────────────────────────────────────────

func TestUtimens_UpdatesMtime(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/ts.mp4", []byte("timestamps"))

	newMtime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	status := fs.Utimens("recordings/ts.mp4", nil, &newMtime, nil)
	assert.Equal(t, gofuse.OK, status)

	ctx := context.Background()
	f, err := meta.GetFile(ctx, "recordings/ts.mp4")
	require.NoError(t, err)
	assert.Equal(t, newMtime, f.ModTime)
}

func TestUtimens_NonExistent(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	mtime := time.Now()
	status := fs.Utimens("nope.mp4", nil, &mtime, nil)
	assert.Equal(t, gofuse.ENOENT, status)
}

func TestUtimens_BackendWithoutUtimer(t *testing.T) {
	// Use a remote-only file on memBackend (which does not implement Utimer).
	fs, meta, _, mb, _ := makeFuseFS(t)
	seedRemoteFile(t, meta, mb, "recordings/remote-ts.mp4", []byte("data"))

	newMtime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	status := fs.Utimens("recordings/remote-ts.mp4", nil, &newMtime, nil)
	assert.Equal(t, gofuse.OK, status)

	// Metadata mtime should still be updated even though backend
	// does not support Utimer.
	ctx := context.Background()
	f, err := meta.GetFile(ctx, "recordings/remote-ts.mp4")
	require.NoError(t, err)
	assert.Equal(t, newMtime, f.ModTime)
}

// ── Truncate tests ───────────────────────────────────────────────────────────

func TestTruncate_NonExistent(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	status := fs.Truncate("nope.mp4", 0, nil)
	assert.Equal(t, gofuse.ENOENT, status)
}

func TestTruncate_Success(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/trunc.mp4", []byte("0123456789"))

	status := fs.Truncate("recordings/trunc.mp4", 5, nil)
	require.Equal(t, gofuse.OK, status)

	// Check the actual file on disk.
	ctx := context.Background()
	localPath, ok := fb.LocalPath("recordings/trunc.mp4")
	require.True(t, ok)
	info, err := os.Stat(localPath)
	require.NoError(t, err)
	assert.Equal(t, int64(5), info.Size())

	// Metadata should reflect the new size.
	require.Eventually(t, func() bool {
		f, err := meta.GetFile(ctx, "recordings/trunc.mp4")
		return err == nil && f.Size == 5
	}, 2*time.Second, 50*time.Millisecond)
}

// ── Mkdir/Rmdir tests ────────────────────────────────────────────────────────

func TestMkdir_AlwaysOK(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	status := fs.Mkdir("some/new/dir", 0o755, nil)
	assert.Equal(t, gofuse.OK, status)
}

func TestRmdir_EmptyDir(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	// No files under this path, so it's an empty virtual directory.
	status := fs.Rmdir("empty-dir", nil)
	assert.Equal(t, gofuse.OK, status)
}

func TestRmdir_NonEmptyDir(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "nonempty/file.txt", []byte("stuff"))

	status := fs.Rmdir("nonempty", nil)
	assert.Equal(t, gofuse.Status(syscall.ENOTEMPTY), status, "should return ENOTEMPTY")
}

// ── Inode helper tests ───────────────────────────────────────────────────────

func TestInode_Unique(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	ino1 := fs.inode("path/a")
	ino2 := fs.inode("path/b")
	assert.NotEqual(t, ino1, ino2, "different paths should get different inodes")
}

func TestInode_Stable(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	ino1 := fs.inode("same/path")
	ino2 := fs.inode("same/path")
	assert.Equal(t, ino1, ino2, "same path should return same inode")
}

// ── isWriteFlags helper test ─────────────────────────────────────────────────

func TestIsWriteFlags(t *testing.T) {
	assert.False(t, isWriteFlags(uint32(os.O_RDONLY)))
	assert.True(t, isWriteFlags(1))  // O_WRONLY
	assert.True(t, isWriteFlags(2))  // O_RDWR
	assert.False(t, isWriteFlags(0)) // O_RDONLY
}

// ── OpenDir entry modes test ─────────────────────────────────────────────────

func TestOpenDir_EntryModes(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "dir/subdir/file.txt", []byte("deep"))
	seedFile(t, fs, meta, fb, "dir/top.txt", []byte("top"))

	entries, status := fs.OpenDir("dir", nil)
	require.Equal(t, gofuse.OK, status)

	for _, e := range entries {
		if e.Name == "subdir" {
			assert.Equal(t, uint32(gofuse.S_IFDIR), e.Mode, "subdir should be a directory")
		}
		if e.Name == "top.txt" {
			assert.Equal(t, uint32(gofuse.S_IFREG), e.Mode, "top.txt should be a regular file")
		}
	}
}

// ── Create verifies stage path for remote backend ────────────────────────────

func TestCreate_MetadataStateWriting(t *testing.T) {
	fs, meta, _, _, _ := makeFuseFS(t)

	// Use a recordings path that should land on tier0 (local file backend).
	fh, status := fs.Create("recordings/writing.mp4", 0, 0o644, nil)
	require.Equal(t, gofuse.OK, status)
	require.NotNil(t, fh)

	ctx := context.Background()
	f, err := meta.GetFile(ctx, "recordings/writing.mp4")
	require.NoError(t, err)
	assert.Equal(t, domain.StateWriting, f.State)
	assert.Equal(t, "tier0", f.CurrentTier)

	fh.Release()
}

// ── Truncate on cold file promotes and truncates ─────────────────────────────

func TestTruncate_ColdFile_Promotes(t *testing.T) {
	fs, meta, fb, mb, _ := makeFuseFS(t)
	coldData := []byte("cold data that will be truncated")
	seedRemoteFile(t, meta, mb, "recordings/cold-trunc.mp4", coldData)

	status := fs.Truncate("recordings/cold-trunc.mp4", 4, nil)
	require.Equal(t, gofuse.OK, status)

	ctx := context.Background()
	// Should now be on tier0 after promotion.
	require.Eventually(t, func() bool {
		localPath, ok := fb.LocalPath("recordings/cold-trunc.mp4")
		if !ok {
			return false
		}
		info, err := os.Stat(localPath)
		return err == nil && info.Size() == 4
	}, 2*time.Second, 50*time.Millisecond)

	f, err := meta.GetFile(ctx, "recordings/cold-trunc.mp4")
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
}

// ── Utimens with both atime and mtime ────────────────────────────────────────

func TestUtimens_BothTimes(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/both.mp4", []byte("data"))

	atime := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	mtime := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	status := fs.Utimens("recordings/both.mp4", &atime, &mtime, nil)
	require.Equal(t, gofuse.OK, status)

	// Check that the file on disk got the times set (file.Backend implements Utimer).
	localPath, ok := fb.LocalPath("recordings/both.mp4")
	require.True(t, ok)
	info, err := os.Stat(localPath)
	require.NoError(t, err)
	assert.Equal(t, mtime.Unix(), info.ModTime().Unix())

	// Metadata mtime should match.
	ctx := context.Background()
	f, err := meta.GetFile(ctx, "recordings/both.mp4")
	require.NoError(t, err)
	assert.Equal(t, mtime, f.ModTime)
}

// ── OpenDir with nested structure ────────────────────────────────────────────

func TestOpenDir_NestedChildren(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "a/b/c/d.txt", []byte("deep"))
	seedFile(t, fs, meta, fb, "a/b/e.txt", []byte("mid"))

	// List "a/b" should show "c" (dir) and "e.txt" (file).
	entries, status := fs.OpenDir("a/b", nil)
	require.Equal(t, gofuse.OK, status)
	assert.Len(t, entries, 2)

	names := make(map[string]uint32)
	for _, e := range entries {
		names[e.Name] = e.Mode
	}
	assert.Equal(t, uint32(gofuse.S_IFDIR), names["c"])
	assert.Equal(t, uint32(gofuse.S_IFREG), names["e.txt"])
}

// ── Stage path for remote open ───────────────────────────────────────────────

func TestOpen_RemoteFile_StaleReStaged(t *testing.T) {
	fs, meta, _, mb, stager := makeFuseFS(t)
	data := []byte("original content")
	seedRemoteFile(t, meta, mb, "recordings/stale.mp4", data)

	// First open stages the file.
	fh1, s1 := fs.Open("recordings/stale.mp4", uint32(os.O_RDONLY), nil)
	require.Equal(t, gofuse.OK, s1)
	fh1.Release()

	stagePath := stager.StagePath("recordings/stale.mp4")
	_, err := os.Stat(stagePath)
	require.NoError(t, err)

	// Simulate the file content changing on the remote side.
	newData := []byte("updated content that is different")
	mb.mu.Lock()
	mb.store["recordings/stale.mp4"] = newData
	mb.mu.Unlock()

	// Update metadata to reflect new size (making staged file stale).
	ctx := context.Background()
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/stale.mp4",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(newData)),
		ModTime:     time.Now(),
	}))

	// Second open should detect staleness and re-stage.
	fh2, s2 := fs.Open("recordings/stale.mp4", uint32(os.O_RDONLY), nil)
	require.Equal(t, gofuse.OK, s2)
	fh2.Release()

	// Read the staged file to verify it was updated.
	staged, err := os.ReadFile(stagePath)
	require.NoError(t, err)
	assert.Equal(t, newData, staged)
}

// ── GetAttr on virtual directory with deeply nested file ─────────────────────

func TestGetAttr_DeepVirtualDirectory(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "a/b/c/d/e.txt", []byte("deep"))

	for _, dir := range []string{"a", "a/b", "a/b/c", "a/b/c/d"} {
		attr, status := fs.GetAttr(dir, nil)
		require.Equal(t, gofuse.OK, status, "dir %q should be found", dir)
		assert.Equal(t, uint32(gofuse.S_IFDIR|0o755), attr.Mode, "dir %q should be a directory", dir)
	}
}

// ── Ensure Create produces a writable local file ─────────────────────────────

func TestCreate_WriteAndReadBack(t *testing.T) {
	fs, _, fb, _, _ := makeFuseFS(t)

	fh, status := fs.Create("test-rw.txt", 0, 0o644, nil)
	require.Equal(t, gofuse.OK, status)

	payload := []byte("write and read back test")
	n, ws := fh.Write(payload, 0)
	require.Equal(t, gofuse.OK, ws)
	assert.Equal(t, uint32(len(payload)), n)

	buf := make([]byte, 100)
	_, rs := fh.Read(buf, 0)
	require.Equal(t, gofuse.EIO, rs)

	fh.Release()

	// Verify on disk.
	localPath, ok := fb.LocalPath("test-rw.txt")
	require.True(t, ok)
	diskData, err := os.ReadFile(localPath)
	require.NoError(t, err)
	assert.Equal(t, payload, diskData)
}

// ── Unlink cleans staged copy ────────────────────────────────────────────────

func TestUnlink_CleansStage(t *testing.T) {
	fs, meta, _, mb, stager := makeFuseFS(t)
	data := []byte("stage me then delete")
	seedRemoteFile(t, meta, mb, "recordings/staged-del.mp4", data)

	// Open to stage.
	fh, s := fs.Open("recordings/staged-del.mp4", uint32(os.O_RDONLY), nil)
	require.Equal(t, gofuse.OK, s)
	fh.Release()

	stagePath := stager.StagePath("recordings/staged-del.mp4")
	_, err := os.Stat(stagePath)
	require.NoError(t, err, "stage file should exist before unlink")

	// Now unlink and re-register metadata so Unlink can find it.
	ctx := context.Background()
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/staged-del.mp4",
		CurrentTier: "tier1",
		State:       domain.StateSynced,
		Size:        int64(len(data)),
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:  "recordings/staged-del.mp4",
		TierName: "tier1",
	}))

	status := fs.Unlink("recordings/staged-del.mp4", nil)
	assert.Equal(t, gofuse.OK, status)

	// Staged copy should be cleaned.
	_, err = os.Stat(stagePath)
	assert.True(t, os.IsNotExist(err), "staged file should be removed after unlink")
}

// ── StatFs tests ─────────────────────────────────────────────────────────────

func TestStatFs_ReturnsNonNil(t *testing.T) {
	fs, _, _, _, _ := makeFuseFS(t)

	out := fs.StatFs("")
	require.NotNil(t, out, "StatFs should return stats for the hot tier")
	assert.NotZero(t, out.Blocks, "should report non-zero total blocks")
	assert.NotZero(t, out.Bfree, "should report non-zero free blocks")
	assert.NotZero(t, out.Bavail, "should report non-zero available blocks")
	assert.NotZero(t, out.Bsize, "should report non-zero block size")
}

func TestStatFs_NamedPath(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	seedFile(t, fs, meta, fb, "recordings/stat.mp4", []byte("data"))

	// StatFs with a named path should still work (returns hot tier stats).
	out := fs.StatFs("recordings/stat.mp4")
	require.NotNil(t, out)
	assert.NotZero(t, out.Blocks)
}

// ── GetAttr hot-tier fallback tests ──────────────────────────────────────────

func TestGetAttr_HotTierFallback(t *testing.T) {
	fs, _, fb, _, _ := makeFuseFS(t)

	// Write a file directly to the hot tier backend WITHOUT recording metadata.
	// This simulates the race where Release/OnWriteComplete hasn't finished yet.
	ctx := context.Background()
	data := []byte("file exists on disk but not in metadata")
	err := fb.Put(ctx, "recordings/race.mp4", bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// GetAttr should find the file via the hot-tier fallback.
	attr, status := fs.GetAttr("recordings/race.mp4", nil)
	require.Equal(t, gofuse.OK, status, "should find file via hot-tier fallback")
	assert.Equal(t, uint32(gofuse.S_IFREG|0o644), attr.Mode)
	assert.Equal(t, uint64(len(data)), attr.Size)
}

// ── Open hot-tier fallback tests ─────────────────────────────────────────────

func TestOpen_HotTierFallback(t *testing.T) {
	fs, _, fb, _, _ := makeFuseFS(t)

	// Write a file directly to the hot tier backend WITHOUT recording metadata.
	ctx := context.Background()
	data := []byte("on disk but not in meta")
	err := fb.Put(ctx, "recordings/race-open.mp4", bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// Open for reading should succeed via the hot-tier fallback.
	fh, status := fs.Open("recordings/race-open.mp4", uint32(os.O_RDONLY), nil)
	require.Equal(t, gofuse.OK, status, "should open file via hot-tier fallback")
	require.NotNil(t, fh)

	buf := make([]byte, 100)
	rr, rs := fh.Read(buf, 0)
	require.Equal(t, gofuse.OK, rs)
	got, _ := rr.Bytes(buf)
	assert.Equal(t, data, got)

	fh.Release()
}

func TestOpen_HotTierFallback_WriteFlags(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)

	// Simulate the faststart race: file exists on disk AND in metadata
	// (from Create), but ReadTarget fails because OnWriteComplete is
	// updating the record. In practice, the metadata row exists from
	// Create so PromoteToHot succeeds — this test verifies that path.
	ctx := context.Background()
	data := []byte("writable race file")
	err := fb.Put(ctx, "recordings/race-write.mp4", bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// Record metadata (as Create would have done).
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/race-write.mp4",
		CurrentTier: "tier0",
		State:       domain.StateWriting,
		Size:        int64(len(data)),
		ModTime:     time.Now(),
	}))

	// Open for writing should succeed — file is already on hot tier.
	fh, status := fs.Open("recordings/race-write.mp4", 2, nil) // O_RDWR
	require.Equal(t, gofuse.OK, status, "should open for write when file is on hot tier")
	require.NotNil(t, fh)
	fh.Release()
}

// ── Rename preserves file content ────────────────────────────────────────────

func TestRename_PreservesContent(t *testing.T) {
	fs, meta, fb, _, _ := makeFuseFS(t)
	data := []byte("preserved content")
	seedFile(t, fs, meta, fb, "recordings/orig.mp4", data)

	status := fs.Rename("recordings/orig.mp4", "recordings/moved.mp4", nil)
	require.Equal(t, gofuse.OK, status)

	// Read from the new path on disk.
	localPath, ok := fb.LocalPath("recordings/moved.mp4")
	require.True(t, ok)
	diskData, err := os.ReadFile(localPath)
	require.NoError(t, err)
	assert.Equal(t, data, diskData)

	// Old path should not exist on disk.
	oldPath := filepath.Join(fb.Root(), "recordings", "orig.mp4")
	_, err = os.Stat(oldPath)
	assert.True(t, os.IsNotExist(err))
}
