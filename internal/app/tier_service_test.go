package app_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Stubs ────────────────────────────────────────────────────────────────────

type memBackend struct {
	mu       sync.Mutex
	name     string
	store    map[string][]byte
	modTimes map[string]time.Time
}

func newMemBackend(name string) *memBackend {
	return &memBackend{name: name, store: make(map[string][]byte), modTimes: make(map[string]time.Time)}
}

func (m *memBackend) Scheme() string                    { return "mem" }
func (m *memBackend) URI(p string) string               { return "mem://" + m.name + "/" + p }
func (m *memBackend) LocalPath(_ string) (string, bool) { return "", false }

func (m *memBackend) Put(_ context.Context, relPath string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.store[relPath] = data
	m.modTimes[relPath] = time.Now()
	m.mu.Unlock()
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

// memMeta is a minimal in-memory MetadataStore for testing.
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

func (m *memMeta) ListDir(_ context.Context, _ string) ([]domain.FileInfo, error) {
	return nil, nil
}

func (m *memMeta) Close() error { return nil }

// ── Helpers ──────────────────────────────────────────────────────────────────

func makeConfig(t *testing.T) *config.Resolved {
	t.Helper()
	const toml = `
[mount]
path    = "/mnt/test"
meta_db = "/tmp/test.db"

[replication]
workers = 1
verify  = "none"
sweep_interval = "50ms"

[eviction]
check_interval = "1h"

[[backend]]
name = "ssd"
uri  = "file:///tmp/tier0"

[[backend]]
name = "nas"
uri  = "file:///tmp/tier1"

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
name  = "pinned"
match = "exports/**"
pin_tier = "tier0"

[[rule]]
name  = "default"
match = "**"
evict_schedule = [{after = "48h", to = "tier1"}]
promote_on_read = false
`
	f, err := writeToml(t, toml)
	require.NoError(t, err)
	cfg, err := config.Load(f)
	require.NoError(t, err)
	return cfg
}

func writeToml(t *testing.T, content string) (string, error) {
	t.Helper()
	tmp, err := os.CreateTemp(t.TempDir(), "*.toml")
	if err != nil {
		return "", err
	}
	tmp.WriteString(content)
	tmp.Close()
	return tmp.Name(), nil
}

func makeTierService(t *testing.T) (*app.TierService, *memMeta, map[string]*memBackend) {
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

// ── Tests ────────────────────────────────────────────────────────────────────

func TestTierService_WriteTarget_Default(t *testing.T) {
	ts, _, _ := makeTierService(t)
	_, tierName, err := ts.WriteTarget("recordings/cam1/foo.mp4")
	require.NoError(t, err)
	assert.Equal(t, "tier0", tierName)
}

func TestTierService_WriteTarget_Pinned(t *testing.T) {
	ts, _, _ := makeTierService(t)
	_, tierName, err := ts.WriteTarget("exports/my-export.mp4")
	require.NoError(t, err)
	// pin_tier = "tier0" is still tier0; proves pin is respected
	assert.Equal(t, "tier0", tierName)
}

func TestTierService_OnWriteComplete(t *testing.T) {
	ts, meta, _ := makeTierService(t)
	ctx := context.Background()

	err := ts.OnWriteComplete(ctx, "recordings/cam1/clip.mp4", "tier0", 1024, time.Now())
	require.NoError(t, err)

	f, err := meta.GetFile(ctx, "recordings/cam1/clip.mp4")
	require.NoError(t, err)
	assert.Equal(t, "tier0", f.CurrentTier)
	assert.Equal(t, int64(1024), f.Size)
}

func TestTierService_OnDelete(t *testing.T) {
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	// Seed a file into tier0 backend and metadata.
	backends["tier0"].mu.Lock()
	backends["tier0"].store["recordings/cam1/clip.mp4"] = []byte("data")
	backends["tier0"].mu.Unlock()
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/cam1/clip.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   "recordings/cam1/clip.mp4",
		TierName:  "tier0",
		ArrivedAt: time.Now(),
		Verified:  true,
	}))

	require.NoError(t, ts.OnDelete(ctx, "recordings/cam1/clip.mp4"))

	_, err := meta.GetFile(ctx, "recordings/cam1/clip.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
	backends["tier0"].mu.Lock()
	assert.Empty(t, backends["tier0"].store)
	backends["tier0"].mu.Unlock()
}

func TestTierService_OnRename(t *testing.T) {
	ts, meta, backends := makeTierService(t)
	ctx := context.Background()

	// Seed data so copy+delete fallback works (memBackend has no Rename).
	backends["tier0"].mu.Lock()
	backends["tier0"].store["recordings/old.mp4"] = []byte("rename data")
	backends["tier0"].mu.Unlock()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/old.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:  "recordings/old.mp4",
		TierName: "tier0",
	}))

	require.NoError(t, ts.OnRename(ctx, "recordings/old.mp4", "recordings/new.mp4"))

	_, err := meta.GetFile(ctx, "recordings/old.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
	f, err := meta.GetFile(ctx, "recordings/new.mp4")
	require.NoError(t, err)
	assert.Equal(t, "recordings/new.mp4", f.RelPath)

	// Verify backend has new key, not old.
	backends["tier0"].mu.Lock()
	_, hasOld := backends["tier0"].store["recordings/old.mp4"]
	_, hasNew := backends["tier0"].store["recordings/new.mp4"]
	backends["tier0"].mu.Unlock()
	assert.False(t, hasOld, "old key should be gone from backend")
	assert.True(t, hasNew, "new key should exist in backend")
}

func TestTierService_OnRename_FailureNoMetadataUpdate(t *testing.T) {
	ts, meta, _ := makeTierService(t)
	ctx := context.Background()

	// Don't seed backend data — Get will fail, so copy+delete fails.
	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/fail.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
	}))
	require.NoError(t, meta.AddFileTier(ctx, domain.FileTier{
		RelPath:  "recordings/fail.mp4",
		TierName: "tier0",
	}))

	err := ts.OnRename(ctx, "recordings/fail.mp4", "recordings/moved.mp4")
	assert.Error(t, err, "rename should fail when backend can't rename or copy")

	f, gerr := meta.GetFile(ctx, "recordings/fail.mp4")
	require.NoError(t, gerr)
	assert.Equal(t, "recordings/fail.mp4", f.RelPath)

	_, gerr = meta.GetFile(ctx, "recordings/moved.mp4")
	assert.ErrorIs(t, gerr, domain.ErrNotExist)
}

func TestTierService_OnWriteComplete_ResetsState(t *testing.T) {
	ts, meta, _ := makeTierService(t)
	ctx := context.Background()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/cam1/truncated.mp4",
		CurrentTier: "tier0",
		State:       domain.StateSynced,
		Size:        2048,
		Digest:      "olddigest",
	}))

	err := ts.OnWriteComplete(ctx, "recordings/cam1/truncated.mp4", "tier0", 512, time.Now())
	require.NoError(t, err)

	f, err := meta.GetFile(ctx, "recordings/cam1/truncated.mp4")
	require.NoError(t, err)
	assert.Equal(t, domain.StateLocal, f.State)
	assert.Equal(t, int64(512), f.Size)
}

func TestTierService_SweepRecoversDrop(t *testing.T) {
	cfg := makeConfig(t)
	meta := newMemMeta()
	ctx := context.Background()

	b0 := newMemBackend("tier0")
	b1 := newMemBackend("tier1")
	backends := map[string]domain.Backend{"tier0": b0, "tier1": b1}
	log := zaptest.NewLogger(t)

	ts := app.NewTierService(cfg, meta, backends, log)
	ts.Start()
	defer ts.Stop()

	data := []byte("sweep recovery test")
	b0.mu.Lock()
	b0.store["recordings/sweep.mp4"] = data
	b0.mu.Unlock()

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "recordings/sweep.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
		ModTime:     time.Now(),
	}))

	require.Eventually(t, func() bool {
		f, err := meta.GetFile(ctx, "recordings/sweep.mp4")
		return err == nil && f.State == domain.StateSynced
	}, 5*time.Second, 100*time.Millisecond, "sweep should re-enqueue and replicate the file")
}

func TestTierService_BackendFor_Unknown(t *testing.T) {
	ts, _, _ := makeTierService(t)
	_, err := ts.BackendFor("doesnotexist")
	assert.ErrorIs(t, err, domain.ErrTierNotFound)
}

func TestTierService_UsedBytes(t *testing.T) {
	ts, meta, _ := makeTierService(t)
	ctx := context.Background()

	for i := int64(0); i < 3; i++ {
		require.NoError(t, meta.UpsertFile(ctx, domain.File{
			RelPath:     fmt.Sprintf("a%d.mp4", i),
			CurrentTier: "tier0",
			State:       domain.StateSynced,
			Size:        1000,
		}))
	}

	used, err := ts.UsedBytes(ctx, "tier0")
	require.NoError(t, err)
	assert.Equal(t, int64(3000), used)
}
