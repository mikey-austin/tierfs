package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikey-austin/tierfs/internal/adapters/meta/sqlite"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(filepath.Join(t.TempDir(), "meta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func makeFile(relPath, tier string) domain.File {
	return domain.File{
		RelPath:     relPath,
		CurrentTier: tier,
		State:       domain.StateLocal,
		Size:        1024,
		ModTime:     time.Now().Truncate(time.Millisecond),
		Digest:      "abc123",
	}
}

func TestUpsertAndGetFile(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	f := makeFile("recordings/cam1/clip.mp4", "tier0")
	require.NoError(t, s.UpsertFile(ctx, f))

	got, err := s.GetFile(ctx, f.RelPath)
	require.NoError(t, err)
	assert.Equal(t, f.RelPath, got.RelPath)
	assert.Equal(t, f.CurrentTier, got.CurrentTier)
	assert.Equal(t, f.Digest, got.Digest)
	assert.Equal(t, f.Size, got.Size)
}

func TestUpsert_UpdatesExisting(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	f := makeFile("a.mp4", "tier0")
	require.NoError(t, s.UpsertFile(ctx, f))
	f.CurrentTier = "tier1"
	f.State = domain.StateSynced
	require.NoError(t, s.UpsertFile(ctx, f))

	got, err := s.GetFile(ctx, f.RelPath)
	require.NoError(t, err)
	assert.Equal(t, "tier1", got.CurrentTier)
	assert.Equal(t, domain.StateSynced, got.State)
}

func TestGetFile_NotExist(t *testing.T) {
	s := newStore(t)
	_, err := s.GetFile(context.Background(), "nope.mp4")
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestDeleteFile(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	f := makeFile("del.mp4", "tier0")
	require.NoError(t, s.UpsertFile(ctx, f))
	require.NoError(t, s.DeleteFile(ctx, f.RelPath))
	_, err := s.GetFile(ctx, f.RelPath)
	assert.ErrorIs(t, err, domain.ErrNotExist)
}

func TestDeleteFile_CascadesToTiers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	f := makeFile("cascade.mp4", "tier0")
	require.NoError(t, s.UpsertFile(ctx, f))
	require.NoError(t, s.AddFileTier(ctx, domain.FileTier{
		RelPath: f.RelPath, TierName: "tier0", ArrivedAt: time.Now(),
	}))
	require.NoError(t, s.DeleteFile(ctx, f.RelPath))
	tiers, err := s.GetFileTiers(ctx, f.RelPath)
	require.NoError(t, err)
	assert.Empty(t, tiers)
}

func TestListFiles(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	files := []string{
		"recordings/cam1/a.mp4",
		"recordings/cam2/b.mp4",
		"thumbnails/thumb.jpg",
	}
	for _, p := range files {
		require.NoError(t, s.UpsertFile(ctx, makeFile(p, "tier0")))
	}

	all, err := s.ListFiles(ctx, "")
	require.NoError(t, err)
	assert.Len(t, all, 3)

	rec, err := s.ListFiles(ctx, "recordings")
	require.NoError(t, err)
	assert.Len(t, rec, 2)

	thumbs, err := s.ListFiles(ctx, "thumbnails")
	require.NoError(t, err)
	assert.Len(t, thumbs, 1)
}

func TestFileTierLifecycle(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	f := makeFile("ft.mp4", "tier0")
	require.NoError(t, s.UpsertFile(ctx, f))

	ft := domain.FileTier{
		RelPath:   f.RelPath,
		TierName:  "tier0",
		ArrivedAt: time.Now().Truncate(time.Millisecond),
		Verified:  false,
	}
	require.NoError(t, s.AddFileTier(ctx, ft))

	tiers, err := s.GetFileTiers(ctx, f.RelPath)
	require.NoError(t, err)
	require.Len(t, tiers, 1)
	assert.False(t, tiers[0].Verified)

	require.NoError(t, s.MarkTierVerified(ctx, f.RelPath, "tier0"))
	ok, err := s.IsTierVerified(ctx, f.RelPath, "tier0")
	require.NoError(t, err)
	assert.True(t, ok)

	arr, err := s.TierArrivedAt(ctx, f.RelPath, "tier0")
	require.NoError(t, err)
	assert.WithinDuration(t, ft.ArrivedAt, arr, time.Millisecond)

	require.NoError(t, s.RemoveFileTier(ctx, f.RelPath, "tier0"))
	tiers, err = s.GetFileTiers(ctx, f.RelPath)
	require.NoError(t, err)
	assert.Empty(t, tiers)
}

func TestFilesAwaitingReplication(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	local := makeFile("local.mp4", "tier0")
	local.State = domain.StateLocal
	require.NoError(t, s.UpsertFile(ctx, local))

	synced := makeFile("synced.mp4", "tier1")
	synced.State = domain.StateSynced
	require.NoError(t, s.UpsertFile(ctx, synced))

	pending, err := s.FilesAwaitingReplication(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "local.mp4", pending[0].RelPath)
}

func TestFilesOnTier(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for _, p := range []string{"a.mp4", "b.mp4"} {
		f := makeFile(p, "tier1")
		f.State = domain.StateSynced
		require.NoError(t, s.UpsertFile(ctx, f))
	}
	f3 := makeFile("c.mp4", "tier0")
	f3.State = domain.StateSynced
	require.NoError(t, s.UpsertFile(ctx, f3))

	tier1Files, err := s.FilesOnTier(ctx, "tier1")
	require.NoError(t, err)
	assert.Len(t, tier1Files, 2)
}

func TestListDir(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	paths := []string{
		"recordings/cam1/2026-03/13/10/00.mp4",
		"recordings/cam1/2026-03/13/10/01.mp4",
		"recordings/cam2/2026-03/13/10/00.mp4",
		"thumbnails/cam1/thumb.jpg",
	}
	for _, p := range paths {
		require.NoError(t, s.UpsertFile(ctx, makeFile(p, "tier0")))
	}

	// Root listing: should have "recordings" and "thumbnails" dirs.
	root, err := s.ListDir(ctx, "")
	require.NoError(t, err)
	names := extractNames(root)
	assert.ElementsMatch(t, []string{"recordings", "thumbnails"}, names)
	for _, e := range root {
		assert.True(t, e.IsDir)
	}

	// recordings/ listing: should have cam1, cam2.
	rec, err := s.ListDir(ctx, "recordings")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"cam1", "cam2"}, extractNames(rec))

	// recordings/cam1/ listing.
	cam1, err := s.ListDir(ctx, "recordings/cam1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"2026-03"}, extractNames(cam1))

	// Deep listing eventually reaches files.
	hour, err := s.ListDir(ctx, "recordings/cam1/2026-03/13/10")
	require.NoError(t, err)
	hourNames := extractNames(hour)
	assert.ElementsMatch(t, []string{"00.mp4", "01.mp4"}, hourNames)
	for _, e := range hour {
		assert.False(t, e.IsDir)
	}
}

func extractNames(infos []domain.FileInfo) []string {
	out := make([]string, len(infos))
	for i, fi := range infos {
		out[i] = fi.Name
	}
	return out
}
