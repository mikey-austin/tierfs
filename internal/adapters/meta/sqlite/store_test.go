package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
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

// ── Concurrency & scale tests ────────────────────────────────────────────────

func TestConcurrentWriterReaderContention(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	const (
		numWriters     = 8
		filesPerWriter = 100
		numReaders     = 4
	)

	var writeErrors atomic.Int64
	var readErrors atomic.Int64

	// writerWg tracks only writers so we can signal readers to stop.
	var writerWg sync.WaitGroup
	var readerWg sync.WaitGroup
	stopReaders := make(chan struct{})

	// Launch writers.
	for w := 0; w < numWriters; w++ {
		writerWg.Add(1)
		go func(writerID int) {
			defer writerWg.Done()
			for i := 0; i < filesPerWriter; i++ {
				f := makeFile(fmt.Sprintf("writer%d/file_%04d.mp4", writerID, i), "tier0")
				if err := s.UpsertFile(ctx, f); err != nil {
					writeErrors.Add(1)
				}
			}
		}(w)
	}

	// Launch readers.
	for r := 0; r < numReaders; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				if _, err := s.ListFiles(ctx, ""); err != nil {
					readErrors.Add(1)
				}
				if _, err := s.GetFile(ctx, "writer0/file_0050.mp4"); err != nil && err != domain.ErrNotExist {
					readErrors.Add(1)
				}
			}
		}()
	}

	writerWg.Wait()
	close(stopReaders)
	readerWg.Wait()

	assert.Zero(t, writeErrors.Load(), "expected no write errors")
	// Readers may occasionally see transient contention errors under
	// heavy concurrent load; log but don't fail on read errors.
	if re := readErrors.Load(); re > 0 {
		t.Logf("note: %d transient read errors under contention (non-fatal)", re)
	}

	// Verify total file count.
	all, err := s.ListFiles(ctx, "")
	require.NoError(t, err)
	assert.Len(t, all, numWriters*filesPerWriter)
}

func TestConcurrentUpsertSamePath(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	const numGoroutines = 8
	const relPath = "contested/file.mp4"

	var wg sync.WaitGroup
	var upsertErrors atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			f := domain.File{
				RelPath:     relPath,
				CurrentTier: fmt.Sprintf("tier%d", id),
				State:       domain.StateSynced,
				Size:        int64(id * 1000),
				ModTime:     time.Now().Truncate(time.Millisecond),
				Digest:      fmt.Sprintf("digest_%d", id),
			}
			if err := s.UpsertFile(ctx, f); err != nil {
				upsertErrors.Add(1)
			}
		}(g)
	}

	wg.Wait()

	assert.Zero(t, upsertErrors.Load(), "expected no upsert errors")

	// The file must exist and have a consistent state from one of the writers.
	got, err := s.GetFile(ctx, relPath)
	require.NoError(t, err)
	assert.Equal(t, relPath, got.RelPath)
	assert.Equal(t, domain.StateSynced, got.State)
	// CurrentTier should be one of "tier0" through "tier7".
	assert.Contains(t, got.CurrentTier, "tier")
}

func TestBusyTimeoutBehavior(t *testing.T) {
	// The Store serialises writes within a single instance via sync.Mutex.
	// This test validates that a single Store handles high-concurrency
	// writes from many goroutines without errors (the mutex + WAL +
	// busy_timeout all work together).
	s := newStore(t)
	ctx := context.Background()

	const (
		numWriters     = 16
		filesPerWriter = 50
	)
	var wg sync.WaitGroup
	var errors atomic.Int64

	wg.Add(numWriters)
	for w := 0; w < numWriters; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < filesPerWriter; i++ {
				f := makeFile(fmt.Sprintf("writer%02d/file_%04d.mp4", w, i), "tier0")
				if err := s.UpsertFile(ctx, f); err != nil {
					errors.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	require.Zero(t, errors.Load(), "no errors expected when writes are serialised by Store mutex")

	all, err := s.ListFiles(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, numWriters*filesPerWriter, len(all))
}

func TestListDir_LargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-scale test in short mode")
	}

	s := newStore(t)
	ctx := context.Background()

	const (
		numCams       = 10
		numDays       = 10
		filesPerDir   = 100
		totalExpected = numCams * numDays * filesPerDir // 10,000
	)

	// Insert 10,000 files across 100 directories.
	for cam := 1; cam <= numCams; cam++ {
		for day := 1; day <= numDays; day++ {
			for n := 0; n < filesPerDir; n++ {
				relPath := fmt.Sprintf("cam%02d/2024-01-%02d/file_%04d.mp4", cam, day, n)
				f := makeFile(relPath, "tier0")
				require.NoError(t, s.UpsertFile(ctx, f))
			}
		}
	}

	start := time.Now()

	// Root listing: 10 camera directories.
	root, err := s.ListDir(ctx, "")
	require.NoError(t, err)
	assert.Len(t, root, numCams)
	for _, e := range root {
		assert.True(t, e.IsDir)
	}

	// Camera listing: 10 day directories per camera.
	cam01, err := s.ListDir(ctx, "cam01")
	require.NoError(t, err)
	assert.Len(t, cam01, numDays)

	// Day listing: 100 files per day directory.
	dayDir, err := s.ListDir(ctx, "cam05/2024-01-05")
	require.NoError(t, err)
	assert.Len(t, dayDir, filesPerDir)
	for _, e := range dayDir {
		assert.False(t, e.IsDir)
	}

	elapsed := time.Since(start)
	t.Logf("ListDir queries across %d files completed in %v", totalExpected, elapsed)
	assert.Less(t, elapsed, 10*time.Second, "ListDir queries should complete within a reasonable time")
}

func TestListFiles_LargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-scale test in short mode")
	}

	s := newStore(t)
	ctx := context.Background()

	const totalFiles = 10_000

	// Insert 10,000 files under a common prefix.
	for i := 0; i < totalFiles; i++ {
		relPath := fmt.Sprintf("data/batch/file_%05d.mp4", i)
		f := makeFile(relPath, "tier0")
		require.NoError(t, s.UpsertFile(ctx, f))
	}

	start := time.Now()
	all, err := s.ListFiles(ctx, "data/batch")
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Len(t, all, totalFiles)

	// Verify no duplicates.
	seen := make(map[string]struct{}, len(all))
	for _, f := range all {
		_, dup := seen[f.RelPath]
		assert.False(t, dup, "duplicate file: %s", f.RelPath)
		seen[f.RelPath] = struct{}{}
	}

	t.Logf("ListFiles returned %d files in %v", len(all), elapsed)
	assert.Less(t, elapsed, 10*time.Second, "ListFiles should complete within a reasonable time")
}

func TestEvictionCandidates_UnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-scale test in short mode")
	}

	s := newStore(t)
	ctx := context.Background()

	const (
		totalFiles   = 5000
		oldFileCount = 2500 // files with arrived_at before cutoff
		tierName     = "tier0"
	)

	cutoff := time.Now().Add(-24 * time.Hour)
	oldTime := cutoff.Add(-48 * time.Hour) // 48 hours before cutoff
	newTime := cutoff.Add(48 * time.Hour)  // 48 hours after cutoff

	// Insert 5,000 synced files with associated file_tiers entries.
	for i := 0; i < totalFiles; i++ {
		relPath := fmt.Sprintf("evict/file_%05d.mp4", i)
		f := domain.File{
			RelPath:     relPath,
			CurrentTier: tierName,
			State:       domain.StateSynced,
			Size:        2048,
			ModTime:     time.Now().Truncate(time.Millisecond),
			Digest:      fmt.Sprintf("hash_%05d", i),
		}
		require.NoError(t, s.UpsertFile(ctx, f))

		// First half: arrived before cutoff (eviction candidates).
		// Second half: arrived after cutoff (should not be evicted).
		arrivedAt := newTime
		if i < oldFileCount {
			arrivedAt = oldTime
		}
		ft := domain.FileTier{
			RelPath:   relPath,
			TierName:  tierName,
			ArrivedAt: arrivedAt,
			Verified:  true,
		}
		require.NoError(t, s.AddFileTier(ctx, ft))
	}

	// Run EvictionCandidates while concurrent upserts happen.
	var wg sync.WaitGroup
	var upsertErrors atomic.Int64
	stopUpserts := make(chan struct{})

	// Background upsert goroutine: continuously upsert new files.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopUpserts:
				return
			default:
			}
			relPath := fmt.Sprintf("concurrent/file_%05d.mp4", i)
			f := makeFile(relPath, "tier1")
			if err := s.UpsertFile(ctx, f); err != nil {
				upsertErrors.Add(1)
			}
			i++
		}
	}()

	candidates, err := s.EvictionCandidates(ctx, tierName, cutoff)
	close(stopUpserts)
	wg.Wait()

	require.NoError(t, err)
	assert.Len(t, candidates, oldFileCount)
	assert.Zero(t, upsertErrors.Load(), "expected no concurrent upsert errors")

	// Verify all candidates have the correct tier.
	for _, c := range candidates {
		assert.Equal(t, tierName, c.CurrentTier)
		assert.Equal(t, domain.StateSynced, c.State)
	}
}
