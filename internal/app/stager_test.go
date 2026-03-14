package app_test

import (
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStagePath_NoCollision(t *testing.T) {
	s := app.NewStager(t.TempDir(), zaptest.NewLogger(t))
	p1 := s.StagePath("a/b__c")
	p2 := s.StagePath("a__b/c")
	assert.NotEqual(t, p1, p2, "paths with __ should not collide")
}

func TestStagePath_Deterministic(t *testing.T) {
	s := app.NewStager(t.TempDir(), zaptest.NewLogger(t))
	assert.Equal(t, s.StagePath("foo/bar.mp4"), s.StagePath("foo/bar.mp4"))
}

func TestStagePath_PreservesBasename(t *testing.T) {
	s := app.NewStager(t.TempDir(), zaptest.NewLogger(t))
	p := s.StagePath("recordings/cam1/clip.mp4")
	assert.Contains(t, filepath.Base(p), "clip.mp4")
}

func TestIsStale_NoSidecar(t *testing.T) {
	s := app.NewStager(t.TempDir(), zaptest.NewLogger(t))
	assert.True(t, s.IsStale("/nonexistent", "abc", time.Now(), 100))
}

func TestIsStale_MatchingMeta(t *testing.T) {
	dir := t.TempDir()
	s := app.NewStager(dir, zaptest.NewLogger(t))
	stagePath := filepath.Join(dir, "test")
	now := time.Now().Truncate(time.Microsecond)
	require.NoError(t, s.WriteMeta(stagePath, app.StageMeta{
		Digest: "abc123", ModTime: now, Size: 100,
	}))
	assert.False(t, s.IsStale(stagePath, "abc123", now, 100))
}

func TestIsStale_DigestMismatch(t *testing.T) {
	dir := t.TempDir()
	s := app.NewStager(dir, zaptest.NewLogger(t))
	stagePath := filepath.Join(dir, "test")
	now := time.Now().Truncate(time.Microsecond)
	require.NoError(t, s.WriteMeta(stagePath, app.StageMeta{
		Digest: "abc123", ModTime: now, Size: 100,
	}))
	assert.True(t, s.IsStale(stagePath, "different", now, 100))
}

func TestIsStale_SizeMismatch(t *testing.T) {
	dir := t.TempDir()
	s := app.NewStager(dir, zaptest.NewLogger(t))
	stagePath := filepath.Join(dir, "test")
	now := time.Now().Truncate(time.Microsecond)
	require.NoError(t, s.WriteMeta(stagePath, app.StageMeta{
		Digest: "abc", ModTime: now, Size: 100,
	}))
	assert.True(t, s.IsStale(stagePath, "abc", now, 999))
}

func TestWriteAndReadMeta_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := app.NewStager(dir, zaptest.NewLogger(t))
	stagePath := filepath.Join(dir, "roundtrip")
	now := time.Now().Truncate(time.Microsecond)
	original := app.StageMeta{Digest: "deadbeef", ModTime: now, Size: 42}
	require.NoError(t, s.WriteMeta(stagePath, original))
	got, err := s.ReadMeta(stagePath)
	require.NoError(t, err)
	assert.Equal(t, original.Digest, got.Digest)
	assert.Equal(t, original.Size, got.Size)
	assert.True(t, original.ModTime.Equal(got.ModTime))
}
