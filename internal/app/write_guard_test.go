package app_test

import (
	"testing"
	"time"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/stretchr/testify/assert"
)

func TestWriteGuard_NoEntry_NotActive(t *testing.T) {
	g := app.NewWriteGuard(0)
	active, reason := g.IsWriteActive("recordings/cam1/clip.mp4")
	assert.False(t, active)
	assert.Empty(t, reason)
}

func TestWriteGuard_OpenHandle_Blocks(t *testing.T) {
	g := app.NewWriteGuard(0)
	g.Open("recordings/clip.mp4")
	active, reason := g.IsWriteActive("recordings/clip.mp4")
	assert.True(t, active)
	assert.Equal(t, "open write handle", reason)
}

func TestWriteGuard_CloseHandle_Unblocks(t *testing.T) {
	g := app.NewWriteGuard(0) // no quiescence
	g.Open("clip.mp4")
	g.Close("clip.mp4")
	active, _ := g.IsWriteActive("clip.mp4")
	assert.False(t, active)
}

func TestWriteGuard_MultipleHandles_AllMustClose(t *testing.T) {
	g := app.NewWriteGuard(0)
	g.Open("clip.mp4")
	g.Open("clip.mp4") // second handle (e.g. concurrent writer)
	g.Close("clip.mp4")

	active, reason := g.IsWriteActive("clip.mp4")
	assert.True(t, active, "one handle still open")
	assert.Equal(t, "open write handle", reason)

	g.Close("clip.mp4")
	active, _ = g.IsWriteActive("clip.mp4")
	assert.False(t, active, "all handles closed with no quiescence")
}

func TestWriteGuard_Quiescence_BlocksAfterClose(t *testing.T) {
	g := app.NewWriteGuard(200 * time.Millisecond)
	g.Open("clip.mp4")
	g.Close("clip.mp4")

	// Immediately after close, quiescence window should still be active.
	active, reason := g.IsWriteActive("clip.mp4")
	assert.True(t, active)
	assert.Equal(t, "quiescence window", reason)
}

func TestWriteGuard_Quiescence_UnblocksAfterWindow(t *testing.T) {
	g := app.NewWriteGuard(50 * time.Millisecond)
	g.Open("clip.mp4")
	g.Close("clip.mp4")

	// Wait for quiescence window to elapse.
	time.Sleep(100 * time.Millisecond)

	active, _ := g.IsWriteActive("clip.mp4")
	assert.False(t, active, "quiescence window should have elapsed")
}

func TestWriteGuard_ReopenDuringQuiescence_ResetsWindow(t *testing.T) {
	// File is closed, quiescence starts. Then app re-opens it.
	// Should be write-active again (open handle).
	g := app.NewWriteGuard(500 * time.Millisecond)
	g.Open("clip.mp4")
	g.Close("clip.mp4")

	// Re-open before quiescence expires.
	g.Open("clip.mp4")
	active, reason := g.IsWriteActive("clip.mp4")
	assert.True(t, active)
	assert.Equal(t, "open write handle", reason)
}

func TestWriteGuard_Forget_ClearsEntry(t *testing.T) {
	g := app.NewWriteGuard(10 * time.Second)
	g.Open("clip.mp4")
	g.Close("clip.mp4")
	g.Forget("clip.mp4")

	active, _ := g.IsWriteActive("clip.mp4")
	assert.False(t, active, "forgotten entry should not block")
}

func TestWriteGuard_DifferentPaths_Independent(t *testing.T) {
	g := app.NewWriteGuard(0)
	g.Open("a.mp4")

	activeA, _ := g.IsWriteActive("a.mp4")
	activeB, _ := g.IsWriteActive("b.mp4")
	assert.True(t, activeA)
	assert.False(t, activeB)
}

func TestWriteGuard_ZeroQuiescence_NoIdleBlock(t *testing.T) {
	g := app.NewWriteGuard(0)
	g.Open("clip.mp4")
	g.Close("clip.mp4")
	// With quiescence=0, close should immediately unblock.
	active, _ := g.IsWriteActive("clip.mp4")
	assert.False(t, active)
}

func TestWriteGuard_Snapshot_IncludesActiveOnly(t *testing.T) {
	g := app.NewWriteGuard(500 * time.Millisecond)
	g.Open("active.mp4")
	g.Open("quiescing.mp4")
	g.Close("quiescing.mp4")
	// "done.mp4" has fully elapsed (no entry)

	snap := g.Snapshot()
	assert.Contains(t, snap, "active.mp4")
	assert.Contains(t, snap, "quiescing.mp4")
	assert.NotContains(t, snap, "done.mp4")

	assert.Equal(t, 1, snap["active.mp4"].OpenCount)
	assert.True(t, snap["quiescing.mp4"].QuiescentSoon)
}

func TestWriteGuard_Concurrent(t *testing.T) {
	g := app.NewWriteGuard(10 * time.Millisecond)
	done := make(chan struct{})

	// 20 goroutines all open/close the same path concurrently.
	for i := 0; i < 20; i++ {
		go func() {
			g.Open("concurrent.mp4")
			time.Sleep(time.Millisecond)
			g.Close("concurrent.mp4")
			done <- struct{}{}
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}

	// After all goroutines finish and quiescence elapses, should be inactive.
	time.Sleep(50 * time.Millisecond)
	active, _ := g.IsWriteActive("concurrent.mp4")
	assert.False(t, active)
}
