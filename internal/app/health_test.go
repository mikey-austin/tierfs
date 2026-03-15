package app_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// errorBackend always returns an error on Stat (simulates unreachable backend).
type errorBackend struct{ name string }

func (e *errorBackend) Scheme() string                    { return "err" }
func (e *errorBackend) URI(p string) string               { return "err://" + p }
func (e *errorBackend) LocalPath(_ string) (string, bool) { return "", false }
func (e *errorBackend) Put(_ context.Context, _ string, r io.Reader, _ int64) error {
	return fmt.Errorf("unreachable")
}
func (e *errorBackend) Get(_ context.Context, _ string) (io.ReadCloser, int64, error) {
	return nil, 0, fmt.Errorf("unreachable")
}
func (e *errorBackend) Stat(_ context.Context, _ string) (*domain.FileInfo, error) {
	return nil, fmt.Errorf("connection refused")
}
func (e *errorBackend) Delete(_ context.Context, _ string) error { return fmt.Errorf("unreachable") }
func (e *errorBackend) List(_ context.Context, _ string) ([]domain.FileInfo, error) {
	return nil, fmt.Errorf("unreachable")
}

// swappableBackend delegates to an inner backend that can be swapped safely.
type swappableBackend struct {
	mu      sync.RWMutex
	current domain.Backend
}

func (s *swappableBackend) inner() domain.Backend {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}
func (s *swappableBackend) Scheme() string                    { return s.inner().Scheme() }
func (s *swappableBackend) URI(p string) string               { return s.inner().URI(p) }
func (s *swappableBackend) LocalPath(p string) (string, bool) { return s.inner().LocalPath(p) }
func (s *swappableBackend) Put(ctx context.Context, p string, r io.Reader, sz int64) error {
	return s.inner().Put(ctx, p, r, sz)
}
func (s *swappableBackend) Get(ctx context.Context, p string) (io.ReadCloser, int64, error) {
	return s.inner().Get(ctx, p)
}
func (s *swappableBackend) Stat(ctx context.Context, p string) (*domain.FileInfo, error) {
	return s.inner().Stat(ctx, p)
}
func (s *swappableBackend) Delete(ctx context.Context, p string) error {
	return s.inner().Delete(ctx, p)
}
func (s *swappableBackend) List(ctx context.Context, p string) ([]domain.FileInfo, error) {
	return s.inner().List(ctx, p)
}

func TestBackendHealth_HealthyBackend(t *testing.T) {
	src := newMemBackend("tier0")
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src}}
	log := zaptest.NewLogger(t)

	h := app.NewBackendHealth([]string{"tier0"}, lookup, time.Hour, 5*time.Second, log)
	// Initial probe runs in Start.
	h.Start()
	defer h.Stop()

	assert.True(t, h.IsHealthy("tier0"), "memBackend returns ErrNotExist on Stat → healthy")
}

func TestBackendHealth_UnhealthyBackend(t *testing.T) {
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{
		"tier0": &errorBackend{name: "tier0"},
	}}
	log := zaptest.NewLogger(t)

	h := app.NewBackendHealth([]string{"tier0"}, lookup, time.Hour, 5*time.Second, log)
	h.Start()
	defer h.Stop()

	assert.False(t, h.IsHealthy("tier0"), "errorBackend should be unhealthy")
}

func TestBackendHealth_RecoveryAfterFailure(t *testing.T) {
	// Start with an error backend, then swap to a healthy one.
	// Use a swappableBackend to avoid data races on the lookup map.
	sw := &swappableBackend{current: &errorBackend{name: "tier0"}}
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": sw}}
	log := zaptest.NewLogger(t)

	h := app.NewBackendHealth([]string{"tier0"}, lookup, 50*time.Millisecond, 5*time.Second, log)
	h.Start()
	defer h.Stop()

	assert.False(t, h.IsHealthy("tier0"), "should start unhealthy")

	// Swap to healthy backend (thread-safe via atomic-style field).
	sw.mu.Lock()
	sw.current = newMemBackend("tier0")
	sw.mu.Unlock()

	// Wait for next probe cycle.
	require.Eventually(t, func() bool {
		return h.IsHealthy("tier0")
	}, 3*time.Second, 25*time.Millisecond, "should recover after backend becomes reachable")
}

func TestBackendHealth_UnknownTier_FailOpen(t *testing.T) {
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{}}
	log := zaptest.NewLogger(t)

	h := app.NewBackendHealth([]string{}, lookup, time.Hour, 5*time.Second, log)
	h.Start()
	defer h.Stop()

	assert.True(t, h.IsHealthy("unknown-tier"), "unknown tier should be assumed healthy")
}

func TestBackendHealth_Statuses_Snapshot(t *testing.T) {
	src := newMemBackend("tier0")
	errB := &errorBackend{name: "tier1"}
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{
		"tier0": src,
		"tier1": errB,
	}}
	log := zaptest.NewLogger(t)

	h := app.NewBackendHealth([]string{"tier0", "tier1"}, lookup, time.Hour, 5*time.Second, log)
	h.Start()
	defer h.Stop()

	statuses := h.Statuses()
	assert.True(t, statuses["tier0"])
	assert.False(t, statuses["tier1"])
}

func TestBackendHealth_MetricsUpdated(t *testing.T) {
	src := newMemBackend("tier0")
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src}}
	log := zaptest.NewLogger(t)
	reg := metrics.New()

	h := app.NewBackendHealth([]string{"tier0"}, lookup, time.Hour, 5*time.Second, log)
	h.SetRegistry(reg)
	h.Start()
	defer h.Stop()

	// Gather and check the metric.
	families, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, mf := range families {
		if mf.GetName() == "tierfs_backend_healthy" {
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "tier" && l.GetValue() == "tier0" {
						assert.Equal(t, 1.0, m.GetGauge().GetValue())
						found = true
					}
				}
			}
		}
	}
	assert.True(t, found, "tierfs_backend_healthy metric should exist for tier0")
}

func TestReplicator_SkipsUnhealthyBackend(t *testing.T) {
	meta := newMemMeta()
	ctx := context.Background()

	src := newMemBackend("tier0")
	dst := newMemBackend("tier1")
	data := []byte("health check test")
	src.store["health.mp4"] = data

	require.NoError(t, meta.UpsertFile(ctx, domain.File{
		RelPath:     "health.mp4",
		CurrentTier: "tier0",
		State:       domain.StateLocal,
		Size:        int64(len(data)),
	}))

	// Create a health checker where tier1 is unhealthy.
	errB := &errorBackend{name: "tier1"}
	healthLookup := &fakeTierLookup{backends: map[string]domain.Backend{
		"tier0": src,
		"tier1": errB,
	}}
	log := zaptest.NewLogger(t)
	health := app.NewBackendHealth([]string{"tier0", "tier1"}, healthLookup, time.Hour, 5*time.Second, log)
	health.Start()
	defer health.Stop()

	// The replicator uses the real backends (dst is healthy), but health says tier1 is down.
	lookup := &fakeTierLookup{backends: map[string]domain.Backend{"tier0": src, "tier1": dst}}
	cfg := app.ReplicatorConfig{
		Workers:       1,
		MaxRetries:    0,
		RetryInterval: 50 * time.Millisecond,
		Verify:        "none",
	}
	r := app.NewReplicator(cfg, meta, lookup, log)
	r.SetHealth(health)
	r.Start()
	defer r.Stop()

	r.Enqueue(app.CopyJob{RelPath: "health.mp4", FromTier: "tier0", ToTier: "tier1"})

	// Give the worker time to process and skip.
	time.Sleep(300 * time.Millisecond)

	// File should NOT have been copied — backend was unhealthy.
	dst.mu.Lock()
	_, hasCopy := dst.store["health.mp4"]
	dst.mu.Unlock()
	assert.False(t, hasCopy, "file should not be copied to unhealthy backend")

	// No failures should be recorded (it's a skip, not a failure).
	_, failed, _ := r.Metrics()
	assert.Equal(t, int64(0), failed)
}
