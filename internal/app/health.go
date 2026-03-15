package app

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// BackendHealth periodically probes each backend for connectivity and
// tracks per-tier health status. The replicator consults it before
// starting a copy to avoid wasting time on unreachable backends.
type BackendHealth struct {
	mu        sync.RWMutex
	statuses  map[string]bool // tier name → healthy
	tiers     TierLookup
	tierNames []string
	interval  time.Duration
	timeout   time.Duration
	log       *zap.Logger
	reg       *metrics.Registry
	stop      chan struct{}
	done      chan struct{}
}

// NewBackendHealth creates a BackendHealth checker.
func NewBackendHealth(tierNames []string, tiers TierLookup, interval, timeout time.Duration, log *zap.Logger) *BackendHealth {
	statuses := make(map[string]bool, len(tierNames))
	for _, n := range tierNames {
		statuses[n] = true // assume healthy until first probe
	}
	return &BackendHealth{
		statuses:  statuses,
		tiers:     tiers,
		tierNames: tierNames,
		interval:  interval,
		timeout:   timeout,
		log:       log.Named("health"),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// SetRegistry wires Prometheus metrics. Call before Start().
func (bh *BackendHealth) SetRegistry(reg *metrics.Registry) {
	bh.reg = reg
}

// Start begins periodic health probes in a background goroutine.
func (bh *BackendHealth) Start() {
	// Run an initial probe immediately.
	bh.probeAll()
	go bh.loop()
}

// Stop signals the health check loop to exit and waits for completion.
func (bh *BackendHealth) Stop() {
	close(bh.stop)
	<-bh.done
}

// IsHealthy returns the last-known health status for a tier.
// Returns true if the tier is unknown (fail-open for safety).
func (bh *BackendHealth) IsHealthy(tierName string) bool {
	bh.mu.RLock()
	defer bh.mu.RUnlock()
	healthy, ok := bh.statuses[tierName]
	if !ok {
		return true // unknown tier = assume healthy
	}
	return healthy
}

// Statuses returns a snapshot of all tier health states.
func (bh *BackendHealth) Statuses() map[string]bool {
	bh.mu.RLock()
	defer bh.mu.RUnlock()
	out := make(map[string]bool, len(bh.statuses))
	for k, v := range bh.statuses {
		out[k] = v
	}
	return out
}

func (bh *BackendHealth) loop() {
	defer close(bh.done)
	ticker := time.NewTicker(bh.interval)
	defer ticker.Stop()
	for {
		select {
		case <-bh.stop:
			return
		case <-ticker.C:
			bh.probeAll()
		}
	}
}

func (bh *BackendHealth) probeAll() {
	for _, name := range bh.tierNames {
		bh.probe(name)
	}
}

func (bh *BackendHealth) probe(tierName string) {
	ctx, cancel := context.WithTimeout(context.Background(), bh.timeout)
	defer cancel()

	backend, err := bh.tiers.BackendFor(tierName)
	if err != nil {
		bh.setStatus(tierName, false)
		return
	}

	// Use Stat on a non-existent sentinel path. ErrNotExist means the
	// backend is reachable (it responded). Any other error means trouble.
	_, serr := backend.Stat(ctx, ".tierfs-health-probe")
	healthy := serr == nil || serr == domain.ErrNotExist
	bh.setStatus(tierName, healthy)
}

func (bh *BackendHealth) setStatus(tierName string, healthy bool) {
	bh.mu.Lock()
	prev, known := bh.statuses[tierName]
	bh.statuses[tierName] = healthy
	bh.mu.Unlock()

	if bh.reg != nil {
		val := 0.0
		if healthy {
			val = 1.0
		}
		bh.reg.BackendHealthy.WithLabelValues(tierName).Set(val)
	}

	// Log transitions.
	if known && prev != healthy {
		if healthy {
			bh.log.Info("backend recovered", zap.String("tier", tierName))
		} else {
			bh.log.Warn("backend unhealthy", zap.String("tier", tierName))
		}
	}
}
