// Package metrics defines the canonical Prometheus metric set for TierFS.
// All metrics are registered on a single non-global Registry so tests can
// create isolated instances without cross-contamination.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "tierfs"

// Registry holds all TierFS Prometheus metrics.
type Registry struct {
	prom *prometheus.Registry

	// ── Backend metrics ──────────────────────────────────────────────────────

	// BackendOps counts backend calls, labelled by backend, operation, and outcome.
	BackendOps *prometheus.CounterVec
	// BackendDuration tracks backend operation latency.
	BackendDuration *prometheus.HistogramVec
	// BackendBytesRead tracks bytes read per backend.
	BackendBytesRead *prometheus.CounterVec
	// BackendBytesWritten tracks bytes written per backend.
	BackendBytesWritten *prometheus.CounterVec

	// ── Metadata store metrics ───────────────────────────────────────────────

	// MetaOps counts metadata store calls, labelled by operation and outcome.
	MetaOps *prometheus.CounterVec
	// MetaDuration tracks metadata operation latency.
	MetaDuration *prometheus.HistogramVec

	// ── Replication metrics ──────────────────────────────────────────────────

	// ReplicationQueueDepth is the current number of pending copy jobs.
	ReplicationQueueDepth prometheus.Gauge
	// ReplicationLagSeconds is the age of the oldest file awaiting replication.
	ReplicationLagSeconds prometheus.Gauge
	// ReplicationTotal counts completed copy jobs, labelled by outcome.
	ReplicationTotal *prometheus.CounterVec
	// ReplicationDuration tracks time to complete a copy, labelled by tier pair.
	ReplicationDuration *prometheus.HistogramVec
	// ReplicationBytes tracks bytes transferred in copy jobs.
	ReplicationBytes *prometheus.CounterVec

	// ── Eviction metrics ─────────────────────────────────────────────────────

	// EvictionTotal counts eviction events, labelled by from_tier.
	EvictionTotal *prometheus.CounterVec

	// ── FUSE metrics ─────────────────────────────────────────────────────────

	// FuseOps counts FUSE syscall dispatches, labelled by operation.
	FuseOps *prometheus.CounterVec
	// FuseDuration tracks FUSE operation latency, labelled by operation.
	FuseDuration *prometheus.HistogramVec
	// FuseStagedBytes tracks bytes staged from remote backends for FUSE reads.
	FuseStagedBytes prometheus.Counter

	// ── Tier state gauges ────────────────────────────────────────────────────

	// TierFileCount tracks the number of files currently on each tier.
	TierFileCount *prometheus.GaugeVec
	// TierBytesUsed tracks bytes consumed on each tier (from metadata).
	TierBytesUsed *prometheus.GaugeVec

	// ── Backend health ──────────────────────────────────────────────────────

	// BackendHealthy is 1 if the backend is reachable, 0 if not.
	BackendHealthy *prometheus.GaugeVec
}

// New creates a Registry with all metrics registered.
func New() *Registry {
	prom := prometheus.NewRegistry()
	prom.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	r := &Registry{prom: prom}

	// latency buckets: 100µs → 30s, suitable for mixed local/NFS/S3 ops
	latencyBuckets := []float64{
		.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
	}
	// size buckets: 1KB → 10GB
	sizeBuckets := []float64{
		1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
	}

	r.BackendOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "backend",
		Name: "operations_total",
		Help: "Total backend operations by backend name, operation type, and outcome.",
	}, []string{"backend", "op", "outcome"})

	r.BackendDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "backend",
		Name:    "operation_duration_seconds",
		Help:    "Duration of backend operations.",
		Buckets: latencyBuckets,
	}, []string{"backend", "op"})

	r.BackendBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "backend",
		Name: "bytes_read_total",
		Help: "Total bytes read from each backend.",
	}, []string{"backend"})

	r.BackendBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "backend",
		Name: "bytes_written_total",
		Help: "Total bytes written to each backend.",
	}, []string{"backend"})

	r.MetaOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "meta",
		Name: "operations_total",
		Help: "Total metadata store operations by operation and outcome.",
	}, []string{"op", "outcome"})

	r.MetaDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "meta",
		Name:    "operation_duration_seconds",
		Help:    "Duration of metadata store operations.",
		Buckets: latencyBuckets,
	}, []string{"op"})

	r.ReplicationQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "replication",
		Name: "queue_depth",
		Help: "Current number of pending replication jobs.",
	})

	r.ReplicationLagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "replication",
		Name: "lag_seconds",
		Help: "Age in seconds of the oldest file awaiting replication.",
	})

	r.ReplicationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "replication",
		Name: "jobs_total",
		Help: "Total replication jobs completed, by from_tier, to_tier, and outcome.",
	}, []string{"from_tier", "to_tier", "outcome"})

	r.ReplicationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "replication",
		Name:    "job_duration_seconds",
		Help:    "Time to complete a full file copy between tiers.",
		Buckets: latencyBuckets,
	}, []string{"from_tier", "to_tier"})

	r.ReplicationBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "replication",
		Name: "bytes_transferred_total",
		Help: "Total bytes transferred by the replicator, by tier pair.",
	}, []string{"from_tier", "to_tier"})

	r.EvictionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "eviction",
		Name: "events_total",
		Help: "Total eviction events, labelled by source tier.",
	}, []string{"from_tier"})

	r.FuseOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "fuse",
		Name: "operations_total",
		Help: "Total FUSE operations dispatched, by operation name and outcome.",
	}, []string{"op", "outcome"})

	r.FuseDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "fuse",
		Name:    "operation_duration_seconds",
		Help:    "FUSE operation latency.",
		Buckets: latencyBuckets,
	}, []string{"op"})

	r.FuseStagedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "fuse",
		Name: "staged_bytes_total",
		Help: "Bytes staged from remote backends to serve FUSE reads.",
	})

	r.TierFileCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "tier",
		Name: "file_count",
		Help: "Number of files currently on each tier (by current_tier).",
	}, []string{"tier"})

	r.TierBytesUsed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "tier",
		Name: "bytes_used",
		Help: "Total bytes used on each tier, from file metadata.",
		// Size buckets for a histogram; we use a gauge so no buckets here.
	}, []string{"tier"})
	_ = sizeBuckets // used by histogram variants if added later

	r.BackendHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "backend",
		Name: "healthy",
		Help: "Whether each backend is reachable (1 = healthy, 0 = unhealthy).",
	}, []string{"tier"})

	prom.MustRegister(
		r.BackendOps, r.BackendDuration, r.BackendBytesRead, r.BackendBytesWritten,
		r.MetaOps, r.MetaDuration,
		r.ReplicationQueueDepth, r.ReplicationLagSeconds, r.ReplicationTotal, r.ReplicationDuration, r.ReplicationBytes,
		r.EvictionTotal,
		r.BackendHealthy,
		r.FuseOps, r.FuseDuration, r.FuseStagedBytes,
		r.TierFileCount, r.TierBytesUsed,
	)

	return r
}

// Handler returns an HTTP handler that serves the Prometheus metrics page.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.prom, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}
