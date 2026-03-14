package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// expectedMetricNames lists all custom TierFS metrics that must be registered.
var expectedMetricNames = []string{
	"tierfs_backend_operations_total",
	"tierfs_backend_operation_duration_seconds",
	"tierfs_backend_bytes_read_total",
	"tierfs_backend_bytes_written_total",
	"tierfs_meta_operations_total",
	"tierfs_meta_operation_duration_seconds",
	"tierfs_replication_queue_depth",
	"tierfs_replication_jobs_total",
	"tierfs_replication_job_duration_seconds",
	"tierfs_replication_bytes_transferred_total",
	"tierfs_eviction_events_total",
	"tierfs_fuse_operations_total",
	"tierfs_fuse_operation_duration_seconds",
	"tierfs_fuse_staged_bytes_total",
	"tierfs_tier_file_count",
	"tierfs_tier_bytes_used",
}

func TestNew_AllMetricsRegistered(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	// Touch every Vec metric so that it appears in Gather() output.
	// Vec metrics only emit metric families after WithLabelValues is called.
	reg.BackendOps.WithLabelValues("x", "x", "x")
	reg.BackendDuration.WithLabelValues("x", "x")
	reg.BackendBytesRead.WithLabelValues("x")
	reg.BackendBytesWritten.WithLabelValues("x")
	reg.MetaOps.WithLabelValues("x", "x")
	reg.MetaDuration.WithLabelValues("x")
	reg.ReplicationTotal.WithLabelValues("x", "x", "x")
	reg.ReplicationDuration.WithLabelValues("x", "x")
	reg.ReplicationBytes.WithLabelValues("x", "x")
	reg.EvictionTotal.WithLabelValues("x")
	reg.FuseOps.WithLabelValues("x", "x")
	reg.FuseDuration.WithLabelValues("x")
	reg.TierFileCount.WithLabelValues("x")
	reg.TierBytesUsed.WithLabelValues("x")

	gathered, err := reg.Gather()
	require.NoError(t, err)

	names := gatherNames(gathered)
	for _, want := range expectedMetricNames {
		assert.Contains(t, names, want, "metric %q should be registered", want)
	}
}

func TestNew_CounterIncrement(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	reg.BackendOps.WithLabelValues("local", "put", "ok").Inc()
	reg.BackendOps.WithLabelValues("local", "put", "ok").Inc()

	val := promCounterValue(t, reg, "tierfs_backend_operations_total",
		map[string]string{"backend": "local", "op": "put", "outcome": "ok"})
	assert.Equal(t, float64(2), val)
}

func TestNew_HistogramObserve(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	reg.BackendDuration.WithLabelValues("local", "get").Observe(0.5)
	reg.BackendDuration.WithLabelValues("local", "get").Observe(1.5)

	gathered, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range gathered {
		if mf.GetName() != "tierfs_backend_operation_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), map[string]string{"backend": "local", "op": "get"}) {
				h := m.GetHistogram()
				require.NotNil(t, h)
				assert.Equal(t, uint64(2), h.GetSampleCount())
				assert.Equal(t, 2.0, h.GetSampleSum())
				return
			}
		}
	}
	t.Fatal("histogram metric not found")
}

func TestNew_GaugeSetAndGet(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	reg.ReplicationQueueDepth.Set(42)

	gathered, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range gathered {
		if mf.GetName() != "tierfs_replication_queue_depth" {
			continue
		}
		for _, m := range mf.GetMetric() {
			g := m.GetGauge()
			require.NotNil(t, g)
			assert.Equal(t, float64(42), g.GetValue())
			return
		}
	}
	t.Fatal("gauge metric not found")
}

func TestNew_IsolatedRegistries(t *testing.T) {
	t.Parallel()
	reg1 := metrics.New()
	reg2 := metrics.New()

	reg1.BackendOps.WithLabelValues("s3", "put", "ok").Add(10)
	reg2.BackendOps.WithLabelValues("s3", "put", "ok").Add(3)

	val1 := promCounterValue(t, reg1, "tierfs_backend_operations_total",
		map[string]string{"backend": "s3", "op": "put", "outcome": "ok"})
	val2 := promCounterValue(t, reg2, "tierfs_backend_operations_total",
		map[string]string{"backend": "s3", "op": "put", "outcome": "ok"})

	assert.Equal(t, float64(10), val1)
	assert.Equal(t, float64(3), val2)
}

func TestHandler_Returns200(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	// Touch a Vec metric so it appears in the HTTP output.
	reg.BackendOps.WithLabelValues("local", "get", "ok").Inc()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "tierfs_backend_operations_total")
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func gatherNames(families []*dto.MetricFamily) []string {
	names := make([]string, 0, len(families))
	for _, mf := range families {
		names = append(names, mf.GetName())
	}
	return names
}

func promCounterValue(t *testing.T, reg *metrics.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	gathered, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range gathered {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
			}
		}
	}
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	matched := 0
	for _, lp := range pairs {
		if v, ok := want[lp.GetName()]; ok && v == lp.GetValue() {
			matched++
		}
	}
	return matched == len(want)
}
