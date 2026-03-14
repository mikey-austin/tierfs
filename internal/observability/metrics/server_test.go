package metrics_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// ephemeralPort finds a free TCP port by briefly listening on :0.
func ephemeralPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestServer_StartAndShutdown(t *testing.T) {
	port := ephemeralPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.MetricsConfig{
		Enabled: true,
		Address: addr,
		Path:    "/metrics",
	}
	reg := metrics.New()
	log := zaptest.NewLogger(t)

	srv := metrics.NewServer(cfg, reg, log)
	require.NotNil(t, srv)

	srv.Start()

	// Give the server a moment to bind.
	var resp *http.Response
	var err error
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < 20; i++ {
		resp, err = client.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}

func TestServer_MetricsEndpoint_ContainsMetrics(t *testing.T) {
	port := ephemeralPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.MetricsConfig{
		Enabled: true,
		Address: addr,
		Path:    "/metrics",
	}
	reg := metrics.New()
	log := zaptest.NewLogger(t)

	// Increment a counter so it appears in the output.
	reg.BackendOps.WithLabelValues("local", "put", "ok").Inc()

	srv := metrics.NewServer(cfg, reg, log)
	require.NotNil(t, srv)
	srv.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	var err error
	for i := 0; i < 20; i++ {
		resp, err = client.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "tierfs_backend_operations_total")
}

func TestServer_Shutdown_Idempotent(t *testing.T) {
	port := ephemeralPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.MetricsConfig{
		Enabled: true,
		Address: addr,
		Path:    "/metrics",
	}
	reg := metrics.New()
	log := zaptest.NewLogger(t)

	srv := metrics.NewServer(cfg, reg, log)
	require.NotNil(t, srv)
	srv.Start()

	// Wait for it to bind.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First shutdown should succeed.
	require.NoError(t, srv.Shutdown(ctx))
	// Second shutdown should not panic (http.Server.Shutdown is idempotent).
	assert.NotPanics(t, func() {
		_ = srv.Shutdown(ctx)
	})
}
