// Package observability is the composition root for all observability concerns.
// Wire() assembles the logger, metrics registry, tracer, and wraps domain ports
// with their decorator counterparts. The returned Stack carries everything
// needed by main() and the application layer.
package observability

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mikey-austin/tierfs/internal/admin"
	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/decorators"
	"github.com/mikey-austin/tierfs/internal/observability/logging"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
	"github.com/mikey-austin/tierfs/internal/observability/tracing"
)

// Stack holds all live observability components. Callers should defer Stack.Shutdown().
type Stack struct {
	Log     *zap.Logger
	Metrics *metrics.Registry
	Tracer  trace.Tracer

	metricsServer *metrics.Server
	tracingProv   *tracing.Provider
	logBuffer     *admin.LogBuffer
}

// Wire builds the full observability stack from config, returning a Stack.
// On error the caller should not proceed; a partial stack is not returned.
func Wire(cfg config.ObservabilityConfig, appName string) (*Stack, error) {
	cfg.Defaults()

	// ── Logger ───────────────────────────────────────────────────────────────
	log, err := logging.Build(cfg.Logging)
	if err != nil {
		return nil, err
	}

	// ── Log buffer (tee for admin UI) ────────────────────────────────────────
	logBuf := admin.NewLogBuffer(500)
	log = log.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, logBuf)
	}))

	// ── Tracing ──────────────────────────────────────────────────────────────
	tp, err := tracing.New(cfg.Tracing)
	if err != nil {
		log.Error("failed to initialise tracer", zap.Error(err))
		return nil, err
	}
	tracer := tracing.Tracer(appName)

	// ── Metrics ──────────────────────────────────────────────────────────────
	reg := metrics.New()
	metricsSrv := metrics.NewServer(cfg.Metrics, reg, log)
	metricsSrv.Start()

	return &Stack{
		Log:           log,
		Metrics:       reg,
		Tracer:        tracer,
		metricsServer: metricsSrv,
		tracingProv:   tp,
		logBuffer:     logBuf,
	}, nil
}

// Shutdown flushes all telemetry. Call deferred from main().
func (s *Stack) Shutdown(ctx context.Context) {
	s.metricsServer.Shutdown(ctx) //nolint:errcheck
	s.tracingProv.Shutdown(ctx)   //nolint:errcheck
	s.Log.Sync()                  //nolint:errcheck
}

// Mux returns the HTTP mux from the metrics server for registering extra routes.
func (s *Stack) Mux() *http.ServeMux {
	return s.metricsServer.Mux()
}

// LogBuffer returns the admin log ring buffer.
func (s *Stack) LogBuffer() *admin.LogBuffer {
	return s.logBuffer
}

// WrapBackend applies the full observability decorator to a raw Backend.
func (s *Stack) WrapBackend(inner domain.Backend, tierName string) domain.Backend {
	return decorators.NewObservableBackend(inner, tierName, s.Metrics, s.Log, s.Tracer)
}

// WrapMetaStore applies the full observability decorator to a raw MetadataStore.
func (s *Stack) WrapMetaStore(inner domain.MetadataStore) domain.MetadataStore {
	return decorators.NewObservableMetaStore(inner, s.Metrics, s.Log, s.Tracer)
}
