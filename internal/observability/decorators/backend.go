// Package decorators provides observability wrappers for the domain ports.
// Each decorator implements the same interface as its target and delegates
// all calls, injecting logging, metrics and tracing around each operation.
//
// Usage:
//
//	raw    := file.New(root)
//	traced := decorators.NewObservableBackend(raw, "tier0", reg, log, tracer)
//
// The decorators are composable: you can stack multiple wrappers if needed,
// though in practice a single ObservableBackend covers all concerns.
package decorators

import (
	"context"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// ObservableBackend wraps a domain.Backend with structured logging, Prometheus
// metrics, and OpenTelemetry tracing. The tierName label is attached to all
// emitted metrics and log fields so dashboards can slice by tier.
type ObservableBackend struct {
	inner    domain.Backend
	tierName string // e.g. "tier0"
	reg      *metrics.Registry
	log      *zap.Logger
	tracer   trace.Tracer
}

// NewObservableBackend wraps inner with full observability.
// tierName is the logical tier this backend serves; it is used as a metric label.
func NewObservableBackend(
	inner domain.Backend,
	tierName string,
	reg *metrics.Registry,
	log *zap.Logger,
	tracer trace.Tracer,
) *ObservableBackend {
	return &ObservableBackend{
		inner:    inner,
		tierName: tierName,
		reg:      reg,
		log:      log.Named("backend").With(zap.String("tier", tierName), zap.String("scheme", inner.Scheme())),
		tracer:   tracer,
	}
}

// Ensure interface compliance at compile time.
var _ domain.Backend = (*ObservableBackend)(nil)

func (o *ObservableBackend) Scheme() string            { return o.inner.Scheme() }
func (o *ObservableBackend) URI(relPath string) string { return o.inner.URI(relPath) }
func (o *ObservableBackend) LocalPath(relPath string) (string, bool) {
	return o.inner.LocalPath(relPath)
}

func (o *ObservableBackend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error {
	ctx, span := o.tracer.Start(ctx, "backend.Put",
		trace.WithAttributes(
			attribute.String("tier", o.tierName),
			attribute.String("rel_path", relPath),
			attribute.Int64("size", size),
		),
	)
	defer span.End()

	start := time.Now()
	o.log.Debug("put start", zap.String("rel_path", relPath), zap.Int64("size", size))

	// Wrap reader to count bytes written.
	cr := &countingReader{r: r}
	err := o.inner.Put(ctx, relPath, cr, size)
	dur := time.Since(start)

	o.reg.BackendDuration.WithLabelValues(o.tierName, "put").Observe(dur.Seconds())
	outcome := outcomeLabel(err)
	o.reg.BackendOps.WithLabelValues(o.tierName, "put", outcome).Inc()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		o.log.Error("put failed",
			zap.String("rel_path", relPath),
			zap.Duration("dur", dur),
			zap.Error(err),
		)
		return err
	}

	o.reg.BackendBytesWritten.WithLabelValues(o.tierName).Add(float64(cr.n))
	o.log.Info("put ok",
		zap.String("rel_path", relPath),
		zap.Int64("bytes", cr.n),
		zap.Duration("dur", dur),
	)
	return nil
}

func (o *ObservableBackend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) {
	ctx, span := o.tracer.Start(ctx, "backend.Get",
		trace.WithAttributes(
			attribute.String("tier", o.tierName),
			attribute.String("rel_path", relPath),
		),
	)

	start := time.Now()
	o.log.Debug("get start", zap.String("rel_path", relPath))

	rc, size, err := o.inner.Get(ctx, relPath)
	dur := time.Since(start)

	o.reg.BackendDuration.WithLabelValues(o.tierName, "get").Observe(dur.Seconds())
	outcome := outcomeLabel(err)
	o.reg.BackendOps.WithLabelValues(o.tierName, "get", outcome).Inc()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		o.log.Debug("get failed",
			zap.String("rel_path", relPath),
			zap.Duration("dur", dur),
			zap.Error(err),
		)
		return nil, 0, err
	}

	o.log.Debug("get ok",
		zap.String("rel_path", relPath),
		zap.Int64("size", size),
		zap.Duration("open_dur", dur),
	)

	// Wrap the body to track bytes read and close the span when reading is done.
	instrumented := &instrumentedReadCloser{
		ReadCloser: rc,
		onClose: func(bytesRead int64) {
			o.reg.BackendBytesRead.WithLabelValues(o.tierName).Add(float64(bytesRead))
			span.SetAttributes(attribute.Int64("bytes_read", bytesRead))
			span.End()
		},
	}
	return instrumented, size, nil
}

func (o *ObservableBackend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) {
	ctx, span := o.tracer.Start(ctx, "backend.Stat",
		trace.WithAttributes(
			attribute.String("tier", o.tierName),
			attribute.String("rel_path", relPath),
		),
	)
	defer span.End()

	start := time.Now()
	fi, err := o.inner.Stat(ctx, relPath)
	dur := time.Since(start)

	o.reg.BackendDuration.WithLabelValues(o.tierName, "stat").Observe(dur.Seconds())
	o.reg.BackendOps.WithLabelValues(o.tierName, "stat", outcomeLabel(err)).Inc()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		o.log.Debug("stat failed", zap.String("rel_path", relPath), zap.Error(err))
	}
	return fi, err
}

func (o *ObservableBackend) Delete(ctx context.Context, relPath string) error {
	ctx, span := o.tracer.Start(ctx, "backend.Delete",
		trace.WithAttributes(
			attribute.String("tier", o.tierName),
			attribute.String("rel_path", relPath),
		),
	)
	defer span.End()

	start := time.Now()
	err := o.inner.Delete(ctx, relPath)
	dur := time.Since(start)

	o.reg.BackendDuration.WithLabelValues(o.tierName, "delete").Observe(dur.Seconds())
	o.reg.BackendOps.WithLabelValues(o.tierName, "delete", outcomeLabel(err)).Inc()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		o.log.Warn("delete failed", zap.String("rel_path", relPath), zap.Error(err))
		return err
	}
	o.log.Info("delete ok", zap.String("rel_path", relPath), zap.Duration("dur", dur))
	return nil
}

func (o *ObservableBackend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) {
	ctx, span := o.tracer.Start(ctx, "backend.List",
		trace.WithAttributes(
			attribute.String("tier", o.tierName),
			attribute.String("prefix", prefix),
		),
	)
	defer span.End()

	start := time.Now()
	items, err := o.inner.List(ctx, prefix)
	dur := time.Since(start)

	o.reg.BackendDuration.WithLabelValues(o.tierName, "list").Observe(dur.Seconds())
	o.reg.BackendOps.WithLabelValues(o.tierName, "list", outcomeLabel(err)).Inc()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		o.log.Error("list failed", zap.String("prefix", prefix), zap.Error(err))
		return nil, err
	}
	span.SetAttributes(attribute.Int("count", len(items)))
	o.log.Debug("list ok", zap.String("prefix", prefix), zap.Int("count", len(items)), zap.Duration("dur", dur))
	return items, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func outcomeLabel(err error) string {
	if err == nil {
		return "ok"
	}
	if err == domain.ErrNotExist {
		return "not_found"
	}
	return "error"
}

// countingReader wraps an io.Reader and counts bytes read.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// instrumentedReadCloser tracks bytes read across the lifetime of a streaming
// body and calls onClose with the total when the body is closed.
type instrumentedReadCloser struct {
	io.ReadCloser
	read    int64
	onClose func(int64)
}

func (i *instrumentedReadCloser) Read(p []byte) (int, error) {
	n, err := i.ReadCloser.Read(p)
	i.read += int64(n)
	return n, err
}

func (i *instrumentedReadCloser) Close() error {
	err := i.ReadCloser.Close()
	i.onClose(i.read)
	return err
}
