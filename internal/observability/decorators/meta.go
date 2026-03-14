package decorators

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// ObservableMetaStore wraps a domain.MetadataStore with structured logging,
// Prometheus metrics, and OpenTelemetry tracing on every method call.
type ObservableMetaStore struct {
	inner  domain.MetadataStore
	reg    *metrics.Registry
	log    *zap.Logger
	tracer trace.Tracer
}

// NewObservableMetaStore wraps inner with full observability.
func NewObservableMetaStore(
	inner domain.MetadataStore,
	reg *metrics.Registry,
	log *zap.Logger,
	tracer trace.Tracer,
) *ObservableMetaStore {
	return &ObservableMetaStore{
		inner:  inner,
		reg:    reg,
		log:    log.Named("meta"),
		tracer: tracer,
	}
}

// Ensure interface compliance at compile time.
var _ domain.MetadataStore = (*ObservableMetaStore)(nil)

// observe wraps an operation with timing, metrics, and a span.
// Returns a done func that must be called at the end of the operation.
func (o *ObservableMetaStore) observe(ctx context.Context, op string) (context.Context, func(error)) {
	ctx, span := o.tracer.Start(ctx, "meta."+op,
		trace.WithAttributes(attribute.String("op", op)),
	)
	start := time.Now()
	return ctx, func(err error) {
		dur := time.Since(start)
		o.reg.MetaDuration.WithLabelValues(op).Observe(dur.Seconds())
		o.reg.MetaOps.WithLabelValues(op, outcomeLabel(err)).Inc()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			o.log.Error("meta op failed",
				zap.String("op", op),
				zap.Duration("dur", dur),
				zap.Error(err),
			)
		} else {
			o.log.Debug("meta op ok", zap.String("op", op), zap.Duration("dur", dur))
		}
		span.End()
	}
}

func (o *ObservableMetaStore) UpsertFile(ctx context.Context, f domain.File) error {
	ctx, done := o.observe(ctx, "UpsertFile")
	err := o.inner.UpsertFile(ctx, f)
	done(err)
	return err
}

func (o *ObservableMetaStore) GetFile(ctx context.Context, relPath string) (*domain.File, error) {
	ctx, done := o.observe(ctx, "GetFile")
	f, err := o.inner.GetFile(ctx, relPath)
	done(err)
	return f, err
}

func (o *ObservableMetaStore) DeleteFile(ctx context.Context, relPath string) error {
	ctx, done := o.observe(ctx, "DeleteFile")
	err := o.inner.DeleteFile(ctx, relPath)
	done(err)
	return err
}

func (o *ObservableMetaStore) ListFiles(ctx context.Context, prefix string) ([]domain.File, error) {
	ctx, done := o.observe(ctx, "ListFiles")
	files, err := o.inner.ListFiles(ctx, prefix)
	done(err)
	return files, err
}

func (o *ObservableMetaStore) AddFileTier(ctx context.Context, ft domain.FileTier) error {
	ctx, done := o.observe(ctx, "AddFileTier")
	err := o.inner.AddFileTier(ctx, ft)
	done(err)
	return err
}

func (o *ObservableMetaStore) GetFileTiers(ctx context.Context, relPath string) ([]domain.FileTier, error) {
	ctx, done := o.observe(ctx, "GetFileTiers")
	tiers, err := o.inner.GetFileTiers(ctx, relPath)
	done(err)
	return tiers, err
}

func (o *ObservableMetaStore) MarkTierVerified(ctx context.Context, relPath, tierName string) error {
	ctx, done := o.observe(ctx, "MarkTierVerified")
	err := o.inner.MarkTierVerified(ctx, relPath, tierName)
	done(err)
	return err
}

func (o *ObservableMetaStore) RemoveFileTier(ctx context.Context, relPath, tierName string) error {
	ctx, done := o.observe(ctx, "RemoveFileTier")
	err := o.inner.RemoveFileTier(ctx, relPath, tierName)
	done(err)
	return err
}

func (o *ObservableMetaStore) TierArrivedAt(ctx context.Context, relPath, tierName string) (time.Time, error) {
	ctx, done := o.observe(ctx, "TierArrivedAt")
	t, err := o.inner.TierArrivedAt(ctx, relPath, tierName)
	done(err)
	return t, err
}

func (o *ObservableMetaStore) IsTierVerified(ctx context.Context, relPath, tierName string) (bool, error) {
	ctx, done := o.observe(ctx, "IsTierVerified")
	ok, err := o.inner.IsTierVerified(ctx, relPath, tierName)
	done(err)
	return ok, err
}

func (o *ObservableMetaStore) FilesOnTier(ctx context.Context, tierName string) ([]domain.File, error) {
	ctx, done := o.observe(ctx, "FilesOnTier")
	files, err := o.inner.FilesOnTier(ctx, tierName)
	done(err)
	return files, err
}

func (o *ObservableMetaStore) FilesAwaitingReplication(ctx context.Context) ([]domain.File, error) {
	ctx, done := o.observe(ctx, "FilesAwaitingReplication")
	files, err := o.inner.FilesAwaitingReplication(ctx)
	done(err)
	return files, err
}

func (o *ObservableMetaStore) ListDir(ctx context.Context, dirPath string) ([]domain.FileInfo, error) {
	ctx, done := o.observe(ctx, "ListDir")
	entries, err := o.inner.ListDir(ctx, dirPath)
	done(err)
	return entries, err
}

func (o *ObservableMetaStore) Close() error {
	err := o.inner.Close()
	if err != nil {
		o.log.Error("meta store close failed", zap.Error(err))
	}
	return err
}
