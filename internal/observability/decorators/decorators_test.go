package decorators_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/mikey-austin/tierfs/internal/observability/decorators"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Fake Backend ─────────────────────────────────────────────────────────────

type fakeBackend struct {
	putErr  error
	getErr  error
	statErr error
	delErr  error
	listErr error

	putCalled  bool
	getCalled  bool
	statCalled bool
	delCalled  bool
	listCalled bool

	data []byte
}

func (f *fakeBackend) Scheme() string                    { return "fake" }
func (f *fakeBackend) URI(relPath string) string         { return "fake://" + relPath }
func (f *fakeBackend) LocalPath(_ string) (string, bool) { return "", false }

func (f *fakeBackend) Put(_ context.Context, _ string, r io.Reader, _ int64) error {
	f.putCalled = true
	if f.putErr != nil {
		return f.putErr
	}
	f.data, _ = io.ReadAll(r)
	return nil
}

func (f *fakeBackend) Get(_ context.Context, _ string) (io.ReadCloser, int64, error) {
	f.getCalled = true
	if f.getErr != nil {
		return nil, 0, f.getErr
	}
	return io.NopCloser(bytes.NewReader(f.data)), int64(len(f.data)), nil
}

func (f *fakeBackend) Stat(_ context.Context, _ string) (*domain.FileInfo, error) {
	f.statCalled = true
	return &domain.FileInfo{Size: 42}, f.statErr
}

func (f *fakeBackend) Delete(_ context.Context, _ string) error {
	f.delCalled = true
	return f.delErr
}

func (f *fakeBackend) List(_ context.Context, _ string) ([]domain.FileInfo, error) {
	f.listCalled = true
	return nil, f.listErr
}

// ── Fake MetadataStore ───────────────────────────────────────────────────────

type fakeMetaStore struct {
	upsertErr error
	getErr    error
}

func (f *fakeMetaStore) UpsertFile(_ context.Context, _ domain.File) error { return f.upsertErr }
func (f *fakeMetaStore) GetFile(_ context.Context, _ string) (*domain.File, error) {
	return &domain.File{RelPath: "x.mp4"}, f.getErr
}
func (f *fakeMetaStore) DeleteFile(_ context.Context, _ string) error { return nil }
func (f *fakeMetaStore) ListFiles(_ context.Context, _ string) ([]domain.File, error) {
	return nil, nil
}
func (f *fakeMetaStore) AddFileTier(_ context.Context, _ domain.FileTier) error { return nil }
func (f *fakeMetaStore) GetFileTiers(_ context.Context, _ string) ([]domain.FileTier, error) {
	return nil, nil
}
func (f *fakeMetaStore) MarkTierVerified(_ context.Context, _, _ string) error { return nil }
func (f *fakeMetaStore) RemoveFileTier(_ context.Context, _, _ string) error   { return nil }
func (f *fakeMetaStore) TierArrivedAt(_ context.Context, _, _ string) (time.Time, error) {
	return time.Now(), nil
}
func (f *fakeMetaStore) IsTierVerified(_ context.Context, _, _ string) (bool, error) {
	return true, nil
}
func (f *fakeMetaStore) FilesOnTier(_ context.Context, _ string) ([]domain.File, error) {
	return nil, nil
}
func (f *fakeMetaStore) FilesAwaitingReplication(_ context.Context) ([]domain.File, error) {
	return nil, nil
}
func (f *fakeMetaStore) OldestAwaitingReplication(_ context.Context) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeMetaStore) EvictionCandidates(_ context.Context, _ string, _ time.Time) ([]domain.File, error) {
	return nil, nil
}
func (f *fakeMetaStore) ListDir(_ context.Context, _ string) ([]domain.FileInfo, error) {
	return nil, nil
}
func (f *fakeMetaStore) Close() error { return nil }

// ── Helpers ──────────────────────────────────────────────────────────────────

func newReg() *metrics.Registry       { return metrics.New() }
func newLog(t *testing.T) *zap.Logger { return zaptest.NewLogger(t) }
func newTracer()                      { /* noop */ }

// ── Backend decorator tests ──────────────────────────────────────────────────

func TestObservableBackend_Put_Success(t *testing.T) {
	fake := &fakeBackend{}
	reg := newReg()
	log := newLog(t)
	tracer := noop.NewTracerProvider().Tracer("test")

	dec := decorators.NewObservableBackend(fake, "tier0", reg, log, tracer)

	data := []byte("hello")
	err := dec.Put(context.Background(), "a/b.mp4", bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.True(t, fake.putCalled)

	// Bytes written counter must have been incremented.
	gathered := promCounterValue(t, reg, "tierfs_backend_bytes_written_total", map[string]string{"backend": "tier0"})
	assert.Equal(t, float64(len(data)), gathered)
}

func TestObservableBackend_Put_Error(t *testing.T) {
	boom := errors.New("disk full")
	fake := &fakeBackend{putErr: boom}
	reg := newReg()
	log := newLog(t)
	tracer := noop.NewTracerProvider().Tracer("test")

	dec := decorators.NewObservableBackend(fake, "tier0", reg, log, tracer)

	err := dec.Put(context.Background(), "x.mp4", bytes.NewReader(nil), 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)

	errCount := promCounterValue(t, reg, "tierfs_backend_operations_total",
		map[string]string{"backend": "tier0", "op": "put", "outcome": "error"})
	assert.Equal(t, float64(1), errCount)
}

func TestObservableBackend_Get_Success(t *testing.T) {
	fake := &fakeBackend{data: []byte("content")}
	reg := newReg()
	log := newLog(t)
	tracer := noop.NewTracerProvider().Tracer("test")

	dec := decorators.NewObservableBackend(fake, "tier0", reg, log, tracer)

	rc, size, err := dec.Get(context.Background(), "x.mp4")
	require.NoError(t, err)
	assert.Equal(t, int64(7), size)

	// Read all and close to trigger bytes-read counter.
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, []byte("content"), got)

	bytesRead := promCounterValue(t, reg, "tierfs_backend_bytes_read_total", map[string]string{"backend": "tier0"})
	assert.Equal(t, float64(7), bytesRead)
}

func TestObservableBackend_Get_NotFound(t *testing.T) {
	fake := &fakeBackend{getErr: domain.ErrNotExist}
	reg := newReg()
	log := newLog(t)
	tracer := noop.NewTracerProvider().Tracer("test")

	dec := decorators.NewObservableBackend(fake, "tier0", reg, log, tracer)

	_, _, err := dec.Get(context.Background(), "missing.mp4")
	require.ErrorIs(t, err, domain.ErrNotExist)

	notFound := promCounterValue(t, reg, "tierfs_backend_operations_total",
		map[string]string{"backend": "tier0", "op": "get", "outcome": "not_found"})
	assert.Equal(t, float64(1), notFound)
}

func TestObservableBackend_Delete(t *testing.T) {
	fake := &fakeBackend{}
	dec := decorators.NewObservableBackend(fake, "tier0", newReg(), newLog(t),
		noop.NewTracerProvider().Tracer("test"))
	require.NoError(t, dec.Delete(context.Background(), "x.mp4"))
	assert.True(t, fake.delCalled)
}

func TestObservableBackend_Stat(t *testing.T) {
	fake := &fakeBackend{}
	dec := decorators.NewObservableBackend(fake, "tier0", newReg(), newLog(t),
		noop.NewTracerProvider().Tracer("test"))
	fi, err := dec.Stat(context.Background(), "x.mp4")
	require.NoError(t, err)
	assert.Equal(t, int64(42), fi.Size)
}

func TestObservableBackend_List(t *testing.T) {
	fake := &fakeBackend{}
	dec := decorators.NewObservableBackend(fake, "tier0", newReg(), newLog(t),
		noop.NewTracerProvider().Tracer("test"))
	_, err := dec.List(context.Background(), "")
	require.NoError(t, err)
	assert.True(t, fake.listCalled)
}

// ── MetaStore decorator tests ─────────────────────────────────────────────────

func TestObservableMetaStore_UpsertFile(t *testing.T) {
	fake := &fakeMetaStore{}
	reg := newReg()
	log := newLog(t)
	tracer := noop.NewTracerProvider().Tracer("test")

	dec := decorators.NewObservableMetaStore(fake, reg, log, tracer)

	err := dec.UpsertFile(context.Background(), domain.File{RelPath: "a.mp4", CurrentTier: "tier0"})
	require.NoError(t, err)

	count := promCounterValue(t, reg, "tierfs_meta_operations_total",
		map[string]string{"op": "UpsertFile", "outcome": "ok"})
	assert.Equal(t, float64(1), count)
}

func TestObservableMetaStore_UpsertFile_Error(t *testing.T) {
	boom := errors.New("db locked")
	fake := &fakeMetaStore{upsertErr: boom}
	reg := newReg()
	log := newLog(t)
	tracer := noop.NewTracerProvider().Tracer("test")

	dec := decorators.NewObservableMetaStore(fake, reg, log, tracer)
	err := dec.UpsertFile(context.Background(), domain.File{})
	require.ErrorIs(t, err, boom)

	errCount := promCounterValue(t, reg, "tierfs_meta_operations_total",
		map[string]string{"op": "UpsertFile", "outcome": "error"})
	assert.Equal(t, float64(1), errCount)
}

func TestObservableMetaStore_GetFile(t *testing.T) {
	fake := &fakeMetaStore{}
	dec := decorators.NewObservableMetaStore(fake, newReg(), newLog(t),
		noop.NewTracerProvider().Tracer("test"))
	f, err := dec.GetFile(context.Background(), "x.mp4")
	require.NoError(t, err)
	assert.Equal(t, "x.mp4", f.RelPath)
}

func TestObservableMetaStore_Close(t *testing.T) {
	fake := &fakeMetaStore{}
	dec := decorators.NewObservableMetaStore(fake, newReg(), newLog(t),
		noop.NewTracerProvider().Tracer("test"))
	assert.NoError(t, dec.Close())
}

// ── Prometheus helpers ────────────────────────────────────────────────────────

// promCounterValue extracts a counter value from the registry by name + labels.
// Returns 0 if not found (metric not yet recorded).
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
