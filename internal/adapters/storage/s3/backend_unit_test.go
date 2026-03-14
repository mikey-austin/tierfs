package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mikey-austin/tierfs/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

type mockS3API struct {
	getObjectFn    func(ctx context.Context, in *s3sdk.GetObjectInput, opts ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error)
	headObjectFn   func(ctx context.Context, in *s3sdk.HeadObjectInput, opts ...func(*s3sdk.Options)) (*s3sdk.HeadObjectOutput, error)
	deleteObjectFn func(ctx context.Context, in *s3sdk.DeleteObjectInput, opts ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectOutput, error)
	listObjectsFn  func(ctx context.Context, in *s3sdk.ListObjectsV2Input, opts ...func(*s3sdk.Options)) (*s3sdk.ListObjectsV2Output, error)
}

func (m *mockS3API) GetObject(ctx context.Context, in *s3sdk.GetObjectInput, opts ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error) {
	return m.getObjectFn(ctx, in, opts...)
}

func (m *mockS3API) HeadObject(ctx context.Context, in *s3sdk.HeadObjectInput, opts ...func(*s3sdk.Options)) (*s3sdk.HeadObjectOutput, error) {
	return m.headObjectFn(ctx, in, opts...)
}

func (m *mockS3API) DeleteObject(ctx context.Context, in *s3sdk.DeleteObjectInput, opts ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectOutput, error) {
	return m.deleteObjectFn(ctx, in, opts...)
}

func (m *mockS3API) ListObjectsV2(ctx context.Context, in *s3sdk.ListObjectsV2Input, opts ...func(*s3sdk.Options)) (*s3sdk.ListObjectsV2Output, error) {
	return m.listObjectsFn(ctx, in, opts...)
}

type mockUploader struct {
	uploadFn func(ctx context.Context, in *s3sdk.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

func (m *mockUploader) Upload(ctx context.Context, in *s3sdk.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	return m.uploadFn(ctx, in, opts...)
}

// ---------------------------------------------------------------------------
// Put tests
// ---------------------------------------------------------------------------

func TestPut_Success(t *testing.T) {
	var captured *s3sdk.PutObjectInput
	up := &mockUploader{
		uploadFn: func(_ context.Context, in *s3sdk.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			captured = in
			return &manager.UploadOutput{}, nil
		},
	}
	b := newForTest("my-bucket", "pfx", &mockS3API{}, up)

	body := bytes.NewReader([]byte("hello"))
	err := b.Put(context.Background(), "dir/file.txt", body, 5)
	require.NoError(t, err)

	require.NotNil(t, captured)
	assert.Equal(t, "my-bucket", *captured.Bucket)
	assert.Equal(t, "pfx/dir/file.txt", *captured.Key)
	assert.Equal(t, int64(5), *captured.ContentLength)
}

func TestPut_Error(t *testing.T) {
	up := &mockUploader{
		uploadFn: func(_ context.Context, _ *s3sdk.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return nil, errors.New("network timeout")
		},
	}
	b := newForTest("bucket", "pfx", &mockS3API{}, up)

	err := b.Put(context.Background(), "f.txt", bytes.NewReader(nil), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3 put")
	assert.Contains(t, err.Error(), "network timeout")
}

// ---------------------------------------------------------------------------
// Get tests
// ---------------------------------------------------------------------------

func TestGet_Success(t *testing.T) {
	api := &mockS3API{
		getObjectFn: func(_ context.Context, in *s3sdk.GetObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error) {
			assert.Equal(t, "bucket", *in.Bucket)
			assert.Equal(t, "pfx/some/key", *in.Key)
			return &s3sdk.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader([]byte("data"))),
				ContentLength: aws.Int64(4),
			}, nil
		},
	}
	b := newForTest("bucket", "pfx", api, &mockUploader{})

	rc, size, err := b.Get(context.Background(), "some/key")
	require.NoError(t, err)
	assert.Equal(t, int64(4), size)

	data, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "data", string(data))
}

func TestGet_NotFound_NoSuchKey(t *testing.T) {
	api := &mockS3API{
		getObjectFn: func(_ context.Context, _ *s3sdk.GetObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error) {
			return nil, &types.NoSuchKey{Message: aws.String("not found")}
		},
	}
	b := newForTest("bucket", "", api, &mockUploader{})

	_, _, err := b.Get(context.Background(), "missing")
	require.ErrorIs(t, err, domain.ErrNotExist)
}

func TestGet_NotFound_NotFound(t *testing.T) {
	api := &mockS3API{
		getObjectFn: func(_ context.Context, _ *s3sdk.GetObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error) {
			return nil, &types.NotFound{Message: aws.String("404")}
		},
	}
	b := newForTest("bucket", "", api, &mockUploader{})

	_, _, err := b.Get(context.Background(), "missing")
	require.ErrorIs(t, err, domain.ErrNotExist)
}

func TestGet_OtherError(t *testing.T) {
	api := &mockS3API{
		getObjectFn: func(_ context.Context, _ *s3sdk.GetObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetObjectOutput, error) {
			return nil, errors.New("access denied")
		},
	}
	b := newForTest("bucket", "", api, &mockUploader{})

	_, _, err := b.Get(context.Background(), "file")
	require.Error(t, err)
	assert.NotErrorIs(t, err, domain.ErrNotExist)
	assert.Contains(t, err.Error(), "access denied")
}

// ---------------------------------------------------------------------------
// Stat tests
// ---------------------------------------------------------------------------

func TestStat_Success(t *testing.T) {
	modTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	api := &mockS3API{
		headObjectFn: func(_ context.Context, in *s3sdk.HeadObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.HeadObjectOutput, error) {
			assert.Equal(t, "bucket", *in.Bucket)
			assert.Equal(t, "pfx/dir/file.bin", *in.Key)
			return &s3sdk.HeadObjectOutput{
				ContentLength: aws.Int64(1024),
				LastModified:  &modTime,
			}, nil
		},
	}
	b := newForTest("bucket", "pfx", api, &mockUploader{})

	fi, err := b.Stat(context.Background(), "dir/file.bin")
	require.NoError(t, err)
	assert.Equal(t, "dir/file.bin", fi.RelPath)
	assert.Equal(t, "file.bin", fi.Name)
	assert.Equal(t, int64(1024), fi.Size)
	assert.Equal(t, modTime, fi.ModTime)
}

func TestStat_NotFound(t *testing.T) {
	api := &mockS3API{
		headObjectFn: func(_ context.Context, _ *s3sdk.HeadObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.HeadObjectOutput, error) {
			return nil, &types.NotFound{Message: aws.String("not found")}
		},
	}
	b := newForTest("bucket", "", api, &mockUploader{})

	_, err := b.Stat(context.Background(), "missing")
	require.ErrorIs(t, err, domain.ErrNotExist)
}

// ---------------------------------------------------------------------------
// Delete tests
// ---------------------------------------------------------------------------

func TestDelete_Success(t *testing.T) {
	var captured *s3sdk.DeleteObjectInput
	api := &mockS3API{
		deleteObjectFn: func(_ context.Context, in *s3sdk.DeleteObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectOutput, error) {
			captured = in
			return &s3sdk.DeleteObjectOutput{}, nil
		},
	}
	b := newForTest("bucket", "pfx", api, &mockUploader{})

	err := b.Delete(context.Background(), "dir/old.log")
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, "bucket", *captured.Bucket)
	assert.Equal(t, "pfx/dir/old.log", *captured.Key)
}

func TestDelete_Error(t *testing.T) {
	api := &mockS3API{
		deleteObjectFn: func(_ context.Context, _ *s3sdk.DeleteObjectInput, _ ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectOutput, error) {
			return nil, errors.New("permission denied")
		},
	}
	b := newForTest("bucket", "", api, &mockUploader{})

	err := b.Delete(context.Background(), "f.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3 delete")
	assert.Contains(t, err.Error(), "permission denied")
}

// ---------------------------------------------------------------------------
// List tests
// ---------------------------------------------------------------------------

func TestList_SinglePage(t *testing.T) {
	mod1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mod2 := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	api := &mockS3API{
		listObjectsFn: func(_ context.Context, in *s3sdk.ListObjectsV2Input, _ ...func(*s3sdk.Options)) (*s3sdk.ListObjectsV2Output, error) {
			assert.Equal(t, "bucket", *in.Bucket)
			assert.Equal(t, "pfx/videos/", *in.Prefix)
			return &s3sdk.ListObjectsV2Output{
				Contents: []types.Object{
					{Key: aws.String("pfx/videos/a.mp4"), Size: aws.Int64(100), LastModified: &mod1},
					{Key: aws.String("pfx/videos/b.mp4"), Size: aws.Int64(200), LastModified: &mod2},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	b := newForTest("bucket", "pfx", api, &mockUploader{})

	entries, err := b.List(context.Background(), "videos")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	assert.Equal(t, "videos/a.mp4", entries[0].RelPath)
	assert.Equal(t, "a.mp4", entries[0].Name)
	assert.Equal(t, int64(100), entries[0].Size)
	assert.Equal(t, mod1, entries[0].ModTime)

	assert.Equal(t, "videos/b.mp4", entries[1].RelPath)
	assert.Equal(t, "b.mp4", entries[1].Name)
	assert.Equal(t, int64(200), entries[1].Size)
	assert.Equal(t, mod2, entries[1].ModTime)
}

func TestList_Empty(t *testing.T) {
	api := &mockS3API{
		listObjectsFn: func(_ context.Context, _ *s3sdk.ListObjectsV2Input, _ ...func(*s3sdk.Options)) (*s3sdk.ListObjectsV2Output, error) {
			return &s3sdk.ListObjectsV2Output{
				Contents:    nil,
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	b := newForTest("bucket", "pfx", api, &mockUploader{})

	entries, err := b.List(context.Background(), "empty-dir")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// ---------------------------------------------------------------------------
// isNotFound tests
// ---------------------------------------------------------------------------

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "NoSuchKey",
			err:  &types.NoSuchKey{Message: aws.String("no such key")},
			want: true,
		},
		{
			name: "NotFound",
			err:  &types.NotFound{Message: aws.String("not found")},
			want: true,
		},
		{
			name: "wrapped NoSuchKey",
			err:  errors.Join(errors.New("outer"), &types.NoSuchKey{Message: aws.String("inner")}),
			want: true,
		},
		{
			name: "other error",
			err:  errors.New("something else"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isNotFound(tc.err))
		})
	}
}
