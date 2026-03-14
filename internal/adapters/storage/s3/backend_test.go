package s3_test

import (
	"testing"

	s3backend "github.com/mikey-austin/tierfs/internal/adapters/storage/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	cases := []struct {
		uri        string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{"s3://my-bucket/cctv", "my-bucket", "cctv", false},
		{"s3://my-bucket/path/to/prefix", "my-bucket", "path/to/prefix", false},
		{"s3://my-bucket", "my-bucket", "", false},
		{"s3://my-bucket/", "my-bucket", "", false},
		{"file:///tmp", "", "", true},
		{"not-a-uri", "", "", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.uri, func(t *testing.T) {
			bucket, prefix, err := s3backend.ParseURI(tc.uri)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantBucket, bucket)
			assert.Equal(t, tc.wantPrefix, prefix)
		})
	}
}

// Integration tests against a real S3-compatible server (MinIO/LocalStack)
// are gated on the TIERFS_S3_TEST_ENDPOINT env var.
// Run with:
//   TIERFS_S3_TEST_ENDPOINT=http://localhost:9000 \
//   TIERFS_S3_TEST_BUCKET=tierfs-test \
//   AWS_ACCESS_KEY_ID=minioadmin \
//   AWS_SECRET_ACCESS_KEY=minioadmin \
//   go test ./internal/adapters/storage/s3/ -run TestIntegration -v

func TestNew_InvalidEndpoint(t *testing.T) {
	// New() should succeed even with an unreachable endpoint;
	// errors surface on the first API call.
	cfg := s3backend.Config{
		Name:      "test",
		Bucket:    "test-bucket",
		Endpoint:  "https://127.0.0.1:19999",
		Region:    "us-east-1",
		PathStyle: true,
		AccessKey: "key",
		SecretKey: "secret",
	}
	b, err := s3backend.New(cfg)
	require.NoError(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, "s3", b.Scheme())
}

func TestURI(t *testing.T) {
	cfg := s3backend.Config{
		Bucket: "my-bucket",
		Prefix: "cctv",
	}
	b, err := s3backend.New(cfg)
	require.NoError(t, err)
	assert.Equal(t, "s3://my-bucket/cctv/recordings/cam1.mp4", b.URI("recordings/cam1.mp4"))
}

func TestLocalPath_AlwaysFalse(t *testing.T) {
	b, _ := s3backend.New(s3backend.Config{Bucket: "b", Region: "us-east-1"})
	_, ok := b.LocalPath("anything")
	assert.False(t, ok)
}
