// Package s3 implements the Backend port for S3-compatible object storage.
// Large files (>multipartThreshold) are uploaded using the S3 manager's
// multipart uploader for parallel chunk transfers. Downloads use GetObject
// with a pre-allocated buffer pool to minimise GC pressure.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mikey-austin/tierfs/internal/domain"
)

const (
	// multipartThreshold: files larger than this use parallel multipart upload.
	multipartThreshold = 100 * 1024 * 1024 // 100 MiB
	// multipartPartSize: each part in a multipart upload.
	multipartPartSize = 16 * 1024 * 1024 // 16 MiB
	// multipartConcurrency: parallel part uploads.
	multipartConcurrency = 4
)

// Config holds configuration for an S3 backend instance.
type Config struct {
	Name      string
	Bucket    string
	Prefix    string // key prefix parsed from URI path, no leading/trailing slash
	Endpoint  string // empty = AWS default
	Region    string
	PathStyle bool   // true for MinIO, Ceph, etc.
	AccessKey string // empty = use env / instance role
	SecretKey string
}

// ParseURI extracts bucket and prefix from an s3://bucket/prefix URI.
func ParseURI(rawURI string) (bucket, prefix string, err error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return "", "", fmt.Errorf("s3: parse uri %q: %w", rawURI, err)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("s3: expected s3:// scheme, got %q", u.Scheme)
	}
	bucket = u.Host
	prefix = strings.Trim(u.Path, "/")
	return bucket, prefix, nil
}

// Backend is an S3-compatible storage backend.
type Backend struct {
	cfg      Config
	client   *s3.Client
	uploader *manager.Uploader
}

// New creates a Backend, establishing the AWS client with the provided config.
func New(cfg Config) (*Backend, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error

	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	})

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = multipartPartSize
		u.Concurrency = multipartConcurrency
		u.LeavePartsOnError = false
	})

	return &Backend{cfg: cfg, client: client, uploader: uploader}, nil
}

func (b *Backend) Scheme() string { return "s3" }

func (b *Backend) URI(relPath string) string {
	return fmt.Sprintf("s3://%s/%s", b.cfg.Bucket, b.key(relPath))
}

// LocalPath always returns false for S3 backends; reads must go through Get.
func (b *Backend) LocalPath(_ string) (string, bool) { return "", false }

// Put uploads data to S3. The uploader automatically selects between
// single-part and multipart based on the configured threshold.
func (b *Backend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error {
	_, err := b.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.cfg.Bucket),
		Key:           aws.String(b.key(relPath)),
		Body:          r,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", relPath, err)
	}
	return nil
}

// Get downloads an object from S3. The returned ReadCloser streams directly
// from S3; the caller must close it. size is from the Content-Length header.
func (b *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(b.key(relPath)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, 0, domain.ErrNotExist
		}
		return nil, 0, fmt.Errorf("s3 get %q: %w", relPath, err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, size, nil
}

// Stat issues a HeadObject to retrieve metadata without downloading.
func (b *Backend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(b.key(relPath)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, domain.ErrNotExist
		}
		return nil, fmt.Errorf("s3 stat %q: %w", relPath, err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	fi := &domain.FileInfo{
		RelPath: relPath,
		Name:    lastName(relPath),
		Size:    size,
	}
	if out.LastModified != nil {
		fi.ModTime = *out.LastModified
	}
	return fi, nil
}

// Delete removes an object from S3.
func (b *Backend) Delete(ctx context.Context, relPath string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(b.key(relPath)),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %q: %w", relPath, err)
	}
	return nil
}

// List returns all objects whose keys start with prefix.
// S3 pagination is handled transparently; returns all results.
func (b *Backend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) {
	keyPrefix := b.key(prefix)
	if prefix != "" && !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	var out []domain.FileInfo
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.cfg.Bucket),
		Prefix: aws.String(keyPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			relPath := b.relFromKey(*obj.Key)
			fi := domain.FileInfo{
				RelPath: relPath,
				Name:    lastName(relPath),
			}
			if obj.Size != nil {
				fi.Size = *obj.Size
			}
			if obj.LastModified != nil {
				fi.ModTime = *obj.LastModified
			}
			out = append(out, fi)
		}
	}
	return out, nil
}

// key converts a relative path to an S3 object key, prepending the configured prefix.
func (b *Backend) key(relPath string) string {
	if b.cfg.Prefix == "" {
		return relPath
	}
	if relPath == "" {
		return b.cfg.Prefix
	}
	return b.cfg.Prefix + "/" + relPath
}

// relFromKey strips the backend prefix from an S3 key, returning the relative path.
func (b *Backend) relFromKey(key string) string {
	if b.cfg.Prefix == "" {
		return key
	}
	return strings.TrimPrefix(key, b.cfg.Prefix+"/")
}

func lastName(relPath string) string {
	parts := strings.Split(relPath, "/")
	return parts[len(parts)-1]
}

// isNotFound returns true for S3 NoSuchKey and 404 NotFound errors.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	return false
}
