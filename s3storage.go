package s3storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var ErrNotFound = errors.New("file not found")

type Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
}

type S3Storage struct {
	Bucket     string
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

type SaveOptions struct {
	ContentType     string
	AutoContentType bool
}

type SaveOption func(*SaveOptions)

func WithContentType(ct string) SaveOption {
	return func(o *SaveOptions) {
		o.ContentType = ct
	}
}

func WithAutoContentType() SaveOption {
	return func(o *SaveOptions) {
		o.AutoContentType = true
	}
}

// NewS3Storage creates an S3 storage client
func NewS3Storage(ctx context.Context, cfg Config) (*S3Storage, error) {

	configOptions := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
		config.WithBaseEndpoint(cfg.Endpoint),
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		provider := credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
		configOptions = append(configOptions, config.WithCredentialsProvider(provider))
	}

	s3cfg, err := config.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(s3cfg)

	// Configure low-memory upload (5MB part size, single worker)
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // minimum allowed by S3 for multipart
		u.Concurrency = 1
	})

	// Configure low-memory download (single worker, 5MB parts)
	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = 5 * 1024 * 1024
		d.Concurrency = 1
	})

	return &S3Storage{
		Bucket:     cfg.Bucket,
		client:     client,
		uploader:   uploader,
		downloader: downloader,
	}, nil
}

// Save uploads a file to S3.
// If contentType is empty, it will be auto-detected from the first 512 bytes.
func (s *S3Storage) Save(ctx context.Context, path string, r io.Reader, opts ...SaveOption) error {
	options := SaveOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	if options.ContentType == "" && options.AutoContentType {
		// Peek first 512 bytes to detect content type
		buf := make([]byte, 512)
		n, err := io.ReadFull(r, buf)
		if err != nil {
			// Only fail for actual errors, not EOF conditions
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				return fmt.Errorf("failed to read file header for content-type detection: %w", err)
			}
		}
		if n > 0 {
			options.ContentType = http.DetectContentType(buf[:n])
			r = io.MultiReader(bytes.NewReader(buf[:n]), r)
		}
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
		Body:   r,
	}

	if options.ContentType != "" {
		input.ContentType = aws.String(options.ContentType)
	}

	_, err := s.uploader.Upload(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			return fmt.Errorf("s3 upload failed for bucket %s, key %s: %s (AWS code: %s): %w",
				s.Bucket, path, apiErr.ErrorMessage(), apiErr.ErrorCode(), err)
		}
		return fmt.Errorf("couldn't upload file %v to %v: %w", path, s.Bucket, err)
	}
	return nil
}

// Open returns a ReadCloser for the object. Caller must close it.
func (s *S3Storage) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		var er *types.NoSuchKey
		if errors.As(err, &er) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to open %s from %s: %w", path, s.Bucket, err)
	}
	return resp.Body, nil
}

// Download streams an S3 object into w.
func (s *S3Storage) Download(ctx context.Context, path string, w io.WriterAt) error {
	_, err := s.downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		var er *types.NoSuchKey
		if errors.As(err, &er) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to download %v from %v: %w", path, s.Bucket, err)
	}
	return nil
}

// Exists checks if an object exists in the S3 bucket.
func (s *S3Storage) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err == nil {
		return true, nil
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check existence of %s in %s: %w", path, s.Bucket, err)
}

// Delete removes an object from S3.
func (s *S3Storage) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return fmt.Errorf("couldn't delete file %s from %s: %w", path, s.Bucket, err)
	}
	return nil
}
