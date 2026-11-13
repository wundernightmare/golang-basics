package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrNotFound is what [Client.Get] and [Client.Head] return for a key that
// does not exist. It is an answer, not a failure: it is not counted in
// s3_request_errors_total.
var ErrNotFound = errors.New("s3: object not found")

// ErrTooLarge is what [Client.Get] returns for an object larger than the
// limit the caller gave.
var ErrTooLarge = errors.New("s3: object larger than the read limit")

// deleteBatch is the most keys one DeleteObjects call takes.
const deleteBatch = 1000

// Operation names: the `operation` label of the metrics and the span names.
const (
	opPut            = "put"
	opGet            = "get"
	opHead           = "head"
	opList           = "list"
	opDelete         = "delete"
	opHeadBucket     = "head_bucket"
	opCreateBucket   = "create_bucket"
	opCreateUpload   = "create_upload"
	opUploadPart     = "upload_part"
	opCompleteUpload = "complete_upload"
	opAbortUpload    = "abort_upload"
)

// api is the part of the AWS SDK client this package uses, spelled out so
// the surface the library depends on is one list.
//
//nolint:interfacebloat // one method per S3 operation used: splitting it would only scatter that list
type api interface {
	PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *awss3.GetObjectInput, opts ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, opts ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, in *awss3.DeleteObjectsInput, opts ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error)
	HeadBucket(ctx context.Context, in *awss3.HeadBucketInput, opts ...func(*awss3.Options)) (*awss3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, in *awss3.CreateBucketInput, opts ...func(*awss3.Options)) (*awss3.CreateBucketOutput, error)
	CreateMultipartUpload(ctx context.Context, in *awss3.CreateMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, in *awss3.UploadPartInput, opts ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, in *awss3.CompleteMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, in *awss3.AbortMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
}

// Object is one entry of a listing.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Client is an S3 client bound to one bucket. It is safe for concurrent use;
// construct one with [New] and share it.
//
// Every call runs under its own deadline (Config.OpTimeout, or
// Config.TransferTimeout for a call that moves a large body) with the SDK's
// retries inside it, under an OpenTelemetry client span, and is measured —
// register [Client.Collectors] to export the metrics.
type Client struct {
	api     api
	cfg     Config
	log     *slog.Logger
	metrics *metrics
	tracer  trace.Tracer
	http    *http.Client
}

// New builds a client from cfg. It does no network I/O: a store that is down
// at boot must not keep a service that can work without it from starting.
// Call [Client.Ensure] to verify (and, with S3_AUTO_CREATE_BUCKET, create) the
// bucket, and register [Client.ReadyCheck]. The caller owns the client and
// must call [Client.Close].
func New(ctx context.Context, cfg Config, log *slog.Logger, opts ...Option) (*Client, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	httpClient, err := newHTTPClient(cfg)
	if err != nil {
		return nil, err
	}

	awsCfg, err := sdkConfig(ctx, cfg, httpClient)
	if err != nil {
		return nil, err
	}

	sdk := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	log.InfoContext(ctx, "s3 client ready", "endpoint", cfg.Endpoint, "bucket", cfg.Bucket, "region", cfg.Region,
		"path_style", cfg.UsePathStyle, "op_timeout", cfg.OpTimeout.String(), "max_attempts", cfg.MaxAttempts)

	return &Client{
		api: sdk, cfg: cfg, log: log, metrics: newMetrics(),
		tracer: newTracer(opts), http: httpClient,
	}, nil
}

// sdkConfig is the SDK configuration. With static credentials it is built by
// hand, so nothing but cfg decides how the client behaves; without them the
// SDK's default chain (IRSA / web identity, instance role, shared config) is
// loaded, which also reads the standard AWS_* environment.
func sdkConfig(ctx context.Context, cfg Config, httpClient *http.Client) (aws.Config, error) {
	checksum := aws.RequestChecksumCalculationWhenRequired
	validation := aws.ResponseChecksumValidationWhenRequired

	if cfg.RequestChecksum == ChecksumWhenSupported {
		checksum = aws.RequestChecksumCalculationWhenSupported
		validation = aws.ResponseChecksumValidationWhenSupported
	}

	retryer := func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = cfg.MaxAttempts
			o.MaxBackoff = cfg.RetryMaxBackoff
		})
	}

	if cfg.AccessKeyID != "" {
		return aws.Config{
			Region:                     cfg.Region,
			Credentials:                credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
			HTTPClient:                 httpClient,
			Retryer:                    retryer,
			RequestChecksumCalculation: checksum,
			ResponseChecksumValidation: validation,
		}, nil
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
		awsconfig.WithRetryer(retryer),
		awsconfig.WithRequestChecksumCalculation(checksum),
		awsconfig.WithResponseChecksumValidation(validation),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("s3: load default aws config: %w", err)
	}

	return awsCfg, nil
}

// newHTTPClient is the transport every call shares: bounded connects and a
// bounded pool. There is no client-wide timeout — each call has its own.
func newHTTPClient(cfg Config) (*http.Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("s3: read S3_CA_FILE: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("s3: S3_CA_FILE %q holds no PEM certificate", cfg.CAFile)
		}

		tlsCfg.RootCAs = pool
	}

	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}

	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSClientConfig:       tlsCfg,
		TLSHandshakeTimeout:   cfg.DialTimeout,
		ForceAttemptHTTP2:     true,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		MaxIdleConns:          cfg.MaxConnsPerHost,
		MaxIdleConnsPerHost:   cfg.MaxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: time.Second,
	}}, nil
}

// Bucket is the bucket the client addresses.
func (c *Client) Bucket() string { return c.cfg.Bucket }

// Close releases the idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// call runs one SDK call under timeout, traced and measured. notFoundOK says
// a missing key is an answer for this operation rather than a failure.
func (c *Client) call(ctx context.Context, operation, key string, timeout time.Duration, notFoundOK bool, run func(context.Context) error) error {
	ctx, span := c.tracer.Start(ctx, "s3 "+operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("rpc.system", "aws-api"),
			attribute.String("rpc.service", "S3"),
			attribute.String("rpc.method", operation),
			attribute.String("aws.s3.bucket", c.cfg.Bucket),
			attribute.String("aws.s3.key", key),
		))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	err := run(ctx)

	c.metrics.duration.WithLabelValues(operation).Observe(time.Since(start).Seconds())

	if err == nil {
		return nil
	}

	if isNotFound(err) {
		if notFoundOK {
			return ErrNotFound
		}

		err = fmt.Errorf("%w: %w", ErrNotFound, err)
	}

	c.metrics.errors.WithLabelValues(operation).Inc()
	span.RecordError(err)
	span.SetStatus(codes.Error, operation+" failed")

	return err
}

// isNotFound reports a missing key or bucket: the modelled errors, or the
// bare 404 a HEAD answers with (it has no body to carry a code).
func isNotFound(err error) bool {
	var (
		noKey    *types.NoSuchKey
		notFound *types.NotFound
		noBucket *types.NoSuchBucket
		apiErr   smithy.APIError
	)

	if errors.As(err, &noKey) || errors.As(err, &notFound) || errors.As(err, &noBucket) {
		return true
	}

	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return true
		}
	}

	return false
}

// Put stores body at key, replacing what is there. The call is idempotent:
// a writer that names its objects deterministically can repeat a put whose
// answer was lost and still end with one object.
func (c *Client) Put(ctx context.Context, key string, body []byte, contentType string) error {
	err := c.call(ctx, opPut, key, c.cfg.OpTimeout, false, func(ctx context.Context) error {
		_, err := c.api.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:        aws.String(c.cfg.Bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
			ContentType:   contentTypeOrNil(contentType),
		})

		return err
	})
	if err != nil {
		return fmt.Errorf("s3: put %q: %w", key, err)
	}

	return nil
}

func contentTypeOrNil(contentType string) *string {
	if contentType == "" {
		return nil
	}

	return aws.String(contentType)
}

// Get reads the whole object at key into memory, refusing one larger than
// limit bytes with [ErrTooLarge] — the caller decides how much one object may
// cost it. A missing key is [ErrNotFound].
func (c *Client) Get(ctx context.Context, key string, limit int64) ([]byte, error) {
	var body []byte

	err := c.call(ctx, opGet, key, c.cfg.TransferTimeout, true, func(ctx context.Context) error {
		out, err := c.api.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(c.cfg.Bucket), Key: aws.String(key)})
		if err != nil {
			return err
		}

		defer func() { _ = out.Body.Close() }()

		if out.ContentLength != nil && *out.ContentLength > limit {
			return fmt.Errorf("%w: %d > %d bytes", ErrTooLarge, *out.ContentLength, limit)
		}

		// One byte past the limit tells "exactly the limit" from "more".
		body, err = io.ReadAll(io.LimitReader(out.Body, limit+1))
		if err != nil {
			return err
		}

		if int64(len(body)) > limit {
			return fmt.Errorf("%w: more than %d bytes", ErrTooLarge, limit)
		}

		return nil
	})
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}

	return body, nil
}

// Head returns the size of the object at key, or [ErrNotFound].
func (c *Client) Head(ctx context.Context, key string) (int64, error) {
	var size int64

	err := c.call(ctx, opHead, key, c.cfg.OpTimeout, true, func(ctx context.Context) error {
		out, err := c.api.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(c.cfg.Bucket), Key: aws.String(key)})
		if err != nil {
			return err
		}

		size = aws.ToInt64(out.ContentLength)

		return nil
	})
	if errors.Is(err, ErrNotFound) {
		return 0, ErrNotFound
	}

	if err != nil {
		return 0, fmt.Errorf("s3: head %q: %w", key, err)
	}

	return size, nil
}

// List iterates the objects under prefix in key order, fetching a page at a
// time (each page is one call under Config.OpTimeout). The sequence ends at
// the first error, which is its last element.
func (c *Client) List(ctx context.Context, prefix string) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		var token *string

		for {
			var page *awss3.ListObjectsV2Output

			err := c.call(ctx, opList, prefix, c.cfg.OpTimeout, false, func(ctx context.Context) error {
				var err error

				page, err = c.api.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
					Bucket: aws.String(c.cfg.Bucket), Prefix: aws.String(prefix), ContinuationToken: token,
				})

				return err
			})
			if err != nil {
				yield(Object{}, fmt.Errorf("s3: list %q: %w", prefix, err))
				return
			}

			for i := range page.Contents {
				entry := &page.Contents[i]
				obj := Object{Key: aws.ToString(entry.Key), Size: aws.ToInt64(entry.Size), LastModified: aws.ToTime(entry.LastModified)}

				if !yield(obj, nil) {
					return
				}
			}

			if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
				return
			}

			token = page.NextContinuationToken
		}
	}
}

// ListPrefixes returns the "directories" directly under prefix — the common
// prefixes up to the next "/" — in key order. prefix should end in "/" (or be
// empty for the bucket root).
func (c *Client) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	var (
		prefixes []string
		token    *string
	)

	for {
		var page *awss3.ListObjectsV2Output

		err := c.call(ctx, opList, prefix, c.cfg.OpTimeout, false, func(ctx context.Context) error {
			var err error

			page, err = c.api.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
				Bucket: aws.String(c.cfg.Bucket), Prefix: aws.String(prefix),
				Delimiter: aws.String("/"), ContinuationToken: token,
			})

			return err
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list prefixes of %q: %w", prefix, err)
		}

		for _, p := range page.CommonPrefixes {
			prefixes = append(prefixes, aws.ToString(p.Prefix))
		}

		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			return prefixes, nil
		}

		token = page.NextContinuationToken
	}
}

// Delete removes keys, a thousand per call; keys that do not exist are not an
// error. It keeps going past a failed batch and reports every failure.
func (c *Client) Delete(ctx context.Context, keys []string) error {
	var errs []error

	for start := 0; start < len(keys); start += deleteBatch {
		batch := keys[start:min(start+deleteBatch, len(keys))]

		ids := make([]types.ObjectIdentifier, len(batch))
		for i, key := range batch {
			ids[i] = types.ObjectIdentifier{Key: aws.String(key)}
		}

		err := c.call(ctx, opDelete, batch[0], c.cfg.OpTimeout, false, func(ctx context.Context) error {
			out, err := c.api.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
				Bucket: aws.String(c.cfg.Bucket),
				Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
			})
			if err != nil {
				return err
			}

			if len(out.Errors) > 0 {
				first := out.Errors[0]

				return fmt.Errorf("%d of %d keys not deleted, first %q: %s %s", len(out.Errors), len(batch),
					aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
			}

			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("s3: delete: %w", err)
	}

	return nil
}

// ensureLocks holds one mutex per endpoint + bucket, see [Client.Ensure].
var ensureLocks sync.Map //nolint:gochecknoglobals // process-wide by purpose: it serializes clients that do not know of each other

// Ensure verifies the bucket is reachable with these credentials and, when
// S3_AUTO_CREATE_BUCKET is set, creates it if it is missing (local stacks and
// tests; a production bucket is provisioned, with its lifecycle rules).
//
// Calls for the same bucket are serialized within the process: two clients
// creating one bucket at the same moment is the one way to make a store
// answer NoSuchBucket for a bucket that both were just told exists (seen
// with RustFS). Across processes the race stays — one more reason the option
// is not for production.
func (c *Client) Ensure(ctx context.Context) error {
	lock, _ := ensureLocks.LoadOrStore(c.cfg.Endpoint+"\x00"+c.cfg.Bucket, &sync.Mutex{})

	mu, _ := lock.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	err := c.headBucket(ctx)
	if err == nil {
		return nil
	}

	if !c.cfg.AutoCreateBucket || !errors.Is(err, ErrNotFound) {
		return err
	}

	err = c.call(ctx, opCreateBucket, "", c.cfg.OpTimeout, false, func(ctx context.Context) error {
		_, err := c.api.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(c.cfg.Bucket)})

		var (
			owned  *types.BucketAlreadyOwnedByYou
			exists *types.BucketAlreadyExists
		)
		if errors.As(err, &owned) || errors.As(err, &exists) {
			return nil // another replica won the race
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("s3: create bucket %q: %w", c.cfg.Bucket, err)
	}

	c.log.InfoContext(ctx, "s3 bucket created", "bucket", c.cfg.Bucket)

	return nil
}

func (c *Client) headBucket(ctx context.Context) error {
	err := c.call(ctx, opHeadBucket, "", c.cfg.OpTimeout, false, func(ctx context.Context) error {
		_, err := c.api.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(c.cfg.Bucket)})

		return err
	})
	if err != nil {
		return fmt.Errorf("s3: bucket %q unreachable: %w", c.cfg.Bucket, err)
	}

	return nil
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that asks the
// store for the bucket with a short timeout.
func (c *Client) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		return c.headBucket(ctx)
	}
}
