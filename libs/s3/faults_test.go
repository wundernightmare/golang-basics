package s3_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// These tests need no object store: a local HTTP server answers the way a
// broken or unusual one does, which a healthy RustFS never will.

const (
	internalError = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>try later</Message></Error>`
	initiated     = `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`
)

func fail(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(internalError))
}

// faulty is a client of a server that answers with handler; one attempt per
// call, so every failure is seen as it is.
func faulty(t *testing.T, handler http.HandlerFunc, mutate ...func(*s3.Config)) *s3.Client {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	cfg := s3.Config{
		Bucket: "b", Endpoint: srv.URL, Insecure: true,
		AccessKeyID: "k", SecretAccessKey: secret.New("s"), MaxAttempts: 1, OpTimeout: 5 * time.Second,
		UploadPartBytes: s3.MinUploadPartBytes,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	client, err := s3.New(context.Background(), cfg, nil)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

func TestNew_Variants(t *testing.T) {
	t.Parallel()

	_, err := s3.New(context.Background(), s3.Config{}, nil)
	require.ErrorContains(t, err, "S3_BUCKET", "an invalid config is refused before anything is built")

	// The SDK's own checksum default, and its default credential chain (no
	// static keys): both must build without touching the network.
	client, err := s3.New(context.Background(), s3.Config{
		Bucket: "events", Region: "eu-west-1", RequestChecksum: s3.ChecksumWhenSupported,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "events", client.Bucket())
	client.Close()
}

// selfSigned is a CA certificate that signed nothing, as PEM.
func selfSigned(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// S3_CA_FILE is the trust store of the endpoint: a store whose certificate
// the file's CA signed is trusted, any other is not.
func TestCAFileDecidesWhomToTrust(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "5")
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	signer := filepath.Join(dir, "signer.pem")
	stranger := filepath.Join(dir, "stranger.pem")

	require.NoError(t, os.WriteFile(signer, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	require.NoError(t, os.WriteFile(stranger, selfSigned(t), 0o600))

	trusting := clientOf(t, srv.URL, nil, func(c *s3.Config) { c.CAFile = signer })
	_, err := trusting.Head(context.Background(), "k")
	require.NoError(t, err)

	distrusting := clientOf(t, srv.URL, nil, func(c *s3.Config) { c.CAFile = stranger })
	_, err = distrusting.Head(context.Background(), "k")
	require.ErrorContains(t, err, "certificate signed by unknown authority")
}

// A store that fails every call: each operation reports its own error, named
// by what it was doing.
func TestClient_StoreFailsEveryCall(t *testing.T) {
	t.Parallel()

	client := faulty(t, func(w http.ResponseWriter, _ *http.Request) { fail(w) },
		func(cfg *s3.Config) { cfg.AutoCreateBucket = true })
	ctx := context.Background()

	_, err := client.Head(ctx, "k")
	require.ErrorContains(t, err, `s3: head "k"`)
	require.NotErrorIs(t, err, s3.ErrNotFound)

	_, err = client.Get(ctx, "k", 10)
	require.ErrorContains(t, err, "s3: get")

	for _, err := range client.List(ctx, "p/") {
		require.ErrorContains(t, err, `s3: list "p/"`)
	}

	_, err = client.ListPrefixes(ctx, "p/")
	require.ErrorContains(t, err, "s3: list prefixes")

	require.ErrorContains(t, client.Delete(ctx, []string{"a", "b"}), "s3: delete")
	err = client.Ensure(ctx)
	require.ErrorContains(t, err, "unreachable")
	require.NotErrorIs(t, err, s3.ErrBucketNotFound, "a failing store is not a missing bucket: nothing is created")

	upload := client.NewUpload(ctx, "big", "application/octet-stream")
	_, err = upload.Write(make([]byte, s3.MinUploadPartBytes))
	require.ErrorContains(t, err, "s3: start upload")
	upload.Abort()
	upload.Abort() // idempotent
}

// What "not found" means depends on what was asked and what the store
// named: a missing key is an answer to get and head, a missing bucket is a
// failure of any call.
func TestNotFound_KeyOrBucket(t *testing.T) {
	t.Parallel()

	bare := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }
	bucketGone := func(w http.ResponseWriter, _ *http.Request) { xml(w, http.StatusNotFound, noSuchBucket) }

	for name, tc := range map[string]struct {
		op     string
		answer http.HandlerFunc
		call   func(ctx context.Context, client *s3.Client) error
		want   error
		not    error
	}{
		"get: no such key": {
			"get", func(w http.ResponseWriter, _ *http.Request) { xml(w, http.StatusNotFound, noSuchKey) },
			func(ctx context.Context, client *s3.Client) error { _, err := client.Get(ctx, "k", 10); return err },
			s3.ErrNotFound, s3.ErrBucketNotFound,
		},
		// HEAD has no body to carry an error code.
		"head: bare 404": {
			"head", bare,
			func(ctx context.Context, client *s3.Client) error { _, err := client.Head(ctx, "k"); return err },
			s3.ErrNotFound, s3.ErrBucketNotFound,
		},
		"get: no such bucket": {
			"get", bucketGone,
			func(ctx context.Context, client *s3.Client) error { _, err := client.Get(ctx, "k", 10); return err },
			s3.ErrBucketNotFound, s3.ErrNotFound,
		},
		"put: no such bucket": {
			"put", bucketGone,
			func(ctx context.Context, client *s3.Client) error { return client.Put(ctx, "k", []byte("x"), "") },
			s3.ErrBucketNotFound, s3.ErrNotFound,
		},
		"ensure: bare 404 on the bucket": {
			"head_bucket", bare,
			func(ctx context.Context, client *s3.Client) error { return client.Ensure(ctx) },
			s3.ErrBucketNotFound, s3.ErrNotFound,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := newFakeStore(t).answer(tc.op, tc.answer).client(t)

			reg := prometheus.NewRegistry()
			reg.MustRegister(client.Collectors()...)

			err := tc.call(context.Background(), client)
			require.ErrorIs(t, err, tc.want)
			require.NotErrorIs(t, err, tc.not)

			if errors.Is(tc.want, s3.ErrBucketNotFound) {
				assert.InDelta(t, 1, testx.Metric(t, reg, "s3_request_errors_total", map[string]string{"operation": tc.op}), 0)
			}
		})
	}
}

func TestEnsure_CreateBucketOutcomes(t *testing.T) {
	t.Parallel()

	// HEAD says the bucket is missing; what PUT answers decides.
	server := func(status int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}
	}
	auto := func(cfg *s3.Config) { cfg.AutoCreateBucket = true }

	raced := faulty(t, server(http.StatusConflict,
		`<?xml version="1.0" encoding="UTF-8"?><Error><Code>BucketAlreadyOwnedByYou</Code><Message>yours</Message></Error>`), auto)
	require.NoError(t, raced.Ensure(context.Background()), "another replica created it first")

	taken := faulty(t, server(http.StatusConflict,
		`<?xml version="1.0" encoding="UTF-8"?><Error><Code>BucketAlreadyExists</Code><Message>taken</Message></Error>`), auto)
	require.ErrorContains(t, taken.Ensure(context.Background()), "BucketAlreadyExists", "the name is another account's: not this client's bucket")

	broken := faulty(t, server(http.StatusInternalServerError, internalError), auto)
	require.ErrorContains(t, broken.Ensure(context.Background()), "s3: create bucket")
}

// DeleteObjects answers 200 and lists the keys it could not delete.
func TestDelete_PartialFailure(t *testing.T) {
	t.Parallel()

	client := faulty(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult>` +
			`<Deleted><Key>a</Key></Deleted>` +
			`<Error><Key>b</Key><Code>AccessDenied</Code><Message>no</Message></Error></DeleteResult>`))
	})

	err := client.Delete(context.Background(), []string{"a", "b"})
	require.ErrorContains(t, err, "AccessDenied")
	require.ErrorContains(t, err, `"b"`)
}

// A batch the store refuses does not stop the ones after it.
func TestDelete_GoesOnPastAFailedBatch(t *testing.T) {
	t.Parallel()

	store := newFakeStore(t).fails("delete")

	keys := make([]string, 1001) // one key more than a batch holds
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}

	require.Error(t, store.client(t).Delete(context.Background(), keys))
	assert.Equal(t, 2, store.calls("delete"))
}

// The limit is the caller's: the largest one reads the whole object, a
// negative one is the caller's mistake, not an object too large.
func TestGet_LimitEdges(t *testing.T) {
	t.Parallel()

	client := newFakeStore(t).client(t)

	body, err := client.Get(context.Background(), "k", math.MaxInt64)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(body))

	_, err = client.Get(context.Background(), "k", -1)
	require.ErrorContains(t, err, "negative")
	require.NotErrorIs(t, err, s3.ErrTooLarge)
}

// A body with no declared length is still held to the limit.
func TestGet_UndeclaredLengthOverTheLimit(t *testing.T) {
	t.Parallel()

	client := faulty(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)
		for range 4 {
			_, _ = w.Write(bytes.Repeat([]byte("x"), 1024))

			flusher.Flush() // chunked: no Content-Length
		}
	})

	_, err := client.Get(context.Background(), "k", 100)
	require.ErrorIs(t, err, s3.ErrTooLarge)
}

// A multipart upload whose part, completion or abort fails: the error names
// the step, and Abort survives a store that refuses it too.
func TestUpload_FailsMidway(t *testing.T) {
	t.Parallel()

	// failAt: "part" fails UploadPart, "complete" fails CompleteMultipartUpload;
	// AbortMultipartUpload always fails.
	server := func(failAt string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query()

			switch {
			case r.Method == http.MethodPost && query.Has("uploads"):
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(initiated))
			case r.Method == http.MethodPut && query.Has("partNumber") && failAt != "part":
				w.Header().Set("ETag", `"etag-`+query.Get("partNumber")+`"`)
				w.WriteHeader(http.StatusOK)
			default:
				fail(w)
			}
		}
	}

	part := make([]byte, s3.MinUploadPartBytes)

	failedPart := faulty(t, server("part")).NewUpload(context.Background(), "big", "application/octet-stream")
	_, err := failedPart.Write(part)
	require.ErrorContains(t, err, "s3: upload part 1")
	failedPart.Abort() // the store refuses the abort as well: logged, not returned

	failedComplete := faulty(t, server("complete")).NewUpload(context.Background(), "big", "application/octet-stream")
	_, err = failedComplete.Write(part)
	require.NoError(t, err)
	_, err = failedComplete.Write([]byte{1, 2, 3}) // the tail Close has to flush
	require.NoError(t, err)
	require.ErrorContains(t, failedComplete.Close(), "s3: complete upload")
	failedComplete.Abort()
}
