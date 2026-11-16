package s3_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// Every call is measured and traced. What the store answered about the
// caller's own question — no such key, an object over the caller's limit — is
// not a failure of the store: no error series, no error on the span.
func TestCallIsMeasuredAndTraced(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		answer http.HandlerFunc
		want   error
		failed bool
	}{
		"an object":      {nil, nil, false},
		"a missing key":  {func(w http.ResponseWriter, _ *http.Request) { xml(w, http.StatusNotFound, noSuchKey) }, s3.ErrNotFound, false},
		"over the limit": {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(make([]byte, 64)) }, s3.ErrTooLarge, false},
		"a store error":  {func(w http.ResponseWriter, _ *http.Request) { fail(w) }, nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spans := testx.Recorder(t)
			store := newFakeStore(t).answer("get", tc.answer).answer("head", func(w http.ResponseWriter, _ *http.Request) { fail(w) })
			client := clientOf(t, store.url, []s3.Option{s3.WithTracerProvider(spans.Provider)})

			reg := prometheus.NewRegistry()
			reg.MustRegister(client.Collectors()...)

			_, err := client.Get(context.Background(), "k", 10)

			switch {
			case tc.failed:
				require.Error(t, err)
			case tc.want != nil:
				require.ErrorIs(t, err, tc.want)
			default:
				require.NoError(t, err)
			}

			get := map[string]string{"operation": "get"}
			assert.InDelta(t, 1, testx.Metric(t, reg, "s3_request_duration_seconds", get), 0)

			ended := spans.Ended()
			require.Len(t, ended, 1)
			assert.Equal(t, "s3 get", ended[0].Name())
			assert.ElementsMatch(t, []attribute.KeyValue{
				attribute.String("rpc.system.name", "aws-api"),
				attribute.String("rpc.method", "S3/GetObject"),
				attribute.String("cloud.region", "us-east-1"),
				attribute.String("aws.s3.bucket", "b"),
				attribute.String("aws.s3.key", "k"),
			}, ended[0].Attributes())

			if tc.failed {
				assert.InDelta(t, 1, testx.Metric(t, reg, "s3_request_errors_total", get), 0)
				assert.Equal(t, codes.Error, ended[0].Status().Code)
			} else {
				assert.Equal(t, codes.Unset, ended[0].Status().Code)

				// The error family is gathered once it has a series: a call
				// that does fail gives it one, so the absent one is told
				// from a misspelt name.
				_, err = client.Head(context.Background(), "k")
				require.Error(t, err)
				testx.NoMetric(t, reg, "s3_request_errors_total", get)
			}
		})
	}
}

// Two clients share one registry when each is registered under its own
// const label (README "Metrics").
func TestTwoClientsOnOneRegistry(t *testing.T) {
	t.Parallel()

	store := newFakeStore(t)
	reg := prometheus.NewRegistry()

	for _, name := range []string{"archive", "dumps"} {
		wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"store": name}, reg)
		for _, c := range store.client(t).Collectors() {
			require.NoError(t, wrapped.Register(c))
		}
	}
}

// S3_MAX_CONNS_PER_HOST caps the connections however many calls run at once.
func TestConnectionsAreCapped(t *testing.T) {
	t.Parallel()

	var inFlight, peak atomic.Int32

	store := newFakeStore(t).answer("head", func(w http.ResponseWriter, _ *http.Request) {
		now := inFlight.Add(1)
		defer inFlight.Add(-1)

		for old := peak.Load(); now > old && !peak.CompareAndSwap(old, now); old = peak.Load() {
		}

		time.Sleep(20 * time.Millisecond)
		w.Header().Set("Content-Length", "5")
	})
	client := store.client(t, func(c *s3.Config) { c.MaxConnsPerHost = 2 })

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _, _ = client.Head(context.Background(), "k") })
	}

	wg.Wait()

	assert.Equal(t, 8, store.calls("head"))
	assert.LessOrEqual(t, peak.Load(), int32(2))
}

// The default sends no CRC32 of the body, which several S3-compatible stores
// reject; when_supported is the SDK's own behaviour.
func TestRequestChecksumMode(t *testing.T) {
	t.Parallel()

	for mode, wantChecksum := range map[string]bool{"": false, s3.ChecksumWhenSupported: true} {
		t.Run("mode "+mode, func(t *testing.T) {
			t.Parallel()

			headers := make(chan http.Header, 1)
			store := newFakeStore(t).answer("put", func(_ http.ResponseWriter, r *http.Request) { headers <- r.Header })
			client := store.client(t, func(c *s3.Config) { c.RequestChecksum = mode })

			require.NoError(t, client.Put(context.Background(), "k", []byte("x"), ""))

			sent := <-headers
			// A header over http, a trailer over https.
			assert.Equal(t, wantChecksum, sent.Get("X-Amz-Checksum-Crc32") != "" || sent.Get("X-Amz-Trailer") != "")
		})
	}
}
