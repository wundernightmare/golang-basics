package s3_test

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
)

// A store that takes the request and never answers: each call ends on the
// deadline its Config field names, with a caller that set none of its own.
func TestCallsEndOnTheirOwnDeadline(t *testing.T) {
	t.Parallel()

	opTimeout := func(c *s3.Config) { c.OpTimeout = short }
	transferTimeout := func(c *s3.Config) { c.TransferTimeout = short }
	part := make([]byte, s3.MinUploadPartBytes)

	for name, tc := range map[string]struct {
		hangs   string
		timeout func(*s3.Config)
		call    func(ctx context.Context, client *s3.Client) error
	}{
		"put under S3_OP_TIMEOUT": {"put", opTimeout, func(ctx context.Context, client *s3.Client) error {
			return client.Put(ctx, "k", []byte("x"), "")
		}},
		"get under S3_TRANSFER_TIMEOUT": {"get", transferTimeout, func(ctx context.Context, client *s3.Client) error {
			_, err := client.Get(ctx, "k", 10)
			return err
		}},
		"part under S3_TRANSFER_TIMEOUT": {"upload_part", transferTimeout, func(ctx context.Context, client *s3.Client) error {
			up := client.NewUpload(ctx, "k", "")
			defer up.Abort()

			_, err := up.Write(part)

			return err
		}},
		"completion under S3_TRANSFER_TIMEOUT": {"complete_upload", transferTimeout, func(ctx context.Context, client *s3.Client) error {
			up := client.NewUpload(ctx, "k", "")
			defer up.Abort()

			_, err := up.Write(part)
			require.NoError(t, err)

			return up.Close()
		}},
		"readiness probe under its own three seconds": {"head_bucket", func(*s3.Config) {}, func(ctx context.Context, client *s3.Client) error {
			return client.ReadyCheck()(ctx)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := newFakeStore(t).hangs(tc.hangs).client(t, tc.timeout)

			start := time.Now()
			err := tc.call(context.Background(), client)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Less(t, time.Since(start), long/3)
		})
	}
}

// The context given to NewUpload bounds the upload: the parts, and the single
// put of an object that never filled one.
func TestUploadEndsWithItsContext(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		hangs string
		write int
	}{
		"a part":         {"upload_part", s3.MinUploadPartBytes},
		"the single put": {"put", 3},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := newFakeStore(t).hangs(tc.hangs).client(t)

			ctx, cancel := context.WithCancel(context.Background())
			timer := time.AfterFunc(short, cancel)

			t.Cleanup(func() { timer.Stop(); cancel() })

			up := client.NewUpload(ctx, "k", "")
			defer up.Abort()

			_, err := up.Write(make([]byte, tc.write))
			if err == nil {
				err = up.Close()
			}

			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

// An endpoint that accepts the connection and never completes the TLS
// handshake costs S3_DIAL_TIMEOUT, not the call's deadline.
func TestConnectIsBoundedByDialTimeout(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		conns []net.Conn
	)

	wg.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			mu.Lock()

			conns = append(conns, conn) // held open, never read
			mu.Unlock()
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()

		wg.Wait()

		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	client := clientOf(t, "https://"+listener.Addr().String(), nil, func(c *s3.Config) { c.DialTimeout = short })

	start := time.Now()
	_, err = client.Head(context.Background(), "k")
	require.Error(t, err)
	assert.Less(t, time.Since(start), long/3)
}

// S3_MAX_ATTEMPTS and S3_RETRY_MAX_BACKOFF reach the SDK: a failing store is
// asked that many times, with pauses no longer than the cap.
func TestFailedCallIsRetriedWithinTheBackoffCap(t *testing.T) {
	t.Parallel()

	store := newFakeStore(t).fails("put")
	client := store.client(t, func(c *s3.Config) {
		c.MaxAttempts = 8
		c.RetryMaxBackoff = time.Millisecond
	})

	start := time.Now()

	require.Error(t, client.Put(context.Background(), "k", []byte("x"), ""))
	assert.Equal(t, 8, store.calls("put"))
	// Uncapped, the SDK's seven pauses are random shares of 50 ms doubling
	// to 3.2 s: about three seconds together.
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

// A caller whose context has ended does not wait behind another client's
// Ensure of the same bucket.
func TestEnsureGivesUpWaitingWithItsContext(t *testing.T) {
	t.Parallel()

	store := newFakeStore(t)
	asked := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	store.answer("head_bucket", func(http.ResponseWriter, *http.Request) {
		once.Do(func() { close(asked) })
		<-release
	})

	first, second := store.client(t), store.client(t)

	var wg sync.WaitGroup

	wg.Go(func() { _ = first.Ensure(context.Background()) })
	t.Cleanup(func() { close(release); wg.Wait() })

	<-asked // the first client holds the bucket's turn

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- second.Ensure(ctx) }()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(long / 3):
		require.Fail(t, "Ensure waited for the other client with a cancelled context")
	}
}
