package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One object store per test binary; tests own their keys by prefix.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

// testBucket is shared by the package's tests (created on first use).
const testBucket = "libs-s3-test"

func storeConfig(t testing.TB) s3.Config {
	t.Helper()

	store := containers.S3(t)

	return s3.Config{
		Bucket: testBucket, Endpoint: store.Endpoint, Region: store.Region,
		AccessKeyID: store.AccessKey, SecretAccessKey: store.SecretKey,
		UsePathStyle: true, Insecure: true, AutoCreateBucket: true,
	}
}

func sharedClient(t testing.TB, mutate ...func(*s3.Config)) *s3.Client {
	t.Helper()

	cfg := storeConfig(t)
	for _, m := range mutate {
		m(&cfg)
	}

	client, err := s3.New(context.Background(), cfg, nil)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.NoError(t, client.Ensure(context.Background()))

	return client
}

func keys(t testing.TB, client *s3.Client, prefix string) []string {
	t.Helper()

	var out []string

	for obj, err := range client.List(context.Background(), prefix) {
		require.NoError(t, err)

		out = append(out, obj.Key)
	}

	return out
}

func TestPutGetHeadDelete(t *testing.T) {
	t.Parallel()

	client := sharedClient(t)
	ctx := context.Background()
	prefix := testx.Unique("obj") + "/"

	require.NoError(t, client.ReadyCheck()(ctx))

	_, err := client.Get(ctx, prefix+"absent", 1<<20)
	require.ErrorIs(t, err, s3.ErrNotFound)

	_, err = client.Head(ctx, prefix+"absent")
	require.ErrorIs(t, err, s3.ErrNotFound)

	require.NoError(t, client.Put(ctx, prefix+"a", []byte("first"), "text/plain"))
	require.NoError(t, client.Put(ctx, prefix+"a", []byte("second!"), "text/plain"), "a put replaces: repeating one is safe")

	body, err := client.Get(ctx, prefix+"a", 1<<20)
	require.NoError(t, err)
	assert.Equal(t, "second!", string(body))

	size, err := client.Head(ctx, prefix+"a")
	require.NoError(t, err)
	assert.Equal(t, int64(7), size)

	body, err = client.Get(ctx, prefix+"a", 7)
	require.NoError(t, err, "exactly the limit is allowed")
	assert.Len(t, body, 7)

	_, err = client.Get(ctx, prefix+"a", 6)
	require.ErrorIs(t, err, s3.ErrTooLarge)

	require.NoError(t, client.Delete(ctx, []string{prefix + "a", prefix + "never-existed"}), "a missing key is not a delete error")
	assert.Empty(t, keys(t, client, prefix))
	require.NoError(t, client.Delete(ctx, nil))
}

func TestListPagesAndPrefixes(t *testing.T) {
	t.Parallel()

	client := sharedClient(t)
	ctx := context.Background()
	prefix := testx.Unique("list") + "/"

	// More than one page (1000 keys) and more than one delete batch.
	const total = 1100

	want := make([]string, 0, total)

	for i := range total {
		dir := "a/"
		if i%2 == 1 {
			dir = "b/"
		}

		want = append(want, prefix+dir+testx.Unique("k"))
	}

	for _, key := range want {
		require.NoError(t, client.Put(ctx, key, []byte("x"), ""))
	}

	got := keys(t, client, prefix)
	assert.ElementsMatch(t, want, got)
	assert.IsIncreasing(t, got, "listing is in key order")

	dirs, err := client.ListPrefixes(ctx, prefix)
	require.NoError(t, err)
	assert.Equal(t, []string{prefix + "a/", prefix + "b/"}, dirs)

	// Stopping early ends the iteration without another page.
	seen := 0

	for _, err := range client.List(ctx, prefix) {
		require.NoError(t, err)

		seen++
		if seen == 3 {
			break
		}
	}

	assert.Equal(t, 3, seen)

	require.NoError(t, client.Delete(ctx, want))
	assert.Empty(t, keys(t, client, prefix))
}

func TestUploadStreamsInParts(t *testing.T) {
	t.Parallel()

	client := sharedClient(t, func(c *s3.Config) { c.UploadPartBytes = s3.MinUploadPartBytes })
	ctx := context.Background()
	prefix := testx.Unique("up") + "/"

	// Two full parts and a tail, written in uneven chunks.
	payload := make([]byte, 2*s3.MinUploadPartBytes+12345)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	up := client.NewUpload(ctx, prefix+"multi", "application/octet-stream")

	for rest := payload; len(rest) > 0; {
		n := min(len(rest), 1_000_003)
		written, err := up.Write(rest[:n])
		require.NoError(t, err)
		require.Equal(t, n, written)

		rest = rest[n:]
	}

	assert.Empty(t, keys(t, client, prefix), "nothing is visible before Close")
	require.NoError(t, up.Close())
	assert.Equal(t, int64(len(payload)), up.Size())
	up.Abort() // a no-op after Close

	got, err := client.Get(ctx, prefix+"multi", int64(len(payload)))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload, got), "the object is the bytes written")

	// Smaller than a part: a single put.
	small := client.NewUpload(ctx, prefix+"small", "")
	_, err = small.Write([]byte("tiny"))
	require.NoError(t, err)
	require.NoError(t, small.Close())

	got, err = client.Get(ctx, prefix+"small", 100)
	require.NoError(t, err)
	assert.Equal(t, "tiny", string(got))

	_, err = small.Write([]byte("more"))
	require.Error(t, err, "a finished upload refuses writes")
	require.Error(t, small.Close())
}

func TestUploadAbortLeavesNoObject(t *testing.T) {
	t.Parallel()

	client := sharedClient(t, func(c *s3.Config) { c.UploadPartBytes = s3.MinUploadPartBytes })
	prefix := testx.Unique("abort") + "/"

	up := client.NewUpload(context.Background(), prefix+"gone", "")
	_, err := up.Write(make([]byte, s3.MinUploadPartBytes+1)) // one part is sent
	require.NoError(t, err)
	up.Abort()
	up.Abort()

	assert.Empty(t, keys(t, client, prefix))
}

func TestEnsureReportsAMissingBucket(t *testing.T) {
	t.Parallel()

	cfg := storeConfig(t)
	cfg.Bucket = "libs-s3-missing"
	cfg.AutoCreateBucket = false

	client, err := s3.New(context.Background(), cfg, nil)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.ErrorIs(t, client.Ensure(context.Background()), s3.ErrNotFound)
	require.Error(t, client.ReadyCheck()(context.Background()))
}

// Chaos: a store that refuses connections fails a call inside its own
// deadline — not the caller's — and recovers without a new client.
//
//nolint:paralleltest // serial: takes the shared store's proxy down
func TestCallsAreBoundedWhenTheStoreIsDown(t *testing.T) {
	proxy := containers.Proxied(t, "s3")
	client := sharedClient(t, func(c *s3.Config) {
		c.Endpoint = "http://" + proxy.Addr
		c.OpTimeout = time.Second
		c.RetryMaxBackoff = 100 * time.Millisecond
	})
	ctx := context.Background() // no deadline of its own
	key := testx.Unique("chaos")

	require.NoError(t, client.Put(ctx, key, []byte("up"), ""))

	proxy.Down()

	start := time.Now()
	err := client.Put(ctx, key, []byte("down"), "")
	require.Error(t, err)
	assert.Less(t, time.Since(start), 3*time.Second, "bounded by S3_OP_TIMEOUT")
	require.Error(t, client.ReadyCheck()(ctx))

	proxy.Up()
	require.NoError(t, client.Put(ctx, key, []byte("back"), ""))

	reg := prometheus.NewRegistry()
	reg.MustRegister(client.Collectors()...)
	assert.GreaterOrEqual(t, testx.Metric(t, reg, "s3_request_errors_total", map[string]string{"operation": "put"}), 1.0)
}

// Chaos: a store that accepts the connection and never answers.
//
//nolint:paralleltest // serial: slows the shared store's proxy down
func TestCallsAreBoundedWhenTheStoreHangs(t *testing.T) {
	proxy := containers.Proxied(t, "s3")
	client := sharedClient(t, func(c *s3.Config) {
		c.Endpoint = "http://" + proxy.Addr
		c.OpTimeout = 500 * time.Millisecond
	})

	proxy.Latency(5 * time.Second)

	start := time.Now()
	err := client.Put(context.Background(), testx.Unique("slow"), []byte("x"), "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
}
