package resilient_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
)

// A response whose body the caller forgot — never read, never closed — must
// not keep its bulkhead slot and its place in the in-flight count for the
// life of the process: once the attempt's time is over nothing more can be
// read, and the client lets go by itself.
func TestSend_ForgottenBodyIsReleasedWhenTheAttemptEnds(t *testing.T) {
	t.Parallel()

	srv := hangAfterHead(t)
	client := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Timeout = 60 * time.Millisecond
		tc.MaxConcurrent = 1
		tc.BreakerFailureRatio = 0 // the breaker is another test's subject
	}))

	forgotten, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	assert.InDelta(t, 1, metric(t, client, "http_client_bulkhead_in_flight", nil), 0)

	require.Eventually(t, func() bool {
		return metric(t, client, "http_client_bulkhead_in_flight", nil) == 0
	}, 5*time.Second, 5*time.Millisecond, "the slot comes back at the attempt timeout")

	_, err = io.ReadAll(forgotten.Body)
	require.ErrorIs(t, err, resilient.KindTimeout, "what is left of the body is a typed timeout")

	// The slot is usable, and a late Close does not release it a second time.
	next, err := client.Send("api", get(t, t.Context(), srv.URL))
	require.NoError(t, err)
	require.NoError(t, forgotten.Body.Close())
	assert.InDelta(t, 1, metric(t, client, "http_client_bulkhead_in_flight", nil), 0, "the late Close released nothing")
	require.NoError(t, next.Body.Close())
	assert.InDelta(t, 0, metric(t, client, "http_client_bulkhead_in_flight", nil), 0)

	// And Shutdown is not held up by the forgotten body.
	another, err := client.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // forgotten on purpose
	require.NoError(t, err)

	_ = another

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, client.Shutdown(ctx))
}

// The caller's context ending releases a forgotten body the same way.
func TestSend_ForgottenBodyIsReleasedWhenTheCallerCancels(t *testing.T) {
	t.Parallel()

	srv := hangAfterHead(t)
	client := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Timeout = 0 // no attempt timeout: only the caller bounds it
		tc.MaxConcurrent = 1
	}))

	ctx, cancel := context.WithCancel(t.Context())

	_, err := client.Send("api", get(t, ctx, srv.URL)) //nolint:bodyclose // forgotten on purpose
	require.NoError(t, err)

	cancel()

	require.Eventually(t, func() bool {
		return metric(t, client, "http_client_bulkhead_in_flight", nil) == 0
	}, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, resilient.StateClosed, mustState(t, client), "a cancellation tells the breaker nothing")
}

func mustState(t *testing.T, client *resilient.Client) resilient.BreakerState {
	t.Helper()

	state, err := client.BreakerState("api")
	require.NoError(t, err)

	return state
}

func TestSend_CancelledBeforeTheFirstAttemptSendsNothing(t *testing.T) {
	t.Parallel()

	srv, hits := server(t, status(http.StatusOK))
	client := newClient(t, target(nil))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := client.SendWithRetry("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindCanceled)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, hits.Load())
	assert.InDelta(t, 1, metric(t, client, "http_client_requests_total", map[string]string{"outcome": "canceled"}), 0)
}

// Cancelling during the wait between attempts ends the call at once: the
// delay here is a full second (Retry-After), the caller gets its answer in
// milliseconds, and the retry never goes out.
func TestSendWithRetry_CancelledDuringBackoffReturnsAtOnce(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	srv, hits := server(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)

		time.AfterFunc(20*time.Millisecond, cancel) // the client is in its backoff by then
	})
	client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.RetryMaxDelay = 5 * time.Second }))

	start := time.Now()
	_, err := client.SendWithRetry("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindCanceled)
	assert.Less(t, time.Since(start), 700*time.Millisecond, "it did not sit out the Retry-After")
	assert.Equal(t, int32(1), hits.Load())
	assert.LessOrEqual(t, metric(t, client, "http_client_retries_total", nil), 0.0, "a retry that was never sent is not counted")
}

// A caller that gives up while waiting for the rate limiter or for a bulkhead
// slot gets a cancellation — not "rate limited" or "bulkhead full", which
// would blame the policy for the caller's own deadline.
func TestSend_CancelledWhileWaitingForALocalLimit(t *testing.T) {
	t.Parallel()

	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") }

	t.Run("rate limiter", func(t *testing.T) {
		t.Parallel()

		srv, hits := server(t, ok)

		client := newClient(t, target(func(tc *resilient.TargetConfig) {
			tc.RateLimit, tc.RateBurst = 0.2, 1 // the next token is five seconds away
			tc.Timeout = 30 * time.Second
		}))
		resp, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		before := hits.Load()

		// A deadline too short for the next token is refused without waiting:
		// that one is the limiter's answer.
		short, cancelShort := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancelShort()

		start := time.Now()
		_, err = client.Send("api", get(t, short, srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindRateLimited)
		assert.Less(t, time.Since(start), 40*time.Millisecond, "it did not wait for a token it could not get in time")

		// A caller who leaves while waiting is a cancellation.
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(50*time.Millisecond, cancel)

		_, err = client.Send("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindCanceled)
		require.NotErrorIs(t, err, resilient.KindRateLimited)
		assert.Equal(t, before, hits.Load(), "nothing was sent")
	})

	t.Run("bulkhead", func(t *testing.T) {
		t.Parallel()

		srv, _ := server(t, ok)

		client := newClient(t, target(func(tc *resilient.TargetConfig) {
			tc.MaxConcurrent, tc.MaxConcurrentWait = 1, 30*time.Second
			tc.Timeout = 30 * time.Second
		}))
		held, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(50*time.Millisecond, cancel)

		_, err = client.Send("api", get(t, ctx, srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindCanceled)
		assert.LessOrEqual(t, metric(t, client, "http_client_bulkhead_rejected_total", nil), 0.0, "the bulkhead did not reject it: the caller left")
		require.NoError(t, held.Body.Close())
	})
}

func TestSend_BulkheadWithoutAWaitRejectsAtOnceAndAWaitTakesAFreedSlot(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })

	t.Run("no wait", func(t *testing.T) {
		t.Parallel()

		client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.MaxConcurrent, tc.MaxConcurrentWait = 1, 0 }))
		held, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err)

		start := time.Now()
		_, err = client.Send("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindBulkheadFull)
		assert.Less(t, time.Since(start), time.Second, "max_concurrent_wait 0 is no wait at all")
		require.NoError(t, held.Body.Close())
	})

	t.Run("slot freed during the wait", func(t *testing.T) {
		t.Parallel()

		client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.MaxConcurrent, tc.MaxConcurrentWait = 1, 10*time.Second }))
		held, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err)

		time.AfterFunc(30*time.Millisecond, func() { _ = held.Body.Close() })

		start := time.Now()
		resp, err := client.Send("api", get(t, t.Context(), srv.URL))
		require.NoError(t, err, "the waiter got the slot the first request gave back")
		assert.Less(t, time.Since(start), 5*time.Second)
		require.NoError(t, resp.Body.Close())
		assert.LessOrEqual(t, metric(t, client, "http_client_bulkhead_rejected_total", nil), 0.0)
	})
}

var errNoReplay = errors.New("the body source is gone")

// A body that cannot be produced a second time ends the retries as the
// caller's bug it is — and says nothing to the breaker about the dependency.
func TestSendWithRetry_BodyThatFailsToReplayIsInvalid(t *testing.T) {
	t.Parallel()

	srv, hits := server(t, status(http.StatusServiceUnavailable))
	client := newClient(t, target(nil))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, srv.URL, strings.NewReader("payload"))
	require.NoError(t, err)

	var calls atomic.Int32

	req.GetBody = func() (io.ReadCloser, error) {
		calls.Add(1)

		return nil, errNoReplay
	}

	_, err = client.SendWithRetry("api", req) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindInvalid)
	require.ErrorIs(t, err, errNoReplay)
	assert.Equal(t, int32(1), hits.Load(), "only the first attempt went out")
	assert.Equal(t, int32(1), calls.Load())
	assert.InDelta(t, 1, metric(t, client, "http_client_requests_total", map[string]string{"outcome": "invalid"}), 0)
}

// A redirect from https to http on the same host is refused before the
// plaintext request is made: the downgrade would send the request — headers,
// credentials — in the clear.
func TestSend_RefusesHTTPSToHTTPRedirect(t *testing.T) {
	t.Parallel()

	var plaintextHits atomic.Int32

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			plaintextHits.Add(1)
		}

		http.Redirect(w, r, "http://"+r.Host+"/landing", http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }),
		resilient.WithHTTPClient(srv.Client()))

	_, err := client.SendWithRetry("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindRedirect)
	require.ErrorContains(t, err, "https→http")
	assert.Zero(t, plaintextHits.Load())
	assert.False(t, resilient.Retryable(err), "a refused redirect is the policy's answer, not a failure to retry")
	assert.Equal(t, resilient.StateClosed, mustState(t, client), "the dependency answered: the breaker hears a success")
}

func TestSend_UnknownMethodIsOneLabelValue(t *testing.T) {
	t.Parallel()

	srv, _ := server(t, status(http.StatusNoContent))
	client := newClient(t, target(nil))

	for _, method := range []string{"PURGE", "BREW"} {
		req, err := http.NewRequestWithContext(t.Context(), method, srv.URL, http.NoBody)
		require.NoError(t, err)

		resp, err := client.Send("api", req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}

	assert.InDelta(t, 2, metric(t, client, "http_client_requests_total", map[string]string{"method": "_OTHER"}), 0,
		"a caller-chosen method must not mint label values")
}

// A certificate the client does not trust is not a transient fault: the next
// attempt meets the same certificate, so there is none — but the breaker
// still counts the target as failing.
func TestSendWithRetry_DoesNotRetryARejectedCertificate(t *testing.T) {
	t.Parallel()

	var handshakes atomic.Int32

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			handshakes.Add(1)
		}
	}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the server logs the client's bad_certificate alert
	srv.StartTLS()
	t.Cleanup(srv.Close)

	client := newClient(t, target(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 })) // trusts the system roots, not httptest's

	_, err := client.SendWithRetry("api", get(t, t.Context(), srv.URL)) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindConnection)

	var verification *tls.CertificateVerificationError

	require.ErrorAs(t, err, &verification)
	assert.False(t, resilient.Retryable(err))
	assert.Equal(t, int32(1), handshakes.Load(), "one connection, one handshake: no retry")
	assert.Equal(t, resilient.StateOpen, mustState(t, client), "the target is unusable, and the breaker knows")
}
