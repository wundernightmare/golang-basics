package integration_test

// The protocol layer: the same guarantees over every wire the edge serves —
// HTTP/1.1 and h2c in cleartext, HTTP/1.1 and HTTP/2 over TLS, HTTP/3 — on a
// healthy network. What differs between protocols is asserted as a
// difference, not skipped.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
)

// multiplexed reports whether the wire carries concurrent requests on one
// connection.
func (w wire) multiplexed() bool { return w.proto != "HTTP/1.1" }

// The server confirms the protocol the client was built for — a client that
// silently fell back to HTTP/1.1 would pass every other test here — and the
// connection is kept between requests.
func TestWire_ServerSeesTheProtocolAndOneConnection(t *testing.T) {
	t.Parallel()

	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			t.Parallel()

			base := link.base(srv)
			client := link.client(t, policy(nil))

			first := whoami(t, client, base)
			assert.Equal(t, link.proto, first.Proto)

			if link.tls {
				assert.Equal(t, "tls1.3", first.TLS)
			}

			for range 3 {
				assert.Equal(t, first.Port, whoami(t, client, base).Port, "the next request reuses the connection")
			}
		})
	}
}

// HTTP/2 and HTTP/3 carry concurrent requests on the one connection: while
// the server holds a slow request, a second one reaches it from the same
// client port and is answered at once. HTTP/1.1 has that connection busy, so
// the second request arrives on another.
//
//nolint:paralleltest // one protocol at a time: Redpanda serves the long polls these make one by one
func TestWire_ASecondRequestWhileOneIsInFlight(t *testing.T) {
	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			base := link.base(srv)
			client := link.client(t, policy(nil))
			empty := topic(t)
			first := whoami(t, client, base)

			var held sync.WaitGroup

			start := time.Now()

			held.Go(func() {
				resp, err := client.Send(api, poll(t, t.Context(), base+"/panda", empty, 0, time.Second))
				if assert.NoError(t, err) {
					assert.Equal(t, "[]", read(t, resp))
				}

				assert.GreaterOrEqual(t, time.Since(start), time.Second, "the server held it for the poll timeout")
			})

			time.Sleep(250 * time.Millisecond) // the poll is with the server now

			during := whoami(t, client, base)
			assert.Less(t, time.Since(start), 900*time.Millisecond, "answered while the slow one was still held")

			if link.multiplexed() {
				assert.Equal(t, first.Port, during.Port, "a second stream on the same connection")
			} else {
				assert.NotEqual(t, first.Port, during.Port, "the kept connection is busy: a second one was opened")
			}

			held.Wait()
		})
	}
}

// A megabyte up and four down arrive intact through the client's body
// wrapper, whatever frames them.
func TestWire_BodiesArriveIntact(t *testing.T) {
	t.Parallel()

	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			t.Parallel()

			base := link.base(srv)
			client := link.client(t, policy(nil))

			raw := make([]byte, 512<<10)
			_, err := rand.Read(raw)
			require.NoError(t, err)

			payload := hex.EncodeToString(raw) // 1 MiB of text: the echo route is a template

			req := request(t, t.Context(), http.MethodPost, base+"/echo", strings.NewReader(payload))
			req.Header.Set("Idempotency-Key", "integration-1")

			resp, err := client.Send(api, req)
			require.NoError(t, err)
			assert.Equal(t, link.proto, resp.Proto)
			assert.Equal(t, http.MethodPost, resp.Header.Get("X-Echo-Method"))
			assert.Equal(t, "integration-1", resp.Header.Get("X-Echo-Idempotency-Key"), "request headers reach the server")
			assert.Equal(t, sha256.Sum256([]byte(payload)), sha256.Sum256([]byte(read(t, resp))), "the echoed megabyte is the one sent")

			resp, err = client.Send(api, get(t, base+"/files/blob"))
			require.NoError(t, err)

			digest := sha256.New()
			n, err := io.Copy(digest, resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, int64(blobSize), n)
			assert.Equal(t, blobDigest(), hex.EncodeToString(digest.Sum(nil)))
		})
	}
}

func blobDigest() string {
	sum := sha256.Sum256([]byte(strings.Repeat("0123456789abcdef", blobSize/16)))

	return hex.EncodeToString(sum[:])
}

// A status ≥ 400 comes back as a typed error carrying the server's own
// answer, and the retryable ones are retried exactly as often as configured.
func TestWire_StatusErrorsAndRetries(t *testing.T) {
	t.Parallel()

	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			t.Parallel()

			base := link.base(srv)
			client := link.client(t, policy(func(tc *resilient.TargetConfig) {
				tc.RetryMaxAttempts = 3
				tc.RetryMaxDelay = 2 * time.Second // room for the server's Retry-After
			}))

			_, err := client.SendWithRetry(api, get(t, base+"/status/404")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindStatus)

			var outbound *resilient.OutboundError

			require.ErrorAs(t, err, &outbound)
			assert.Equal(t, http.StatusNotFound, outbound.StatusCode)
			assert.Equal(t, "status 404", string(outbound.Body))
			assert.InDelta(t, 1, requests(t, client, "4xx"), 0, "a 404 is an answer: not retried")

			_, err = client.SendWithRetry(api, get(t, base+"/status/502")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindStatus)
			assert.InDelta(t, 3, requests(t, client, "5xx"), 0, "a 502 is retried up to retry_max_attempts")

			_, err = client.SendWithRetry(api, get(t, base+"/status/501")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindStatus)
			assert.InDelta(t, 4, requests(t, client, "5xx"), 0, "a 501 will not get better: sent once")

			// The server names its own delay; the client waits it out, once per retry.
			start := time.Now()
			_, err = client.SendWithRetry(api, get(t, base+"/busy")) //nolint:bodyclose // error
			require.ErrorAs(t, err, &outbound)
			assert.Equal(t, http.StatusServiceUnavailable, outbound.StatusCode)
			assert.Equal(t, time.Second, outbound.RetryAfter)
			assert.GreaterOrEqual(t, time.Since(start), 2*time.Second, "two retries, a Retry-After of one second before each")
			assert.InDelta(t, 4, metric(t, client, "http_client_retries_total", nil), 0, "two for the 502, two for /busy")
		})
	}
}

// A server that takes longer than the target's timeout is cut off at the
// timeout. What happens to the connection is the protocol's business: HTTP/2
// and HTTP/3 reset one stream and keep the connection, HTTP/1.1 has nothing
// to reset and drops it.
//
//nolint:paralleltest // one protocol at a time: Redpanda serves the long polls these make one by one
func TestWire_AttemptTimeoutOnASlowServer(t *testing.T) {
	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			base := link.base(srv)
			client := link.client(t, policy(func(tc *resilient.TargetConfig) { tc.Timeout = 300 * time.Millisecond }))
			empty := topic(t)
			before := whoami(t, client, base)

			start := time.Now()
			_, err := client.Send(api, poll(t, t.Context(), base+"/panda", empty, 0, time.Second)) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindTimeout)
			assert.InDelta(t, 300, time.Since(start).Milliseconds(), 250, "cut at the target's timeout, not at the server's pace")
			assert.True(t, resilient.Retryable(err))

			after := whoami(t, client, base)
			if link.multiplexed() {
				assert.Equal(t, before.Port, after.Port, "the timed-out stream was reset; the connection lives on")
			} else {
				assert.NotEqual(t, before.Port, after.Port, "HTTP/1.1 can only abandon the connection")
			}
		})
	}
}

// A response the caller never reads: the server stalls on flow control with
// megabytes still to send, the attempt times out, and the client gives the
// bulkhead slot back on its own. The next request goes through.
func TestWire_ForgottenBodyDoesNotHoldTheTarget(t *testing.T) {
	t.Parallel()

	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			t.Parallel()

			base := link.base(srv)
			client := link.client(t, policy(func(tc *resilient.TargetConfig) {
				tc.Timeout = 400 * time.Millisecond
				tc.MaxConcurrent = 1
				tc.BreakerFailureRatio = 0
			}))

			_, err := client.Send(api, get(t, base+"/files/blob")) //nolint:bodyclose // forgotten on purpose
			require.NoError(t, err)
			assert.InDelta(t, 1, metric(t, client, "http_client_bulkhead_in_flight", nil), 0)

			_, err = client.Send(api, get(t, base+"/whoami")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindBulkheadFull, "the only slot is taken while the attempt lasts")

			require.Eventually(t, func() bool {
				return metric(t, client, "http_client_bulkhead_in_flight", nil) == 0
			}, 5*time.Second, 10*time.Millisecond, "released at the attempt timeout")

			assert.Equal(t, link.proto, whoami(t, client, base).Proto, "and the target is usable again")
		})
	}
}

// Closing a large body after the first kilobyte is the caller's choice, not
// a failure: the breaker hears a success, and a multiplexed connection is
// kept (the stream is reset) where HTTP/1.1 has to drop it.
func TestWire_ClosingABodyEarly(t *testing.T) {
	t.Parallel()

	srv := web(t)

	for _, link := range wires() {
		t.Run(link.name, func(t *testing.T) {
			t.Parallel()

			base := link.base(srv)
			client := link.client(t, policy(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }))
			before := whoami(t, client, base)

			resp, err := client.Send(api, get(t, base+"/files/blob"))
			require.NoError(t, err)

			head := make([]byte, 1024)
			_, err = io.ReadFull(resp.Body, head)
			require.NoError(t, err)
			assert.Equal(t, strings.Repeat("0123456789abcdef", 64), string(head))
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, resilient.StateClosed, breakerState(t, client), "with breaker_min_requests 1, one failure would have opened it")

			after := whoami(t, client, base)
			if link.multiplexed() {
				assert.Equal(t, before.Port, after.Port)
			} else {
				assert.NotEqual(t, before.Port, after.Port, "megabytes unread: the connection cannot be reused")
			}
		})
	}
}

// One record produced over every protocol, read back from the broker: the
// client's requests are what a real API understood them to be.
//
//nolint:paralleltest // one protocol at a time: Redpanda serves the long polls these make one by one
func TestWire_ProducesToRedpandaThroughEveryProtocol(t *testing.T) {
	into := topic(t)
	srv := web(t)

	var want []string

	// A generous timeout: the broker may still be finishing the long polls
	// earlier tests walked away from.
	patient := policy(func(tc *resilient.TargetConfig) { tc.Timeout = 30 * time.Second })

	for _, link := range wires() {
		client := link.client(t, patient)
		value := strings.ReplaceAll(link.name, " ", "-")
		want = append(want, value)

		resp, err := client.Send(api, produce(t, t.Context(), link.base(srv)+"/panda", into, value))
		require.NoError(t, err, link.name)
		assert.Contains(t, read(t, resp), `"offsets"`, link.name)
	}

	assert.Equal(t, want, stored(t, into), "five protocols, five records, in order")

	// The API's own error document reaches the caller.
	client := newClient(t, patient, nil)
	bad := produce(t, t.Context(), "http://"+redpanda(t).Proxy, into, "x")
	bad.Header.Set("Accept", "text/plain")

	_, err := client.Send(api, bad) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus)

	var outbound *resilient.OutboundError

	require.ErrorAs(t, err, &outbound)
	assert.Equal(t, http.StatusNotAcceptable, outbound.StatusCode)
	assert.JSONEq(t, `{"error_code":406,"message":"not_acceptable"}`, string(outbound.Body))
	assert.Len(t, stored(t, into), len(want), "the refused request stored nothing")
}
