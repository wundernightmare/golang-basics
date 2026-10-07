package httpx_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func requestIDServer(t testing.TB) (*httpx.Server, *testx.LogBuffer) {
	t.Helper()
	srv, buf := loggedServer(t, httpx.Config{TrustedProxies: []string{"10.0.0.0/8"}})
	srv.Mux().HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {
		srv.Logger().InfoContext(r.Context(), "in handler")
		_, _ = io.WriteString(w, httpx.RequestIDFromContext(r.Context()))
	})
	return srv, buf
}

func TestRequestID_GeneratedEchoedAndLogged(t *testing.T) {
	srv, buf := requestIDServer(t)
	rec := get(srv.Handler(), "/ok")

	id := rec.Header().Get(httpx.RequestIDHeader)
	require.Len(t, id, 16, "16 hex chars")
	assert.Equal(t, id, rec.Body.String(), "the handler sees the same id through the context")
	assert.Equal(t, id, buf.Find(t, map[string]any{"msg": "in handler"})["request_id"])
	assert.Equal(t, id, buf.Find(t, map[string]any{"msg": "request"})["request_id"], "the access-log line too")

	rec = get(srv.Handler(), "/ok")
	assert.NotEqual(t, id, rec.Header().Get(httpx.RequestIDHeader), "fresh per request")
}

func TestRequestID_InboundHonouredOnlyFromTrustedProxyAndWhenSane(t *testing.T) {
	srv, _ := requestIDServer(t)
	for _, tc := range []struct {
		name, in, peer string
		kept           bool
	}{
		{"ULID from proxy", "01J9ZK4Q7W1X2Y3Z4A5B6C7D8E", "10.0.0.1:1234", true},
		{"UUID from proxy", "7f3c4d5e-1234-4abc-9def-0123456789ab", "10.0.0.1:1234", true},
		{"128 chars from proxy", strings.Repeat("x", 128), "10.0.0.1:1234", true},
		{"space", "has space", "10.0.0.1:1234", false},
		{"control char", "bad\x7f", "10.0.0.1:1234", false},
		{"too long", strings.Repeat("x", 129), "10.0.0.1:1234", false},
		{"sane but untrusted peer", "01J9ZK4Q7W1X2Y3Z4A5B6C7D8E", "203.0.113.9:1234", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ok", nil)
			req.RemoteAddr = tc.peer
			req.Header.Set(httpx.RequestIDHeader, tc.in)
			rec := do(srv.Handler(), req)
			out := rec.Header().Get(httpx.RequestIDHeader)
			if tc.kept {
				assert.Equal(t, tc.in, out)
				return
			}
			assert.NotEqual(t, tc.in, out, "%q must be replaced", tc.in)
			assert.Len(t, out, 16)
		})
	}
}

func TestClientIP_XForwardedForWalk(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{TrustedProxies: []string{"10.0.0.0/8", "::1"}})
	srv.Mux().HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, httpx.ClientIPFromContext(r.Context()))
	})
	for _, tc := range []struct {
		name, peer string
		xff        []string
		want       string
	}{
		{"no proxy", "192.0.2.1:1", nil, "192.0.2.1"},
		{"untrusted peer ignores XFF", "192.0.2.1:1", []string{"6.6.6.6"}, "192.0.2.1"},
		{"trusted peer, one hop", "10.0.0.1:1", []string{"203.0.113.7"}, "203.0.113.7"},
		{"skips trusted hops from the right", "10.0.0.1:1", []string{"6.6.6.6, 203.0.113.7, 10.0.0.9"}, "203.0.113.7"},
		{"multiple header lines", "10.0.0.1:1", []string{"6.6.6.6", "203.0.113.7"}, "203.0.113.7"},
		{"all hops trusted", "10.0.0.1:1", []string{"10.0.0.2"}, "10.0.0.1"},
		{"malformed hop stops the walk", "10.0.0.1:1", []string{"203.0.113.7, garbage, 10.0.0.2"}, "10.0.0.1"},
		{"trusted IPv6 peer", "[::1]:1", []string{"2001:db8::1"}, "2001:db8::1"},
		{"IPv4-mapped peer", "[::ffff:10.0.0.1]:1", []string{"203.0.113.8"}, "203.0.113.8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tc.peer
			for _, v := range tc.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			rec := do(srv.Handler(), req)
			assert.Equal(t, tc.want, rec.Body.String())
		})
	}
	// The access log attributes the request to the same address.
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "request", "client_ip": "2001:db8::1"}))
}

func TestRequestID_Helpers(t *testing.T) {
	id := httpx.NewRequestID()
	assert.Regexp(t, `^[0-9a-f]{16}$`, id)
	assert.NotEqual(t, id, httpx.NewRequestID())

	ctx := httpx.WithRequestID(context.Background(), "job-7")
	assert.Equal(t, "job-7", httpx.RequestIDFromContext(ctx))
	assert.Empty(t, httpx.RequestIDFromContext(context.Background()))
	assert.Empty(t, httpx.RouteFromContext(context.Background()))
	assert.Empty(t, httpx.ClientIPFromContext(context.Background()))
}
