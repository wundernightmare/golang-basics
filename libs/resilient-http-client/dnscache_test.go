package resilient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDNSCache_DialsAndCachesHostname(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	dc := newDNSCache(&net.Dialer{Timeout: time.Second}, 10*time.Second, time.Minute)
	addr := net.JoinHostPort("localhost", port)

	conn, err := dc.dialContext(context.Background(), "tcp", addr)
	require.NoError(t, err)
	_ = conn.Close()

	// The hostname resolution should now be cached.
	dc.mu.RLock()
	_, cached := dc.entries["localhost"]
	dc.mu.RUnlock()
	assert.True(t, cached, "localhost lookup should be cached after a dial")

	// A second dial succeeds (served from the cache).
	conn2, err := dc.dialContext(context.Background(), "tcp", addr)
	require.NoError(t, err)
	_ = conn2.Close()
}

func TestDNSCache_IPLiteralBypassesCache(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	dc := newDNSCache(&net.Dialer{Timeout: time.Second}, 10*time.Second, time.Minute)
	conn, err := dc.dialContext(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	_ = conn.Close()

	dc.mu.RLock()
	n := len(dc.entries)
	dc.mu.RUnlock()
	assert.Equal(t, 0, n, "IP-literal dials must not populate the DNS cache")
}

func TestNewDNSCache_TTLFlooredAtMin(t *testing.T) {
	dc := newDNSCache(&net.Dialer{}, time.Minute, time.Second) // max < min
	assert.Equal(t, time.Minute, dc.ttl, "ttl should be floored at minTTL")
}
