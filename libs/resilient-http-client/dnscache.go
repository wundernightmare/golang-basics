package resilient

import (
	"context"
	"net"
	"sync"
	"time"
)

// dnsCache is a tiny TTL-aware caching layer in front of the dialer. Go's
// standard resolver does not surface per-record TTLs, so each resolved host is
// cached for a fixed window in [minTTL, maxTTL] (we use maxTTL, floored at
// minTTL) rather than the upstream DNS TTL. This removes a resolver round-trip
// from the hot path for stable API endpoints.
type dnsCache struct {
	dialer *net.Dialer
	ttl    time.Duration

	mu      sync.RWMutex
	entries map[string]dnsEntry
}

type dnsEntry struct {
	ips       []string
	expiresAt time.Time
}

func newDNSCache(dialer *net.Dialer, minTTL, maxTTL time.Duration) *dnsCache {
	ttl := maxTTL
	if ttl < minTTL {
		ttl = minTTL
	}
	return &dnsCache{dialer: dialer, ttl: ttl, entries: make(map[string]dnsEntry)}
}

// dialContext resolves host through the cache, then dials each candidate IP in
// order until one connects. IP-literal hosts bypass the cache entirely.
func (d *dnsCache) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return d.dialer.DialContext(ctx, network, addr)
	}
	if net.ParseIP(host) != nil {
		return d.dialer.DialContext(ctx, network, addr)
	}

	ips, err := d.lookup(ctx, host)
	if err != nil {
		return nil, err
	}

	var firstErr error
	for _, ip := range ips {
		conn, derr := d.dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if derr == nil {
			return conn, nil
		}
		if firstErr == nil {
			firstErr = derr
		}
	}
	return nil, firstErr
}

// lookup returns cached IPs for host, refreshing on a cache miss or expiry.
func (d *dnsCache) lookup(ctx context.Context, host string) ([]string, error) {
	d.mu.RLock()
	ent, ok := d.entries[host]
	d.mu.RUnlock()
	if ok && time.Now().Before(ent.expiresAt) {
		return ent.ips, nil
	}

	resolver := d.dialer.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupHost(ctx, host)
	if err != nil {
		// Serve a stale entry if we have one — a transient resolver blip should
		// not take down requests to a host we resolved moments ago.
		if ok && len(ent.ips) > 0 {
			return ent.ips, nil
		}
		return nil, err
	}

	d.mu.Lock()
	d.entries[host] = dnsEntry{ips: ips, expiresAt: time.Now().Add(d.ttl)}
	d.mu.Unlock()
	return ips, nil
}
