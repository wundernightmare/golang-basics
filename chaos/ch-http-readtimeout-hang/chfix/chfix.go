// Package chfix is a client-side fix for clickhouse-go's HTTP mode, where
// Options.ReadTimeout only bounds the wait for response headers.
//
// It plugs into the official Options.DialContext hook (no fork, no wrapper
// around the driver) and returns connections that arm a fresh deadline before
// every socket Read and Write. A call that makes no progress for the idle
// timeout fails with os.ErrDeadlineExceeded instead of blocking, which bounds
// the two phases the driver leaves open: reading the response body and writing
// the request body.
//
// Semantics to be aware of:
//   - the timeout is per socket operation (an idle timeout), not a total query
//     timeout — still pass a context deadline for that;
//   - a legitimate response that stays silent between blocks for longer than
//     the read timeout is cut off, exactly as the native protocol does;
//   - an idle keep-alive connection is dropped after the read timeout and
//     re-dialled on the next query.
package chfix

import (
	"context"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Apply sets Options.DialContext so that every connection enforces
// opt.ReadTimeout as an idle timeout on reads and on writes.
func Apply(opt *clickhouse.Options) {
	opt.DialContext = DialContext(opt.DialTimeout, opt.ReadTimeout, opt.ReadTimeout)
}

// DialContext returns a dial function for clickhouse.Options.DialContext.
// A zero readIdle or writeIdle disables the corresponding deadline.
func DialContext(dialTimeout, readIdle, writeIdle time.Duration) func(ctx context.Context, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: dialTimeout}
	return func(ctx context.Context, addr string) (net.Conn, error) {
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return &idleConn{Conn: c, readIdle: readIdle, writeIdle: writeIdle}, nil
	}
}

type idleConn struct {
	net.Conn
	readIdle, writeIdle time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	if c.readIdle > 0 {
		if err := c.Conn.SetReadDeadline(time.Now().Add(c.readIdle)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(p)
}

// Write re-arms the deadline per chunk: one large Write that keeps making
// progress must not fail, one that is stuck must.
func (c *idleConn) Write(p []byte) (int, error) {
	if c.writeIdle <= 0 {
		return c.Conn.Write(p)
	}
	const chunk = 64 << 10
	written := 0
	for written < len(p) {
		end := min(written+chunk, len(p))
		if err := c.Conn.SetWriteDeadline(time.Now().Add(c.writeIdle)); err != nil {
			return written, err
		}
		n, err := c.Conn.Write(p[written:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
