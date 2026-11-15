package testx

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

// DNS is a name server on loopback whose answers a test changes while its
// client runs: a name that does not exist, a record that moves, a server
// that fails or says nothing. The client under test resolves through the
// real resolver — Go's ([DNS.Resolver]) or gRPC's (`dns://<Addr>/name:port`)
// — so what it sees is what that resolver makes of a real answer, not an
// error a test made up.
//
// It answers A queries over UDP with a TTL of zero; every other name is
// NXDOMAIN, and a known name has no AAAA record.
type DNS struct {
	conn net.PacketConn

	mu      sync.Mutex
	records map[string][]netip.Addr
	mode    dnsMode
	queries map[string]int
	held    []heldQuery // received while silent
}

type heldQuery struct {
	query []byte
	from  net.Addr
}

type dnsMode int

const (
	dnsAnswers dnsMode = iota
	dnsFails
	dnsSilent
)

// NewDNS starts a name server that knows no names; it stops with the test.
func NewDNS(tb testing.TB) *DNS {
	tb.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0") //nolint:noctx // a test fixture on loopback
	require.NoError(tb, err)

	server := &DNS{conn: conn, records: map[string][]netip.Addr{}, queries: map[string]int{}}
	done := make(chan struct{})

	go func() {
		defer close(done)

		server.serve()
	}()

	tb.Cleanup(func() {
		// A resolver still waiting for an answer waits out its own timeout
		// (seconds), whatever became of the request it worked for: answer
		// it, so no test leaves resolver goroutines behind.
		server.Answer()

		_ = conn.Close()

		<-done
	})

	return server
}

// Addr is the server's host:port — the authority of a gRPC `dns://` target.
func (d *DNS) Addr() string { return d.conn.LocalAddr().String() }

// Resolver is Go's own resolver asking this server and nothing else.
func (d *DNS) Resolver() *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var dialer net.Dialer

			return dialer.DialContext(ctx, network, d.Addr())
		},
	}
}

// Set makes name resolve to addrs (IPv4), replacing what it resolved to.
func (d *DNS) Set(name string, addrs ...string) {
	parsed := make([]netip.Addr, len(addrs))
	for i, addr := range addrs {
		parsed[i] = netip.MustParseAddr(addr)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.records[fqdn(name)] = parsed
}

// Remove makes name not exist (NXDOMAIN).
func (d *DNS) Remove(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.records, fqdn(name))
}

// Fail makes the server answer every query with SERVFAIL until [DNS.Answer].
func (d *DNS) Fail() { d.setMode(dnsFails) }

// Silence makes the server read queries and answer none until [DNS.Answer]:
// what an unreachable name server looks like.
func (d *DNS) Silence() { d.setMode(dnsSilent) }

// Answer ends [DNS.Fail] and [DNS.Silence], and answers — late — the
// queries received in silence.
func (d *DNS) Answer() {
	d.mu.Lock()
	d.mode = dnsAnswers
	held := d.held
	d.held = nil
	d.mu.Unlock()

	for _, late := range held {
		d.answer(late.query, late.from, false)
	}
}

// Queries is how many A queries for name the server has received.
func (d *DNS) Queries(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.queries[fqdn(name)]
}

func (d *DNS) setMode(mode dnsMode) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mode = mode
}

func fqdn(name string) string { return strings.ToLower(strings.TrimSuffix(name, ".")) + "." }

func (d *DNS) serve() {
	buf := make([]byte, 1500)

	for {
		n, from, err := d.conn.ReadFrom(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}

		if err != nil {
			continue
		}

		// A copy: a query held while the server is silent outlives buf,
		// which the next read overwrites.
		d.answer(append([]byte(nil), buf[:n]...), from, true)
	}
}

// answer replies to one query, or holds it while the server is silent.
// count is false for a held query answered late: it was counted on arrival.
func (d *DNS) answer(query []byte, from net.Addr, count bool) {
	var parser dnsmessage.Parser

	header, err := parser.Start(query)
	if err != nil {
		return
	}

	question, err := parser.Question()
	if err != nil {
		return
	}

	name := strings.ToLower(question.Name.String())

	d.mu.Lock()
	addrs, known := d.records[name]
	mode := d.mode

	if count && question.Type == dnsmessage.TypeA {
		d.queries[name]++
	}

	if mode == dnsSilent {
		d.held = append(d.held, heldQuery{query: query, from: from})
	}
	d.mu.Unlock()

	if mode == dnsSilent {
		return
	}

	answer := dnsmessage.Message{
		ID:                 header.ID,
		Response:           true,
		RecursionDesired:   header.RecursionDesired,
		RecursionAvailable: true,
		Questions:          []dnsmessage.Question{question},
	}

	switch {
	case mode == dnsFails:
		answer.RCode = dnsmessage.RCodeServerFailure
	case !known:
		answer.RCode = dnsmessage.RCodeNameError
	case question.Type == dnsmessage.TypeA:
		for _, addr := range addrs {
			answer.Answers = append(answer.Answers, dnsmessage.Resource{
				Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET},
				Body:   &dnsmessage.AResource{A: addr.As4()},
			})
		}
	}

	if reply, err := answer.Pack(); err == nil {
		_, _ = d.conn.WriteTo(reply, from)
	}
}
