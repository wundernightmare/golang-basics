package testx

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
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
	question, ok := parseQuestion(query)
	if !ok {
		return
	}

	d.mu.Lock()
	addrs, known := d.records[question.name]
	mode := d.mode

	if count && question.qtype == dnsTypeA {
		d.queries[question.name]++
	}

	if mode == dnsSilent {
		d.held = append(d.held, heldQuery{query: query, from: from})
	}
	d.mu.Unlock()

	switch {
	case mode == dnsSilent:
		return
	case mode == dnsFails:
		_, _ = d.conn.WriteTo(question.reply(dnsServerFailure, nil), from)
	case !known:
		_, _ = d.conn.WriteTo(question.reply(dnsNameError, nil), from)
	case question.qtype == dnsTypeA:
		_, _ = d.conn.WriteTo(question.reply(dnsSuccess, addrs), from)
	default:
		_, _ = d.conn.WriteTo(question.reply(dnsSuccess, nil), from)
	}
}

// The wire format, RFC 1035 section 4, as far as an A-record server needs it.
// Written out here rather than imported from golang.org/x/net/dns/dnsmessage:
// for this one file golang.org/x/net would enter the go.mod of every module
// whose tests use this package.
const (
	dnsHeaderLen = 12
	dnsTypeA     = 1
	dnsClassINET = 1

	dnsSuccess       = 0
	dnsServerFailure = 2
	dnsNameError     = 3

	dnsFlagResponse           = 1 << 15
	dnsFlagRecursionDesired   = 1 << 8
	dnsFlagRecursionAvailable = 1 << 7
)

// dnsQuestion is the first question of a query.
type dnsQuestion struct {
	id    uint16
	flags uint16
	raw   []byte // the question section as sent: the reply echoes it byte for byte
	name  string // lower case, with the trailing dot
	qtype uint16
}

// parseQuestion reads the header and the first question of a query; what
// follows it (EDNS options) is ignored.
func parseQuestion(msg []byte) (dnsQuestion, bool) {
	if len(msg) < dnsHeaderLen || binary.BigEndian.Uint16(msg[4:]) == 0 {
		return dnsQuestion{}, false
	}

	var name strings.Builder

	at := dnsHeaderLen

	for {
		if at >= len(msg) {
			return dnsQuestion{}, false
		}

		length := int(msg[at])
		at++

		if length == 0 {
			break
		}

		// A length above 63 is a compression pointer, which a question's
		// only name has nothing to point at.
		if length > 63 || at+length > len(msg) {
			return dnsQuestion{}, false
		}

		name.Write(msg[at : at+length])
		name.WriteByte('.')

		at += length
	}

	if at+4 > len(msg) {
		return dnsQuestion{}, false
	}

	return dnsQuestion{
		id:    binary.BigEndian.Uint16(msg),
		flags: binary.BigEndian.Uint16(msg[2:]),
		raw:   msg[dnsHeaderLen : at+4],
		name:  strings.ToLower(name.String()),
		qtype: binary.BigEndian.Uint16(msg[at:]),
	}, true
}

// reply packs the response to q: the question echoed, rcode, and an A record
// with a TTL of zero per address.
func (q dnsQuestion) reply(rcode uint16, addrs []netip.Addr) []byte {
	flags := dnsFlagResponse | dnsFlagRecursionAvailable | q.flags&dnsFlagRecursionDesired | rcode

	out := make([]byte, 0, dnsHeaderLen+len(q.raw)+16*len(addrs))
	out = binary.BigEndian.AppendUint16(out, q.id)
	out = binary.BigEndian.AppendUint16(out, flags)
	out = binary.BigEndian.AppendUint16(out, 1)                  // questions
	out = binary.BigEndian.AppendUint16(out, uint16(len(addrs))) //nolint:gosec // G115: a test's handful of addresses
	out = append(out, 0, 0, 0, 0)                                // no authority, no additional records
	out = append(out, q.raw...)

	for _, addr := range addrs {
		ip := addr.As4()

		out = append(out, 0xC0, dnsHeaderLen) // the owner name: a pointer to the question's
		out = binary.BigEndian.AppendUint16(out, dnsTypeA)
		out = binary.BigEndian.AppendUint16(out, dnsClassINET)
		out = append(out, 0, 0, 0, 0) // TTL
		out = binary.BigEndian.AppendUint16(out, uint16(len(ip)))
		out = append(out, ip[:]...)
	}

	return out
}
