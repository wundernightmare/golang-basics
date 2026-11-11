// trivial — how can a query with "nothing to execute" hang far beyond ReadTimeout?
//
// The matrix shows a trivial query is bounded by ReadTimeout as long as the
// connection dies BEFORE the response headers arrive. This experiment cuts the
// connection in the one place ReadTimeout does not cover: after the response
// headers, before the (tiny) body. An in-process TCP middlebox sits between
// clickhouse-go and ClickHouse and, once armed, lets the response headers
// through and then either
//
//	stall     — goes silent but keeps the TCP connection open and ACKing
//	            (L4/L7 balancer, sidecar, NAT that lost its backend), or
//	blackhole — drops every packet (nftables), i.e. a real lost connection.
//
// It also measures how often ClickHouse naturally delivers headers and body in
// separate TCP reads, i.e. how wide the window for this race is.
//
//	trivial hang  -mode stall|blackhole -op select1|exec -api native|std -port N -cap 25m   (ports outside the 28100-28399 range published by podman)
//	trivial split -n 3000 -port N
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"chaoslab/chfix"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	toxiAPI   = "http://127.0.0.1:28474"
	plainPort = 28399 // toxiproxy listener that forwards to ClickHouse untouched
)

// nativeProto switches the experiment to the native TCP protocol (9000). There
// is no header/body boundary there, so the middlebox cuts the connection after
// the first 4 bytes of the response instead.
var nativeProto bool

func upstreamPort() int {
	if nativeProto {
		return plainPort - 1
	}
	return plainPort
}

func ensurePlainProxy() error {
	name, target := "ch_plain", "chaoslab-ch:8123"
	if nativeProto {
		name, target = "ch_plain_native", "chaoslab-ch:9000"
	}
	body := fmt.Sprintf(`{"name":"%s","listen":"0.0.0.0:%d","upstream":"%s","enabled":true}`, name, upstreamPort(), target)
	resp, err := http.Post(toxiAPI+"/proxies", "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("toxiproxy: %s: %s", resp.Status, b)
	}
	return nil
}

type middlebox struct {
	ln    net.Listener
	port  int
	mode  string
	armed atomic.Bool
	cutAt atomic.Int64 // unix nanos when the cut happened

	mu       sync.Mutex
	total    int             // responses seen
	separate int             // header block and first body byte arrived in different reads
	gaps     []time.Duration // delay between them
}

func newMiddlebox(port int, mode string) (*middlebox, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	m := &middlebox{ln: ln, port: port, mode: mode}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go m.serve(c)
		}
	}()
	return m, nil
}

func (m *middlebox) serve(client net.Conn) {
	up, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", upstreamPort()))
	if err != nil {
		client.Close()
		return
	}
	newResp := make(chan struct{}, 64) // one token per request seen
	go func() {                        // requests: client -> upstream, forwarded verbatim
		buf := make([]byte, 64<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				if bytes.HasPrefix(buf[:n], []byte("POST ")) {
					newResp <- struct{}{}
				}
				if _, werr := up.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// responses: upstream -> client
	buf := make([]byte, 64<<10)
	var (
		inHeaders  = true
		hdr        []byte
		hdrDoneAt  time.Time
		waitBody   bool
		stalled    bool
		expectResp = func() {
			select {
			case <-newResp:
			default:
			}
			inHeaders, hdr, waitBody = true, nil, false
		}
	)
	for {
		n, err := up.Read(buf)
		if n > 0 && !stalled {
			chunk := buf[:n]
			if nativeProto {
				if m.armed.Load() {
					head := chunk[:min(4, n)]
					_, _ = client.Write(head)
					if m.mode == "blackhole" {
						time.Sleep(50 * time.Millisecond)
						_ = nftDrop(m.port)
						_, _ = client.Write(chunk[len(head):])
					} else {
						stalled = true
					}
					m.cutAt.Store(time.Now().UnixNano())
					m.armed.Store(false)
					continue
				}
				if _, werr := client.Write(chunk); werr != nil {
					return
				}
				continue
			}
			if waitBody { // first bytes after a header-only read
				m.mu.Lock()
				m.separate++
				m.gaps = append(m.gaps, time.Since(hdrDoneAt))
				m.mu.Unlock()
				waitBody = false
			}
			if inHeaders {
				hdr = append(hdr, chunk...)
				if i := bytes.Index(hdr, []byte("\r\n\r\n")); i >= 0 {
					inHeaders = false
					rest := len(hdr) - (i + 4) // body bytes that came in the same read
					m.mu.Lock()
					m.total++
					m.mu.Unlock()
					if m.armed.Load() {
						// Deliver exactly the header block, then cut.
						headOnly := chunk[:len(chunk)-rest]
						switch m.mode {
						case "stall":
							_, _ = client.Write(headOnly)
							stalled = true // keep both sockets open, forward nothing more
						case "blackhole":
							_, _ = client.Write(headOnly)
							time.Sleep(50 * time.Millisecond) // let the headers reach the client
							_ = nftDrop(m.port)
							_, _ = client.Write(chunk[len(chunk)-rest:])
						}
						m.cutAt.Store(time.Now().UnixNano())
						m.armed.Store(false)
						continue
					}
					if rest == 0 {
						waitBody, hdrDoneAt = true, time.Now()
					}
				}
			} else if bytes.HasSuffix(chunk, []byte("0\r\n\r\n")) {
				expectResp() // end of chunked body: next bytes start a new response
			}
			if _, werr := client.Write(chunk); werr != nil {
				return
			}
			if !inHeaders && bytes.HasSuffix(chunk, []byte("0\r\n\r\n")) {
				expectResp()
			}
		}
		if err != nil {
			if !stalled {
				client.Close()
			}
			return
		}
	}
}

func nft(script string) error {
	cmd := exec.Command("sudo", "-n", "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nft: %w: %s", err, out)
	}
	return nil
}

func nftDrop(port int) error {
	return nft(fmt.Sprintf(`add table inet chaoslab
add chain inet chaoslab t%[1]d { type filter hook output priority 0; }
add rule inet chaoslab t%[1]d oifname lo tcp dport %[1]d drop
add rule inet chaoslab t%[1]d oifname lo tcp sport %[1]d drop
`, port))
}

func nftHeal(port int) {
	_ = nft(fmt.Sprintf("flush chain inet chaoslab t%[1]d\ndelete chain inet chaoslab t%[1]d\n", port))
}

// noKeepAlive makes the client dial without TCP keep-alive probes — what you
// effectively get behind anything that answers probes on the server's behalf.
var noKeepAlive bool

// fix enables chfix (idle read/write deadlines via Options.DialContext).
var fix bool

func chOptions(port int) *clickhouse.Options {
	o := baseOptions(port)
	if fix {
		chfix.Apply(o)
		return o
	}
	if noKeepAlive {
		d := &net.Dialer{Timeout: o.DialTimeout, KeepAlive: -1}
		o.DialContext = func(ctx context.Context, addr string) (net.Conn, error) {
			return d.DialContext(ctx, "tcp", addr)
		}
	}
	return o
}

func baseOptions(port int) *clickhouse.Options {
	p := clickhouse.HTTP
	if nativeProto {
		p = clickhouse.Native
	}
	return &clickhouse.Options{
		Addr:         []string{fmt.Sprintf("127.0.0.1:%d", port)},
		Protocol:     p,
		Auth:         clickhouse.Auth{Database: "default", Username: "default"},
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
}

const noopDDL = "CREATE TABLE IF NOT EXISTS chaos.sink (s String) ENGINE = Null" // already exists: nothing to do

// ops returns the trivial operation under test, bound to the chosen API.
func ops(api, op string, port int) (do, ping func(ctx context.Context) error, err error) {
	switch api {
	case "native":
		conn, err := clickhouse.Open(chOptions(port))
		if err != nil {
			return nil, nil, err
		}
		if op == "exec" {
			return func(ctx context.Context) error { return conn.Exec(ctx, noopDDL) }, conn.Ping, nil
		}
		if nativeProto { // Ping is a protocol-level packet there; run a real query instead
			sel := func(ctx context.Context) error {
				var one uint8
				return conn.QueryRow(ctx, "SELECT 1").Scan(&one)
			}
			return sel, sel, nil
		}
		return conn.Ping, conn.Ping, nil
	case "std":
		db := clickhouse.OpenDB(chOptions(port))
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		sel := func(ctx context.Context) error {
			var one uint8
			return db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		}
		if op == "exec" {
			return func(ctx context.Context) error { _, err := db.ExecContext(ctx, noopDDL); return err }, sel, nil
		}
		return sel, sel, nil
	}
	return nil, nil, fmt.Errorf("unknown api %q", api)
}

type hangResult struct {
	Proto     string  `json:"proto"`
	Mode      string  `json:"mode"`
	Op        string  `json:"op"`
	API       string  `json:"api"`
	Elapsed   float64 `json:"elapsed_s"`
	Hung      bool    `json:"hung"`
	Cap       float64 `json:"cap_s"`
	Err       string  `json:"err"`
	BlockedIn string  `json:"blocked_in,omitempty"`
	Second    string  `json:"second_query,omitempty"` // a second SELECT 1 on the same pool of one
}

func stackLabel() string {
	buf := make([]byte, 4<<20)
	s := string(buf[:runtime.Stack(buf, true)])
	var out []string
	for needle, label := range map[string]string{
		"net/http.(*bodyEOFSignal).Read":    "reading HTTP response body",
		"net/http.(*persistConn).roundTrip": "http.Transport.RoundTrip",
		"database/sql.(*DB).conn":           "database/sql pool wait (no timeout)",
	} {
		if strings.Contains(s, needle) {
			out = append(out, label)
		}
	}
	sort.Strings(out)
	return strings.Join(out, "; ")
}

func cmdHang(args []string) error {
	fs := flag.NewFlagSet("hang", flag.ExitOnError)
	mode := fs.String("mode", "stall", "stall|blackhole")
	op := fs.String("op", "select1", "select1|exec")
	api := fs.String("api", "native", "native|std")
	port := fs.Int("port", 28680, "")
	capD := fs.Duration("cap", 25*time.Minute, "")
	fs.BoolVar(&noKeepAlive, "no-keepalive", false, "dial without TCP keep-alive")
	fs.BoolVar(&fix, "fix", false, "apply chfix")
	fs.BoolVar(&nativeProto, "native-proto", false, "native TCP protocol instead of HTTP")
	_ = fs.Parse(args)

	if err := ensurePlainProxy(); err != nil {
		return err
	}
	m, err := newMiddlebox(*port, *mode)
	if err != nil {
		return err
	}
	if *mode == "blackhole" {
		defer nftHeal(*port)
	}
	do, ping, err := ops(*api, *op, *port)
	if err != nil {
		return err
	}
	res := hangResult{Proto: map[bool]string{false: "http", true: "native"}[nativeProto], Mode: *mode, Op: *op, API: *api, Cap: capD.Seconds()}
	for range 3 { // warm keep-alive connection, healthy baseline
		if err := do(context.Background()); err != nil {
			return fmt.Errorf("warm-up: %w", err)
		}
	}

	m.armed.Store(true)
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- do(context.Background()) }() // prod: no deadline

	// 5s into the hang: what does another SELECT 1 (no deadline) on the same pool of one see?
	second := make(chan string, 1)
	go func() {
		time.Sleep(5 * time.Second)
		t := time.Now()
		pd := make(chan error, 1)
		go func() { pd <- ping(context.Background()) }()
		select {
		case err := <-pd:
			second <- fmt.Sprintf("returned after %.1fs: %v", time.Since(t).Seconds(), err)
		case <-time.After(20 * time.Second):
			second <- "ALSO HUNG (>20s): " + stackLabel()
		}
	}()

	select {
	case err := <-done:
		res.Elapsed = time.Since(start).Seconds()
		if err != nil {
			res.Err = err.Error()
		}
	case <-time.After(*capD):
		res.Hung, res.Elapsed = true, time.Since(start).Seconds()
		res.BlockedIn = stackLabel()
	}
	select {
	case res.Second = <-second:
	case <-time.After(30 * time.Second):
	}
	return json.NewEncoder(os.Stdout).Encode(res)
}

// cmdSplit measures how often headers and body of a trivial response arrive in
// separate TCP reads — the natural window in which a lost connection leaves the
// client with headers but no body.
func cmdSplit(args []string) error {
	fs := flag.NewFlagSet("split", flag.ExitOnError)
	n := fs.Int("n", 3000, "")
	port := fs.Int("port", 28681, "")
	_ = fs.Parse(args)
	if err := ensurePlainProxy(); err != nil {
		return err
	}
	m, err := newMiddlebox(*port, "stall")
	if err != nil {
		return err
	}
	out := map[string]any{}
	for _, op := range []string{"select1", "exec"} {
		m.mu.Lock()
		m.total, m.separate, m.gaps = 0, 0, nil
		m.mu.Unlock()
		do, _, err := ops("native", op, *port)
		if err != nil {
			return err
		}
		for range *n {
			if err := do(context.Background()); err != nil {
				return err
			}
		}
		m.mu.Lock()
		gaps := append([]time.Duration(nil), m.gaps...)
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		st := map[string]any{"responses": m.total, "headers_in_separate_read": m.separate}
		if len(gaps) > 0 {
			st["gap_p50"] = gaps[len(gaps)/2].String()
			st["gap_p99"] = gaps[len(gaps)*99/100].String()
			st["gap_max"] = gaps[len(gaps)-1].String()
		}
		m.mu.Unlock()
		out[op] = st
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: trivial hang|split [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "hang":
		err = cmdHang(os.Args[2:])
	case "split":
		err = cmdSplit(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
