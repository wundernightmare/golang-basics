// chmatrix — chaos matrix for the hypothesis:
//
//	"clickhouse-go in HTTP mode can hang far longer than Options.ReadTimeout
//	 when the connection breaks".
//
// Every case runs in its own process against its own Toxiproxy listener, so
// cases are isolated and can run in parallel. A case = operation x fault x
// client API (native | database/sql) x context mode (no deadline | deadline).
//
//	chmatrix matrix [-cap 45s] [-par 12] [-faults a,b] [-ops a,b] [-out dir]
//	chmatrix case -op … -fault … -api … -ctx … -port … -cap …
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"chaoslab/chfix"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	readTimeout = 2 * time.Second // the timeout under test
	dialTimeout = 2 * time.Second
	ctxDeadline = 5 * time.Second // used by -ctx deadline
	healWait    = 15 * time.Second

	streamSQL      = "SELECT number, randomString(1024) FROM system.numbers LIMIT 3000000" // ~3 GiB body
	streamTrigger  = 20000                                                                 // rows read before the fault is injected
	slowHeadersSQL = "SELECT sleep(3)"                                                     // 3s before the first byte: ReadTimeout must fire
	// first block immediately, then 3s of silence before each of the next two blocks
	slowBodySQL = "SELECT if(number = 0, 0, sleepEachRow(3)) FROM numbers(3) SETTINGS max_block_size = 1"
	insertRows  = 48000 // x 1 KiB = ~48 MiB request body
)

type result struct {
	Proto     string  `json:"proto"`
	Op        string  `json:"op"`
	Fault     string  `json:"fault"`
	API       string  `json:"api"`
	Ctx       string  `json:"ctx"`
	Elapsed   float64 `json:"elapsed_s"` // from fault injection (or op start) to op return
	Hung      bool    `json:"hung"`      // still blocked when the observation cap expired
	Cap       float64 `json:"cap_s"`
	Err       string  `json:"err"`
	BlockedIn string  `json:"blocked_in,omitempty"`
	Healed    string  `json:"after_heal,omitempty"` // what happened once the fault was removed
	SetupErr  string  `json:"setup_err,omitempty"`
}

// proto selects the wire protocol under test: "http" (8123) or "native" (TCP 9000).
var proto = "http"

// fix enables chfix: per-operation read/write deadlines via Options.DialContext.
var fix bool

func options(port int) *clickhouse.Options {
	p := clickhouse.HTTP
	if proto == "native" {
		p = clickhouse.Native
	}
	o := &clickhouse.Options{
		Addr:         []string{fmt.Sprintf("127.0.0.1:%d", port)},
		Protocol:     p,
		Auth:         clickhouse.Auth{Database: "default", Username: "default"},
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
	if fix {
		chfix.Apply(o)
	}
	return o
}

// client hides the difference between the native API and database/sql.
type client interface {
	ping(ctx context.Context) error
	// stream reads the whole result set, calling onRow after every row.
	stream(ctx context.Context, query string, onRow func(n int) error) error
	// insertPrepare buffers rows client-side; the returned func sends them.
	insertPrepare(ctx context.Context) (send func() error, err error)
	close()
}

type nativeClient struct{ conn driver.Conn }

func (c *nativeClient) ping(ctx context.Context) error { return c.conn.Ping(ctx) }
func (c *nativeClient) close()                         { _ = c.conn.Close() }

func (c *nativeClient) stream(ctx context.Context, query string, onRow func(int) error) error {
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		if err := onRow(n); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *nativeClient) insertPrepare(ctx context.Context) (func() error, error) {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO chaos.sink (s)")
	if err != nil {
		return nil, err
	}
	payload := strings.Repeat("x", 1024)
	for range insertRows {
		if err := batch.Append(payload); err != nil {
			return nil, err
		}
	}
	return batch.Send, nil
}

type stdClient struct{ db *sql.DB }

func (c *stdClient) ping(ctx context.Context) error { return c.db.PingContext(ctx) }
func (c *stdClient) close()                         { _ = c.db.Close() }

func (c *stdClient) stream(ctx context.Context, query string, onRow func(int) error) error {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		if err := onRow(n); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *stdClient) insertPrepare(ctx context.Context) (func() error, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO chaos.sink (s)")
	if err != nil {
		return nil, err
	}
	payload := strings.Repeat("x", 1024)
	for range insertRows {
		if _, err := stmt.ExecContext(ctx, payload); err != nil {
			return nil, err
		}
	}
	return tx.Commit, nil
}

func open(api string, port int) (client, error) {
	switch api {
	case "native":
		conn, err := clickhouse.Open(options(port))
		if err != nil {
			return nil, err
		}
		return &nativeClient{conn: conn}, nil
	case "std":
		db := clickhouse.OpenDB(options(port))
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		return &stdClient{db: db}, nil
	}
	return nil, fmt.Errorf("unknown api %q", api)
}

// runOp executes one operation. inject() is called exactly once, at the point
// where the fault should hit; it returns the moment measurement starts.
func runOp(op string, c client, ctxFor func() (context.Context, context.CancelFunc), inject func() error) error {
	switch op {
	case "ping-new": // fault first, then the very first connection + SELECT 1
		if err := inject(); err != nil {
			return err
		}
		ctx, cancel := ctxFor()
		defer cancel()
		return c.ping(ctx)
	case "ping-idle": // warm keep-alive connection, fault, SELECT 1
		if err := c.ping(context.Background()); err != nil {
			return fmt.Errorf("warm-up: %w", err)
		}
		if err := inject(); err != nil {
			return err
		}
		ctx, cancel := ctxFor()
		defer cancel()
		return c.ping(ctx)
	case "select-slow-headers": // server is slow to send the first byte
		if err := inject(); err != nil {
			return err
		}
		ctx, cancel := ctxFor()
		defer cancel()
		return c.stream(ctx, slowHeadersSQL, func(int) error { return nil })
	case "select-slow-body": // headers + first block fast, then gaps longer than ReadTimeout
		if err := inject(); err != nil {
			return err
		}
		ctx, cancel := ctxFor()
		defer cancel()
		return c.stream(ctx, slowBodySQL, func(int) error { return nil })
	case "select-stream": // fault while the response body is being streamed
		ctx, cancel := ctxFor()
		defer cancel()
		return c.stream(ctx, streamSQL, func(n int) error {
			if n == streamTrigger {
				return inject()
			}
			return nil
		})
	case "insert-big": // fault right before a large request body is written
		ctx, cancel := ctxFor()
		defer cancel()
		send, err := c.insertPrepare(ctx)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		if err := inject(); err != nil {
			return err
		}
		return send()
	}
	return fmt.Errorf("unknown op %q", op)
}

var frameRe = regexp.MustCompile(`(?m)^([^\s].*)\(.*\)$`)

// blockedIn finds the goroutine running the op and summarises where it (and
// the HTTP transport serving it) is parked.
func blockedIn() (label, full string) {
	buf := make([]byte, 4<<20)
	buf = buf[:runtime.Stack(buf, true)]
	full = string(buf)
	interesting := []struct{ needle, label string }{
		{"net/http.(*persistConn).writeLoop", ""}, // refined below
		{"net/http.(*bodyEOFSignal).Read", "reading HTTP response body (not covered by ReadTimeout)"},
		{"net/http.(*persistConn).roundTrip", "waiting inside http.Transport.RoundTrip"},
		{"database/sql.(*DB).conn", "waiting for database/sql pool"},
	}
	var labels []string
	for _, g := range strings.Split(full, "\n\n") {
		if strings.Contains(g, "net/http.(*persistConn).writeLoop") && strings.Contains(g, "net.(*conn).Write") {
			labels = append(labels, "writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet)")
		}
		if strings.Contains(g, "net/http.(*bodyEOFSignal).Read") && !strings.Contains(g, "main.runOp") {
			labels = append(labels, "reading HTTP response body (not covered by ReadTimeout)")
		}
		if !strings.Contains(g, "main.runOp") {
			continue
		}
		before := len(labels)
		if strings.Contains(g, "internal/poll.(*FD).Write") {
			labels = append(labels, "socket Write blocked: clickhouse-go sets a read deadline only, no write deadline")
		}
		for _, it := range interesting[1:] {
			if strings.Contains(g, it.needle) {
				labels = append(labels, it.label)
				break
			}
		}
		if len(labels) == before && before == 0 {
			fr := frameRe.FindAllStringSubmatch(g, 4)
			var names []string
			for _, f := range fr {
				names = append(names, f[1])
			}
			labels = append(labels, strings.Join(names, " <- "))
		}
	}
	sort.Strings(labels)
	return strings.Join(labels, "; "), full
}

func runCase(args []string) error {
	fs := flag.NewFlagSet("case", flag.ExitOnError)
	op := fs.String("op", "", "")
	faultName := fs.String("fault", "", "")
	api := fs.String("api", "native", "")
	ctxMode := fs.String("ctx", "none", "none|deadline")
	port := fs.Int("port", 0, "")
	capD := fs.Duration("cap", 45*time.Second, "")
	stackDir := fs.String("stacks", "", "")
	fs.StringVar(&proto, "proto", "http", "http|native")
	fs.BoolVar(&fix, "fix", false, "apply chfix")
	_ = fs.Parse(args)

	res := result{Proto: proto, Op: *op, Fault: *faultName, API: *api, Ctx: *ctxMode, Cap: capD.Seconds()}
	emit := func() error { return json.NewEncoder(os.Stdout).Encode(res) }

	f, ok := faults[*faultName]
	if !ok {
		return fmt.Errorf("unknown fault %q", *faultName)
	}
	if err := createProxy(*port); err != nil {
		res.SetupErr = err.Error()
		return emit()
	}
	defer deleteProxy(*port)

	c, err := open(*api, *port)
	if err != nil {
		res.SetupErr = err.Error()
		return emit()
	}

	ctxFor := func() (context.Context, context.CancelFunc) {
		if *ctxMode == "deadline" {
			return context.WithTimeout(context.Background(), ctxDeadline)
		}
		return context.Background(), func() {} // prod: "no timeouts"
	}

	var (
		mu       sync.Mutex
		t0       time.Time
		injected bool
	)
	inject := func() error {
		err := f.inject(*port)
		mu.Lock()
		t0, injected = time.Now(), true
		mu.Unlock()
		return err
	}
	healedOnce := sync.OnceValue(func() error { return f.heal(*port) })
	defer func() {
		mu.Lock()
		inj := injected
		mu.Unlock()
		if inj {
			_ = healedOnce()
		}
	}()

	done := make(chan error, 1)
	go func() { done <- runOp(*op, c, ctxFor, inject) }()

	// The cap counts from fault injection, not from process start.
	capTimer := time.NewTimer(time.Hour)
	go func() {
		for {
			mu.Lock()
			inj, start := injected, t0
			mu.Unlock()
			if inj {
				capTimer.Reset(time.Until(start.Add(*capD)))
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	since := func() float64 {
		mu.Lock()
		defer mu.Unlock()
		return time.Since(t0).Seconds()
	}

	select {
	case err := <-done:
		res.Elapsed = since()
		if err != nil {
			res.Err = err.Error()
		}
	case <-capTimer.C:
		res.Hung, res.Elapsed = true, since()
		label, full := blockedIn()
		res.BlockedIn = label
		if *stackDir != "" {
			name := fmt.Sprintf("%s__%s__%s__%s.txt", *op, *faultName, *api, *ctxMode)
			_ = os.WriteFile(filepath.Join(*stackDir, name), []byte(full), 0o644)
		}
		// Does it come back once the network is healthy again?
		healAt := time.Now()
		if err := healedOnce(); err != nil {
			res.Healed = "heal failed: " + err.Error()
			break
		}
		select {
		case err := <-done:
			res.Healed = fmt.Sprintf("returned %.1fs after heal: %v", time.Since(healAt).Seconds(), err)
		case <-time.After(healWait):
			res.Healed = fmt.Sprintf("STILL HUNG %.0fs after the fault was removed", healWait.Seconds())
		}
	}
	return emit()
}

type caseSpec struct {
	op, fault, api, ctx string
	cap                 time.Duration
}

func runMatrix(args []string) error {
	fs := flag.NewFlagSet("matrix", flag.ExitOnError)
	capD := fs.Duration("cap", 45*time.Second, "observation cap per case (from fault injection)")
	par := fs.Int("par", 12, "parallel cases")
	faultsF := fs.String("faults", "toxi-timeout,toxi-timeout-down,toxi-bandwidth,toxi-reset,toxi-down,nft-drop,nft-drop-in,ch-pause", "")
	opsF := fs.String("ops", "ping-new,ping-idle,select-stream,insert-big", "")
	apisF := fs.String("apis", "native,std", "")
	ctxF := fs.String("ctx", "none,deadline", "")
	out := fs.String("out", "out", "output dir")
	basePort := fs.Int("base-port", 28100, "")
	controls := fs.Bool("controls", true, "also run fault=none control cases")
	fs.StringVar(&proto, "proto", "http", "wire protocol: http|native")
	fs.BoolVar(&fix, "fix", false, "apply chfix (idle read/write deadlines via Options.DialContext)")
	_ = fs.Parse(args)

	stackDir := filepath.Join(*out, "stacks")
	if err := os.MkdirAll(stackDir, 0o755); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}

	var parallel, serial []caseSpec
	for _, api := range strings.Split(*apisF, ",") {
		for _, ctx := range strings.Split(*ctxF, ",") {
			if *controls {
				for _, op := range []string{"ping-new", "ping-idle", "select-slow-headers", "select-slow-body", "insert-big"} {
					parallel = append(parallel, caseSpec{op, "none", api, ctx, *capD})
				}
			}
			for _, fl := range strings.Split(*faultsF, ",") {
				for _, op := range strings.Split(*opsF, ",") {
					cs := caseSpec{op, fl, api, ctx, *capD}
					if fl == "ch-pause" { // freezes the shared ClickHouse: must not overlap other cases
						cs.cap = min(*capD, 30*time.Second)
						serial = append(serial, cs)
					} else {
						parallel = append(parallel, cs)
					}
				}
			}
		}
	}

	var (
		mu      sync.Mutex
		results []result
		nextP   = *basePort
	)
	runOne := func(cs caseSpec) {
		mu.Lock()
		port := nextP
		nextP++
		mu.Unlock()
		cmd := exec.Command(self, "case", "-op", cs.op, "-fault", cs.fault, "-api", cs.api, "-ctx", cs.ctx,
			"-port", fmt.Sprint(port), "-cap", cs.cap.String(), "-stacks", stackDir, "-proto", proto, fmt.Sprintf("-fix=%v", fix))
		cmd.Stderr = os.Stderr
		outB, err := cmd.Output()
		r := result{Op: cs.op, Fault: cs.fault, API: cs.api, Ctx: cs.ctx}
		if err != nil {
			r.SetupErr = "case process: " + err.Error()
		} else if err := json.Unmarshal(outB, &r); err != nil {
			r.SetupErr = "bad case output: " + string(outB)
		}
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
		fmt.Fprintf(os.Stderr, "done %-20s %-18s %-6s %-8s hung=%-5v %.1fs %s%s\n", r.Op, r.Fault, r.API, r.Ctx, r.Hung, r.Elapsed, r.SetupErr, trunc(r.Err, 90))
	}

	sem := make(chan struct{}, *par)
	var wg sync.WaitGroup
	for _, cs := range parallel {
		wg.Add(1)
		sem <- struct{}{}
		go func() { defer wg.Done(); defer func() { <-sem }(); runOne(cs) }()
	}
	wg.Wait()
	for _, cs := range serial {
		runOne(cs)
	}

	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		ka := []string{a.Ctx, a.Fault, a.Op, a.API}
		kb := []string{b.Ctx, b.Fault, b.Op, b.API}
		for i := range ka {
			if ka[i] != kb[i] {
				if i == 0 {
					return ka[i] > kb[i] // "none" before "deadline"
				}
				return ka[i] < kb[i]
			}
		}
		return false
	})
	jf, err := os.Create(filepath.Join(*out, "results.jsonl"))
	if err != nil {
		return err
	}
	defer jf.Close()
	enc := json.NewEncoder(jf)
	var md strings.Builder
	fmt.Fprintf(&md, "protocol=%s fix=%v ReadTimeout=%s DialTimeout=%s, ctx deadline (when used)=%s, pool=1\n\n", proto, fix, readTimeout, dialTimeout, ctxDeadline)
	md.WriteString("| ctx | fault | op | api | result | x ReadTimeout | error / where it is blocked | after heal |\n|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		_ = enc.Encode(r)
		verdict := fmt.Sprintf("%.1fs", r.Elapsed)
		detail := trunc(r.Err, 140)
		if r.Hung {
			verdict = fmt.Sprintf("**HUNG >%.0fs**", r.Cap)
			detail = r.BlockedIn
		}
		if r.SetupErr != "" {
			verdict, detail = "SETUP ERROR", r.SetupErr
		}
		if detail == "" {
			detail = "ok (no error)"
		}
		fmt.Fprintf(&md, "| %s | %s | %s | %s | %s | %.0fx | %s | %s |\n", r.Ctx, r.Fault, r.Op, r.API, verdict,
			r.Elapsed/readTimeout.Seconds(), strings.ReplaceAll(detail, "|", "\\|"), r.Healed)
	}
	if err := os.WriteFile(filepath.Join(*out, "results.md"), []byte(md.String()), 0o644); err != nil {
		return err
	}
	fmt.Print(md.String())
	return nil
}

func trunc(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: chmatrix matrix|case [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "matrix":
		err = runMatrix(os.Args[2:])
	case "case":
		err = runCase(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
