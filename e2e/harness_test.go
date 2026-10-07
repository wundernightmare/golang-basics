//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The harness spawns one real service binary per call to start: free
// loopback ports chosen here, output captured and dumped when the test
// fails, readiness and identity (/version) checked before the test body
// runs, and a SIGTERM + wait on teardown so a cover-built binary flushes its
// counters to GOCOVERDIR.

const (
	readyTimeout = 30 * time.Second // /readyz 200 after spawn (tasks runs migrations first)
	stopTimeout  = 15 * time.Second // SIGTERM → exit, then SIGKILL
)

// client is shared by every request the suite makes; the per-request
// timeout keeps a wedged child from hanging the run.
var client = &http.Client{Timeout: 5 * time.Second}

// service is one running child process.
type service struct {
	name     string
	apiURL   string // "" for a worker (no API listener)
	adminURL string
	token    string // the ADMIN_TOKEN it was started with

	cmd  *exec.Cmd
	out  *syncBuffer
	done chan struct{} // closed once the process has exited
	err  error         // cmd.Wait's result, valid after done is closed
}

// spec describes how to start a service.
type spec struct {
	name   string            // binary name and /version service name
	prefix string            // env prefix, e.g. "PING_"
	api    bool              // has an API listener (HTTP_ADDR)
	env    map[string]string // extra env, keys without the prefix
}

// start spawns the service described by sp and returns it once /readyz on
// its admin port answers 200 and /version names it. The process is stopped
// (SIGTERM, wait, SIGKILL past the deadline) in t.Cleanup, and its output is
// logged when the test failed.
func start(t *testing.T, sp spec) *service {
	t.Helper()

	bin := filepath.Join(binDir(t), sp.name)
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("service binary %s: %v — build it first: scripts/build-service.sh %s %s (set E2E_BIN_DIR for another dir)",
			bin, err, sp.name, bin)
	}

	s := &service{name: sp.name, token: randomToken(t), out: &syncBuffer{}, done: make(chan struct{})}
	env := map[string]string{
		"ADMIN_ADDR":          freeAddr(t),
		"ADMIN_TOKEN":         s.token,
		"HTTP_SHUTDOWN_DELAY": "0s",
		"LOG_FORMAT":          "json",
		"LOG_LEVEL":           "info",
	}
	s.adminURL = "http://" + env["ADMIN_ADDR"]
	if sp.api {
		env["HTTP_ADDR"] = freeAddr(t)
		s.apiURL = "http://" + env["HTTP_ADDR"]
	}
	for k, v := range sp.env {
		env[k] = v
	}

	s.cmd = exec.Command(bin) //nolint:gosec // the binary path is the harness's own build output
	s.cmd.Env = childEnv(sp.prefix, env)
	s.cmd.Stdout = s.out
	s.cmd.Stderr = s.out
	require.NoError(t, s.cmd.Start(), "start %s", sp.name)
	go func() {
		s.err = s.cmd.Wait()
		close(s.done)
	}()

	t.Cleanup(func() {
		s.stop(t)
		if t.Failed() {
			t.Logf("── %s output ──\n%s", s.name, s.out.String())
		}
	})

	s.waitReady(t)
	s.checkVersion(t)
	return s
}

// waitReady polls /readyz until it answers 200, failing early (with the
// child's output) when the process exits first.
func (s *service) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for {
		select {
		case <-s.done:
			t.Fatalf("%s exited before becoming ready (%v); its output follows", s.name, s.err)
		default:
		}
		if code, _, err := s.get(s.adminURL+"/readyz", ""); err == nil && code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: /readyz not 200 within %s", s.name, readyTimeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Ready and still running: a child that raced to exit right after the
	// probe would otherwise surface as a confusing connection error later.
	select {
	case <-s.done:
		t.Fatalf("%s exited right after becoming ready (%v)", s.name, s.err)
	default:
	}
}

// buildInfo is the /version body.
type buildInfo struct {
	Service       string `json:"service"`
	Version       string `json:"version"`
	Revision      string `json:"revision"`
	BuildTime     string `json:"build_time"`
	GoVersion     string `json:"go_version"`
	StartedAt     string `json:"started_at"`
	UptimeSeconds *int64 `json:"uptime_seconds"`
}

// checkVersion asserts the admin port belongs to the process just started —
// never pass against a stray one left on the port.
func (s *service) checkVersion(t *testing.T) {
	t.Helper()
	bi := s.version(t)
	require.Equal(t, s.name, bi.Service, "/version on %s names another service", s.adminURL)
}

func (s *service) version(t *testing.T) buildInfo {
	t.Helper()
	code, body, err := s.get(s.adminURL+"/version", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code, "GET /version: %s", body)
	var bi buildInfo
	require.NoError(t, json.Unmarshal(body, &bi), "GET /version body: %s", body)
	return bi
}

// terminate sends SIGTERM and waits (up to timeout) for the process to exit,
// reporting whether it did.
func (s *service) terminate(timeout time.Duration) bool {
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// stop is the teardown: SIGTERM, wait for the exit (so coverage counters are
// written), SIGKILL past the deadline.
func (s *service) stop(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
		return
	default:
	}
	if s.terminate(stopTimeout) {
		return
	}
	t.Errorf("%s did not exit within %s of SIGTERM; killing it", s.name, stopTimeout)
	_ = s.cmd.Process.Kill()
	<-s.done
}

// exitCode is the child's exit status (valid once done is closed); -1 when
// it was killed by a signal.
func (s *service) exitCode() int {
	return s.cmd.ProcessState.ExitCode()
}

// get issues a GET with an optional bearer token.
func (s *service) get(url, token string) (int, []byte, error) {
	return do(http.MethodGet, url, token, nil)
}

func do(method, url, token string, body io.Reader) (int, []byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, url, body)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// metric sums every sample of the named counter/gauge on the admin /metrics
// (all label sets). A missing metric reads as 0.
func (s *service) metric(t *testing.T, name string) float64 {
	t.Helper()
	code, body, err := s.get(s.adminURL+"/metrics", "")
	if err != nil || code != http.StatusOK {
		t.Logf("%s /metrics: %d %v", s.name, code, err)
		return 0
	}
	var sum float64
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		rest, ok := strings.CutPrefix(line, name)
		if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '{') {
			continue
		}
		// `name{labels} value [timestamp]` or `name value [timestamp]`.
		if i := strings.LastIndexByte(rest, '}'); i >= 0 {
			rest = rest[i+1:]
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[0], 64); err == nil {
			sum += v
		}
	}
	return sum
}

// logLines decodes the child's JSON log lines (non-JSON output is skipped).
func (s *service) logLines() []map[string]any {
	var lines []map[string]any
	sc := bufio.NewScanner(strings.NewReader(s.out.String()))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) == nil {
			lines = append(lines, m)
		}
	}
	return lines
}

// ── environment ─────────────────────────────────────────────────────────────

// binDir is E2E_BIN_DIR, default ../.build (the workspace root's .build/).
func binDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("E2E_BIN_DIR")
	if dir == "" {
		dir = filepath.Join("..", ".build")
	}
	abs, err := filepath.Abs(dir)
	require.NoError(t, err)
	return abs
}

// childEnv is the parent's environment minus anything already carrying the
// service's prefix (a developer's PING_HTTP_ADDR must not leak in), plus
// the prefixed harness settings, plus GOCOVERDIR when E2E_COVER_DIR is set.
func childEnv(prefix string, env map[string]string) []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, prefix) || strings.HasPrefix(kv, "GOCOVERDIR=") {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range env {
		out = append(out, prefix+k+"="+v)
	}
	if dir := os.Getenv("E2E_COVER_DIR"); dir != "" {
		if abs, err := filepath.Abs(dir); err == nil {
			_ = os.MkdirAll(abs, 0o755)
			out = append(out, "GOCOVERDIR="+abs)
		}
	}
	return out
}

// freeAddr returns a loopback address with a port the kernel just handed
// out. The listener is closed before the child binds it — a tiny window,
// far smaller than the fixed-port collisions it replaces.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func randomToken(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return hex.EncodeToString(b)
}

// dataEnv returns the datastore URLs for tasks/consumer, skipping the test
// when any is unset.
func dataEnv(t *testing.T) (dbURL, valkeyURL, brokers string) {
	t.Helper()
	dbURL, valkeyURL, brokers = os.Getenv("E2E_DATABASE_URL"), os.Getenv("E2E_VALKEY_URL"), os.Getenv("E2E_KAFKA_BROKERS")
	if dbURL == "" || valkeyURL == "" || brokers == "" {
		t.Skip("set E2E_DATABASE_URL, E2E_VALKEY_URL and E2E_KAFKA_BROKERS to run the tasks/consumer suite (just infra-up)")
	}
	return dbURL, valkeyURL, brokers
}

// eventually polls cond every 100ms until it holds or timeout passes.
func eventually(t *testing.T, timeout time.Duration, msg string, cond func(c *assert.CollectT)) {
	t.Helper()
	assert.EventuallyWithT(t, cond, timeout, 100*time.Millisecond, msg)
}

// syncBuffer is a bytes.Buffer safe for the two copy goroutines exec starts
// (stdout, stderr) and the test reading it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
