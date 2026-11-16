package valkey_test

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// fakeServer speaks as much RESP3 as [valkey.Cache] sends: the handshake,
// PING, GET, SET (NX / IFEQ / PX), DEL, PUBLISH and SUBSCRIBE. It exists for
// what a healthy server never does — going silent, refusing a subscription —
// and for the logic of the cache that needs no server semantics beyond a
// map. What Valkey itself answers is the integration suite's
// (libs/valkey/integration).
type fakeServer struct {
	addr     string
	listener net.Listener
	conns    sync.WaitGroup

	// silent: commands are read and never answered — a peer that stopped
	// talking without closing anything.
	silent atomic.Bool
	// refuseSubscribe answers SUBSCRIBE the way an ACL does.
	refuseSubscribe atomic.Bool

	mu     sync.Mutex
	values map[string]string
	ttls   map[string]time.Duration
	open   map[net.Conn]struct{}
}

func newFakeServer(t *testing.T, tlsConfig *tls.Config) *fakeServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}

	s := &fakeServer{
		addr: listener.Addr().String(), listener: listener,
		values: map[string]string{}, ttls: map[string]time.Duration{}, open: map[net.Conn]struct{}{},
	}

	s.conns.Go(s.accept)
	t.Cleanup(s.close)

	return s
}

func (s *fakeServer) close() {
	_ = s.listener.Close()

	s.mu.Lock()
	for conn := range s.open {
		_ = conn.Close()
	}
	s.mu.Unlock()

	s.conns.Wait()
}

func (s *fakeServer) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		s.open[conn] = struct{}{}
		s.mu.Unlock()

		s.conns.Go(func() { s.serve(conn) })
	}
}

func (s *fakeServer) serve(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.open, conn)
		s.mu.Unlock()

		_ = conn.Close()
	}()

	in := bufio.NewReader(conn)

	for {
		cmd, err := readCommand(in)
		if err != nil {
			return
		}

		if s.silent.Load() {
			continue
		}

		if _, err := io.WriteString(conn, s.reply(cmd)); err != nil {
			return
		}
	}
}

// readCommand reads one command: an array of bulk strings.
func readCommand(reader *bufio.Reader) ([]string, error) {
	line := func() (string, error) {
		l, err := reader.ReadString('\n')

		return strings.TrimSuffix(l, "\r\n"), err
	}

	header, err := line()
	if err != nil {
		return nil, err
	}

	n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
	if err != nil {
		return nil, fmt.Errorf("not an array: %q", header)
	}

	cmd := make([]string, n)

	for i := range cmd {
		size, err := line()
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(strings.TrimPrefix(size, "$"))
		if err != nil {
			return nil, fmt.Errorf("not a bulk string: %q", size)
		}

		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}

		cmd[i] = string(buf[:length])
	}

	if n == 0 {
		return nil, errors.New("empty command")
	}

	return cmd, nil
}

const (
	respOK   = "+OK\r\n"
	respNull = "_\r\n"
)

func bulk(v string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v) }

func (s *fakeServer) reply(cmd []string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch strings.ToUpper(cmd[0]) {
	case "HELLO":
		return "%2\r\n" + bulk("version") + bulk("8.1.0") + bulk("proto") + ":3\r\n"
	case "CLIENT", "SELECT":
		return respOK
	case "PING":
		return "+PONG\r\n"
	case "GET":
		v, ok := s.values[cmd[1]]
		if !ok {
			return respNull
		}

		return bulk(v)
	case "SET":
		return s.set(cmd[1], cmd[2], cmd[3:])
	case "DEL":
		n := 0

		for _, key := range cmd[1:] {
			if _, ok := s.values[key]; ok {
				n++
			}

			delete(s.values, key)
			delete(s.ttls, key)
		}

		return fmt.Sprintf(":%d\r\n", n)
	case "PUBLISH":
		return ":0\r\n"
	case "SUBSCRIBE":
		if s.refuseSubscribe.Load() {
			return "-NOPERM this user has no permissions to access one of the channels used as arguments\r\n"
		}

		return ">3\r\n" + bulk("subscribe") + bulk(cmd[1]) + ":1\r\n"
	}

	return fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd[0])
}

func (s *fakeServer) set(key, value string, flags []string) string {
	current, exists := s.values[key]

	var ttl time.Duration

	for i := 0; i < len(flags); i++ {
		switch strings.ToUpper(flags[i]) {
		case "NX":
			if exists {
				return respNull
			}
		case "IFEQ":
			i++

			if !exists || current != flags[i] {
				return respNull
			}
		case "PX":
			i++

			ms, _ := strconv.Atoi(flags[i])
			if ms <= 0 {
				return "-ERR invalid expire time in 'set' command\r\n"
			}

			ttl = time.Duration(ms) * time.Millisecond
		}
	}

	s.values[key] = value
	s.ttls[key] = ttl

	return respOK
}

// goSilent stops answering for the rest of the test. The connections are cut
// before the cache is closed: the library's Close waits a second for the
// replies a silent peer owes it.
func (s *fakeServer) goSilent(t *testing.T) {
	t.Helper()

	s.silent.Store(true)
	t.Cleanup(s.close)
}

// put stores a value the way another client would have.
func (s *fakeServer) put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.values[key] = value
}

func (s *fakeServer) get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.values[key]

	return v, ok
}

func (s *fakeServer) ttl(key string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ttls[key]
}

// fakeCache is a [valkey.Cache] connected to a fake server of its own, with
// its log captured.
func fakeCache(t *testing.T, cfg valkey.Config) (*valkey.Cache, *fakeServer, *testx.LogBuffer) {
	t.Helper()

	server := newFakeServer(t, nil)
	logs := &testx.LogBuffer{}
	cfg.Addr = server.addr

	cache, err := valkey.New(cfg, slog.New(slog.NewJSONHandler(logs, nil)))
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	return cache, server, logs
}
