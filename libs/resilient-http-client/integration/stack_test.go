package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// Images of the suite's own fixtures (Postgres and Toxiproxy come from
// libs/testx/containers). testcontainers prefixes them with
// TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX in a closed network.
const (
	caddyImage    = "caddy:2.10.2"
	redpandaImage = "redpandadata/redpanda:v26.2.3"
)

// Ports inside the edge (Caddy) container.
const (
	edgePlain   = "8080/tcp" // cleartext: HTTP/1.1 and h2c
	edgeTLS     = "8443/tcp" // TLS: HTTP/1.1 and HTTP/2
	edgeQUIC    = "8443/udp" // HTTP/3
	edgeTLS13   = "8444/tcp" // TLS 1.3 only
	edgeMTLS    = "8445/tcp" // client certificate required
	edgeExpired = "8446/tcp" // serves an expired certificate
)

// Ports inside the Redpanda container.
const (
	pandaProxy = "8082/tcp" // HTTP Proxy (REST produce / fetch)
	pandaAdmin = "9644/tcp" // admin API
)

// blobSize is the size of /files/blob: large enough to outlast a socket
// buffer, so a cut or a throttle lands in the middle of the body.
const blobSize = 4 << 20

// edge is where the suite reaches Caddy, as "host:port".
type edge struct {
	Plain, TLS, QUIC, TLS13, MTLS, Expired string
}

// panda is where the suite reaches Redpanda's HTTP APIs, as "host:port".
type panda struct {
	Proxy, Admin string
}

// redpanda starts the suite's Redpanda: a real, stateful HTTP API (the HTTP
// Proxy) whose answers show what the client really sent — how many times a
// produce request arrived is how many records the topic holds.
func redpanda(tb testing.TB) panda {
	tb.Helper()

	return containers.Fixture(tb, "redpanda", func(ctx context.Context) (panda, testcontainers.Container, error) {
		ctr, err := testcontainers.Run(ctx, redpandaImage,
			testcontainers.WithExposedPorts(pandaProxy, pandaAdmin),
			testcontainers.WithCmd("redpanda", "start",
				"--mode", "dev-container", "--smp", "1", "--memory", "1G", "--overprovisioned",
				"--kafka-addr", "0.0.0.0:9092", "--advertise-kafka-addr", "127.0.0.1:9092",
				"--pandaproxy-addr", "0.0.0.0:8082", "--advertise-pandaproxy-addr", "127.0.0.1:8082",
				"--rpc-addr", "0.0.0.0:33145", "--advertise-rpc-addr", "127.0.0.1:33145"),
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForHTTP("/v1/status/ready").WithPort(pandaAdmin),
				wait.ForHTTP("/brokers").WithPort(pandaProxy),
			).WithStartupTimeoutDefault(90*time.Second)))
		if err != nil {
			return panda{}, ctr, err
		}

		return panda{
			Proxy: "localhost:" + containers.HostPort(tb, ctr, pandaProxy),
			Admin: "localhost:" + containers.HostPort(tb, ctr, pandaAdmin),
		}, ctr, nil
	})
}

// caddyfile is the edge's configuration. One site per listener, all serving
// the same routes:
//
//	/whoami              what the server saw: protocol, TLS version, the client's port
//	/status/<code>       that status
//	/busy                503 with Retry-After: 1
//	/echo                the request body back; method and Idempotency-Key in headers
//	/files/blob          a 4 MiB file
//	/redirect/…          same-host, loop, cross-host, https→http
//	/panda/…             reverse proxy to Redpanda's HTTP Proxy
func caddyfile(pandaUpstream string) string {
	var routes strings.Builder

	for _, code := range []int{200, 204, 400, 404, 409, 500, 501, 502, 503, 504} {
		fmt.Fprintf(&routes, "\thandle /status/%d {\n\t\trespond \"status %d\" %d\n\t}\n", code, code, code)
	}

	return `{
	admin off
	auto_https off
	default_sni localhost
	servers :8080 {
		protocols h1 h2c
	}
	servers :8443 {
		protocols h1 h2 h3
	}
}

(app) {
	request_body {
		max_size 32MB
	}
	handle /whoami {
		respond "proto={http.request.proto} tls={http.request.tls.version} resumed={http.request.tls.resumed} port={http.request.remote.port}"
	}
` + routes.String() + `	handle /status/429 {
		header Retry-After 1
		respond "slow down" 429
	}
	handle /busy {
		header Retry-After 1
		respond "busy" 503
	}
	handle /echo {
		header X-Echo-Method {http.request.method}
		header X-Echo-Idempotency-Key {http.request.header.Idempotency-Key}
		respond "{http.request.body}"
	}
	handle_path /files/* {
		root * /srv
		file_server
	}
	handle /redirect/same {
		redir * /whoami 302
	}
	handle /redirect/loop {
		redir * /redirect/loop 302
	}
	handle /redirect/cross {
		redir * http://elsewhere.invalid/whoami 302
	}
	handle /redirect/downgrade {
		redir * http://{http.request.hostport}/whoami 302
	}
	handle_path /panda/* {
		reverse_proxy ` + pandaUpstream + ` {
			flush_interval -1
		}
	}
	handle {
		respond "no such route" 404
	}
}

:8080 {
	import app
}

:8443 {
	tls /certs/server.crt /certs/server.key
	import app
}

:8444 {
	tls /certs/server.crt /certs/server.key {
		protocols tls1.3
	}
	import app
}

:8445 {
	tls /certs/server.crt /certs/server.key {
		client_auth {
			mode require_and_verify
			trust_pool file /certs/ca.crt
		}
	}
	import app
}

:8446 {
	tls /certs/expired.crt /certs/expired.key
	import app
}
`
}

// web starts the suite's edge: Caddy, a production HTTP server, speaking
// HTTP/1.1, h2c, HTTP/2 and HTTP/3 with the suite's certificates.
func web(tb testing.TB) edge {
	tb.Helper()

	rp := redpanda(tb)
	_, pandaPort, _ := strings.Cut(rp.Proxy, ":")

	return containers.Fixture(tb, "edge", func(ctx context.Context) (edge, testcontainers.Container, error) {
		file := func(path string, content []byte) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader(content), ContainerFilePath: path, FileMode: 0o644}
		}

		blob := bytes.Repeat([]byte("0123456789abcdef"), blobSize/16)

		ctr, err := testcontainers.Run(ctx, caddyImage,
			testcontainers.WithExposedPorts(edgePlain, edgeTLS, edgeQUIC, edgeTLS13, edgeMTLS, edgeExpired),
			testcontainers.WithFiles(
				file("/etc/caddy/Caddyfile", []byte(caddyfile("host.docker.internal:"+pandaPort))),
				file("/certs/ca.crt", certs.caPEM),
				file("/certs/server.crt", certs.serverCert),
				file("/certs/server.key", certs.serverKey),
				file("/certs/expired.crt", certs.expiredCert),
				file("/certs/expired.key", certs.expiredKey),
				file("/srv/blob", blob),
			),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				// Linux needs the alias; Docker Desktop / OrbStack resolve it anyway.
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			}),
			testcontainers.WithWaitStrategy(wait.ForHTTP("/whoami").WithPort(edgePlain).
				WithStartupTimeout(60*time.Second)))
		if err != nil {
			return edge{}, ctr, err
		}

		at := func(port string) string { return "localhost:" + containers.HostPort(tb, ctr, port) }

		return edge{
			Plain: at(edgePlain), TLS: at(edgeTLS), QUIC: at(edgeQUIC),
			TLS13: at(edgeTLS13), MTLS: at(edgeMTLS), Expired: at(edgeExpired),
		}, ctr, nil
	})
}

// Media types of Redpanda's HTTP Proxy (the Kafka REST v2 dialect).
const (
	kafkaJSON = "application/vnd.kafka.json.v2+json" // a produce body, a fetch answer
	kafkaV2   = "application/vnd.kafka.v2+json"      // a produce answer
)

// topic creates an empty single-partition topic of its own for the test.
func topic(tb testing.TB) string {
	tb.Helper()

	redpanda(tb)

	name := testx.Unique("it")
	code, out, err := containers.FixtureContainer(tb, "redpanda").Exec(tb.Context(), []string{"rpk", "topic", "create", name})
	require.NoError(tb, err)

	logs, _ := io.ReadAll(out)
	require.Zerof(tb, code, "rpk topic create %s: %s", name, logs)

	return name
}

// produce builds the request that appends one record to topic: a POST, the
// textbook non-idempotent call — sent twice, it is stored twice.
func produce(tb testing.TB, ctx context.Context, base, topic, value string) *http.Request {
	tb.Helper()

	req := request(tb, ctx, http.MethodPost, base+"/topics/"+topic,
		strings.NewReader(`{"records":[{"value":"`+value+`"}]}`))
	req.Header.Set("Content-Type", kafkaJSON)
	req.Header.Set("Accept", kafkaV2)

	return req
}

// poll builds a fetch of topic from its first record that the server holds
// open for wait when there is nothing new at offset: a real server taking its
// time, for every protocol the edge fronts it with.
func poll(tb testing.TB, ctx context.Context, base, topic string, offset int, wait time.Duration) *http.Request {
	tb.Helper()

	req := request(tb, ctx, http.MethodGet, fmt.Sprintf("%s/topics/%s/partitions/0/records?offset=%d&timeout=%d&max_bytes=1048576",
		base, topic, offset, wait.Milliseconds()), nil)
	req.Header.Set("Accept", kafkaJSON)

	return req
}

// stored returns the values the topic holds, read through a plain client on
// a direct connection: the ground truth the tests compare the client's view
// with.
func stored(tb testing.TB, topic string) []string {
	tb.Helper()

	rp := redpanda(tb)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resp, err := http.DefaultClient.Do(poll(tb, ctx, "http://"+rp.Proxy, topic, 0, 300*time.Millisecond))
	require.NoError(tb, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(tb, http.StatusOK, resp.StatusCode)

	var records []struct {
		Value string `json:"value"`
	}

	require.NoError(tb, json.NewDecoder(resp.Body).Decode(&records))

	values := make([]string, 0, len(records))
	for _, r := range records {
		values = append(values, r.Value)
	}

	return values
}
