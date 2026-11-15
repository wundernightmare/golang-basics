package containers

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ToxiproxyImage is the Toxiproxy release the chaos helpers run.
const ToxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"

// Proxy listen ports inside the Toxiproxy container, one per dependency.
var proxyPorts = map[string]int{"postgres": 8666, "valkey": 8667, "s3": 8668}

// Spare listen ports for [ProxyTo]: proxies in front of a package's own
// fixtures, handed out by name in the order they are asked for.
const (
	sparePortFirst = 8680
	sparePortCount = 12
)

var spare struct {
	mu     sync.Mutex
	byName map[string]int
}

var toxi shared[*toxiclient.Client]

// toxiproxy starts the shared Toxiproxy container once. The proxies reach
// their upstreams through the host-mapped ports (host.docker.internal), so no
// Docker network wiring is needed.
func toxiproxy(tb testing.TB) *toxiclient.Client {
	tb.Helper()

	return use(tb, &toxi, "toxiproxy", func(ctx context.Context) (*toxiclient.Client, testcontainers.Container, error) {
		exposed := []string{tctoxiproxy.ControlPort}
		for _, p := range proxyPorts {
			exposed = append(exposed, strconv.Itoa(p)+"/tcp")
		}

		for p := sparePortFirst; p < sparePortFirst+sparePortCount; p++ {
			exposed = append(exposed, strconv.Itoa(p)+"/tcp")
		}

		container, err := tctoxiproxy.Run(ctx, ToxiproxyImage,
			testcontainers.WithExposedPorts(exposed...),
			testcontainers.WithWaitStrategy(wait.ForListeningPort(tctoxiproxy.ControlPort).WithStartupTimeout(60*time.Second)),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				// Linux needs the alias; Docker Desktop / OrbStack resolve it anyway.
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			}),
		)
		if err != nil {
			return nil, container, err // with the container: use removes it
		}

		uri, err := container.URI(ctx)
		if err != nil {
			return nil, container, err
		}

		return toxiclient.NewClient(uri), container, nil
	})
}

// Proxy is a Toxiproxy proxy in front of one shared dependency.
type Proxy struct {
	tb    testing.TB
	proxy *toxiclient.Proxy
	// Addr is the host:port the service must be wired to.
	Addr string
}

// Proxied returns a proxy in front of the named shared dependency ("postgres",
// "valkey" or "s3"), creating the container and the proxy on first use. The
// dependency itself is started through [Postgres] / [Valkey] / [S3] as usual;
// only the address the service dials changes (for S3: "http://" + Addr, with
// the store's credentials — the signature covers the Host the client used).
func Proxied(tb testing.TB, dep string) *Proxy {
	tb.Helper()

	port, ok := proxyPorts[dep]
	require.Truef(tb, ok, "containers: no proxy port for %q", dep)

	var upstreamHost string

	switch dep {
	case "postgres":
		Postgres(tb)
		upstreamHost = mappedHostPort(tb, postgres.container, "5432/tcp")
	case "valkey":
		Valkey(tb)
		upstreamHost = mappedHostPort(tb, valkey.container, "6379/tcp")
	case "s3":
		S3(tb)
		upstreamHost = mappedHostPort(tb, s3store.container, s3Port)
	}

	return proxyOn(tb, dep, port, upstreamHost)
}

// ProxyTo returns a proxy in front of one TCP port of any container — a
// package's own [Fixture], usually — creating it on first use. name
// identifies the proxy within the test binary: the same name returns the same
// proxy (and, like every [Proxy], is restored when the test ends), so tests
// that share one are serial. A dozen distinct names are available.
//
// Toxiproxy speaks TCP only: a UDP port (HTTP/3) cannot be proxied.
func ProxyTo(tb testing.TB, name string, upstream testcontainers.Container, port string) *Proxy {
	tb.Helper()

	upstreamHost := mappedHostPort(tb, upstream, port)

	spare.mu.Lock()

	listen, ok := spare.byName[name]
	if !ok {
		if spare.byName == nil {
			spare.byName = map[string]int{}
		}

		require.Lessf(tb, len(spare.byName), sparePortCount, "containers: no spare proxy port left for %q", name)

		listen = sparePortFirst + len(spare.byName)
		spare.byName[name] = listen
	}

	spare.mu.Unlock()

	return proxyOn(tb, name, listen, upstreamHost)
}

// proxyOn creates (or finds) the proxy name listening on port inside the
// Toxiproxy container, in front of the host-mapped upstream port.
func proxyOn(tb testing.TB, name string, port int, upstreamHost string) *Proxy {
	tb.Helper()

	cli := toxiproxy(tb)

	p, err := cli.Proxy(name)
	if err != nil { // not created yet
		p, err = cli.CreateProxy(name, fmt.Sprintf("0.0.0.0:%d", port), "host.docker.internal:"+upstreamHost)
		require.NoError(tb, err)
	}

	addr := mappedHostPort(tb, toxi.container, strconv.Itoa(port)+"/tcp")
	pr := &Proxy{tb: tb, proxy: p, Addr: "localhost:" + addr}
	tb.Cleanup(pr.Reset)

	return pr
}

// Down makes the dependency unreachable (connections refused) until Up or
// the test ends.
func (p *Proxy) Down() { p.tb.Helper(); require.NoError(p.tb, p.proxy.Disable()) }

// Up restores the dependency.
func (p *Proxy) Up() { p.tb.Helper(); require.NoError(p.tb, p.proxy.Enable()) }

// Latency adds d of delay to every response until Reset or the test ends.
func (p *Proxy) Latency(d time.Duration) {
	p.tb.Helper()
	_, err := p.proxy.AddToxic("latency", "latency", "downstream", 1.0, toxiclient.Attributes{"latency": d.Milliseconds()})
	require.NoError(p.tb, err)
}

// Toxic adds any other Toxiproxy toxic to the given stream ("upstream": what
// the service sends, "downstream": what it receives) until Reset or the test
// ends — the failure modes Down and Latency do not cover:
//
//	p.Toxic("timeout", "downstream", map[string]any{"timeout": 0})     // black hole: accepted, never answered
//	p.Toxic("reset_peer", "downstream", map[string]any{"timeout": 0})  // TCP RST on every connection
//	p.Toxic("limit_data", "downstream", map[string]any{"bytes": 512})  // connection cut mid-reply
//	p.Toxic("slicer", "downstream", map[string]any{"average_size": 7, "delay": 100}) // replies in tiny delayed pieces
func (p *Proxy) Toxic(kind, stream string, attributes map[string]any) {
	p.tb.Helper()
	_, err := p.proxy.AddToxic(kind+"_"+stream, kind, stream, 1.0, toxiclient.Attributes(attributes))
	require.NoError(p.tb, err)
}

// Reset removes every toxic and re-enables the proxy.
func (p *Proxy) Reset() {
	toxics, err := p.proxy.Toxics()
	if err == nil {
		for _, t := range toxics {
			_ = p.proxy.RemoveToxic(t.Name)
		}
	}

	_ = p.proxy.Enable()
}

// Pause freezes the shared Kafka container (dep is "kafka", the one target)
// until the returned func or the test's cleanup runs. A frozen dependency does
// not refuse connections, it stops answering: exactly what timeouts, readiness
// deadlines and best-effort paths are there for. Kafka gets this instead of a
// [Proxied] address because its advertised listeners would let a client
// bypass a proxy.
//
// The freeze is SIGSTOP / SIGCONT through `kill -<sig> -1` in the container,
// which reaches every process but PID 1 (see signalAll). That freezes the
// workload only where PID 1 is a wrapper: the testcontainers Kafka module
// (v0.45.0) starts the broker's JVM from a shell. The other shared
// dependencies run their server as PID 1 — the Postgres postmaster, RustFS,
// and valkey-server (the Valkey module replaces the image's entrypoint with
// it) — so the same signal would leave them answering, and Pause refuses
// them rather than freeze nothing; cut those off with [Proxied].
//
// Not the cgroup freezer (`docker pause`): the effect on clients is the same,
// but that path proved able to wedge a desktop Docker daemon (OrbStack) on
// unpause during development. Not `docker kill -s STOP` either: it reaches
// PID 1 only, the shell, and the JVM would keep running.
func Pause(tb testing.TB, dep string) (unpause func()) {
	tb.Helper()

	if dep != "kafka" {
		tb.Fatalf("containers: cannot pause %q: only \"kafka\" runs its server below PID 1, which the freeze cannot reach", dep)
	}

	Kafka(tb)

	container := kafka.container

	signalAll := func(sig string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// -1: every process the sender may signal except itself and PID 1
		// (Linux kill(2)) — the container's workload only where PID 1 is not
		// the server, which is why Pause takes Kafka alone.
		code, _, err := container.Exec(ctx, []string{"/bin/sh", "-c", "kill -" + sig + " -1"})
		if err != nil {
			return err
		}

		if code != 0 {
			return fmt.Errorf("kill -%s -1 exited %d", sig, code)
		}

		return nil
	}
	require.NoError(tb, signalAll("STOP"))

	var once sync.Once

	unpause = func() {
		once.Do(func() {
			if err := signalAll("CONT"); err != nil {
				tb.Logf("containers: unpause %s: %v", dep, err)
			}
		})
	}
	tb.Cleanup(unpause)

	return unpause
}

// mappedHostPort returns "<host port>" for a container's exposed port.
func mappedHostPort(tb testing.TB, c testcontainers.Container, port string) string {
	tb.Helper()

	ctx := context.Background()
	mp, err := c.MappedPort(ctx, port)
	require.NoError(tb, err)

	return mp.Port()
}
