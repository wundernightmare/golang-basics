package testx

// Chaos: dependencies that fail, slow down or hang, on demand, so the code
// paths written for those cases — optional readiness checks, best-effort
// publishing, cache bypass, breakers, timeouts — are exercised rather than
// trusted. Two mechanisms:
//
//   - [Proxy] puts Toxiproxy between the service and a shared container
//     (Postgres, Valkey): the proxy can be disabled (connection refused) or
//     given a latency toxic (slow), then reset. The service is wired to the
//     proxied address instead of the container's.
//   - [Pause] freezes a shared container (Kafka, whose advertised listeners
//     would let a client bypass a proxy): connections hang until the caller's
//     timeout fires, which is what a wedged broker looks like.
//
// Both restore the dependency on t.Cleanup, so a chaos test can share the
// package's containers with the rest of the suite as long as it is not
// parallel with tests that need the dependency healthy.

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
var proxyPorts = map[string]int{"postgres": 8666, "valkey": 8667}

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
		c, err := tctoxiproxy.Run(ctx, ToxiproxyImage,
			testcontainers.WithExposedPorts(exposed...),
			testcontainers.WithWaitStrategy(wait.ForListeningPort(tctoxiproxy.ControlPort).WithStartupTimeout(60*time.Second)),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				// Linux needs the alias; Docker Desktop / OrbStack resolve it anyway.
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			}),
		)
		if err != nil {
			return nil, nil, err
		}
		uri, err := c.URI(ctx)
		if err != nil {
			return nil, nil, err
		}
		return toxiclient.NewClient(uri), c, nil
	})
}

// Proxy is a Toxiproxy proxy in front of one shared dependency.
type Proxy struct {
	tb    testing.TB
	proxy *toxiclient.Proxy
	// Addr is the host:port the service must be wired to.
	Addr string
}

// Proxied returns a proxy in front of the named shared dependency ("postgres"
// or "valkey"), creating the container and the proxy on first use. The
// dependency itself is started through [Postgres] / [Valkey] as usual; only
// the address the service dials changes.
func Proxied(tb testing.TB, dep string) *Proxy {
	tb.Helper()
	port, ok := proxyPorts[dep]
	require.Truef(tb, ok, "testx: no proxy port for %q", dep)

	var upstreamHost string
	switch dep {
	case "postgres":
		Postgres(tb)
		upstreamHost = mappedHostPort(tb, postgres.container, "5432/tcp")
	case "valkey":
		Valkey(tb)
		upstreamHost = mappedHostPort(tb, valkey.container, "6379/tcp")
	}
	cli := toxiproxy(tb)
	name := dep
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

// Pause freezes the named shared container ("kafka", "postgres", "valkey")
// until the returned func or the test's cleanup runs. A frozen dependency does
// not refuse connections, it stops answering: exactly what timeouts, readiness
// deadlines and best-effort paths are there for.
//
// The freeze is SIGSTOP / SIGCONT to *every* process in the container
// (`kill -STOP -1` through exec) rather than the cgroup freezer (`docker
// pause`): the effect on clients is the same, `docker kill -s STOP` would only
// reach PID 1 (an entrypoint script in most images — the broker's JVM would
// keep running), and the freezer path proved able to wedge a desktop Docker
// daemon (OrbStack) on unpause during development.
func Pause(tb testing.TB, dep string) (unpause func()) {
	tb.Helper()
	var c testcontainers.Container
	switch dep {
	case "kafka":
		Kafka(tb)
		c = kafka.container
	case "postgres":
		Postgres(tb)
		c = postgres.container
	case "valkey":
		Valkey(tb)
		c = valkey.container
	default:
		tb.Fatalf("testx: cannot pause %q", dep)
	}
	signalAll := func(sig string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// -1: every process the sender may signal, except itself and init —
		// exactly the container's workload.
		code, _, err := c.Exec(ctx, []string{"/bin/sh", "-c", "kill -" + sig + " -1"})
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
				tb.Logf("testx: unpause %s: %v", dep, err)
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
