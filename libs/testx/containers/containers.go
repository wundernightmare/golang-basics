package containers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"

	"github.com/wundernightmare/golang-basics/libs/testx"
)

// Images the shared fixtures run. testcontainers prefixes them with
// TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX in a closed network.
const (
	PostgresImage = "postgres:18-alpine"
	ValkeyImage   = "valkey/valkey:9.0"
	KafkaImage    = "confluentinc/confluent-local:7.5.0"
	// RustFSImage is the S3-compatible object store (Apache-2.0, a single
	// static binary): what the S3 fixtures run instead of AWS.
	RustFSImage = "rustfs/rustfs:1.0.5"
)

// S3Store is how a test reaches the shared object store: a plaintext
// endpoint and the root credentials the container was started with. The
// store starts empty; a test creates its bucket (libs/s3 does, with
// AutoCreateBucket) and owns a key prefix in it.
type S3Store struct {
	Endpoint  string // http://host:port
	AccessKey string
	SecretKey string
	Region    string
}

// Credentials and port of the S3 fixture.
const (
	s3AccessKey = "testaccess"
	s3SecretKey = "testsecret-0123456789"
	s3Port      = "9000/tcp"
)

// shared is one dependency per test binary: started on first use, reused by
// every test in the package, terminated by [Main]. Tests isolate through
// names (testx.Unique), which is both faster (one start instead of one per test)
// and closer to how the code runs in production — against a shared server.
type shared[V any] struct {
	once      sync.Once
	value     V
	err       error
	terminate func(context.Context) error
	container testcontainers.Container // for the chaos helpers (pause / proxy upstream)
}

var (
	postgres shared[string]
	valkey   shared[string]
	kafka    shared[[]string]
	s3store  shared[S3Store]
)

// Main terminates the shared containers after the package's tests have run,
// then checks for leaked goroutines as testx.Main does (opts are its goleak
// ignores). Wire it once per package that uses the containers:
//
//	func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }
//
// The teardown does not depend on testcontainers' Ryuk, which the GitLab
// integration job and rootless podman run without. It runs only when m.Run
// returns: a test binary that panics, is killed or runs into -timeout leaves
// its containers to Ryuk, and without Ryuk they stay until removed by hand.
func Main(m *testing.M, opts ...goleak.Option) int {
	code := m.Run()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	terms := []func(context.Context) error{postgres.terminate, valkey.terminate, kafka.terminate, s3store.terminate}
	terms = append(terms, fixtures.terminators()...)
	terms = append(terms, toxi.terminate) // last: it fronts the others

	for _, term := range terms {
		if term == nil {
			continue
		}

		if err := term(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "containers: a container was not removed: %v\n", err)
		}
	}

	if code != 0 {
		return code
	}

	// After the containers are gone, nothing the tests started may still run.
	return testx.CheckLeaks(opts...)
}

// Postgres returns the DSN of the package's shared Postgres, starting it on
// first use. Skips under -short and — unless CI is set to a non-empty value —
// on a machine whose container runtime does not answer; a container that
// does not start on a runtime that answers fails the test.
func Postgres(tb testing.TB) string {
	tb.Helper()

	return use(tb, &postgres, "postgres", func(ctx context.Context) (string, testcontainers.Container, error) {
		ctr, err := tcpostgres.Run(ctx, PostgresImage,
			tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
			// The data directory in memory: a fixture needs no durability,
			// and initdb plus every commit's fsync through the container's
			// overlay filesystem is most of what a store test waits for.
			testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql": "rw"}),
			// The log line says the server is up; the listening-port check says the
			// host-side port mapping is, which can lag the log by a moment. The
			// second occurrence of the line: the image's entrypoint first runs a
			// temporary server to initialise the database, which logs it too (the
			// testcontainers postgres module v0.45.0 waits the same way,
			// BasicWaitStrategies).
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return "", ctr, err
		}

		dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")

		return dsn, ctr, err
	})
}

// Valkey returns the URL of the package's shared Valkey.
func Valkey(tb testing.TB) string {
	tb.Helper()

	return use(tb, &valkey, "valkey", func(ctx context.Context) (string, testcontainers.Container, error) {
		// Log + listening port, not the module's exec-based default: exec
		// stalls on rootless podman under concurrent starts, and the log line
		// alone can precede the host-side port mapping by a moment. The port
		// check is host-side only: its in-container half needs awk, nc or
		// bash's /dev/tcp, and the valkey image has none of them (dash as
		// /bin/sh), so it would loop until the startup timeout.
		c, err := tcvalkey.Run(ctx, ValkeyImage,
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForLog("Ready to accept connections"),
				wait.ForListeningPort("6379/tcp").SkipInternalCheck(),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return "", c, err
		}

		url, err := c.ConnectionString(ctx)

		return url, c, err
	})
}

// Kafka returns the broker seed list of the package's shared Kafka (KRaft).
func Kafka(tb testing.TB) []string {
	tb.Helper()

	return use(tb, &kafka, "kafka", func(ctx context.Context) ([]string, testcontainers.Container, error) {
		c, err := tckafka.Run(ctx, KafkaImage)
		if err != nil {
			return nil, c, err
		}

		brokers, err := c.Brokers(ctx)

		return brokers, c, err
	})
}

// S3 returns the package's shared S3-compatible object store (RustFS).
func S3(tb testing.TB) S3Store {
	tb.Helper()

	return use(tb, &s3store, "s3", func(ctx context.Context) (S3Store, testcontainers.Container, error) {
		ctr, err := testcontainers.Run(ctx, RustFSImage,
			testcontainers.WithExposedPorts(s3Port),
			testcontainers.WithEnv(map[string]string{
				"RUSTFS_ACCESS_KEY": s3AccessKey,
				"RUSTFS_SECRET_KEY": s3SecretKey,
			}),
			// Objects in memory, for the same reason as Postgres above: on
			// an overlay filesystem a thousand small PUTs took 40s and a
			// batch delete ran past the client's timeout.
			testcontainers.WithTmpfs(map[string]string{"/data": "rw,mode=1777"}),
			// /health answers once the store serves the S3 API; the port
			// alone opens a moment earlier.
			testcontainers.WithWaitStrategy(wait.ForHTTP("/health").WithPort(s3Port).
				WithStartupTimeout(60*time.Second)))
		if err != nil {
			return S3Store{}, ctr, err
		}

		endpoint, err := ctr.PortEndpoint(ctx, s3Port, "http")

		return S3Store{Endpoint: endpoint, AccessKey: s3AccessKey, SecretKey: s3SecretKey, Region: "us-east-1"}, ctr, err
	})
}

// fixtureSet is the package's own fixtures (see [Fixture]), by name.
type fixtureSet struct {
	mu     sync.Mutex
	byName map[string]any // *shared[V]
	// order holds a getter per fixture, not its terminator: that is set
	// only once the fixture has started, after it was registered here.
	order []func() func(context.Context) error
}

var fixtures fixtureSet

func (f *fixtureSet) terminators() []func(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]func(context.Context) error, 0, len(f.order))
	for _, terminate := range slices.Backward(f.order) { // reverse start order
		out = append(out, terminate())
	}

	return out
}

// Fixture runs a container only one package needs — an HTTP server with a
// particular configuration, a broker flavour — under the same rules as the
// shared dependencies: started once per test binary on first use, skipped
// under -short, required in CI, terminated by [Main]. start returns the
// value tests use (an address, a struct of them) and the container.
//
// start reports a failure by returning the error, together with the container
// if one was created: that is what gets the container removed and the start
// retried. A start that fails the test itself (require, t.Fatal) fails every
// later caller of the fixture and leaves its container behind.
//
// The name identifies the fixture within the test binary; asking for it again
// returns the same value. The container is reachable for [ProxyTo] through
// [FixtureContainer].
func Fixture[V any](tb testing.TB, name string, start func(context.Context) (V, testcontainers.Container, error)) V {
	tb.Helper()

	return use(tb, fixture[V](tb, name), name, start)
}

// FixtureContainer returns the container of a started [Fixture].
func FixtureContainer(tb testing.TB, name string) testcontainers.Container {
	tb.Helper()

	fixtures.mu.Lock()
	defer fixtures.mu.Unlock()

	holder, ok := fixtures.byName[name].(interface {
		ctr() testcontainers.Container
	})
	if !ok || holder.ctr() == nil {
		tb.Fatalf("containers: fixture %q is not started", name)
	}

	return holder.ctr()
}

func (s *shared[V]) ctr() testcontainers.Container { return s.container }

func fixture[V any](tb testing.TB, name string) *shared[V] {
	tb.Helper()

	fixtures.mu.Lock()
	defer fixtures.mu.Unlock()

	if existing, ok := fixtures.byName[name]; ok {
		typed, ok := existing.(*shared[V])
		if !ok {
			tb.Fatalf("containers: fixture %q was started with another value type", name)
		}

		return typed
	}

	if fixtures.byName == nil {
		fixtures.byName = map[string]any{}
	}

	created := &shared[V]{}
	fixtures.byName[name] = created
	fixtures.order = append(fixtures.order, func() func(context.Context) error { return created.terminate })

	return created
}

// HostPort returns the host-side port a container's exposed port ("8080/tcp",
// "8443/udp") is mapped to.
func HostPort(tb testing.TB, c testcontainers.Container, port string) string {
	tb.Helper()

	return mappedHostPort(tb, c, port)
}

// use starts the container once and applies the skip/fail policy for every
// caller.
func use[V any](tb testing.TB, s *shared[V], name string,
	start func(context.Context) (V, testcontainers.Container, error),
) V {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping container-backed test in -short mode")
	}

	return useIn(tb, s, name, start, inCI(), providerHealth)
}

// inCI reads CI as scripts/cover.sh does: set and empty is not CI.
func inCI() bool { return os.Getenv("CI") != "" }

// errStartDidNotReturn is what every later caller of a fixture gets when its
// starter left through t.Fatal, require or a panic instead of returning.
var errStartDidNotReturn = errors.New("the start function did not return (it must return its error, not fail the test)")

// useIn is use with its environment passed in: whether this is CI, and how to
// ask the container runtime whether it is there at all. Three attempts: a
// container runtime under load can stall an inspect call during a concurrent
// start.
func useIn[V any](tb testing.TB, s *shared[V], name string,
	start func(context.Context) (V, testcontainers.Container, error),
	inCI bool, runtimeHealth func(context.Context) error,
) V {
	tb.Helper()

	s.once.Do(func() {
		ctx := context.Background()
		// sync.Once counts a start that left through runtime.Goexit or a
		// panic as done: without this the callers after it would get the
		// zero value as if the fixture had started.
		s.err = errStartDidNotReturn

		for attempt := 1; attempt <= 3; attempt++ {
			s.value, s.container, s.err = start(ctx)
			if s.err == nil {
				s.terminate = terminator(s.container)
				return
			}
			// testcontainers returns the half-started container with the
			// error, and a starter passes both on: without Ryuk (rootless
			// podman, the GitLab dind job) nothing else would remove it. A
			// module that failed before creating one returns a nil pointer,
			// which TerminateContainer ignores (testcontainers-go v0.45.0;
			// TestUseRemovesAHalfStartedContainer).
			if s.container != nil {
				_ = testcontainers.TerminateContainer(s.container)
				s.container = nil
			}

			tb.Logf("%s: start attempt %d/3 failed: %v", name, attempt, s.err)
		}
	})

	if s.err == nil {
		return s.value
	}

	if inCI {
		tb.Fatalf("%s container must start in CI: %v", name, s.err)
	}

	// A machine without a container runtime skips; a runtime that is there
	// and did not start the container is a broken fixture — a wait strategy
	// that no longer matches, an image that does not pull — and a skip would
	// report it as a green run of no tests.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := runtimeHealth(ctx); err != nil {
		tb.Skipf("no container runtime to start %s on: %v", name, err)
	}

	tb.Fatalf("%s container did not start on a healthy container runtime: %v", name, s.err)

	return s.value
}

// providerHealth asks the container runtime testcontainers would use whether
// it answers. A panic counts as "it does not": without a reachable daemon
// testcontainers-go v0.45.0 panics while building its client
// (core.MustExtractDockerSocket) — its own SkipIfProviderIsNotHealthy
// recovers for the same reason, and takes *testing.T where this package
// takes testing.TB.
func providerHealth(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("container runtime: %v", r)
		}
	}()

	provider, err := testcontainers.ProviderDefault.GetProvider()
	if err != nil {
		return err
	}

	return provider.Health(ctx) // closes the provider's client itself
}

// terminator removes a container without waiting for it to shut down: a
// fixture has nothing worth flushing, and the graceful stop is what a test
// binary would spend its last seconds on — the Kafka image ignores SIGTERM
// for the whole default grace period (10s, measured), once per package.
func terminator(c testcontainers.Container) func(context.Context) error {
	return func(ctx context.Context) error {
		return testcontainers.TerminateContainer(c, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0))
	}
}
