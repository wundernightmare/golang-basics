package testx

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Images the shared fixtures run. testcontainers prefixes them with
// TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX in a closed network.
const (
	PostgresImage = "postgres:18-alpine"
	ValkeyImage   = "valkey/valkey:9.0"
	KafkaImage    = "confluentinc/confluent-local:7.5.0"
)

// shared is one dependency per test binary: started on first use, reused by
// every test in the package, terminated by [Main]. Tests isolate through
// names ([Unique]), which is both faster (one start instead of one per test)
// and closer to how the code runs in production — against a shared server.
type shared[V any] struct {
	once      sync.Once
	value     V
	err       error
	terminate func(context.Context) error
}

var (
	postgres shared[string]
	valkey   shared[string]
	kafka    shared[[]string]
	seq      atomic.Uint64
)

// Unique returns a name unique within the test binary, for the schema, key
// prefix or topic a test owns on a shared container.
func Unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), seq.Add(1))
}

// Main terminates the shared containers after the package's tests have run.
// Wire it once per package that uses them:
//
//	func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }
//
// Without it the containers are still reaped by testcontainers' Ryuk when the
// process exits; Main makes the teardown explicit and Ryuk-independent (the
// GitLab integration job runs with Ryuk disabled).
func Main(m *testing.M) int {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, term := range []func(context.Context) error{postgres.terminate, valkey.terminate, kafka.terminate} {
		if term != nil {
			_ = term(ctx)
		}
	}
	return code
}

// Postgres returns the DSN of the package's shared Postgres, starting it on
// first use. Skips under -short and, outside CI, when Docker is unavailable.
func Postgres(tb testing.TB) string {
	tb.Helper()
	return use(tb, &postgres, "postgres", func(ctx context.Context) (string, func(context.Context) error, error) {
		c, err := tcpostgres.Run(ctx, PostgresImage,
			tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
			// The log line says the server is up; the listening-port check says the
			// host-side port mapping is, which can lag the log by a moment.
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return "", nil, err
		}
		dsn, err := c.ConnectionString(ctx, "sslmode=disable")
		return dsn, terminator(c), err
	})
}

// Valkey returns the URL of the package's shared Valkey.
func Valkey(tb testing.TB) string {
	tb.Helper()
	return use(tb, &valkey, "valkey", func(ctx context.Context) (string, func(context.Context) error, error) {
		// Log + listening port, not the module's exec-based default: exec
		// stalls on rootless podman under concurrent starts, and the log line
		// alone can precede the host-side port mapping by a moment.
		c, err := tcvalkey.Run(ctx, ValkeyImage,
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForLog("Ready to accept connections"),
				wait.ForListeningPort("6379/tcp"),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return "", nil, err
		}
		url, err := c.ConnectionString(ctx)
		return url, terminator(c), err
	})
}

// Kafka returns the broker seed list of the package's shared Kafka (KRaft).
func Kafka(tb testing.TB) []string {
	tb.Helper()
	return use(tb, &kafka, "kafka", func(ctx context.Context) ([]string, func(context.Context) error, error) {
		c, err := tckafka.Run(ctx, KafkaImage)
		if err != nil {
			return nil, nil, err
		}
		brokers, err := c.Brokers(ctx)
		return brokers, terminator(c), err
	})
}

// use starts the container once (three attempts: a container runtime under
// load can stall an inspect call during a concurrent start) and applies the
// skip/fail policy for every caller.
func use[V any](tb testing.TB, s *shared[V], name string,
	start func(context.Context) (V, func(context.Context) error, error),
) V {
	tb.Helper()
	if testing.Short() {
		tb.Skip("skipping container-backed test in -short mode")
	}
	s.once.Do(func() {
		ctx := context.Background()
		for attempt := 1; attempt <= 3; attempt++ {
			s.value, s.terminate, s.err = start(ctx)
			if s.err == nil {
				return
			}
			tb.Logf("%s: start attempt %d/3 failed: %v", name, attempt, s.err)
		}
	})
	if s.err != nil {
		if _, inCI := os.LookupEnv("CI"); inCI {
			tb.Fatalf("%s container must start in CI: %v", name, s.err)
		}
		tb.Skipf("could not start %s container (docker unavailable?): %v", name, s.err)
	}
	return s.value
}

func terminator(c testcontainers.Container) func(context.Context) error {
	return func(ctx context.Context) error {
		return testcontainers.TerminateContainer(c, testcontainers.StopContext(ctx))
	}
}
