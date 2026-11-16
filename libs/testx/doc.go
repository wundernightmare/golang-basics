// Package testx is the shared test harness of the workspace — the one place
// the "how" of testing lives so a test in any module reads as *what* it
// checks. Tests are plain `func TestX(t *testing.T)` with testify; this
// package adds only what the standard library and testify lack:
//
//   - the signals are observable in-process: [Recorder] records spans from
//     the real OpenTelemetry SDK, [LogBuffer] captures the real slog output,
//     [Metric], [NoMetric] and [LintMetrics] read the real Prometheus
//     registry;
//   - [Unique] names the schema / key prefix / topic a test owns on a shared
//     dependency;
//   - [DNS] is a name server on loopback whose answers a test changes while
//     its client runs (no such name, a record that moves, no answer);
//   - [Main] is the TestMain that fails a test binary which leaves goroutines
//     running.
//
// It is deliberately light: no Docker, no test framework. The heavy helpers
// live in separate modules so that only the packages that need them carry
// their dependency trees:
//
//   - libs/testx/containers — one Postgres / Valkey / Kafka / S3 (RustFS) container per
//     test binary (testcontainers), plus chaos helpers (Toxiproxy, SIGSTOP);
//   - libs/testx/contract — validation of HTTP exchanges against an OpenAPI
//     document and of events against a JSON Schema.
//
// Import any of them from _test files only; the depguard rule in
// .golangci.yml enforces it.
package testx
