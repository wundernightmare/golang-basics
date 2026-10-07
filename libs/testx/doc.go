// Package testx is the shared test harness of the workspace — the one place
// the "how" of testing lives so a test in any module reads as *what* it
// checks. Tests are plain `func TestX(t *testing.T)` with testify; this
// package adds only what the standard library and testify lack:
//
//   - the signals are observable in-process: [Recorder] records spans from
//     the real OpenTelemetry SDK, [LogBuffer] captures the real slog output,
//     [Metric] and [LintMetrics] read the real Prometheus registry;
//   - [Unique] names the schema / key prefix / topic a test owns on a shared
//     dependency.
//
// It is deliberately light: no Docker, no test framework. The heavy helpers
// live in separate modules so that only the packages that need them carry
// their dependency trees:
//
//   - libs/testx/containers — one Postgres / Valkey / Kafka container per
//     test binary (testcontainers), plus chaos helpers (Toxiproxy, SIGSTOP);
//   - libs/testx/contract — validation of HTTP exchanges against the OpenAPI
//     documents and of events against the JSON Schemas under api/.
//
// Import any of them from _test files only; the depguard rule in
// .golangci.yml enforces it.
package testx
