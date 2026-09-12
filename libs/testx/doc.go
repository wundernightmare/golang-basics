// Package testx is the shared test harness of the workspace — the one place
// the "how" of testing lives so a test in any module reads as *what* it
// checks:
//
//   - every test is an Allure test: [Run] wraps a plain test, [T] is the
//     handle for testo suites (testo + the testo-allure plugin), [Options]
//     sets tags and honours ALLURE_RESULTS_DIR;
//   - one container per test binary, not per test: [Postgres], [Valkey] and
//     [Kafka] start their dependency once, retry the start, skip locally
//     without Docker and fail in CI; [Main] terminates them after the run.
//     Tests isolate through names ([Unique]) — a schema, a key prefix, a
//     topic — never through fresh containers;
//   - the signals are observable in-process: [Recorder] records spans from
//     the real OpenTelemetry SDK, [LogBuffer] captures the real slog output,
//     [Metric] and [LintMetrics] read the real Prometheus registry.
//
// Import it from _test files only. Its dependencies (testcontainers, testo,
// the OTel SDK) never reach a service binary.
package testx
