// Package containers starts the workspace's test dependencies — one
// container per test binary, not per test — and makes them misbehave on
// demand:
//
//   - [Postgres], [Valkey], [Kafka] and [S3] start their dependency once and
//     retry the start. Without a container runtime they skip locally and
//     fail in CI (CI set to a non-empty value); a container that does not
//     start on a runtime that answers fails everywhere. [Main] terminates
//     them after the run. Tests isolate through names
//     (testx.Unique) — a schema, a key prefix, a topic — never through fresh
//     containers;
//   - [Proxied] puts Toxiproxy in front of a dependency so it can be cut off
//     or slowed down; [Pause] freezes the Kafka container so it hangs instead
//     of refusing;
//   - [Fixture] runs a container only one package needs under the same
//     rules, and [ProxyTo] puts Toxiproxy in front of any container's port.
//
// The chaos helpers are there so that the code paths written for a failing
// dependency — optional readiness checks, best-effort publishing, cache
// bypass, breakers, timeouts — are exercised rather than trusted. They
// restore the dependency on t.Cleanup, so a chaos test can share the
// package's containers with the rest of the suite as long as it is not
// parallel with tests that need the dependency healthy.
//
// It is its own module so that only container-backed test packages carry
// testcontainers, the Docker client and Toxiproxy in their dependency graph.
// Import it from _test files only.
package containers
