// Package containers starts the workspace's test dependencies — one
// container per test binary, not per test — and makes them misbehave on
// demand:
//
//   - [Postgres], [Valkey] and [Kafka] start their dependency once, retry the
//     start, skip locally without Docker and fail in CI; [Main] terminates
//     them after the run. Tests isolate through names (testx.Unique) — a
//     schema, a key prefix, a topic — never through fresh containers;
//   - [Proxied] puts Toxiproxy in front of a dependency so it can be cut off
//     or slowed down; [Pause] freezes a container so it hangs instead of
//     refusing.
//
// It is its own module so that only container-backed test packages carry
// testcontainers, the Docker client and Toxiproxy in their dependency graph.
// Import it from _test files only.
package containers
