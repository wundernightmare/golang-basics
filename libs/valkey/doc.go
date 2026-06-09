// Package valkey is the shared cache layer for golang-basics services — the
// Valkey (Redis-compatible) analogue of libs/pgx. It wraps the official
// [github.com/valkey-io/valkey-go] client with environment-driven config, a
// readiness check that plugs into libs/httpx, and a small set of cache-aside
// helpers so services don't re-implement get-or-load every time.
//
// Valkey is the BSD-licensed fork of Redis; valkey-go is its first-party Go
// client (RESP3, automatic pipelining, opt-in client-side caching) and is the
// reliable, actively-maintained choice over the now relicensed redis clients.
//
// A service typically does:
//
//	cfg, _ := valkey.LoadConfig("TASKS_")
//	c, _ := valkey.New(cfg, logger)
//	defer c.Close()
//	srv.Health.Register("valkey", c.ReadyCheck())
//	task, _ := valkey.Aside(ctx, c, "task:"+id, time.Minute, func() (Task, error) { … })
package valkey
