// Package s3 is the shared object-storage client for golang-basics services:
// the official AWS SDK for Go v2 with what a service would otherwise wrap
// around it each time — environment/YAML-driven config, a deadline on every
// call, a readiness check that plugs into libs/httpx, per-operation metrics
// and tracing, bounded reads and streaming multipart uploads.
//
// It speaks to AWS S3 and to S3-compatible stores (S3_ENDPOINT); the test
// suite and the local stack run RustFS.
//
// A service typically does:
//
//	store, err := s3.New(ctx, cfg.S3, logger)
//	if err != nil { … }
//	defer store.Close()
//	srv.Health.Register("s3", store.ReadyCheck(), httpx.Optional())
//	srv.Metrics.Registry.MustRegister(store.Collectors()...)
//
//	err = store.Put(ctx, key, body, "application/zstd")
//
// [New] does no network I/O, so a store that is down does not stop a service
// that can work without it from starting; [Client.Ensure] verifies the bucket
// when the service wants to know at boot.
package s3
