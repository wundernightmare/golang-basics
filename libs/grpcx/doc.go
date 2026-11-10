// Package grpcx builds outbound gRPC connections with a resilience policy per
// logical target (a dependency). It is wiring, not a client of its own: the
// policy that grpc-go already implements is configured, not re-implemented.
//
//	cfg, err := grpcx.LoadConfig(yamlBytes)
//	c, err := grpcx.New(cfg, grpcx.WithLogger(log), grpcx.WithRegisterer(reg))
//	defer c.Close()
//
//	conn, err := c.Conn("billing")
//	invoices := billingv1.NewInvoicesClient(conn)
//	resp, err := invoices.Get(ctx, req)
//	if errors.Is(err, grpcx.ErrCircuitOpen) { /* the dependency is down; fail fast */ }
//
// # What grpc-go does (configured through the service config)
//
// The default deadline of a call, retries with jittered exponential backoff
// for the listed status codes (and the server's pushback), retry throttling,
// load balancing, keepalive pings, TLS.
//
// # What this package adds
//
// A circuit breaker per target (libs/resilience — the one the HTTP client
// uses), Prometheus metrics under the workspace's metrics contract, and
// otelgrpc tracing with an explicit provider.
//
// # What counts as a failure
//
// The breaker counts Unavailable, DeadlineExceeded, Internal, Unknown and
// DataLoss — the caller's own deadline included, so a deadline shorter than
// the target's timeout cannot hide an outage. Every other status means the
// dependency answered (NotFound, InvalidArgument, ResourceExhausted, …), and
// the caller cancelling says nothing. It sees a call once, after gRPC's own
// retries; a stream when it ends.
package grpcx
