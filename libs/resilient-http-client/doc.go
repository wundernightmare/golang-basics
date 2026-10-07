// Package resilient is an outbound HTTP client with a resilience policy per
// logical target (a dependency): a per-attempt timeout, a rate limiter, a
// bulkhead (fixed concurrency cap), a circuit breaker and budgeted retries.
//
// One [Client] serves the whole process. Each request names its target:
//
//	cfg, err := resilient.LoadConfig(yamlBytes) // or resilient.DefaultConfig("billing")
//	c, err := resilient.New(cfg, resilient.WithLogger(log), resilient.WithRegisterer(reg))
//	defer c.Shutdown(ctx)
//
//	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://billing.internal/v1/invoices/42", nil)
//	resp, err := c.SendWithRetry("billing", req)
//	switch {
//	case err == nil:
//		defer resp.Body.Close() // status < 400; closing releases the slot
//	case errors.Is(err, resilient.KindCircuitOpen):
//		// the dependency is down; fail fast
//	}
//
// # Order of an attempt
//
// The per-attempt timeout starts, then the breaker admits (or rejects), the
// rate limiter and the bulkhead are waited on — within the timeout — and the
// request is sent through an otelhttp transport (traceparent injected, client
// span "METHOD target"). The caller's context is the overall deadline.
//
// # What counts as a failure
//
// The breaker counts 5xx, attempt timeouts and connection errors. It ignores
// the caller cancelling, local rejections (rate limit, bulkhead, open
// breaker, shutdown) and 4xx — including 429, which is retried but says the
// dependency is up. Half-open admits breaker_half_open_probes probes, each
// identified by the generation it was admitted in; results from earlier
// generations never decide a probe.
//
// # What it does not do
//
// No response cache, request coalescing, fallbacks, DNS cache or adaptive
// concurrency: those were removed as incorrect or out of scope. There is no
// hedging, no per-request policy override, and no retry of non-idempotent
// requests without an Idempotency-Key. Redirects to another host are refused
// unless allow_cross_host_redirects is set.
package resilient
