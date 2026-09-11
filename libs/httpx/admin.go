package httpx

import (
	"net/http"
	"net/http/pprof" //nolint:gosec // registered on the admin mux only, never on the API listener
)

// newAdminMux builds the operational surface served on Config.AdminAddr:
//
//	GET /healthz         liveness
//	GET /readyz          readiness (gate + registered checks)
//	GET /metrics         Prometheus exposition
//	GET /version         build identity (see [Build])
//	GET /debug/pprof/…   Go runtime profiles (cpu, heap, goroutine, block, mutex, trace)
//
// It is a plain [http.ServeMux] rather than gin: none of the API middleware
// (access log, request metrics, tracing) applies here, so probes and scrapes
// never show up as traffic and a runtime agent can poll pprof for free.
// Keep this listener off the ingress; it is the one that exposes internals.
func newAdminMux(h *Health, m *Metrics, build BuildInfo) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.LiveHandler())
	mux.HandleFunc("GET /readyz", h.ReadyHandler())
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, build)
	})
	// Explicit registration so nothing depends on http.DefaultServeMux.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
