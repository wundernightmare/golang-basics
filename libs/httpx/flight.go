package httpx

import (
	"bytes"
	"context"
	"net/http"
	"runtime/trace"
	"time"
)

// The flight recorder: runtime/trace keeps the last seconds of the Go
// execution trace — every goroutine's scheduling, blocking, GC, syscalls — in
// a ring buffer in memory, and a snapshot is taken only when something went
// wrong. That is the evidence a profile cannot give: not where CPU went on
// average, but what this process was doing in the seconds before one request
// took too long.
//
// It is off unless DIAG_FLIGHT_RECORDER is set, because it is not free (the
// runtime traces all the time: a per cent or two of CPU, and
// DIAG_FLIGHT_MAX_BYTES of memory). Like everything in diagnostics.go it
// also needs somewhere to put a snapshot: with neither a dump sink nor
// DIAG_DUMP_DIR no recorder is started. A snapshot is taken
//
//   - when a request took DIAG_FLIGHT_SLOW_REQUEST or longer (0 = never),
//   - when the memory-pressure capture fires, next to its profiles,
//   - when the service asks ([Server.CaptureFlightTrace]),
//
// at most once per DIAG_FLIGHT_COOLDOWN, and shipped like every other
// artifact: to the dump sink, or kept in DIAG_DUMP_DIR. Open it with
// `go tool trace <file>`.
//
// The runtime allows one flight recorder per process: a second server in the
// same process logs that and runs without one.

const (
	dumpKindFlight = "flight"
	flightFile     = "flight.trace"
)

// startFlight starts the recorder when DIAG_FLIGHT_RECORDER asks for one; the
// caller (diagnostics.start) starts the watcher that serves the requests right
// after. flightOn is raised only once the runtime accepted the recorder, so
// requestFlight never queues a request nobody will read.
func (d *diagnostics) startFlight(ctx context.Context) (stop func()) {
	if !d.cfg.DiagFlightRecorder {
		return func() {}
	}

	recorder := trace.NewFlightRecorder(trace.FlightRecorderConfig{
		MinAge:   d.cfg.DiagFlightMinAge,
		MaxBytes: uint64(d.cfg.DiagFlightMaxBytes), //nolint:gosec // G115: Validate rejects a negative DIAG_FLIGHT_MAX_BYTES
	})
	if err := recorder.Start(); err != nil {
		d.log.WarnContext(ctx, "diagnostics: flight recorder not started", "error", err.Error())

		return func() {}
	}

	d.flight = recorder
	d.flightOn.Store(true)

	return func() {
		d.flightOn.Store(false)
		recorder.Stop()
	}
}

// requestFlight asks the watcher for a snapshot and returns at once: the
// request that was slow is not made slower by the evidence of it. It reports
// whether the request was queued — false when no recorder is running
// (DIAG_FLIGHT_RECORDER off, neither a sink nor a dump directory, the runtime
// refused the recorder, the server not running) or one request is already
// pending. A queued request is still subject to the cooldown.
func (d *diagnostics) requestFlight(reason string) bool {
	if !d.flightOn.Load() {
		return false
	}

	select {
	case d.flightRequests <- reason:
		return true
	default:
		return false
	}
}

// snapshotFlight runs on the watcher: it writes the recorder's window out and
// ships it, unless the last snapshot is younger than the cooldown.
func (d *diagnostics) snapshotFlight(ctx context.Context, reason string) {
	if d.flight == nil {
		return
	}

	now := d.now()
	if !d.lastFlight.IsZero() && now.Sub(d.lastFlight) < d.cfg.DiagFlightCooldown {
		return
	}

	d.lastFlight = now

	var window bytes.Buffer
	if _, err := d.flight.WriteTo(&window); err != nil {
		d.dumps.WithLabelValues(dumpKindFlight, dumpFailed).Inc()
		d.log.ErrorContext(ctx, "diagnostics: flight recorder snapshot failed", "reason", reason, "error", err.Error())

		return
	}

	d.log.WarnContext(ctx, "diagnostics: execution trace captured", "reason", reason, "bytes", window.Len())
	d.deliver(ctx, artifact{
		kind: dumpKindFlight, at: now.UTC(), name: now.UTC().Format(dumpStamp) + "-" + flightFile,
		body: window.Bytes(), contentType: contentPprof,
	})
}

// CaptureFlightTrace asks for a snapshot of the flight recorder for a reason
// of the service's own — a missed deadline in a worker, a batch that took too
// long. reason goes to the log line. It returns false when no recorder is
// running — DIAG_FLIGHT_RECORDER is off, there is neither a dump sink nor
// DIAG_DUMP_DIR, the runtime refused a second recorder in the process, or
// [Server.Run] is not running — or a snapshot is already pending; the
// cooldown still applies.
func (s *Server) CaptureFlightTrace(reason string) bool { return s.diag.requestFlight(reason) }

// flightTrigger asks for a snapshot when a request took too long. NewServer
// puts it in front of entry — it is the one middleware that wraps the whole
// chain — so the time it measures is the whole request.
func (s *Server) flightTrigger(next http.Handler) http.Handler {
	threshold := s.cfg.DiagFlightSlowRequest

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		defer func() {
			if time.Since(start) >= threshold {
				s.diag.requestFlight("slow request: " + r.Method + " " + r.URL.Path)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
