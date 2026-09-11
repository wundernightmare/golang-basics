package httpx

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// LogConfig configures [NewLogger]. See [Config] for the environment keys.
type LogConfig struct {
	Service string    // added as a "service" attribute to every record when set
	Level   string    // debug|info|warn|error (unknown → info)
	Format  string    // "text" for human-readable output, anything else → JSON
	Writer  io.Writer // destination; nil means os.Stdout (tests pass a buffer)

	// Sampling of debug/info records, per message per second: the first
	// SampleInitial pass, then every SampleThereafter-th. Warn and error are
	// never sampled. SampleInitial 0 disables sampling. Dropped counts are
	// exported as log_dropped_total{level} by the server's registry.
	SampleInitial    int
	SampleThereafter int
	SampleTick       time.Duration // window for the counters (default 1s)
}

// NewLogger builds a [slog.Logger] writing one line per record to stdout, in
// JSON (the default) or text. Every record gets trace_id / span_id attributes
// when the context carries an active OpenTelemetry span — pass the request or
// message context via the *Context methods (log.InfoContext(ctx, …)) — and
// debug/info records are sampled per cfg. The output is plain stdout so any
// log collector that tails container output picks it up unchanged.
func NewLogger(cfg LogConfig) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Level)}
	w := cfg.Writer
	if w == nil {
		w = os.Stdout
	}

	var h slog.Handler
	if strings.EqualFold(cfg.Format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	h = traceHandler{h}
	if cfg.SampleInitial > 0 {
		h = newSamplingHandler(h, cfg.SampleInitial, cfg.SampleThereafter, cfg.SampleTick)
	}
	log := slog.New(h)
	if cfg.Service != "" {
		log = log.With("service", cfg.Service)
	}
	return log
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// traceHandler stamps trace_id / span_id onto every record whose context
// carries a valid span, so a log line can be joined to its trace by any
// backend without the call sites knowing about tracing.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r = r.Clone()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// SignalContext returns a context that is cancelled on SIGINT or SIGTERM,
// the standard trigger for graceful shutdown. The returned stop function
// releases the signal handler and should be deferred by the caller.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
