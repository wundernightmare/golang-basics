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
	Level   string    // debug|info|warn|error (unknown → info); the base level, changeable at runtime via [LogLevel]
	Format  string    // "text" for human-readable output, anything else → JSON
	Writer  io.Writer // destination; nil means os.Stdout (tests pass a buffer)

	// Sampling of debug/info records, per message per second: the first
	// SampleInitial pass, then every SampleThereafter-th. Warn and error are
	// never sampled, nor is anything logged under a [WithDebugLogging]
	// context. SampleInitial 0 disables sampling. Dropped counts are
	// exported as log_dropped_total{level} by the server's registry.
	SampleInitial    int
	SampleThereafter int
	SampleTick       time.Duration // window for the counters (default 1s)
}

// NewLogger builds a [slog.Logger] writing one line per record to stdout, in
// JSON (the default) or text. Every record gets trace_id / span_id attributes
// when the context carries an active OpenTelemetry span and request_id when
// it carries one of the server's requests — pass the request or message
// context via the *Context methods (log.InfoContext(ctx, …)) — and debug/info
// records are sampled per cfg. The output is plain stdout so any log
// collector that tails container output picks it up unchanged.
//
// The level is not fixed: [LogLevelOf] (or [Server].LogLevel) changes it at
// runtime, and a context marked by [WithDebugLogging] bypasses it and the
// sampler for the records emitted with it.
func NewLogger(cfg LogConfig) *slog.Logger {
	level := newLogLevel(parseLevel(cfg.Level))
	// The inner handler gets the same LevelVar for completeness, but the
	// decision is levelHandler's: slog consults only the outermost Enabled,
	// and Handle does not re-check the level.
	opts := &slog.HandlerOptions{Level: &level.lv}
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
	h = &ctxHandler{inner: h}
	if cfg.SampleInitial > 0 {
		h = newSamplingHandler(h, cfg.SampleInitial, cfg.SampleThereafter, cfg.SampleTick)
	}
	h = levelHandler{Handler: h, level: level}
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

// ctxHandler stamps what the context knows about the unit of work onto every
// record: trace_id / span_id when it carries a valid span, request_id when
// it carries one of the server's requests. A log line can then be joined to
// its trace, or to the client's X-Request-Id, by any backend without the call
// sites knowing about either.
//
// The attributes always land at the top level of the record, also under a
// logger built with WithGroup: a backend joins on a top-level trace_id, and
// {"db":{"trace_id":…}} would be invisible to it. Attrs and groups added
// before any group are delegated to the inner handler straight away (the hot
// path stays allocation-free); once a group is open the operations are
// replayed per record on top of the context attributes instead.
type ctxHandler struct {
	inner slog.Handler // with every pre-group WithAttrs applied
	ops   []handlerOp  // WithAttrs / WithGroup calls after the first group, in order
}

type handlerOp struct {
	group string
	attrs []slog.Attr
}

func (h *ctxHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	var attrs []slog.Attr
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		attrs = append(attrs,
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	if id := RequestIDFromContext(ctx); id != "" {
		attrs = append(attrs, slog.String("request_id", id))
	}
	if len(h.ops) == 0 {
		if len(attrs) > 0 {
			r = r.Clone()
			r.AddAttrs(attrs...)
		}
		return h.inner.Handle(ctx, r)
	}
	inner := h.inner
	if len(attrs) > 0 {
		inner = inner.WithAttrs(attrs)
	}
	for _, op := range h.ops {
		if op.group != "" {
			inner = inner.WithGroup(op.group)
		} else {
			inner = inner.WithAttrs(op.attrs)
		}
	}
	return inner.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(h.ops) == 0 {
		return &ctxHandler{inner: h.inner.WithAttrs(attrs)}
	}
	ops := append(append([]handlerOp(nil), h.ops...), handlerOp{attrs: attrs})
	return &ctxHandler{inner: h.inner, ops: ops}
}

func (h *ctxHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	ops := append(append([]handlerOp(nil), h.ops...), handlerOp{group: name})
	return &ctxHandler{inner: h.inner, ops: ops}
}

// SignalContext returns a context that is cancelled on SIGINT or SIGTERM,
// the standard trigger for graceful shutdown. The handler is released as
// soon as the first signal arrives, so a second one gets the default
// disposition — immediate exit — for an operator who does not want to wait
// for the drain. The returned stop function should be deferred by the
// caller.
func SignalContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}
