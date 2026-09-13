package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// LogLevel is the runtime-adjustable minimum level of a logger built by
// [NewLogger]. The admin listener drives it (PUT /admin/log-level) so an
// operator can turn debug on for one pod without a redeploy; the change is
// always temporary — [LogLevel.Set] takes a TTL after which the level reverts
// to the base level the service was configured with — because a debug level
// left on by accident is how a disk fills up at 3 a.m.
//
// Records emitted under a context marked by [WithDebugLogging] bypass the
// level entirely (see [Config].DebugToken), so a single request can be
// debugged without touching this at all.
type LogLevel struct {
	mu      sync.Mutex
	lv      slog.LevelVar
	base    slog.Level
	gen     uint64 // bumped on every Set/Reset so a stale timer never reverts a newer change
	expires time.Time
	timer   *time.Timer
}

func newLogLevel(base slog.Level) *LogLevel {
	l := &LogLevel{base: base}
	l.lv.Set(base)
	return l
}

// Base returns the level the logger was configured with — the one every
// runtime change reverts to.
func (l *LogLevel) Base() slog.Level { return l.base }

// Level returns the level in effect right now and, when it was set at
// runtime, the time it reverts to the base level (the zero time when the base
// level is in effect or the change is permanent).
func (l *LogLevel) Level() (slog.Level, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lv.Level(), l.expires
}

// Set switches the logger to level. With ttl > 0 the level reverts to
// [LogLevel.Base] once ttl elapses; ttl <= 0 keeps it until the next Set or
// [LogLevel.Reset]. The admin endpoint never passes 0 — it caps every change
// at Config.LogLevelMaxTTL — but a service may. Returns the level that was in
// effect before.
func (l *LogLevel) Set(level slog.Level, ttl time.Duration) (previous slog.Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	previous = l.lv.Level()
	l.gen++
	gen := l.gen
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.expires = time.Time{}
	l.lv.Set(level)
	if ttl > 0 {
		l.expires = time.Now().Add(ttl)
		l.timer = time.AfterFunc(ttl, func() { l.revert(gen) })
	}
	return previous
}

// Reset reverts to the base level immediately, cancelling any pending TTL.
// Returns the level that was in effect before.
func (l *LogLevel) Reset() (previous slog.Level) {
	return l.Set(l.base, 0)
}

// revert is the timer callback: it only acts if no newer Set/Reset happened
// since the timer was armed.
func (l *LogLevel) revert(gen uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.gen != gen {
		return
	}
	l.lv.Set(l.base)
	l.expires = time.Time{}
	l.timer = nil
}

// levelHandler is the outermost handler of a [NewLogger] logger: it owns the
// level decision (slog asks only the outermost handler's Enabled), so the
// level can change at runtime through [LogLevel] and a context marked by
// [WithDebugLogging] can bypass it for one request.
type levelHandler struct {
	slog.Handler
	level *LogLevel
}

func (h levelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if DebugLogging(ctx) {
		return true
	}
	return level >= h.level.lv.Level()
}

func (h levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return levelHandler{Handler: h.Handler.WithAttrs(attrs), level: h.level}
}

func (h levelHandler) WithGroup(name string) slog.Handler {
	return levelHandler{Handler: h.Handler.WithGroup(name), level: h.level}
}

// LogLevelOf returns the runtime level control of a logger built by
// [NewLogger] (also reachable as [Server].LogLevel), or nil for any other
// logger. Derived loggers (log.With, log.WithGroup) share their parent's.
func LogLevelOf(log *slog.Logger) *LogLevel {
	if lh, ok := log.Handler().(levelHandler); ok {
		return lh.level
	}
	return nil
}

// errUnknownLevel is returned by parseLevelStrict for anything that is not
// debug|info|warn|error.
var errUnknownLevel = errors.New("unknown level")

// parseLevelStrict is parseLevel without the "unknown → info" fallback: the
// admin endpoint must reject a typo, not silently apply info.
func parseLevelStrict(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%w %q (want debug|info|warn|error)", errUnknownLevel, level)
	}
}

// levelString renders a level the way the config spells it ("debug", not
// slog's "DEBUG"), so what GET /admin/log-level returns is what PUT accepts.
func levelString(l slog.Level) string { return strings.ToLower(l.String()) }
