package rdkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// metadataClient is what a connectivity check needs from either client.
type metadataClient interface {
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*ckafka.Metadata, error)
}

// pingTimeout bounds a connectivity check whose ctx has no deadline.
const pingTimeout = 10 * time.Second

// ping asks the brokers for cluster metadata: answered means connected and
// authenticated. It waits for librdkafka's answer, bounded by ctx's deadline
// (pingTimeout without one) passed down as the request timeout — it never
// leaves a call running inside a client that may be closed next.
func ping(ctx context.Context, cl metadataClient) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timeout := pingTimeout
	if dl, ok := ctx.Deadline(); ok {
		timeout = min(time.Until(dl), timeout)
	}
	_, err := cl.GetMetadata(nil, false, max(int(timeout.Milliseconds()), 1))
	return err
}

// logFoldWindow folds identical librdkafka log lines: a broker that is down
// makes every client log the same "Connection refused" every reconnect
// backoff, which would otherwise drown the service's own lines.
const logFoldWindow = time.Minute

// pumpLogs forwards librdkafka's log lines (go.logs.channel.enable) to slog
// until the channel closes. Levels are syslog's: 0–3 error, 4 warn, 5–6
// info, 7 debug — except facility FAIL: a broker connection that failed or
// dropped, which librdkafka retries on its own (reconnect.backoff.ms), is a
// warning. Whether the service is actually cut off shows in readiness and
// kafka_client_brokers_up, not in one connect attempt (librdkafka tries every
// resolved address, so `localhost` alone fails over [::1] first). A line repeated within logFoldWindow is counted instead of
// logged; the next one after the window carries the count.
func pumpLogs(logs <-chan ckafka.LogEvent, log *slog.Logger, role string) {
	type seen struct {
		at         time.Time
		suppressed int
	}
	last := map[string]*seen{}
	for ev := range logs {
		key := ev.Tag + "|" + ev.Message
		now := time.Now()
		if s, ok := last[key]; ok && now.Sub(s.at) < logFoldWindow {
			s.suppressed++
			continue
		}
		attrs := []any{"kafka_client", role, "facility", ev.Tag, "client_name", ev.Name}
		if s, ok := last[key]; ok && s.suppressed > 0 {
			attrs = append(attrs, "suppressed", s.suppressed)
		}
		last[key] = &seen{at: now}
		if len(last) > 1000 { // a stream of distinct lines: start over rather than grow
			last = map[string]*seen{key: last[key]}
		}
		log.Log(context.Background(), logLevel(ev), "librdkafka: "+ev.Message, attrs...)
	}
}

// logLevel maps a librdkafka log line to slog (see pumpLogs).
func logLevel(ev ckafka.LogEvent) slog.Level {
	level := slog.LevelDebug
	switch {
	case ev.Level <= 3 && ev.Tag == "FAIL":
		level = slog.LevelWarn
	case ev.Level <= 3:
		level = slog.LevelError
	case ev.Level == 4:
		level = slog.LevelWarn
	case ev.Level <= 6:
		level = slog.LevelInfo
	}
	return level
}

// rdStats is the subset of librdkafka's statistics JSON (STATISTICS.md)
// the client metrics read.
type rdStats struct {
	MsgSize    int64 `json:"msg_size"`
	MsgSizeMax int64 `json:"msg_size_max"`
	Brokers    map[string]struct {
		NodeID      int    `json:"nodeid"`
		State       string `json:"state"`
		ReqTimeouts int64  `json:"req_timeouts"`
		TxErrs      int64  `json:"txerrs"`
		RxErrs      int64  `json:"rxerrs"`
	} `json:"brokers"`
}

// statsSnapshot is what one statistics event boils down to. brokerErrors
// holds each broker's own cumulative error count, keyed by the broker's name;
// the holder turns them into one monotonic total.
type statsSnapshot struct {
	at            time.Time
	brokersUp     int
	brokerErrors  map[string]int64
	requestErrors int64
	queueBytes    int64
	queueMaxBytes int64
}

func parseStats(raw string) (statsSnapshot, error) {
	var s rdStats
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return statsSnapshot{}, fmt.Errorf("kafka: parse statistics: %w", err)
	}
	snap := statsSnapshot{at: time.Now(), queueBytes: s.MsgSize, queueMaxBytes: s.MsgSizeMax,
		brokerErrors: make(map[string]int64, len(s.Brokers))}
	for name, b := range s.Brokers {
		if b.NodeID < 0 { // bootstrap and coordinator entries, not real brokers
			continue
		}
		if b.State == "UP" {
			snap.brokersUp++
		}
		snap.brokerErrors[name] = b.ReqTimeouts + b.TxErrs + b.RxErrs
	}
	return snap, nil
}

// statsHolder keeps the latest snapshot for the collectors.
type statsHolder struct {
	mu   sync.Mutex
	snap statsSnapshot
	// seen is each broker's last cumulative error count, total the sum of
	// their increases: a counter must never go down, but a per-broker count
	// does when librdkafka drops a broker handle (the cluster shrank, a
	// broker was replaced) and starts a new one from zero. Entries are kept
	// after a broker disappears so its return is not counted twice.
	seen  map[string]int64
	total int64
}

func (h *statsHolder) update(raw string) {
	snap, err := parseStats(raw)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.seen == nil {
		h.seen = map[string]int64{}
	}
	for name, n := range snap.brokerErrors {
		if prev := h.seen[name]; n >= prev {
			h.total += n - prev
		} else {
			h.total += n // a new handle under an old name: its count started at zero
		}
		h.seen[name] = n
	}
	snap.requestErrors = h.total
	h.snap = snap
}

// latest returns the snapshot when it is fresh enough to report: a client
// that stopped reporting must not freeze its last numbers on a dashboard.
func (h *statsHolder) latest(maxAge time.Duration) (statsSnapshot, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.snap.at.IsZero() || time.Since(h.snap.at) > maxAge {
		return statsSnapshot{}, false
	}
	return h.snap, true
}

// statsMaxAge is how long a snapshot stays reportable: three missed
// statistics periods, at least a minute.
func statsMaxAge(cfg kafka.Config) time.Duration {
	return max(3*time.Duration(cfg.StatisticsInterval)*time.Millisecond, time.Minute)
}

// clientStats turns the latest snapshot into kafka.ClientStats.
func (h *statsHolder) clientStats(cfg kafka.Config) kafka.ClientStats {
	s, ok := h.latest(statsMaxAge(cfg))
	if !ok {
		return kafka.ClientStats{}
	}
	return kafka.ClientStats{Valid: true, BrokersUp: int64(s.brokersUp), RequestErrors: s.requestErrors,
		QueueBytes: s.queueBytes, QueueMaxBytes: s.queueMaxBytes}
}
