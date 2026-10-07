# kafka

Shared Kafka layer for golang-basics services, on
[franz-go](https://github.com/twmb/franz-go) (pure Go, no librdkafka): a
synchronous **Producer**, a consumer-group **Consumer** loop with retries,
dead-lettering and correct commits, TLS/SASL, Prometheus collectors and
trace propagation through record headers.

## Usage

```go
cfg, err := kafka.LoadConfig("TASKS_") // or embed kafka.Config in the service config (httpx.LoadYAML)

prod, err := kafka.NewProducer(ctx, cfg, log)
defer prod.Close()
srv.Metrics.Registry.MustRegister(prod.Collectors()...)
srv.Health.Register("kafka", prod.ReadyCheck(), httpx.Optional())
err = prod.Publish(ctx, "", key, value,
	kafka.Header{Key: kafka.HeaderEventID, Value: []byte(id)},
	kafka.Header{Key: kafka.HeaderEventType, Value: []byte("task.created")})

cons, err := kafka.NewConsumer(ctx, cfg, log)
srv.Metrics.Registry.MustRegister(cons.Collectors()...)
err = cons.Run(ctx, func(ctx context.Context, m kafka.Message) error {
	typ, _ := m.Header(kafka.HeaderEventType)
	if unknown(typ) {
		return kafka.Permanent(fmt.Errorf("unknown event type %q", typ)) // no retries: straight to the DLQ
	}
	return apply(ctx, m) // an error is retried with backoff
})
```

In a service config, embed it (the keys already carry `KAFKA_`, so no
`envPrefix`):

```go
type Config struct {
	httpx.Config `yaml:",inline"`
	Kafka kafka.Config `yaml:"kafka"`
}
```

## Producer

- `Publish(ctx, topic, key, value, headers...)` blocks until **every in-sync
  replica** acknowledges (`RequiredAcks(AllISRAcks)`; the idempotent producer
  is on, so a retried produce does not duplicate within a partition), bounded
  by `KAFKA_PUBLISH_TIMEOUT` — a hanging broker must not hold a request.
- **Trace context**: the publish span is a child of the span in `ctx`, and the
  record leaves with a `traceparent` header. When the headers already carry a
  `traceparent` (an outbox relay replaying the context stored with the event),
  that trace wins, so the consumer continues the original request's trace.
- **Topic auto-creation is off** unless `KAFKA_ALLOW_AUTO_TOPIC_CREATION=true`:
  a typo must not create a topic. Pre-provision topics (and their `.dlq`).
- `kafka_publish_total{topic,result}` with a bounded `result`: `ok`,
  `timeout`, `too_large`, `auth`, `unknown_topic`, `other`.

## Consumer: delivery semantics

| Situation | What happens |
| --- | --- |
| handler returns `nil` | offset committed after the batch (`kafka_consume_total{result="ok"}`) |
| handler returns an error | retried up to `KAFKA_CONSUMER_MAX_RETRIES` times, backoff `KAFKA_CONSUMER_RETRY_BACKOFF` ×2 per retry, ±50 % jitter, capped at `KAFKA_CONSUMER_RETRY_BACKOFF_MAX`; success → `result="retried"` |
| error wrapped with `kafka.Permanent` | no retries |
| retries exhausted / permanent, DLQ on (default) | record published to `KAFKA_DEAD_LETTER_TOPIC` (default `{topic}.dlq`) with its key, value and headers plus `dlq_reason` (`permanent`\|`retries_exhausted`), `dlq_error`, `dlq_source_topic`, `dlq_source_partition`, `dlq_source_offset`, `dlq_attempts`; then committed and the loop moves on (`result="dead_lettered"`) |
| …, DLQ off (`KAFKA_DEAD_LETTER_TOPIC=none`) | **not** committed; `Run` returns the error and the process stops (`result="failed"`). The poison record is redelivered on restart — for streams where skipping is never acceptable, it makes the problem visible instead of silent |
| DLQ publish itself fails (after retries) | as DLQ off: `Run` returns the error, nothing skipped |
| handler panics | recovered, treated as an error (retried, then DLQ) |
| each attempt | its own deadline (`KAFKA_CONSUMER_HANDLER_TIMEOUT`), a `process` span continuing the producer's trace, and `request_id` = the `event_id` header (or a fresh id) via `httpx.WithRequestID` |

**Batches and commits.** Up to `KAFKA_CONSUMER_MAX_POLL_RECORDS` records are
processed in partition order, then everything finished is committed in one
request, then the group may rebalance (`BlockRebalanceOnPoll` +
`AllowRebalance`): a partition never moves with processed-but-uncommitted
records on it. `OnPartitionsRevoked` commits anything still pending (a
commit that failed earlier, or on `Close`); `OnPartitionsLost` drops it (the
new owner redelivers). A failed commit is counted
(`kafka_consumer_commit_errors_total`) and logged at error, and the records
stay pending for the next commit.

**Shutdown.** On context cancellation the in-flight batch gets
`KAFKA_CONSUMER_DRAIN_TIMEOUT` to finish (handlers keep a live context until
then); what finished is committed with a context detached from the cancelled
one; what did not is redelivered to the next owner. `Run` then leaves the
group and closes the client. A `Consumer` is single-use.

**At-least-once.** A record can be delivered twice (crash between processing
and commit, a lost partition, a producer retry across a broker failover), so
handlers must be idempotent — `services/consumer` shows dedupe by `event_id`.

**Lag.** Every `KAFKA_LAG_INTERVAL` the consumer computes, from the broker,
end offset minus committed offset **for the partitions this member owns**,
and swaps the whole snapshot in atomically (a scrape never sees a half-reset
set). Summed across replicas, each partition counts once. A failed poll keeps
the previous snapshot, bumps `kafka_consumer_lag_poll_errors_total` and warns
at most once a minute; `kafka_consumer_lag_last_success_timestamp_seconds`
tells a stale gauge from a healthy zero.

## Configuration

Keys under the service prefix (e.g. `CONSUMER_`); YAML key in parentheses.
Zero means "unset" everywhere, so the DLQ is disabled with the word `none`
and "no retries" is a negative number.

| Env | Default | |
| --- | --- | --- |
| `KAFKA_BROKERS` (`brokers`) | `localhost:9092` | comma-separated seeds |
| `KAFKA_TOPIC` / `KAFKA_TOPICS` (`topic`, `topics`) | `tasks.events` / the topic | produce target / subscribe set |
| `KAFKA_GROUP` (`group`) | `tasks-consumer` | |
| `KAFKA_CLIENT_ID` (`client_id`) | `golang-basics` | |
| `KAFKA_DIAL_TIMEOUT` (`dial_timeout`) | `10s` | |
| `KAFKA_PUBLISH_TIMEOUT` (`publish_timeout`) | `5s` | also bounds each DLQ publish |
| `KAFKA_ALLOW_AUTO_TOPIC_CREATION` (`allow_auto_topic_creation`) | `false` | |
| `KAFKA_TLS_ENABLED` (`tls_enabled`) | `false` | TLS 1.2+ |
| `KAFKA_TLS_CA_CERT` (`tls_ca_cert`) | system roots | PEM file, added to the system pool |
| `KAFKA_TLS_CLIENT_CERT` / `KAFKA_TLS_CLIENT_KEY` | none | PEM files, mTLS |
| `KAFKA_TLS_INSECURE_SKIP_VERIFY` | `false` | dev only |
| `KAFKA_SASL_MECHANISM` (`sasl_mechanism`) | none | `plain`, `scram-sha-256`, `scram-sha-512` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | | the password is `secret` (redacted in `/admin/config`); PLAIN without TLS logs a warning |
| `KAFKA_LAG_INTERVAL` (`lag_interval`) | `15s` | negative = off |
| `KAFKA_CONSUMER_MAX_POLL_RECORDS` | `100` | batch size |
| `KAFKA_CONSUMER_HANDLER_TIMEOUT` | `30s` | per attempt |
| `KAFKA_CONSUMER_MAX_RETRIES` | `5` | retries after the first attempt; negative = none |
| `KAFKA_CONSUMER_RETRY_BACKOFF` / `_MAX` | `200ms` / `30s` | |
| `KAFKA_DEAD_LETTER_TOPIC` | `{topic}.dlq` | `{topic}` = source topic; `none` disables |
| `KAFKA_CONSUMER_DRAIN_TIMEOUT` | `10s` | also bounds each commit |

`Config.Validate` (run by `LoadConfig` and the constructors) rejects an
unknown SASL mechanism or one without credentials, half a client key pair,
TLS options without TLS, a backoff cap below the initial backoff and negative
budgets.

## Metrics

| Metric | |
| --- | --- |
| `kafka_publish_total{topic,result}` | see Producer |
| `kafka_producer_publish_duration_seconds{topic}` | |
| `kafka_consume_total{topic,result}` | `ok`\|`retried`\|`dead_lettered`\|`failed`, one per record |
| `kafka_consumer_handler_duration_seconds{topic}` | one observation per attempt |
| `kafka_consumer_commit_errors_total` | |
| `kafka_consumer_dlq_total{topic}` | by dead-letter topic |
| `kafka_consumer_fetch_errors_total{topic}` | |
| `kafka_consumer_assigned_partitions{topic}` | |
| `kafka_consumer_rebalances_total{event}` | `assigned`\|`revoked`\|`lost` |
| `kafka_consumer_group_lag{topic,partition}` | owned partitions only |
| `kafka_consumer_lag_poll_errors_total`, `kafka_consumer_lag_last_success_timestamp_seconds` | |

## Tests

`go test -short ./...` runs the unit tests (config and TLS/SASL mapping,
backoff, metric lint, the lag snapshot swap). `go test ./...` adds the
integration suite against one Kafka container per test binary
(`libs/testx/containers`): round trip with headers and both trace paths,
retry-then-success, permanent and exhausted records on the DLQ with their
headers and offsets committed, DLQ disabled stopping `Run`, shutdown mid-batch
committing exactly what finished (checked by restarting a member of the same
group), a two-member rebalance with no loss and bounded duplicates plus
per-member lag, and topic auto-creation being opt-in. On rootless Podman set
`TESTCONTAINERS_RYUK_DISABLED=true`.
