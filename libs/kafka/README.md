# kafka

The shared Kafka layer of golang-basics: one API (`kafka.Producer`,
`kafka.Consumer`), one config key set, one error taxonomy, one set of metric
names — and **two client libraries** behind it, picked at build time:

| Module | Client | Build |
| --- | --- | --- |
| [`libs/kafka`](.) | — the provider-neutral core: config, `Send` / `Run`, commit / retry / DLQ / drain policy, trace propagation, metrics, the `Driver` interface | pure Go |
| [`libs/kafka-rdkafka`](../kafka-rdkafka) | **librdkafka** via [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) — the same engine as nodejs-basics (`@confluentinc/kafka-javascript`) | cgo; static musl binary in the image. **The default** |
| [`libs/kafka-franz`](../kafka-franz) | **[franz-go](https://github.com/twmb/franz-go)** | pure Go, `CGO_ENABLED=0` |
| [`libs/kafka/kafkatest`](kafkatest) | the conformance suite both providers run against a real broker | test-only; a module of its own |
| `libs/kafka-rdkafka/integration`, `libs/kafka-franz/integration` | each provider's broker-backed suite: the conformance suite with that provider linked | test-only; modules of their own |

The Docker stack the broker tests need (testcontainers, the Docker client)
is required only by `kafkatest` and the two `integration` modules: the
`go.mod` of `libs/kafka` and of either provider has none of it. A provider's
own `go.mod` therefore never requires `kafkatest`.

A provider registers itself when imported, the `database/sql` way. Services
import it in one file per provider, selected by a build tag
(`services/{tasks,consumer}/internal/kafkaprovider`):

```sh
scripts/build-service.sh tasks                         # librdkafka (default)
KAFKA_PROVIDER=franz scripts/build-service.sh tasks    # franz-go: -tags kafka_franz, CGO_ENABLED=0
just docker-build tasks                                # KAFKA_PROVIDER from the env / .env, default rdkafka
```

There is no runtime switch: a binary links exactly one client (a second
`kafka.Register` panics), so a franz-go build carries no cgo and a librdkafka
build no franz-go. `kafka_client_info{provider}` and the startup log say which
one a running service has.

- [Choosing a provider](#choosing-a-provider)
- [Usage](#usage)
- [Configuration — every key, both clients](#configuration--every-key-both-clients)
- [Memory and buffers](#memory-and-buffers)
- [Compression](#compression)
- [Delivery guarantees](#delivery-guarantees)
- [Consumer: commits, retries, dead-lettering](#consumer-commits-retries-dead-lettering)
- [Rebalancing](#rebalancing)
- [TLS, SASL, custom CA, mTLS](#tls-sasl-custom-ca-mtls)
- [Topics are broker policy](#topics-are-broker-policy)
- [Errors](#errors)
- [Observability](#observability)
- [Tests](#tests)

## Choosing a provider

| | librdkafka (`rdkafka`, default) | franz-go (`franz`) |
| --- | --- | --- |
| Implementation | C library (librdkafka 2.x), Go bindings over cgo; the bundled static `librdkafka.a` is linked in | pure Go |
| Build / image | needs a C toolchain; the image builds a **fully static musl binary** (Alpine builder, `-tags musl`, `-extldflags -static`) on `distroless/static`. `go test -race` and cross-compiling need cgo too | `CGO_ENABLED=0`, cross-compiles anywhere, fastest builds |
| Binary size (tasks image) | ≈ 46 MB | ≈ 39 MB |
| Configuration | every key maps to a librdkafka property; **any other property** passes through `KAFKA_*_EXTRA_PROPERTIES` — the same names a platform team documents for every librdkafka client (Java's are mostly the same) | typed options only; `KAFKA_*_EXTRA_PROPERTIES` is a config error (there is nothing string-keyed to pass to) — what the named keys don't cover needs code |
| Memory model | separate bounded queues: send queue (bytes + count), prefetch queue (bytes) per consumer | one producer buffer (bytes + count, in-flight included); prefetch = concurrent fetches × fetch size |
| Same engine as | nodejs-basics, rust-basics (`rdkafka`), most non-JVM clients — one set of tuning knowledge, one set of known bugs | — |
| Maturity | the reference non-JVM client, maintained by Confluent; behaviour documented in CONFIGURATION.md / STATISTICS.md | mature and widely used, single main maintainer, very close protocol coverage |
| Client metrics | librdkafka statistics (`statistics.interval.ms`): brokers, request errors, queue | franz-go hooks (connections, request errors), buffered bytes |
| Client logs | librdkafka's log lines → slog (repeats folded for a minute) | none wired (franz-go logs through a `kgo.Logger`, not set here) |
| Debugging | `KAFKA_EXTRA_PROPERTIES={"debug":"broker,protocol"}` | code (hooks, a logger) |

Pick **librdkafka** when the platform hands out librdkafka properties, when
other services already run librdkafka and you want one tuning vocabulary, or
when you need a property the named keys don't cover. Pick **franz-go** when a
cgo-free build matters more (no C toolchain in the build, `-race` without cgo,
cross-compilation) and the named keys are enough. The behaviour your code
sees is the same either way: the conformance suite runs both.

## Usage

```go
cfg, err := kafka.LoadConfig("TASKS_") // or embed kafka.Config in the service config (httpx.LoadYAML)

prod, err := kafka.NewProducer(ctx, cfg, log)
defer prod.Close()
srv.Metrics.Registry.MustRegister(prod.Collectors()...)
srv.Health.Register("kafka", prod.ReadyCheck(), httpx.Optional())
err = prod.Send(ctx, kafka.Message{Key: key, Value: value, Headers: []kafka.Header{
	{Key: kafka.HeaderEventID, Value: []byte(id)},
	{Key: kafka.HeaderEventType, Value: []byte("task.created")},
}})
if kafka.Rejected(err) { /* the record itself is wrong: park it */ }

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
`envPrefix`), and import the provider wiring once in `main`:

```go
type Config struct {
	httpx.Config `yaml:",inline"`
	Kafka kafka.Config `yaml:"kafka"`
}

import _ "github.com/tracehubmmp/golang-basics/services/tasks/internal/kafkaprovider"
```

`NewProducer` checks the brokers once and fails the boot when none answers:
a wrong address or credential should not reach production as a running pod.
A service that has another place to put its data, and must start — and
restart, mid-outage — while the brokers are away, passes
`kafka.WithLazyConnect()`: the producer is built without the check, connects
on the first send, and its sends fail as `unavailable` until the brokers are
back (`services/ingest` does this; the conformance suite proves it for both
providers).

## Configuration — every key, both clients

Env keys carry the service prefix (`TASKS_KAFKA_BROKERS`); the YAML key is
the same under `kafka:` in snake case (`kafka.security_protocol`). Every key
is written to the client **explicitly** — no setting is left to whatever the
client library's default happens to be in the version you link. `<service>
--config-reference` prints them all with their defaults. Zero means "unset"
(the default fills it), so the DLQ is disabled with `none` (`-` is read the
same way) and "no statistics" is a negative number. Two keys are the
exception, because their zero is a setting: `KAFKA_PRODUCER_LINGER_MS=0` (no
batching window) and `KAFKA_CONSUMER_MAX_RETRIES=0` (no retries) stay 0. Their
defaults apply where the key is absent from the environment and the YAML; a
`kafka.Config` built in code has 0 in both unless it sets them.

**`Validate` is the whole check.** `kafka.Config.Validate()` — called by
`LoadConfig`, by both constructors and by `--check-config` — also asks the
linked provider (`Driver.Validate`), and each client has its own ranges: a
config that passes builds a producer and a consumer, a value one client does
not take is refused with the key's name. The ranges that differ are in the
tables below. One thing only a constructor can report: with librdkafka, an
unknown or malformed property in `KAFKA_*_EXTRA_PROPERTIES` — librdkafka
checks properties when a client is created. `NewProducer` and `NewConsumer`
do (the consumer for its dead-letter producer too), so it still fails at boot.

Every refusal of a config — from `Validate`, from `LoadConfig` (a value that
does not parse included) and from a constructor before it builds a client —
wraps `kafka.ErrConfig` (`errors.Is`), which tells a wrong config apart from
brokers that do not answer.

**Connection**

| Key | librdkafka | franz-go | Default | Why |
| --- | --- | --- | --- | --- |
| `KAFKA_BROKERS` | `bootstrap.servers` | `SeedBrokers` | **required** | no default address: a service where Kafka is optional is on exactly when it is set |
| `KAFKA_CLIENT_ID` | `client.id` | `ClientID` | `golang-basics` | shows in broker logs and quotas; set it per service |
| `KAFKA_DIAL_TIMEOUT` | `socket.connection.setup.timeout.ms` (at least `1s`) | `DialTimeout` | `10s` | also bounds the boot connectivity check (a metadata request) |
| `KAFKA_ALLOW_AUTO_TOPIC_CREATION` | `allow.auto.create.topics` | `AllowAutoTopicCreation` | `false` | see [Topics are broker policy](#topics-are-broker-policy) |
| `KAFKA_STATISTICS_INTERVAL_MS` | `statistics.interval.ms` | — (hooks are live; ≤ 0 turns the client gauges off) | `15000` | client metrics refresh; negative = off |
| — | `socket.keepalive.enable=true` | Go's default TCP keepalive | fixed | cloud LBs / NAT drop idle connections silently |

**Security** — see [TLS, SASL, custom CA, mTLS](#tls-sasl-custom-ca-mtls)

| Key | librdkafka | franz-go | Default |
| --- | --- | --- | --- |
| `KAFKA_SECURITY_PROTOCOL` | `security.protocol` | `DialTLSConfig` + `SASL` | `plaintext` |
| `KAFKA_SASL_MECHANISM` | `sasl.mechanism` | `plain` / `scram` mechanism | — (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) |
| `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` / `_FILE` | `sasl.username`, `sasl.password` | `Auth{User, Pass}` | — |
| `KAFKA_SSL_CA_LOCATION` | `ssl.ca.location`: a PEM file, a directory of certificates, or `probe` | `tls.Config.RootCAs`: a PEM file (the file alone); a directory or `probe` is a config error | system roots |
| `KAFKA_SSL_CERTIFICATE_LOCATION`, `KAFKA_SSL_KEY_LOCATION` | `ssl.certificate.location`, `ssl.key.location` | `tls.Config.Certificates` | — (mTLS) |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | `ssl.endpoint.identification.algorithm` | host name check on / chain-only `VerifyConnection` | `https` |

**Memory** — see [Memory and buffers](#memory-and-buffers); the same names
and defaults as nodejs-basics

| Key | librdkafka | franz-go | Default |
| --- | --- | --- | --- |
| `KAFKA_PRODUCER_QUEUE_MAX_KBYTES` | `queue.buffering.max.kbytes` | `MaxBufferedBytes` (×1024) | `65536` (64 MiB) |
| `KAFKA_PRODUCER_QUEUE_MAX_MESSAGES` | `queue.buffering.max.messages` | `MaxBufferedRecords` | `100000` |
| `KAFKA_MESSAGE_MAX_BYTES` | `message.max.bytes` | `ProducerBatchMaxBytes` | `1000000` (the broker's default `message.max.bytes`) |
| `KAFKA_CONSUMER_QUEUE_MAX_KBYTES` | `queued.max.messages.kbytes` | `MaxConcurrentFetches` = max(1, this ÷ fetch size) | `4096` (4 MiB) |
| `KAFKA_CONSUMER_FETCH_MAX_BYTES` | `fetch.max.bytes`, `receive.message.max.bytes` = this + 512 | `FetchMaxBytes`, `BrokerMaxReadBytes` = max(this + 1 MiB, 16 MiB) | `4194304` (4 MiB) |
| `KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES` | `max.partition.fetch.bytes` | `FetchMaxPartitionBytes` | `1048576` (1 MiB) |

Checked at boot: the largest record fits in the send queue, a fetch can hold
the largest record (`FETCH_MAX_BYTES ≥ MESSAGE_MAX_BYTES`), a partition's
share is not above the whole fetch.

**Producer**

| Key | librdkafka | franz-go | Default |
| --- | --- | --- | --- |
| `KAFKA_TOPIC` | the record's topic when `Send` gets none | same | `tasks.events` |
| `KAFKA_PUBLISH_TIMEOUT` | `message.timeout.ms` (at least `1ms`, and longer than the linger) | `RecordDeliveryTimeout` (at least `1s`) | `5s` |
| `KAFKA_PRODUCER_COMPRESSION_TYPE` | `compression.type` | `ProducerBatchCompression(codec, none)` | `lz4` |
| `KAFKA_PRODUCER_LINGER_MS` | `linger.ms` (up to 900000) | `ProducerLinger` (up to 60000) | `5`; `0` = no batching window |
| — | `acks=all`, `enable.idempotence=true` | `RequiredAcks(AllISRAcks)`, idempotent (franz-go's default) | fixed |

**Consumer**

| Key | librdkafka | franz-go | Default |
| --- | --- | --- | --- |
| `KAFKA_TOPICS`, `KAFKA_GROUP` | subscription, `group.id` | `ConsumeTopics` (+ `ConsumeRegex` when a topic starts with `^`), `ConsumerGroup` | the topic, `tasks-consumer` |
| `KAFKA_CONSUMER_ASSIGNMENT_STRATEGY` | `partition.assignment.strategy` | `Balancers(...)` | `cooperative-sticky` |
| `KAFKA_CONSUMER_SESSION_TIMEOUT_MS` | `session.timeout.ms` | `SessionTimeout` | `45000` (Kafka's default since 3.0) |
| `KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS` | `max.poll.interval.ms` | `RebalanceTimeout` (closest: franz-go has no poll interval) | `300000` |
| `KAFKA_CONSUMER_AUTO_OFFSET_RESET` | `auto.offset.reset` | `ConsumeResetOffset` | `earliest` (a new group must not skip a backlog) |
| — | `enable.auto.commit=false`, `enable.auto.offset.store=false` | `DisableAutoCommit`, `BlockRebalanceOnPoll` | fixed: the loop commits |
| `KAFKA_CONSUMER_MAX_POLL_RECORDS` | records per batch (loop) | `PollRecords(max)` | `100` |
| `KAFKA_CONSUMER_HANDLER_TIMEOUT` | per attempt (loop) | same | `30s` |
| `KAFKA_CONSUMER_MAX_RETRIES`, `_RETRY_BACKOFF`, `_RETRY_BACKOFF_MAX` | loop | same | `5` (`0` = no retries), `200ms`, `30s` |
| `KAFKA_DEAD_LETTER_TOPIC` | loop | same | `{topic}.dlq`; `none` (or `-`) = stop instead. Never a topic the consumer reads |
| `KAFKA_CONSUMER_DRAIN_TIMEOUT` | loop | same | `10s` |

**A topic that starts with `^` is a regular expression**, in both providers:
`KAFKA_TOPICS=^tasks\..*,audit.events` reads every topic the pattern matches
plus `audit.events`. The pattern is matched from the `^` on and is not
anchored at the end unless it says so (`$`). The dialects differ at the
edges — librdkafka has its own regex engine, franz-go uses Go's `regexp`
(RE2: no backreferences or lookaround), and a pattern Go cannot compile is a
config error with franz-go — so keep to the common subset. franz-go never
matches internal topics (`__consumer_offsets`). With a pattern in the
subscription, `KAFKA_DEAD_LETTER_TOPIC` must be a fixed name the pattern does
not match (or `none`): the default `{topic}.dlq` would be matched by the
pattern its source matched, and the consumer would read — and park again —
its own dead letters. `Validate` refuses that pair, and likewise a dead-letter
topic that is one of the subscribed names.

**Anything else (librdkafka only)**

| Key | Applies to | Format |
| --- | --- | --- |
| `KAFKA_EXTRA_PROPERTIES` | every client | env: a JSON object (`{"socket.timeout.ms":30000,"debug":"broker"}`, values may be strings, numbers or booleans); YAML: a mapping |
| `KAFKA_PRODUCER_EXTRA_PROPERTIES` | producers (and the consumer's DLQ producer) | same |
| `KAFKA_CONSUMER_EXTRA_PROPERTIES` | consumers | same |

They are applied after the named keys, and they **may not** set a property a
named key owns (`queue.buffering.max.kbytes`, `fetch.message.max.bytes`,
`partition.assignment.strategy`, `sasl.password`, …) or one the library
manages (`acks`, `enable.idempotence`, `enable.auto.commit`,
`enable.auto.offset.store`, `socket.keepalive.enable`,
`delivery.report.only.error` — `Send` waits for the report of every record —
and `go.*`): one source of truth per setting. The
error names the property and the key to use — never the value, which may be
a password. Secret-looking properties are redacted in `/admin/config`. With
franz-go linked, setting any of the three is a config error (exit 78) that
says why.

## Memory and buffers

The defaults of both clients are sized for throughput, not for a pod with a
memory limit: librdkafka keeps up to **1 GiB** of unsent records per
producer and prefetches up to **64 MiB per partition** (`queued.max.messages.kbytes`
65536 + 100 000 messages); franz-go has **no byte bound at all** on the
producer buffer and runs fetches against every broker concurrently with
50 MiB responses. A broker outage (records pile up) or a consumer waking up
to a deep backlog (prefetch races ahead of the handler) then ends in an
OOM kill — before the first commit, so the restart does it again. Every
bound is therefore a named key, always written.

**librdkafka**

- producer ≈ `PRODUCER_QUEUE_MAX_KBYTES` (records waiting, including those
  being retried) + in flight per broker up to 5 requests × `MESSAGE_MAX_BYTES`
  → with the defaults ≈ 64 MiB + 5 MiB × brokers. Full queue →
  `queue_full`, which `Send` waits out within `KAFKA_PUBLISH_TIMEOUT`
  (back-pressure, not an error, unless it lasts).
- consumer ≈ `CONSUMER_QUEUE_MAX_KBYTES` (the prefetch queue, all
  partitions together) + one fetch response per broker (`CONSUMER_FETCH_MAX_BYTES`)
  + the batch the handler holds (`MAX_POLL_RECORDS` records) → ≈ 4 MiB +
  4 MiB × brokers.
- a consumer also builds a producer for the dead-letter topic on first use —
  same producer bounds, normally empty. `NewConsumer` builds one and closes
  it again, so producer properties librdkafka refuses stop the boot, not the
  first poison record.

**franz-go**

- producer ≤ `PRODUCER_QUEUE_MAX_KBYTES` — franz-go counts a record from
  `Send` until it is acknowledged, so in-flight batches are inside the bound;
  `PRODUCER_QUEUE_MAX_MESSAGES` caps the count. Full → `queue_full`, waited
  out the same way.
- consumer ≈ `MaxConcurrentFetches` × `FETCH_MAX_BYTES` (=
  `CONSUMER_QUEUE_MAX_KBYTES` rounded to whole fetches, at least one) + one
  response being read per broker (`BrokerMaxReadBytes`) + the handler's
  batch → ≈ 4 MiB + 16 MiB × brokers with the defaults. `BrokerMaxReadBytes`
  has a 16 MiB floor because the same bound applies to metadata responses,
  which grow with the cluster's topic count.

`MESSAGE_MAX_BYTES`, `CONSUMER_FETCH_MAX_BYTES` and
`CONSUMER_PARTITION_FETCH_MAX_BYTES` travel in the Kafka protocol as int32:
a value above 2147483647 is a config error. franz-go reads at most 1 GiB
from a broker in one response, so with that provider
`CONSUMER_FETCH_MAX_BYTES` above 1073741824 is a config error too.

Rules of thumb: raise `PRODUCER_QUEUE_MAX_KBYTES` for bursty producers (it is
how long you survive a broker outage at your send rate), raise the consumer
queue / fetch size for many small records and fast handlers, keep
`MESSAGE_MAX_BYTES` equal to the topic's `max.message.bytes` — and size the
pod for the sum.

## Compression

`lz4` by default, on both clients: the cheapest codec in CPU and latency at a
ratio close enough for JSON events. `zstd` compresses better (bandwidth- or
storage-bound links, large text payloads) at more CPU; `none` for
pre-compressed or encrypted payloads; `snappy` / `gzip` for brokers or
consumers that can't do the others.

Compression is per batch, so it only pays when records share a batch:
`KAFKA_PRODUCER_LINGER_MS` (5 ms) lets concurrent sends join one. A single
record per batch (one `Send` at a time, linger 0) gains little. franz-go
sends a batch uncompressed when compression did not make it smaller. The
broker keeps the producer's codec unless the topic sets `compression.type`
itself.

## Delivery guarantees

- `Send` returns after **every in-sync replica** has the record (`acks=all`):
  together with the topic's `min.insync.replicas` (≥ 2 in production) an
  acknowledged record survives a broker loss.
- **Idempotence** is always on: the client's own retries (leader moves,
  timeouts) neither duplicate nor reorder a record within a partition.
- `KAFKA_PUBLISH_TIMEOUT` is the client's delivery deadline
  (`message.timeout.ms` / `RecordDeliveryTimeout`), its retries included —
  not a wait in front of them. A record that times out may still have been
  written: retrying (the outbox does) is at-least-once, and the consumer
  dedupes by `event_id`.
- `linger.ms` 5 is the batching trade-off: up to 5 ms of latency per send for
  shared batches and their compression under load. Set 0 for one-at-a-time
  low-latency sends.
- **A `Send` whose context ends** returns an `unavailable` error for that
  call only. A context that is already over sends nothing. One that ends
  while the record waits leaves the record with the client, which may still
  deliver it (until `KAFKA_PUBLISH_TIMEOUT`): a retry can duplicate it, as
  at-least-once allows. It never fails the sends of other callers — with
  either provider.
- Ordering is per key (= per partition). The outbox relay sends one event at
  a time in id order; a rejected event gives up its place.

## Consumer: commits, retries, dead-lettering

The loop is the same for both clients (it lives in this package; a provider
only polls, commits and reports group changes).

| Situation | What happens |
| --- | --- |
| handler returns `nil` | offset committed after the batch (`kafka_consumer_records_total{result="ok"}`) |
| handler returns an error | retried up to `KAFKA_CONSUMER_MAX_RETRIES` times, backoff `KAFKA_CONSUMER_RETRY_BACKOFF` ×2 per retry, ±50 % jitter, capped at `KAFKA_CONSUMER_RETRY_BACKOFF_MAX`; success → `result="retried"` |
| error wrapped with `kafka.Permanent` | no retries |
| retries exhausted / permanent, DLQ on (default) | record sent to `KAFKA_DEAD_LETTER_TOPIC` (default `{topic}.dlq`) with its key, value and headers plus `dlq_reason` (`permanent`\|`retries_exhausted`), `dlq_error`, `dlq_source_topic`, `dlq_source_partition`, `dlq_source_offset`, `dlq_attempts` (the same names as nodejs-basics); then committed and the loop moves on (`result="dead_lettered"`) |
| …, DLQ off (`KAFKA_DEAD_LETTER_TOPIC=none`) | **not** committed; `Run` returns the error and the process stops (`result="failed"`). The poison record is redelivered on restart — for streams where skipping is never acceptable |
| DLQ send itself fails (after retries) | as DLQ off: `Run` returns the error, nothing skipped |
| handler panics | recovered, treated as an error (retried, then DLQ) |
| `Run(ctx, nil)` | an error before anything is polled |
| `Close` from another goroutine while `Run` is polling | `Run` returns `nil`; a record a handler is still working on is not committed |
| each attempt | its own deadline (`KAFKA_CONSUMER_HANDLER_TIMEOUT`), a `process` span continuing the producer's trace, and `request_id` = the `event_id` header (or a fresh id) |

**Commits.** Up to `KAFKA_CONSUMER_MAX_POLL_RECORDS` records are processed in
partition order, then everything finished is committed in one request — the
committed offset is the **next** one to read (last processed + 1; the
conformance suite checks the exact number) — and only then may the group
rebalance. A failed commit is counted and logged; the records stay pending
for the next commit or the revoke callback.

**Shutdown.** On cancellation the in-flight batch gets
`KAFKA_CONSUMER_DRAIN_TIMEOUT` to finish; what finished is committed with a
detached context, what did not is redelivered to the next owner; then the
member leaves the group (committing on revoke) and the client closes.

**`Close` while `Run` is running is supported**, from another goroutine and with
either provider: it is the way to stop a consumer whose context the caller
does not own. `Run` returns `nil`. Unlike a cancelled context it does not
drain: a record a handler is working on is not committed and goes to the
group's next owner, so prefer cancelling the context where the batch should
finish first.

**At-least-once.** A record can be delivered twice (crash between processing
and commit, a lost partition, a producer retry), so handlers are idempotent —
`services/consumer` dedupes by `event_id`.

**Retries block the partition** (and the poll loop): one record's retries
plus the rest of its batch must fit in `KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS`
(5 min) or the member is considered stuck and its partitions move. With the
defaults the worst record costs 6 × 30 s + backoff ≈ 4 min — lower the
handler timeout or the retries before raising the interval.

## Rebalancing

- **`cooperative-sticky`** by default: a member joining or leaving moves only
  the partitions that must move; the others keep being consumed. `range` /
  `roundrobin` (eager) stop the whole group on every change.
- **A live group cannot switch** between eager (`range`, `roundrobin`) and
  cooperative (`cooperative-sticky`) by a rolling deploy: old and new members
  share no protocol and the group cannot form. Stop the group (scale to
  zero), then start it with the new strategy. Members built with different
  providers can share a group as long as the strategy is the same.
- **Commit on revoke.** The loop commits before it lets a rebalance happen;
  the revoke callback commits anything still pending (a commit that failed
  earlier, or on shutdown) before the partitions move. That is the last
  commit for those partitions: if it fails, their offsets are forgotten all
  the same (the new owner re-reads from the last commit — at-least-once), and
  no commit ever names a partition the member does not own — the broker
  would accept it and move the new owner's position back. Lost partitions
  (session expired) are dropped, never committed — they already belong to
  someone else.
- **How each client holds a rebalance back.** franz-go: `BlockRebalanceOnPoll`
  — after a poll the client waits for the loop's `AllowRebalance` (called
  after the commit). librdkafka: group changes are only delivered from inside
  `Poll`, through the rebalance callback, so a batch being processed is never
  interrupted; records polled for a partition revoked in the same `Poll` are
  dropped (the new owner gets them).
- **Timeouts.** `KAFKA_CONSUMER_SESSION_TIMEOUT_MS` (45 s): how long a member
  may be silent (heartbeats run in the background) before the group moves its
  partitions. `KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS` (5 min): how long the
  loop may go without polling (librdkafka leaves the group after it); in
  franz-go the closest knob is the rebalance timeout, how long a member may
  take to finish its batch and rejoin.

## TLS, SASL, custom CA, mTLS

Both clients take librdkafka's model: `KAFKA_SECURITY_PROTOCOL` picks TLS
and/or SASL, the other keys must match it (a CA file next to `plaintext` is a
config error, not a silent clear-text connection).

```sh
TASKS_KAFKA_SECURITY_PROTOCOL=sasl_ssl
TASKS_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
TASKS_KAFKA_SASL_USERNAME=app
TASKS_KAFKA_SASL_PASSWORD_FILE=/etc/kafka-secret/sasl-password   # a mounted Secret, read once
TASKS_KAFKA_SSL_CA_LOCATION=/etc/kafka-secret/ca.crt             # the private CA
# mTLS, when the broker requires a client certificate:
TASKS_KAFKA_SSL_CERTIFICATE_LOCATION=/etc/kafka-secret/client.crt
TASKS_KAFKA_SSL_KEY_LOCATION=/etc/kafka-secret/client.key
```

- The CA file **is** the trust store, in both clients: with it set, a broker
  certificate signed by a public authority is refused. A publicly trusted
  cluster needs no CA key at all (the system roots).
- The broker's host name is verified against its certificate
  (`ssl.endpoint.identification.algorithm=https`); `none` skips only that —
  the chain is still verified against the CA, in both clients.
- `PLAIN` over `sasl_plaintext` sends the password in clear text: allowed,
  logged as a warning; use `sasl_ssl`.
- [`examples/kafka-sasl-ssl`](../../examples/kafka-sasl-ssl) runs a Redpanda
  with one SASL_SSL listener and a private CA (`just kafka-sasl-ssl-up`),
  ships a Kubernetes ConfigMap + Secret for the same settings, and `just
  kafka-sasl-ssl-test` connects both providers to it.

## Topics are broker policy

Topics — and their `.dlq` — are provisioned by the platform (partitions,
replication, retention, `min.insync.replicas`, `max.message.bytes`), not
created by the app: a typo must not create a topic. Auto-creation is
requested only with `KAFKA_ALLOW_AUTO_TOPIC_CREATION=true` (dev and tests),
and even then the broker decides (`auto.create.topics.enable`). A missing
topic is `not_provisioned` (or `unavailable` once the delivery timeout
passes) — retryable: it goes through once the topic exists.

## Errors

Every `Send` error is a `*kafka.SendError{Kind, Retryable, Code}` — the same
kinds as nodejs-basics' `KAFKA_SEND_ERRORS`, mapped from each client's codes.
A broker's answer is classified once, in this package
(`kafka.BrokerSendErrorKind`, by Kafka protocol code), so it is the same kind
whichever client is linked; a provider maps only the errors its client raises
locally. A code nothing names is `unknown`:

| Kind | Retryable | librdkafka / Kafka codes |
| --- | --- | --- |
| `queue_full` | yes | local queue full (`_QUEUE_FULL`; franz-go `ErrMaxBuffered`) — waited out within the timeout first |
| `unavailable` | yes | message / request timed out, transport, all brokers down, no leader, not enough replicas, storage, throttled |
| `not_provisioned` | yes | unknown topic / partition; topic, cluster or transactional-id authorization; SASL authentication failed, illegal SASL state, unsupported SASL mechanism |
| `fatal` | yes | the producer's idempotence state is lost — librdkafka's client is replaced underneath |
| `rejected` | **no** | message too large, record list too large, invalid / corrupt record, policy violation, invalid topic |
| `unknown` | yes | anything else: kept rather than lost |

`kafka.Rejected(err)` is the one question a caller asks: the outbox relay
counts rejections towards `OUTBOX_MAX_ATTEMPTS` and retries everything else.
The kind is also the `result` label of `kafka_producer_records_total`.

**After `Close`** a producer's `Send` fails with an `unavailable` error that
wraps `kafka.ErrClientClosed`, and so does its readiness check
(`errors.Is(err, kafka.ErrClientClosed)`): a closed client is told apart from
an outage. `Producer.Close` and `Consumer.Close` are idempotent.

**A consumer client that failed for good** (librdkafka's fatal errors — a
static member, `group.instance.id` via the passthrough, fenced by another
instance with the same id) ends `Consumer.Run` with an error wrapping
`kafka.ErrClientFailed`, and its readiness check fails: the service exits and
is restarted instead of polling a dead client behind a green `/readyz`. (The
franz-go provider has no static membership here, so it has no such error.)

**Closing never races a call.** The librdkafka producer replaces a failed
client and closes the old one only when no call is inside it, and creates
nothing once `Close` began. The consumer counts every call into its client —
the loop's poll, readiness pings, commits whose caller stopped waiting — and
destroys the client only once they returned (a poll notices `Close` within
100 ms); a call stuck past 30 s makes `Close` give up and leave the client to
the exiting process — destroying it under the call would crash it.

## Observability

**Metrics** — the workspace's metrics contract
([docs/METRICS.md](../../docs/METRICS.md)): the same names whichever client
is linked, and the same as nodejs-basics. No partition label anywhere.

| Metric | |
| --- | --- |
| `kafka_producer_records_total{client,topic,result}` | `ok` or the error kind; `client="producer"` for `Producer.Send`, `client="consumer"` for the consumer's dead-letter publishes (topic = the DLQ topic) |
| `kafka_producer_send_duration_seconds{client,topic}` | send → acknowledgement |
| `kafka_consumer_records_total{topic,result}` | `ok`\|`retried`\|`dead_lettered`\|`failed`, one per record |
| `kafka_consumer_handler_duration_seconds{topic}` | one observation per attempt |
| `kafka_consumer_commit_errors_total`, `kafka_consumer_fetch_errors_total` | |
| `kafka_consumer_assigned_partitions` | partitions this member owns |
| `kafka_consumer_rebalances_total{event}` | `assigned`\|`revoked`\|`lost` |
| `kafka_client_info{client,provider}` | which library (always 1) |
| `kafka_client_brokers_up{client}` | brokers with a live connection |
| `kafka_client_request_errors_total{client}` | request timeouts + transport errors (monotonic: a broker handle librdkafka replaces does not make it go down) |
| `kafka_producer_queue_bytes`, `kafka_producer_queue_max_bytes` | the send buffer and its bound: alert when the first nears the second |

The consumer's `topic` is a topic named in `KAFKA_TOPICS`; one matched by a
`^regex` subscription is labelled `other`, so the series follow the config,
not whatever the cluster grows (the dead-letter topic of such a consumer is a
fixed name, so its `topic` label is fixed too). The client gauges are off for both providers
with a negative `KAFKA_STATISTICS_INTERVAL_MS`. With librdkafka they refresh
every interval and disappear when the client stops reporting (three missed
periods, at least a minute), so a stuck client does not show its last healthy
numbers; with franz-go they are read from the client at scrape time.
librdkafka hands a consumer its statistics only inside a poll, so a
consumer's `kafka_client_brokers_up` and `kafka_client_request_errors_total`
do not move while a batch is being handled (handlers, retries and their
backoff included) and are absent once that lasts longer than the three
periods; a producer's are refreshed by its own event loop, whatever the
application does.

`kafka_consumer_fetch_errors_total` counts while the brokers are away, with
either provider — it is the series to alert on for a consumer that cannot
fetch. The rates differ (each client reports at its own pace), so alert on
"increasing", not on a number.

**Two producers in one process** (two clusters, two configurations) have the
same series. Register each through a registerer that adds a label of your
choosing — the library adds none. Each, not only the second: one metric name
carries one set of label names, and a bare producer beside a wrapped one is
refused just the same:

```go
events := prometheus.WrapRegistererWith(prometheus.Labels{"producer": "events"}, srv.Metrics.Registry)
events.MustRegister(eventsProducer.Collectors()...)
audit := prometheus.WrapRegistererWith(prometheus.Labels{"producer": "audit"}, srv.Metrics.Registry)
audit.MustRegister(auditProducer.Collectors()...)
```

A producer and a consumer need none of this: their series differ by `client`.

**Consumer-group lag is the broker's metric, not ours.** The broker knows
every group's committed offset and every partition's end, once, also while
no member runs; a member only knows what it owns right now, and summing
members double-counts during a rebalance. So the services export no lag:

| Broker | Lag metrics |
| --- | --- |
| Redpanda | `redpanda_kafka_consumer_group_lag_sum{redpanda_group}` / `_lag_max` on `/public_metrics`, with the cluster property `enable_consumer_group_metrics: [group, partition, consumer_lag]` (docker/deps.yml sets it; recomputed every `consumer_group_lag_collection_interval_sec`, 60 s) |
| Apache Kafka | an exporter beside the cluster: [kafka-exporter](https://github.com/danielqsj/kafka_exporter) (`kafka_consumergroup_lag`) or [Burrow](https://github.com/linkedin/Burrow) (lag *status* per group); managed offerings export it themselves (MSK, Confluent Cloud) |

Lag growing while `kafka_consumer_records_total` is flat = the worker is
stuck; growing while it climbs = under-provisioned.

**Traces** — a `publish <topic>` producer span per `Send`, a `process <topic>`
consumer span per handler attempt, messaging semconv attributes; the W3C
context travels in the record headers. When a record already carries a
`traceparent` (the outbox stores the request's), that trace wins, so a
consumer's work joins the request that wrote the event.

**Logs** — librdkafka's own lines go to slog (`facility`, `kafka_client`),
repeats folded for a minute with a `suppressed` count. Syslog levels 0–3 are
errors except facility `FAIL` (a broker connection failed or dropped), which
librdkafka retries itself and is logged as a warning; a broker that stays
unreachable shows in readiness and `kafka_client_brokers_up`.

## Tests

| Where | What |
| --- | --- |
| `libs/kafka` (`go test ./...`, no broker) | config defaults and validation (buffers, security, strategies, the dead-letter topic), the JSON / YAML property maps and their redaction, the error kinds, metric lint, header propagation; the loop against a scripted client: handler panics, the retry backoff, dead-letter publish retries, commit and fetch error counters, lost partitions, `Run` misuse, `Send` bounds and `Close` |
| `libs/kafka-rdkafka`, `libs/kafka-franz` (unit, no broker) | every named key reaches the client (librdkafka accepts the property map; franz-go's option values), the passthrough rules, the local code → kind maps, statistics parsing; librdkafka: `Close` during a poll, a poll during an outage, the dead-letter producer checked at boot |
| **conformance** (`libs/kafka/kafkatest`, run by `libs/kafka-rdkafka/integration` and `libs/kafka-franz/integration` against one Kafka container each) | round trip with headers and both trace paths, client gauges, retries, `Permanent` and exhausted records on the DLQ with their headers and the exact committed offset, DLQ off stopping `Run`, shutdown mid-batch committing exactly what finished (checked by restarting a member), a two-member rebalance without loss and with bounded duplicates, reported to the member that gives partitions up as revoked and not as lost, a session that expires behind a frozen broker (`containers.Pause`) reported as lost with the member rejoining and reading on, `Close` during `Run`, a `^regex` subscription, a cancelled `Send` failing no other, a validated config building its clients at each client's range edges, the error taxonomy (missing topic, oversized record) |
| `TestSASLSSL` (both `integration` modules) | produce + consume through SASL_SSL with a private CA: an autonomous Redpanda with SCRAM-SHA-256, or `examples/kafka-sasl-ssl` with SCRAM-SHA-512 when `KAFKA_SASL_SSL_BROKERS` is set (`just kafka-sasl-ssl-test`) |
| `TestTrustStore` (both `integration` modules) | what `KAFKA_SSL_CA_LOCATION` means, against Redpanda with TLS from a private CA and SASL/SCRAM (a container of the suite's own): the CA file connects, the system roots alone do not, another CA's file does not — and, in a child process whose system root *is* the broker's CA, another CA's file still does not (the file replaces the system roots; skipped for librdkafka, which does not read `SSL_CERT_FILE`) |
| services | the tasks / consumer integration suites and e2e run with the default build; CI runs them with `-tags kafka_franz` too |

On rootless Podman set `TESTCONTAINERS_RYUK_DISABLED=true`. A new provider
implements `kafka.Driver`, registers itself in `init`, and passes
`kafkatest.Run` from an `integration` module next to it (own `go.mod`, which
requires `libs/kafka/kafkatest`; the provider's `go.mod` does not) — nothing
else in the repo changes.
