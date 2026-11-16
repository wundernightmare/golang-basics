package kafka

import (
	"context"
	"errors"
	"fmt"
)

// What [Producer.Send] can fail with — one registry, so callers branch on
// a kind and a retryable flag instead of librdkafka codes. Every error
// Send returns is a *[SendError]:
//
//	if err := p.Send(ctx, msg); err != nil {
//		if kafka.Rejected(err) {
//			// the record itself is wrong — fix it or park it, retrying won't help
//		} else {
//			// keep the event and send it again later (the outbox does this)
//		}
//	}
//
// Before an error reaches the caller, two layers already retried: the
// client retries broker-side failures (leader moves, timeouts, lost
// connections) until KAFKA_PUBLISH_TIMEOUT — an "unavailable" means that
// budget is spent; and Send waits out a full local queue ("queue_full")
// within the same budget.
//
// Only "rejected" is not retryable: everything else is the broker, the
// network or the deployment, and an event kept for later is never lost.
// The kinds are also the result label of kafka_producer_records_total, and
// the same as nodejs-basics' KAFKA_SEND_ERRORS. Each driver maps its client's
// error codes onto them: README "Errors" is the table of record for which
// code is which kind.
const (
	// KindQueueFull: the local send queue is full (KAFKA_PRODUCER_QUEUE_MAX_*):
	// the broker is slower than the app.
	KindQueueFull = "queue_full"
	// KindUnavailable: brokers, leaders or replicas unavailable, or not
	// acknowledged within KAFKA_PUBLISH_TIMEOUT.
	KindUnavailable = "unavailable"
	// KindNotProvisioned: the topic does not exist (and the broker does not
	// auto-create it) or the credentials / ACLs refuse the write — the
	// deployment, not the record.
	KindNotProvisioned = "not_provisioned"
	// KindFatal: the producer failed fatally (idempotence state) and is
	// being replaced.
	KindFatal = "fatal"
	// KindRejected: the record itself — larger than KAFKA_MESSAGE_MAX_BYTES,
	// invalid, refused by a broker policy, or an invalid topic name.
	KindRejected = "rejected"
	// KindUnknown: anything not listed. Retryable, so an event is kept
	// rather than lost.
	KindUnknown = "unknown"
)

// SendError is the one error type [Producer.Send] returns.
type SendError struct {
	// Kind is one of the Kind* constants.
	Kind string
	// Retryable is false only for [KindRejected]: the same record fails the
	// same way.
	Retryable bool
	// Code is the client's error code (librdkafka's, or the Kafka protocol
	// code for franz-go), 0 when there is none.
	Code int
	Err  error
}

func (e *SendError) Error() string { return fmt.Sprintf("kafka send failed (%s): %v", e.Kind, e.Err) }
func (e *SendError) Unwrap() error { return e.Err }

// NewSendError builds the error a driver returns for a failed send. A
// spent or cancelled context is always [KindUnavailable], whatever kind the
// driver guessed: the record was not refused, the wait ran out.
func NewSendError(kind string, code int, err error) *SendError {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		kind = KindUnavailable
	}

	return &SendError{Kind: kind, Retryable: kind != KindRejected, Code: code, Err: err}
}

// Rejected reports whether a [Producer.Send] error is the record's own
// fault: the broker (or the client, before sending) refused this record and
// will refuse it again — too large or malformed. Retrying it is pointless;
// an outbox counts it towards giving up on the row. Everything else —
// timeouts, a broker that is down, a topic not provisioned yet, a credential
// problem — is an outage of the path, not of the record: it fails every
// record alike and passes once fixed, so it is retried as is.
func Rejected(err error) bool {
	var se *SendError
	return errors.As(err, &se) && !se.Retryable
}

// brokerSendErrorKinds is the README "Errors" table for the errors a broker
// answers with: Kafka protocol error code → kind.
var brokerSendErrorKinds = map[int16]string{ //nolint:gochecknoglobals // read-only lookup table (Go has no const maps)
	5:  KindUnavailable, // LEADER_NOT_AVAILABLE
	6:  KindUnavailable, // NOT_LEADER_OR_FOLLOWER
	7:  KindUnavailable, // REQUEST_TIMED_OUT
	13: KindUnavailable, // NETWORK_EXCEPTION
	19: KindUnavailable, // NOT_ENOUGH_REPLICAS
	20: KindUnavailable, // NOT_ENOUGH_REPLICAS_AFTER_APPEND
	56: KindUnavailable, // KAFKA_STORAGE_ERROR
	89: KindUnavailable, // THROTTLING_QUOTA_EXCEEDED

	3:  KindNotProvisioned, // UNKNOWN_TOPIC_OR_PARTITION
	29: KindNotProvisioned, // TOPIC_AUTHORIZATION_FAILED
	31: KindNotProvisioned, // CLUSTER_AUTHORIZATION_FAILED
	33: KindNotProvisioned, // UNSUPPORTED_SASL_MECHANISM
	34: KindNotProvisioned, // ILLEGAL_SASL_STATE
	53: KindNotProvisioned, // TRANSACTIONAL_ID_AUTHORIZATION_FAILED
	58: KindNotProvisioned, // SASL_AUTHENTICATION_FAILED

	2:  KindRejected, // CORRUPT_MESSAGE
	10: KindRejected, // MESSAGE_TOO_LARGE
	17: KindRejected, // INVALID_TOPIC_EXCEPTION
	18: KindRejected, // RECORD_LIST_TOO_LARGE
	44: KindRejected, // POLICY_VIOLATION
	87: KindRejected, // INVALID_RECORD
}

// BrokerSendErrorKind is the kind of a send that a broker refused with the
// given Kafka protocol error code, [KindUnknown] for a code the table does
// not name. Both drivers classify a broker's answer here, so it is the same
// kind whichever client is linked; a driver's own map holds only the errors
// its client raises locally.
func BrokerSendErrorKind(code int16) string {
	if kind, ok := brokerSendErrorKinds[code]; ok {
		return kind
	}

	return KindUnknown
}
