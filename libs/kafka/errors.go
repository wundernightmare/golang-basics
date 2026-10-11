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
// The kinds are also the result label of kafka_publish_total. Each driver
// maps its client's error codes onto them (the same codes Node's
// KAFKA_SEND_ERRORS lists).
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
