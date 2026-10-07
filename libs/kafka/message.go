package kafka

import (
	"context"
	"errors"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- retry-backoff jitter is not security-sensitive; a CSPRNG (crypto/rand) is unnecessary here.
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header is one record header. Header keys the libs and services agree on:
//
//	event_id      unique id of the event (the consumer's idempotency key and request_id)
//	event_type    e.g. task.created — what the consumer dispatches on
//	content_type  e.g. application/json
//	traceparent   W3C trace context (written by the producer, see [Producer.Publish])
type Header struct {
	Key   string
	Value []byte
}

// Well-known header keys.
const (
	HeaderEventID     = "event_id"
	HeaderEventType   = "event_type"
	HeaderContentType = "content_type"
	HeaderTraceparent = "traceparent"
)

// Dead-letter header keys, added to a record parked on the DLQ topic next to
// its original headers.
const (
	HeaderDLQReason          = "dlq_reason"           // "permanent" | "retries_exhausted"
	HeaderDLQError           = "dlq_error"            // the last handler error, truncated
	HeaderDLQSourceTopic     = "dlq_source_topic"     // where the record was consumed from
	HeaderDLQSourcePartition = "dlq_source_partition" // decimal
	HeaderDLQSourceOffset    = "dlq_source_offset"    // decimal
	HeaderDLQAttempts        = "dlq_attempts"         // decimal: handler invocations made
)

// Message is one consumed record, handed to a [Handler]. It exposes the fields a
// service handler usually needs without leaking the franz-go record type.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
	Headers   []Header
	Timestamp time.Time
	// Attempt is 1 on the first delivery to the handler and grows with each
	// retry of the same record within this process.
	Attempt int
}

// Header returns the value of the first header named key, and whether it was
// present.
func (m Message) Header(key string) ([]byte, bool) {
	for _, h := range m.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

// Handler processes a single [Message]. Returning nil lets the consumer commit
// the record's offset. Returning an error retries the record with backoff;
// wrap it with [Permanent] to skip the retries (a record that can never
// succeed, such as an undecodable payload). See [Consumer.Run] for what
// happens once retries run out.
//
// ctx carries the record's "process" span, continued from the trace the
// producer put in the record headers, the event id as the request_id (see
// httpx.WithRequestID) and the per-attempt deadline: log with the *Context
// methods and pass ctx to downstream calls so the whole hop is one trace.
type Handler func(ctx context.Context, msg Message) error

// permanentError marks a handler error that retrying cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the consumer does not retry the record: it goes to
// the dead-letter topic straight away (or stops the consumer when the DLQ is
// disabled). Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked with
// [Permanent].
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// backoff returns the delay before retry number n (0-based): initial·2ⁿ
// capped at maxDelay, with "equal jitter" (half fixed, half random) so a
// fleet of consumers retrying the same outage does not retry in lockstep.
func backoff(n int, initial, maxDelay time.Duration) time.Duration {
	d := initial
	for i := 0; i < n && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1) //nolint:gosec // jitter, not a secret
}

// sleep waits for d or until ctx is done, reporting whether it slept fully.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func toMessage(r *kgo.Record, attempt int) Message {
	var hs []Header
	if len(r.Headers) > 0 {
		hs = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			hs[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	return Message{
		Topic: r.Topic, Key: r.Key, Value: r.Value, Partition: r.Partition, Offset: r.Offset,
		Headers: hs, Timestamp: r.Timestamp, Attempt: attempt,
	}
}

func headerValue(hs []kgo.RecordHeader, key string) (string, bool) {
	for _, h := range hs {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}
