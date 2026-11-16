package valkey

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

// Replace writes value at key only when the key currently holds exactly old,
// and reports whether it did (SET … IFEQ, Valkey ≥ 8.1). It is how a refresh
// overwrites the value it started from: a tombstone ([Cache.Tombstone]) or a
// newer value written in between makes the comparison fail, so a refresh
// that raced an invalidation cannot resurrect stale data. A positive ttl is
// required.
func (c *Cache) Replace(ctx context.Context, key, old, value string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("valkey: replace %q: ttl must be positive", key)
	}

	err := c.do(ctx, "set", c.client.B().Set().Key(key).Value(value).Ifeq(old).Px(ttl).Build()).Error()
	switch {
	case valkeygo.IsValkeyNil(err):
		return false, nil // absent, tombstoned or changed: not written
	case err != nil:
		return false, fmt.Errorf("valkey: replace %q: %w", key, err)
	}

	return true, nil
}

// Publish sends message to every current subscriber of channel. Delivery is
// at most once: a subscriber that is disconnected at that moment never sees
// it (see [Cache.Subscribe]).
func (c *Cache) Publish(ctx context.Context, channel, message string) error {
	if err := c.do(ctx, "publish", c.client.B().Publish().Channel(channel).Message(message).Build()).Error(); err != nil {
		return fmt.Errorf("valkey: publish %q: %w", channel, err)
	}

	return nil
}

// Subscribe delivers the messages published on channel to onMessage until ctx
// is done, and returns ctx's error then (or the error that ended the
// subscription for good — [ErrClosed] when that was [Cache.Close]; nil if the
// channel was unsubscribed on a connection that stayed healthy).
//
// Pub/Sub is at most once, and Subscribe reconnects and resubscribes on its
// own for as long as ctx lives — pausing between attempts as
// resubscribeDelay says, from the shortest pause again after every
// subscription the server confirmed — so messages published while the
// connection was down are lost without any error here. onSubscribed is the signal for that: it runs every time the
// subscription is (re-)established — the first time included — and whatever
// the subscriber derived from earlier messages must be treated as incomplete
// from that point. Both callbacks run on the connection's read loop: they
// must be fast and must not call back into the cache.
func (c *Cache) Subscribe(ctx context.Context, channel string, onMessage func(message string), onSubscribed func()) error {
	// failed counts the attempts since the server last confirmed the
	// subscription, so that every outage starts from the shortest pause.
	// Atomic: the hook runs on the connection's read loop, the loop below
	// on the caller's goroutine.
	var failed atomic.Int32

	ctx = valkeygo.WithOnSubscriptionHook(ctx, func(sub valkeygo.PubSubSubscription) {
		if sub.Kind != "subscribe" {
			return
		}

		failed.Store(0)

		if onSubscribed != nil {
			onSubscribed()
		}
	})

	// UPSTREAM-WORKAROUND(valkey-go ≤ 1.0.79): the resubscription is this
	// loop, not the library's retry, which clientOption turns off. Each
	// attempt builds its own command, so nothing is shared with a connection
	// that failed (docs/UPSTREAM-WORKAROUNDS.md).
	for {
		err := c.client.Receive(ctx, c.client.B().Subscribe().Channel(channel).Build(), func(msg valkeygo.PubSubMessage) {
			onMessage(msg.Message)
		})
		// nil: the library closed the message stream on a connection without
		// an error, which is what an unsubscribe confirmed by the server does
		// (valkey-go 1.0.79, pipe.Receive). No failure to recover from.
		if err == nil {
			return nil
		}

		// For good: the caller is done, the cache was closed, or the server
		// answered and said no (ACL, a wrong mode) — asking again changes
		// nothing.
		if c.closed.Load() && !errors.Is(err, valkeygo.ErrClosing) {
			// Close can also surface as the connection's own error.
			return fmt.Errorf("valkey: subscribe %q: %w: %w", channel, ErrClosed, err)
		}

		if _, refused := valkeygo.IsValkeyErr(err); refused || ctx.Err() != nil || errors.Is(err, valkeygo.ErrClosing) {
			return fmt.Errorf("valkey: subscribe %q: %w", channel, closedErr(err))
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("valkey: subscribe %q: %w", channel, ctx.Err())
		case <-time.After(resubscribeDelay(int(failed.Add(1)) - 1)):
		}
	}
}

// resubscribeDelay is the pause after the failed attempt number attempt
// (from 0) of one outage: 50ms doubling to one second — quick after a blip,
// unhurried against a server that stays down.
func resubscribeDelay(attempt int) time.Duration {
	const (
		first = 50 * time.Millisecond
		limit = time.Second
	)

	if attempt >= 5 { // 50ms << 5 is already past the limit
		return limit
	}

	return min(first<<attempt, limit)
}
