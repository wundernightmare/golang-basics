package valkey

import (
	"context"
	"fmt"
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
// subscription for good).
//
// Pub/Sub is at most once, and the client reconnects and resubscribes on its
// own, so messages published while the connection was down are lost without
// any error here. onSubscribed is the signal for that: it runs every time the
// subscription is (re-)established — the first time included — and whatever
// the subscriber derived from earlier messages must be treated as incomplete
// from that point. Both callbacks run on the connection's read loop: they
// must be fast and must not call back into the cache.
func (c *Cache) Subscribe(ctx context.Context, channel string, onMessage func(message string), onSubscribed func()) error {
	ctx = valkeygo.WithOnSubscriptionHook(ctx, func(sub valkeygo.PubSubSubscription) {
		if sub.Kind == "subscribe" && onSubscribed != nil {
			onSubscribed()
		}
	})

	err := c.client.Receive(ctx, c.client.B().Subscribe().Channel(channel).Build(), func(msg valkeygo.PubSubMessage) {
		onMessage(msg.Message)
	})
	if err != nil {
		return fmt.Errorf("valkey: subscribe %q: %w", channel, err)
	}

	return nil
}
