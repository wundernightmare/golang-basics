package valkey

import valkeygo "github.com/valkey-io/valkey-go"

// ClientOption exposes the Config → valkey-go translation to the tests.
func (c Config) ClientOption() (valkeygo.ClientOption, error) { return c.clientOption() }

// NewBounded exposes the PoolSize guard to the tests.
func NewBounded(client valkeygo.Client, size int) valkeygo.Client { return newBounded(client, size) }
