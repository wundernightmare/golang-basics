package valkey

import (
	"context"
	"errors"

	valkeygo "github.com/valkey-io/valkey-go"
)

// blockingPipeline is the pipeline length from which the library moves a
// DoMulti to a dedicated connection (its DefaultBlockingPipeline, set
// explicitly so the guard below and the library agree on it).
const blockingPipeline = valkeygo.DefaultBlockingPipeline

// bounded makes Config.PoolSize a bound that holds. The library's own pool
// counter does not: a caller whose context ends while it waits for a
// dedicated connection — or that arrives with a context already ended — is
// handed a placeholder that was never counted, and returning it decrements
// the pool's size all the same (valkey-go ≤ 1.0.79, pool.Acquire / Store).
// Every such caller raises the effective limit by one for the life of the
// client, so under the very overload the limit exists for — callers timing
// out in the queue — it stops limiting.
// TestUpstreamPoolLimitLeaksOnACancelledWaiter pins that behaviour; when it
// fails after an upgrade, this type can go.
//
// The guard is a semaphore in front of everything that takes a dedicated
// connection: at most PoolSize callers are inside the library's pool at
// once, so it never has a queue to mis-count, and a caller waits here
// instead, leaving when its context ends. Ordinary pipelined commands pass
// straight through.
//
// DoStream / DoMultiStream are refused ([ErrStreamUnsupported]): they hold
// a connection of the library's second pool — same size, same flaw — until
// the caller drains the returned stream, which is a struct this wrapper can
// neither wrap nor observe, so it could not tell when to free the slot.
// Passing them through would leave a second, leaking PoolSize next to the
// one enforced here.
type bounded struct {
	valkeygo.Client

	slots chan struct{}
}

// ErrStreamUnsupported is the error of every stream returned by DoStream /
// DoMultiStream of [Cache.Client]: a streaming call would hold a connection
// outside the PoolSize bound. Read the value with an ordinary command, or
// build a separate valkey-go client for streaming.
var ErrStreamUnsupported = errors.New("valkey: DoStream / DoMultiStream are not available through this client (they would escape pool_size)")

func newBounded(client valkeygo.Client, size int) *bounded {
	return &bounded{Client: client, slots: make(chan struct{}, size)}
}

// acquire takes a slot, or returns ctx's error without taking one.
func (b *bounded) acquire(ctx context.Context) error {
	// An ended context loses even when a slot is free: select picks at
	// random among ready cases, and the library must never see this call.
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case b.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bounded) release() { <-b.slots }

func (b *bounded) Do(ctx context.Context, cmd valkeygo.Completed) valkeygo.ValkeyResult {
	if !cmd.IsBlock() {
		return b.Client.Do(ctx, cmd)
	}

	if err := b.acquire(ctx); err != nil {
		return valkeygo.NewErrorResult(err)
	}
	defer b.release()

	return b.Client.Do(ctx, cmd)
}

func (b *bounded) DoMulti(ctx context.Context, multi ...valkeygo.Completed) []valkeygo.ValkeyResult {
	if !dedicated(multi) {
		return b.Client.DoMulti(ctx, multi...)
	}

	if err := b.acquire(ctx); err != nil {
		out := make([]valkeygo.ValkeyResult, len(multi))
		for i := range out {
			out[i] = valkeygo.NewErrorResult(err)
		}

		return out
	}
	defer b.release()

	return b.Client.DoMulti(ctx, multi...)
}

// dedicated reports whether the library will run this pipeline on a
// dedicated connection: one of its commands blocks, or it is long.
func dedicated(multi []valkeygo.Completed) bool {
	if len(multi) >= blockingPipeline {
		return true
	}

	for i := range multi {
		if multi[i].IsBlock() {
			return true
		}
	}

	return false
}

// Dedicated and Dedicate have no context in the library's API: like there,
// the caller waits for a connection as long as it takes.
func (b *bounded) Dedicated(fn func(valkeygo.DedicatedClient) error) error {
	b.slots <- struct{}{}
	defer b.release()

	return b.Client.Dedicated(fn)
}

func (b *bounded) Dedicate() (valkeygo.DedicatedClient, func()) {
	b.slots <- struct{}{}

	client, cancel := b.Client.Dedicate()

	return client, func() {
		cancel()
		b.release()
	}
}

// DoStream is refused: see [ErrStreamUnsupported].
func (b *bounded) DoStream(context.Context, valkeygo.Completed) valkeygo.ValkeyResultStream {
	return valkeygo.NewErrorResultStream(ErrStreamUnsupported)
}

// DoMultiStream is refused: see [ErrStreamUnsupported].
func (b *bounded) DoMultiStream(context.Context, ...valkeygo.Completed) valkeygo.MultiValkeyResultStream {
	return valkeygo.NewErrorResultStream(ErrStreamUnsupported)
}
