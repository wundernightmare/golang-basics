package resilient

import (
	"context"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptive_AdditiveIncreaseUpToMax(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		l := NewAdaptiveLimiter(2, 1, 5)
		for range 20 {
			require.NoError(t, l.Acquire(context.Background()))
			l.OnSuccess()
			l.Release()
		}
		assert.Equal(t, 5, l.CurrentLimit())
	}, "resilient-http-client", "unit")
}

func TestAdaptive_MultiplicativeDecreaseToMin(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		l := NewAdaptiveLimiter(8, 2, 16)

		require.NoError(t, l.Acquire(context.Background()))
		l.OnFailure()
		l.Release()
		assert.Equal(t, 4, l.CurrentLimit())

		require.NoError(t, l.Acquire(context.Background()))
		l.OnFailure()
		l.Release()
		assert.Equal(t, 2, l.CurrentLimit())

		require.NoError(t, l.Acquire(context.Background()))
		l.OnFailure()
		l.Release()
		assert.Equal(t, 2, l.CurrentLimit(), "floored at min")
	}, "resilient-http-client", "unit")
}

func TestAdaptive_GatesConcurrencyAtLimit(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		l := NewAdaptiveLimiter(2, 1, 10)
		require.NoError(t, l.Acquire(context.Background()))
		require.NoError(t, l.Acquire(context.Background()))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		err := l.Acquire(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "third acquire should block then time out")
	}, "resilient-http-client", "unit")
}

func TestAdaptive_WaiterAdmittedOnRelease(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		l := NewAdaptiveLimiter(1, 1, 4)
		require.NoError(t, l.Acquire(context.Background()))

		admitted := make(chan struct{})
		go func() {
			_ = l.Acquire(context.Background())
			close(admitted)
		}()

		// The waiter is blocked until we release.
		select {
		case <-admitted:
			t.Fatal("waiter admitted before release")
		case <-time.After(20 * time.Millisecond):
		}

		l.Release()
		select {
		case <-admitted:
		case <-time.After(time.Second):
			t.Fatal("waiter not admitted after release")
		}
	}, "resilient-http-client", "unit")
}

func TestAdaptive_CancelledWaiterDoesNotLeakSlot(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		l := NewAdaptiveLimiter(1, 1, 4)
		require.NoError(t, l.Acquire(context.Background())) // saturate

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		require.Error(t, l.Acquire(ctx)) // times out while queued

		// After releasing the one held slot, a fresh acquire must succeed promptly:
		// the cancelled waiter must not have consumed it.
		l.Release()
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		assert.NoError(t, l.Acquire(ctx2))
	}, "resilient-http-client", "unit")
}

func TestAdaptive_ClampsInitial(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		assert.Equal(t, 5, NewAdaptiveLimiter(100, 1, 5).CurrentLimit())
		assert.Equal(t, 2, NewAdaptiveLimiter(1, 2, 5).CurrentLimit())
	}, "resilient-http-client", "unit")
}
