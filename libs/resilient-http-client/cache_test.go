package resilient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryCache_MissReturnsFalse(t *testing.T) {
	c := NewInMemoryCache(100, time.Minute)
	_, ok := c.Get(context.Background(), "nope")
	assert.False(t, ok)
}

func TestInMemoryCache_SetThenGet(t *testing.T) {
	c := NewInMemoryCache(100, time.Minute)
	c.Set(context.Background(), "k", CachedResponse{Status: 200, Body: []byte("hello")}, time.Minute)
	got, ok := c.Get(context.Background(), "k")
	require.True(t, ok)
	assert.Equal(t, 200, got.Status)
	assert.Equal(t, []byte("hello"), got.Body)
}

func TestInMemoryCache_StoresIndependentCopy(t *testing.T) {
	c := NewInMemoryCache(100, time.Minute)
	body := []byte("mutate-me")
	c.Set(context.Background(), "k", CachedResponse{Status: 200, Body: body}, time.Minute)
	body[0] = 'X' // mutate the caller's buffer after storing

	got, ok := c.Get(context.Background(), "k")
	require.True(t, ok)
	assert.Equal(t, []byte("mutate-me"), got.Body, "cache must own an independent copy")
}

func TestInMemoryCache_Expiry(t *testing.T) {
	c := NewInMemoryCache(100, time.Minute)
	c.Set(context.Background(), "k", CachedResponse{Status: 200, Body: []byte("x")}, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	_, ok := c.Get(context.Background(), "k")
	assert.False(t, ok, "expired entry should be a miss")
}

func TestInMemoryCache_LRUEviction(t *testing.T) {
	c := NewInMemoryCache(2, time.Minute)
	ctx := context.Background()
	c.Set(ctx, "a", CachedResponse{Status: 200, Body: []byte("a")}, time.Minute)
	c.Set(ctx, "b", CachedResponse{Status: 200, Body: []byte("b")}, time.Minute)
	// Touch "a" so "b" becomes least-recently-used.
	_, _ = c.Get(ctx, "a")
	c.Set(ctx, "c", CachedResponse{Status: 200, Body: []byte("c")}, time.Minute)

	assert.Equal(t, 2, c.Len())
	_, okA := c.Get(ctx, "a")
	_, okB := c.Get(ctx, "b")
	_, okC := c.Get(ctx, "c")
	assert.True(t, okA)
	assert.False(t, okB, "b was LRU and should have been evicted")
	assert.True(t, okC)
}

func TestInMemoryCache_DefaultTTLWhenNonPositive(t *testing.T) {
	c := NewInMemoryCache(10, 20*time.Millisecond)
	c.Set(context.Background(), "k", CachedResponse{Status: 200, Body: []byte("x")}, 0)
	_, ok := c.Get(context.Background(), "k")
	assert.True(t, ok)
	time.Sleep(40 * time.Millisecond)
	_, ok = c.Get(context.Background(), "k")
	assert.False(t, ok, "should expire per the default TTL")
}
