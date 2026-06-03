package resilient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkCircuitBreaker_Allow(b *testing.B) {
	cb := NewCircuitBreaker(0.5, 10, 10*time.Second, 30*time.Second)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if cb.Allow() {
				cb.RecordSuccess()
			}
		}
	})
}

func BenchmarkFullJitter(b *testing.B) {
	for b.Loop() {
		_ = FullJitter(5, 100*time.Millisecond, 30*time.Second)
	}
}

func BenchmarkAdaptive_AcquireRelease(b *testing.B) {
	l := NewAdaptiveLimiter(256, 1, 1024)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = l.Acquire(ctx)
			l.OnSuccess()
			l.Release()
		}
	})
}

func BenchmarkInMemoryCache_GetHit(b *testing.B) {
	c := NewInMemoryCache(1024, time.Minute)
	ctx := context.Background()
	c.Set(ctx, "k", CachedResponse{Status: 200, Body: []byte("payload")}, time.Minute)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get(ctx, "k")
		}
	})
}

func BenchmarkSend(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.OutboundTargets = []TargetConfig{{Name: "t", RateLimit: 1_000_000}}
	c, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	req := Request{Target: "t", URL: srv.URL}

	b.ResetTimer()
	for b.Loop() {
		resp, err := c.Send(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
