package resilient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkBreaker_AllowRecord(b *testing.B) {
	tc := DefaultTarget("t")
	br := newBreaker(&tc, time.Now, nil)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if tk, ok := br.allow(); ok {
				br.record(tk, outcomeSuccess)
			}
		}
	})
}

func BenchmarkSend(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	c, err := New(DefaultConfig("api"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Shutdown(context.Background()) }()
	req, err := http.NewRequestWithContext(b.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		resp, err := c.Send("api", req)
		if err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
