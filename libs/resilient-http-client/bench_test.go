package resilient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkSend(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	c, err := New(DefaultConfig("api"))
	if err != nil {
		b.Fatal(err)
	}

	defer func() { _ = c.Shutdown(context.Background()) }()

	req, err := http.NewRequestWithContext(b.Context(), http.MethodGet, srv.URL, http.NoBody)
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
