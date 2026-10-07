package resilient

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// newTransport clones http.DefaultTransport — proxy from the environment,
// HTTP/2, dial and TLS timeouts — and overrides only the pool settings and
// the HTTP/2 health check.
func newTransport(cfg *Config) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the stdlib's documented type
	t.MaxIdleConns = cfg.MaxIdleConns
	t.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	t.MaxConnsPerHost = cfg.MaxConnsPerHost
	t.IdleConnTimeout = cfg.IdleConnTimeout
	t.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
	if cfg.HTTP2PingInterval > 0 {
		t.HTTP2 = &http.HTTP2Config{SendPingTimeout: cfg.HTTP2PingInterval, PingTimeout: cfg.HTTP2PingTimeout}
	}
	return t
}

// instrument wraps rt so every attempt injects traceparent and records a
// client span named "METHOD target".
func instrument(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return otelhttp.NewTransport(rt, otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		target, _ := r.Context().Value(targetKey{}).(string)
		return r.Method + " " + target
	}))
}

// targetKey carries the target name to the span-name formatter.
type targetKey struct{}

// redirectError is a redirect the policy refused.
type redirectError struct{ msg string }

func (e *redirectError) Error() string { return e.msg }

// redirectPolicy follows at most maxRedirects redirects, and only to the host
// (and no weaker scheme) of the original request unless crossHost. next, the
// caller's own CheckRedirect, runs after it.
func redirectPolicy(maxRedirects int, crossHost bool, next func(*http.Request, []*http.Request) error) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if maxRedirects == 0 {
			return http.ErrUseLastResponse
		}
		if len(via) > maxRedirects {
			return &redirectError{msg: fmt.Sprintf("stopped after %d redirects", maxRedirects)}
		}
		orig := via[0].URL
		if !crossHost {
			if !strings.EqualFold(req.URL.Host, orig.Host) {
				return &redirectError{msg: "refused cross-host redirect to " + req.URL.Host}
			}
			if orig.Scheme == "https" && req.URL.Scheme != "https" {
				return &redirectError{msg: "refused https→http redirect"}
			}
		}
		if next != nil {
			return next(req, via)
		}
		return nil
	}
}

func isRedirectError(err error) bool {
	var re *redirectError
	return errors.As(err, &re)
}
