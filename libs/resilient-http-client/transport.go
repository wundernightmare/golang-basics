package resilient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// newTransport builds the client's own transport. It does not clone
// http.DefaultTransport: that is a variable a process may have replaced —
// tracing agents wrap it — and New must not depend on what it holds. Proxy,
// dialer, HTTP/2 and the TLS and 100-continue timeouts are set as net/http
// sets them on its default transport (go1.27); the rest comes from cfg.
// roots, when not nil, is the whole trust store (a target's ca_file): the
// system's authorities are not consulted.
func newTransport(cfg *Config, roots *x509.CertPool) *http.Transport {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,

		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
	if roots != nil {
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}

	if cfg.HTTP2PingInterval > 0 {
		transport.HTTP2 = &http.HTTP2Config{SendPingTimeout: cfg.HTTP2PingInterval, PingTimeout: cfg.HTTP2PingTimeout}
	}

	return transport
}

// instrument wraps base so every attempt injects traceparent and records a
// client span named "METHOD target". A nil tp / prop leaves otelhttp's
// default: the global provider / propagator.
//
// otelhttp v0.72.0 has no option to redact the span's url.full and strips
// only the user info from it, so spanURL, which runs inside the span,
// replaces the attribute.
func instrument(base http.RoundTripper, tp trace.TracerProvider, prop propagation.TextMapPropagator) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	opts := []otelhttp.Option{otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		target, _ := r.Context().Value(targetKey{}).(string)
		return r.Method + " " + target
	})}
	if tp != nil {
		opts = append(opts, otelhttp.WithTracerProvider(tp))
	}

	if prop != nil {
		opts = append(opts, otelhttp.WithPropagators(prop))
	}

	return otelhttp.NewTransport(spanURL{base}, opts...)
}

// spanURL sets url.full of the client span it runs in to what a log line
// gets (logURL): a query string carries credentials.
type spanURL struct{ base http.RoundTripper }

func (t spanURL) RoundTrip(r *http.Request) (*http.Response, error) {
	trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("url.full", logURL(r.URL)))

	return t.base.RoundTrip(r)
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
