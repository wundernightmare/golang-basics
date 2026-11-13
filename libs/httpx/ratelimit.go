package httpx

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

// rateLimiter is the server's inbound limit: a token bucket for all requests
// and, optionally, one per client address. It protects the service itself —
// the last line, not the first: an ingress or a gateway should shed load
// before it gets here, and a per-tenant quota belongs in the service's own
// middleware, behind its authenticator.
//
// A request over the limit is answered 429 with Retry-After, and it is still
// traced, logged and counted by the middleware around this one.
type rateLimiter struct {
	global *rate.Limiter // nil: no global limit

	perClient rate.Limit // 0: no per-client limit
	burst     int
	maxKeys   int

	mu      sync.Mutex
	clients map[string]*rate.Limiter

	limited *prometheus.CounterVec
}

// The scopes of http_rate_limited_total.
const (
	scopeGlobal = "global"
	scopeClient = "client"
)

func bucketDepth(limit float64, burst int) int {
	if burst > 0 {
		return burst
	}

	return int(max(1, math.Ceil(limit)))
}

// newRateLimiter returns nil when cfg sets no limit.
func newRateLimiter(cfg Config, reg prometheus.Registerer) *rateLimiter {
	if cfg.RateLimit <= 0 && cfg.RateLimitPerClient <= 0 {
		return nil
	}

	limiter := &rateLimiter{
		perClient: rate.Limit(cfg.RateLimitPerClient),
		burst:     bucketDepth(cfg.RateLimitPerClient, cfg.RateBurstPerClient),
		maxKeys:   cfg.RateClientsMax,
		clients:   map[string]*rate.Limiter{},
		limited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_rate_limited_total",
			Help: "Requests answered 429 by the server's own rate limit, by scope (global, client).",
		}, []string{"scope"}),
	}
	if cfg.RateLimit > 0 {
		limiter.global = rate.NewLimiter(rate.Limit(cfg.RateLimit), bucketDepth(cfg.RateLimit, cfg.RateBurst))
	}

	reg.MustRegister(limiter.limited)

	return limiter
}

// client returns the bucket of one client address, or nil when the table is
// full of clients that are all in use — then the request is shed: a table
// that grows with the number of addresses is the attack.
func (l *rateLimiter) client(key string, now time.Time) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lim, ok := l.clients[key]; ok {
		return lim
	}

	if len(l.clients) >= l.maxKeys {
		// A bucket that has refilled is indistinguishable from a new one:
		// forgetting it changes nothing for its client.
		for k, lim := range l.clients {
			if lim.TokensAt(now) >= float64(l.burst) {
				delete(l.clients, k)
			}
		}

		if len(l.clients) >= l.maxKeys {
			return nil
		}
	}

	lim := rate.NewLimiter(l.perClient, l.burst)
	l.clients[key] = lim

	return lim
}

// take asks lim for one token now; when there is none it reports how long
// until there is, and takes nothing.
func take(lim *rate.Limiter, now time.Time) (ok bool, wait time.Duration) {
	reservation := lim.ReserveN(now, 1)
	if !reservation.OK() {
		return false, time.Second
	}

	if wait = reservation.DelayFrom(now); wait > 0 {
		reservation.CancelAt(now)

		return false, wait
	}

	return true, 0
}

func (l *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()

		// The client's own bucket first: a client over its share must not
		// spend the tokens everyone else lives on.
		if l.perClient > 0 {
			lim := l.client(ClientIPFromContext(r.Context()), now)
			if lim == nil {
				l.reject(w, r, scopeClient, time.Second)

				return
			}

			if ok, wait := take(lim, now); !ok {
				l.reject(w, r, scopeClient, wait)

				return
			}
		}

		if l.global != nil {
			if ok, wait := take(l.global, now); !ok {
				l.reject(w, r, scopeGlobal, wait)

				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func (l *rateLimiter) reject(w http.ResponseWriter, r *http.Request, scope string, wait time.Duration) {
	l.limited.WithLabelValues(scope).Inc()
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	writeProblem(w, r, NewProblem(http.StatusTooManyRequests, "rate limit exceeded; retry after the time in Retry-After"))
}
