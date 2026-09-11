package pgx

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Collectors returns Prometheus collectors over the pool's live statistics
// ([pgxpool.Pool.Stat]), for the service to register on its metrics registry:
//
//	pgxpool_conns{state="idle|acquired|constructing"}   connections by state
//	pgxpool_max_conns                                    configured ceiling
//	pgxpool_acquire_total                                successful acquires
//	pgxpool_acquire_duration_seconds_total               time spent waiting to acquire
//	pgxpool_empty_acquire_total                          acquires that had to wait for a free conn
//	pgxpool_canceled_acquire_total                       acquires cancelled by context
//
// pgxpool_empty_acquire_total climbing while pgxpool_conns{state="acquired"}
// sits at pgxpool_max_conns is the "pool exhausted" signal; the acquire
// duration counter turns it into seconds of wait per second.
func (db *DB) Collectors() []prometheus.Collector {
	stat := db.pool.Stat
	gauge := func(name, help string, f func() float64, labels prometheus.Labels) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, f)
	}
	counter := func(name, help string, f func() float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, f)
	}
	const conns, connsHelp = "pgxpool_conns", "Connections in the pool by state."
	return []prometheus.Collector{
		gauge(conns, connsHelp, func() float64 { return float64(stat().IdleConns()) }, prometheus.Labels{"state": "idle"}),
		gauge(conns, connsHelp, func() float64 { return float64(stat().AcquiredConns()) }, prometheus.Labels{"state": "acquired"}),
		gauge(conns, connsHelp, func() float64 { return float64(stat().ConstructingConns()) }, prometheus.Labels{"state": "constructing"}),
		gauge("pgxpool_max_conns", "Configured maximum pool size.", func() float64 { return float64(stat().MaxConns()) }, nil),
		counter("pgxpool_acquire_total", "Successful connection acquires.", func() float64 { return float64(stat().AcquireCount()) }),
		counter("pgxpool_acquire_duration_seconds_total", "Cumulative time spent acquiring connections.",
			func() float64 { return stat().AcquireDuration().Seconds() }),
		counter("pgxpool_empty_acquire_total", "Acquires that waited because the pool was empty.",
			func() float64 { return float64(stat().EmptyAcquireCount()) }),
		counter("pgxpool_canceled_acquire_total", "Acquires cancelled by their context while waiting.",
			func() float64 { return float64(stat().CanceledAcquireCount()) }),
	}
}
