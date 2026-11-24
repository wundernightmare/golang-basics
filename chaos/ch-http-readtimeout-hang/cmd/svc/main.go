// svc — a minimal replica of the production service from the incident:
//
//   - pgx pool of ONE connection to PgBouncer (transaction pooling), no timeouts;
//   - clickhouse-go (official) over HTTP, pool of ONE connection, ReadTimeout set;
//   - a worker that takes a Postgres lock and, while holding it, talks to ClickHouse
//     with context.Background();
//   - /readyz = SELECT 1 against both databases through the same pools.
//
// With -op-timeout > 0 every worker iteration gets a deadline (the fix).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"chaoslab/chfix"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const lockKey = 4242

type service struct {
	name      string
	pg        *pgxpool.Pool
	ch        driver.Conn
	job       string
	opTimeout time.Duration
	phase     atomic.Value // string: where the worker is right now
	phaseAt   atomic.Int64
	runs      atomic.Int64
	log       *slog.Logger
}

func (s *service) setPhase(p string) {
	s.phase.Store(p)
	s.phaseAt.Store(time.Now().UnixNano())
}

// iteration is the production code path: lock in Postgres, work in ClickHouse, commit.
func (s *service) iteration(ctx context.Context) error {
	s.setPhase("pg-begin")
	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		// Rollback must not inherit an already-expired deadline.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rctx)
	}()

	s.setPhase("pg-wait-lock")
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return fmt.Errorf("lock: %w", err)
	}

	switch s.job {
	case "insert":
		s.setPhase("ch-insert")
		batch, err := s.ch.PrepareBatch(ctx, "INSERT INTO chaos.sink (s)")
		if err != nil {
			return fmt.Errorf("ch prepare: %w", err)
		}
		payload := strings.Repeat("x", 1024)
		for range 48000 { // ~48 MiB request body
			if err := batch.Append(payload); err != nil {
				return fmt.Errorf("ch append: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("ch send: %w", err)
		}
	case "stream":
		s.setPhase("ch-query")
		rows, err := s.ch.Query(ctx, "SELECT number, randomString(1024) FROM system.numbers LIMIT 400000")
		if err != nil {
			return fmt.Errorf("ch query: %w", err)
		}
		s.setPhase("ch-stream")
		for rows.Next() {
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("ch rows: %w", err)
		}
	}

	s.setPhase("pg-commit")
	if _, err := tx.Exec(ctx, "UPDATE chaos_job SET runs = runs + 1, last_by = $1, last_at = now() WHERE id = 1", s.name); err != nil {
		return fmt.Errorf("update: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *service) worker() {
	for {
		ctx, cancel := context.Background(), context.CancelFunc(func() {}) // prod: no timeouts anywhere
		if s.opTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, s.opTimeout)
		}
		start := time.Now()
		err := s.iteration(ctx)
		cancel()
		if err != nil {
			s.log.Error("iteration failed", "err", err, "took", time.Since(start).Round(time.Millisecond))
		} else {
			s.runs.Add(1)
		}
		s.setPhase("idle")
		time.Sleep(1500 * time.Millisecond)
	}
}

type check struct {
	OK   bool   `json:"ok"`
	Took string `json:"took"`
	Err  string `json:"err,omitempty"`
}

func timed(f func() error) check {
	start := time.Now()
	err := f()
	c := check{OK: err == nil, Took: time.Since(start).Round(time.Millisecond).String()}
	if err != nil {
		c.Err = err.Error()
	}
	return c
}

// readyz: SELECT 1 on both databases, through the very same single-connection pools.
func (s *service) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second) // probe timeoutSeconds: 1
	defer cancel()
	pgCh, chCh := make(chan check, 1), make(chan check, 1)
	go func() {
		pgCh <- timed(func() error {
			var one int
			return s.pg.QueryRow(ctx, "SELECT 1").Scan(&one)
		})
	}()
	go func() {
		chCh <- timed(func() error {
			var one uint8
			return s.ch.QueryRow(ctx, "SELECT 1").Scan(&one)
		})
	}()
	out := map[string]any{"postgres": <-pgCh, "clickhouse": <-chCh}
	ready := out["postgres"].(check).OK && out["clickhouse"].(check).OK
	out["ready"] = ready
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *service) state(w http.ResponseWriter, _ *http.Request) {
	phase, _ := s.phase.Load().(string)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": s.name, "phase": phase, "runs": s.runs.Load(),
		"in_phase_s": time.Since(time.Unix(0, s.phaseAt.Load())).Round(100 * time.Millisecond).Seconds(),
	})
}

func run() error {
	name := flag.String("name", "a", "replica name")
	addr := flag.String("addr", "127.0.0.1:28501", "admin listen address")
	pgDSN := flag.String("pg", "postgres://postgres@127.0.0.1:28301/app", "PgBouncer DSN")
	chAddr := flag.String("ch", "127.0.0.1:28302", "ClickHouse HTTP address")
	job := flag.String("job", "insert", "insert|stream: what the worker does in ClickHouse under the PG lock")
	opTimeout := flag.Duration("op-timeout", 0, "deadline per worker iteration; 0 = none (as in prod)")
	chFix := flag.Bool("ch-fix", false, "apply chfix: idle read/write deadlines on the ClickHouse connection")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("replica", *name)

	cfg, err := pgxpool.ParseConfig(*pgDSN)
	if err != nil {
		return err
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // PgBouncer transaction pooling
	pg, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer pg.Close()

	chOpt := &clickhouse.Options{
		Addr:         []string{*chAddr},
		Protocol:     clickhouse.HTTP,
		Auth:         clickhouse.Auth{Database: "default", Username: "default"},
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second, // "we have a read timeout, so it cannot hang"
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
	if *chFix {
		chfix.Apply(chOpt)
	}
	ch, err := clickhouse.Open(chOpt)
	if err != nil {
		return err
	}
	defer ch.Close()

	s := &service{name: *name, pg: pg, ch: ch, job: *job, opTimeout: *opTimeout, log: log}
	s.setPhase("starting")
	go s.worker()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /state", s.state)
	mux.HandleFunc("GET /stacks", func(w http.ResponseWriter, _ *http.Request) {
		buf := make([]byte, 4<<20)
		_, _ = w.Write(buf[:runtime.Stack(buf, true)])
	})
	log.Info("listening", "addr", *addr, "job", *job, "op_timeout", *opTimeout)
	return (&http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
