package resilience

import (
	"flag"
	"os"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Property tests: instead of an example and its expected answer, a rule that
// must hold for every sequence of calls — rapid generates the sequences and,
// when one breaks the rule, shrinks it to the shortest that still does.
// A failure prints how to replay it (`-rapid.failfile` / `-rapid.seed`).

// -short is how the unit layer runs, and rapid answers it with a fifth of
// the checks. These properties take milliseconds and their rarer sequences
// (a result arriving after the breaker tripped, recovered and tripped again)
// need the full count, so -short gets it too — unless the caller chose a
// number (-rapid.checks, RAPID_CHECKS).
func TestMain(m *testing.M) {
	flag.Parse()

	chosen := os.Getenv("RAPID_CHECKS") != ""

	flag.Visit(func(f *flag.Flag) { chosen = chosen || f.Name == "rapid.checks" })

	if testing.Short() && !chosen {
		if err := flag.Set("rapid.checks", "500"); err != nil { // rapid divides by five under -short
			panic(err)
		}
	}

	os.Exit(m.Run())
}

// event is one add to the window model: when, and how much.
type event struct {
	at   time.Time
	a, b int
}

// modelSum is the window written the obvious way — every event kept, the
// ones in the last windowSlots slots added up — to compare the ring buffer
// against.
func modelSum(events []event, slot time.Duration, now time.Time) (a, b int) {
	current := now.UnixNano() / int64(slot)

	for _, e := range events {
		if current-e.at.UnixNano()/int64(slot) < windowSlots {
			a += e.a
			b += e.b
		}
	}

	return a, b
}

// epoch is where the generated clocks start: any instant after 1970 — the
// window indexes its slots by Unix time.
func epoch(t *rapid.T) time.Time {
	return time.Unix(rapid.Int64Range(0, 4_000_000_000).Draw(t, "start"), 0)
}

// The ring of ten slots forgets exactly what has left the window: whatever
// the length, however time advances (not at all, by nanoseconds, by many
// windows at once), its totals equal those of a model that keeps every event.
func TestWindowEqualsItsModel(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		length := time.Duration(rapid.Int64Range(1, int64(time.Hour)).Draw(t, "length"))
		win := newWindow(length)
		now := epoch(t)

		var events []event

		t.Repeat(map[string]func(*rapid.T){
			"advance": func(t *rapid.T) {
				// Up to three windows: steps inside a slot, across slots and
				// past the whole ring are all reached.
				now = now.Add(time.Duration(rapid.Int64Range(0, 3*int64(length)).Draw(t, "by")))
			},
			"add": func(t *rapid.T) {
				a, b := rapid.IntRange(0, 5).Draw(t, "a"), rapid.IntRange(0, 5).Draw(t, "b")
				win.add(now, a, b)
				events = append(events, event{at: now, a: a, b: b})
			},
			"": func(t *rapid.T) { // checked after every step
				gotA, gotB := win.sum(now)
				wantA, wantB := modelSum(events, win.slot, now)

				if gotA != wantA || gotB != wantB {
					t.Fatalf("sum at %s = (%d, %d), the model says (%d, %d)", now.Format(time.RFC3339Nano), gotA, gotB, wantA, wantB)
				}
			},
		})
	})
}

// A wall clock can step back (NTP). The window may then count imprecisely,
// but never invents anything: totals stay within what was added, and
// nothing panics.
func TestWindowSurvivesAClockSteppingBack(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		length := time.Duration(rapid.Int64Range(1, int64(time.Minute)).Draw(t, "length"))
		win := newWindow(length)
		start := epoch(t).Add(time.Hour)

		var totalA, totalB int

		t.Repeat(map[string]func(*rapid.T){
			"add anywhere in the last hour": func(t *rapid.T) {
				at := start.Add(-time.Duration(rapid.Int64Range(0, int64(time.Hour)).Draw(t, "back")))
				a, b := rapid.IntRange(0, 5).Draw(t, "a"), rapid.IntRange(0, 5).Draw(t, "b")

				win.add(at, a, b)

				totalA += a
				totalB += b

				gotA, gotB := win.sum(at)
				if gotA < 0 || gotB < 0 || gotA > totalA || gotB > totalB {
					t.Fatalf("sum = (%d, %d) with (%d, %d) ever added", gotA, gotB, totalA, totalB)
				}
			},
		})
	})
}

// The retry budget's promise: at the moment a retry is granted, the retries
// already granted in the window are below minRetries + ratio × requests —
// and it is refused exactly when they are not. Checked against the event
// model, so the window underneath is exercised through the public calls.
func TestRetryBudgetGrantsExactlyWhatItsRuleAllows(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		length := time.Duration(rapid.Int64Range(int64(time.Millisecond), int64(time.Minute)).Draw(t, "window"))
		ratio := rapid.Float64Range(0, 2).Draw(t, "ratio")
		minRetries := rapid.IntRange(0, 5).Draw(t, "min retries")
		now := epoch(t)

		budget := NewRetryBudget(ratio, minRetries, length, func() time.Time { return now })

		var events []event // a = requests, b = retries granted

		t.Repeat(map[string]func(*rapid.T){
			"advance": func(t *rapid.T) {
				now = now.Add(time.Duration(rapid.Int64Range(0, 2*int64(length)).Draw(t, "by")))
			},
			"request": func(*rapid.T) {
				budget.Request()

				events = append(events, event{at: now, a: 1})
			},
			"retry": func(t *rapid.T) {
				requests, retries := modelSum(events, budget.w.slot, now)
				allowed := float64(retries) < float64(minRetries)+ratio*float64(requests)

				if got := budget.TryRetry(); got != allowed {
					t.Fatalf("TryRetry = %t with %d retries and %d requests in the window (min %d, ratio %v)",
						got, retries, requests, minRetries, ratio)
				}

				if allowed {
					events = append(events, event{at: now, b: 1})
				}
			},
		})
	})
}

// What the budget is for: however the calls are interleaved, an outage
// cannot multiply traffic. Without the clock moving, the retries granted
// never exceed minRetries + ratio × requests — and with no budget at all
// (window 0) nothing is ever refused.
func TestRetryBudgetBoundsAmplification(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		ratio := rapid.Float64Range(0, 2).Draw(t, "ratio")
		minRetries := rapid.IntRange(0, 5).Draw(t, "min retries")
		now := epoch(t)
		budget := NewRetryBudget(ratio, minRetries, time.Minute, func() time.Time { return now })

		var requests, granted int

		t.Repeat(map[string]func(*rapid.T){
			"request": func(*rapid.T) {
				budget.Request()

				requests++
			},
			"retry": func(*rapid.T) {
				if budget.TryRetry() {
					granted++
				}
			},
			"": func(t *rapid.T) {
				// The grant that crosses the line is the last one: at most
				// one retry above the exact bound.
				if limit := float64(minRetries) + ratio*float64(requests); float64(granted) > limit+1 {
					t.Fatalf("%d retries granted for %d requests: the bound is %v", granted, requests, limit)
				}
			},
		})

		if unlimited := NewRetryBudget(ratio, minRetries, 0, time.Now); unlimited != nil || !unlimited.TryRetry() {
			t.Fatalf("a zero window is no budget: every retry is allowed")
		}
	})
}

// issued is a ticket the breaker handed out and nobody recorded yet, with
// what the test knew when it was granted.
type issued struct {
	ticket      Ticket
	transitions int // state changes seen by the time Allow returned
}

// The breaker as a state machine. Requests are admitted, finish in any
// order with any outcome — late, after the breaker has moved on, included —
// and the clock moves freely. Whatever the sequence:
//
//   - the state only moves closed → open → half-open → {closed, open};
//   - while open, nothing is admitted before OpenTimeout has passed, and the
//     first Allow after it is admitted as a probe;
//   - half-open hands out HalfOpenProbes probe slots in total — a probe that
//     succeeded keeps its slot, an ignored one gives it back — closes only
//     after that many probes succeeded, and re-opens only on a probe's failure;
//   - closed opens only on a failure, with at least MinRequests results in;
//   - a result that arrives after the breaker changed state changes nothing,
//     and an ignored result never changes the state.
//
//nolint:gocyclo // one state machine: its actions and their checks read together, as the list above
func TestBreakerIsAStateMachineThatKeepsItsPromises(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		cfg := BreakerConfig{
			FailureRatio:   rapid.Float64Range(0.05, 1).Draw(t, "failure ratio"),
			MinRequests:    rapid.IntRange(1, 4).Draw(t, "min requests"),
			Window:         time.Duration(rapid.Int64Range(int64(time.Millisecond), int64(time.Minute)).Draw(t, "window")),
			OpenTimeout:    time.Duration(rapid.Int64Range(int64(time.Millisecond), int64(time.Minute)).Draw(t, "open timeout")),
			HalfOpenProbes: rapid.IntRange(1, 3).Draw(t, "half-open probes"),
		}
		if errs := cfg.Validate(); len(errs) > 0 {
			t.Fatalf("the generated config is invalid: %v", errs)
		}

		var (
			now         = epoch(t)
			state       = StateClosed // as onChange reports it
			transitions int
			openedAt    time.Time
			results     int // closed: fresh results recorded since it closed
			successes   int // half-open: probes that succeeded
			inFlight    []issued
		)

		breaker := NewBreaker(cfg, func() time.Time { return now }, func(from, to State) {
			if from != state {
				t.Fatalf("a transition from %s, but the state was %s", from, state)
			}

			legal := from == StateClosed && to == StateOpen ||
				from == StateOpen && to == StateHalfOpen ||
				from == StateHalfOpen && (to == StateClosed || to == StateOpen)
			if !legal {
				t.Fatalf("an impossible transition: %s → %s", from, to)
			}

			state, transitions = to, transitions+1
			results, successes = 0, 0

			if to == StateOpen {
				openedAt = now
			}
		})

		// probesInFlight counts the unrecorded probes of the current state.
		probesInFlight := func() int {
			n := 0

			for _, held := range inFlight {
				if held.ticket.probe && held.transitions == transitions {
					n++
				}
			}

			return n
		}

		t.Repeat(map[string]func(*rapid.T){
			"advance": func(t *rapid.T) {
				// The instants that decide something — exactly the timeout,
				// one nanosecond short of it — are drawn as often as any
				// other step: a uniform draw would never land on them.
				now = now.Add(rapid.OneOf(
					rapid.Just(cfg.OpenTimeout),
					rapid.Just(cfg.OpenTimeout-1),
					rapid.Map(rapid.Int64Range(0, 2*int64(cfg.OpenTimeout)), func(ns int64) time.Duration { return time.Duration(ns) }),
				).Draw(t, "by"))
			},
			"allow": func(t *rapid.T) {
				before, probesBefore := state, probesInFlight()
				ticket, admitted := breaker.Allow()

				switch before {
				case StateClosed:
					if !admitted || ticket.probe {
						t.Fatalf("closed admits everything as an ordinary request: admitted=%t probe=%t", admitted, ticket.probe)
					}
				case StateOpen:
					if waited := now.Sub(openedAt) >= cfg.OpenTimeout; admitted != waited {
						t.Fatalf("open for %s of %s: admitted=%t", now.Sub(openedAt), cfg.OpenTimeout, admitted)
					}

					if admitted && (state != StateHalfOpen || !ticket.probe) {
						t.Fatalf("the first request after the timeout is a half-open probe: state=%s probe=%t", state, ticket.probe)
					}
				case StateHalfOpen:
					// A probe that succeeded keeps its slot: half-open asks for
					// HalfOpenProbes verdicts in total, not that many at a time.
					if free := probesBefore+successes < cfg.HalfOpenProbes; admitted != free {
						t.Fatalf("half-open with %d probes in flight and %d succeeded, of %d: admitted=%t",
							probesBefore, successes, cfg.HalfOpenProbes, admitted)
					}
				}

				if admitted {
					inFlight = append(inFlight, issued{ticket: ticket, transitions: transitions})
				}

				if n := probesInFlight(); state == StateHalfOpen && n+successes > cfg.HalfOpenProbes {
					t.Fatalf("%d probes in flight and %d succeeded, %d allowed", n, successes, cfg.HalfOpenProbes)
				}
			},
			"record": func(t *rapid.T) {
				if len(inFlight) == 0 {
					t.Skip("nothing in flight")
				}

				i := rapid.IntRange(0, len(inFlight)-1).Draw(t, "which")
				held := inFlight[i]
				inFlight = append(inFlight[:i], inFlight[i+1:]...)

				// Failures twice as likely: the breaker has to trip, recover
				// and trip again for a result to arrive late.
				outcome := rapid.SampledFrom([]Outcome{OutcomeSuccess, OutcomeFailure, OutcomeFailure, OutcomeIgnored}).Draw(t, "outcome")
				before, seen := state, transitions
				stale := held.transitions != transitions

				if !stale && before == StateClosed && outcome != OutcomeIgnored {
					results++
				}

				if !stale && before == StateHalfOpen && outcome == OutcomeSuccess {
					successes++
				}

				counted, succeeded := results, successes // onChange resets them

				breaker.Record(held.ticket, outcome)

				if transitions == seen {
					if !stale && before == StateHalfOpen && outcome == OutcomeFailure {
						t.Fatalf("a failed probe must re-open the breaker")
					}

					if !stale && before == StateHalfOpen && succeeded >= cfg.HalfOpenProbes {
						t.Fatalf("%d probes succeeded of %d needed: the breaker must close", succeeded, cfg.HalfOpenProbes)
					}

					return
				}

				switch {
				case stale:
					t.Fatalf("a result from before the last transition moved the breaker %s → %s", before, state)
				case outcome == OutcomeIgnored:
					t.Fatalf("an ignored result moved the breaker %s → %s", before, state)
				case before == StateClosed && (outcome != OutcomeFailure || counted < cfg.MinRequests):
					t.Fatalf("closed opened on %v after %d results (min %d)", outcome, counted, cfg.MinRequests)
				case before == StateHalfOpen && state == StateClosed && (outcome != OutcomeSuccess || succeeded < cfg.HalfOpenProbes):
					t.Fatalf("half-open closed on %v after %d successful probes of %d", outcome, succeeded, cfg.HalfOpenProbes)
				case before == StateHalfOpen && state == StateOpen && outcome != OutcomeFailure:
					t.Fatalf("half-open re-opened on %v", outcome)
				}
			},
			"": func(t *rapid.T) {
				if got := breaker.State(); got != state {
					t.Fatalf("State() = %s, the transitions say %s", got, state)
				}
			},
		})
	})
}
