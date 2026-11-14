package tiercache_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	"github.com/tracehubmmp/golang-basics/libs/tiercache"
	"github.com/tracehubmmp/golang-basics/libs/tiercache/credential"
)

// fullPropertyChecks keeps rapid at its full number of checks under -short,
// which is how the unit layer runs and where rapid would do a fifth: the
// properties here run in virtual time, in milliseconds, and the rarer
// histories (a value outliving its hard deadline) need the full count. A
// number the caller chose (-rapid.checks, RAPID_CHECKS) is left alone.
func fullPropertyChecks() {
	flag.Parse()

	chosen := os.Getenv("RAPID_CHECKS") != ""

	flag.Visit(func(f *flag.Flag) { chosen = chosen || f.Name == "rapid.checks" })

	if testing.Short() && !chosen {
		if err := flag.Set("rapid.checks", "500"); err != nil { // rapid divides by five under -short
			panic(err)
		}
	}
}

// commit is one committed write to the source of truth, on the bubble's clock.
type commit struct {
	version int
	at      time.Time
}

// What a reader may be given, as one rule for every history: two processes
// share an L2, rows are committed with and without an invalidation, time
// passes — by seconds, past the soft deadline, past the hard one — and any
// process reads any key at any moment. Every read then
//
//   - succeeds and returns that key's value, at a version that was committed;
//   - is never older than the last version whose commit was followed by
//     Invalidate — in either process, the one that invalidated or the other;
//   - is never older than the hard TTL allows: the version that was current
//     HardTTL (plus its jitter) ago, or a later one.
//
// The examples in cache_test.go each fix one path through this; here the
// paths are drawn, in virtual time (testing/synctest), against the in-memory
// L2.
func TestReadsAreNeverStalerThanTheCachePromises(t *testing.T) {
	t.Parallel()

	keys := []tiercache.Key{tiercache.KeyOf("a"), tiercache.KeyOf("b"), tiercache.KeyOf("c")}

	// The deadlines are jittered by ±10%: nothing lives past 1.1 × HardTTL.
	const oldest = hardTTL + hardTTL/10

	synctest.Test(t, func(t *testing.T) {
		rapid.Check(inBubble{t}, func(t *rapid.T) {
			remote, src := newFakeRemote(), newSource()
			name := "prop-" + strconv.FormatInt(time.Now().UnixNano(), 36)

			open := func() *tiercache.Cache[credential.Credential] {
				cache, err := tiercache.New(tiercache.Config{Name: name, SoftTTL: softTTL, HardTTL: hardTTL},
					credential.Codec{}, src.load, remote, nil)
				if err != nil {
					t.Fatalf("new cache: %v", err)
				}

				return cache
			}

			processes := []*tiercache.Cache[credential.Credential]{open(), open()}
			defer processes[0].Close()
			defer processes[1].Close()

			history := map[tiercache.Key][]commit{}
			floor := map[tiercache.Key]int{} // the last invalidated version

			write := func(key tiercache.Key) int {
				version := len(history[key]) + 1
				src.set(key, version)
				history[key] = append(history[key], commit{version: version, at: time.Now()})

				return version
			}

			for _, key := range keys {
				write(key)
			}

			synctest.Wait() // both subscriptions are up

			t.Repeat(map[string]func(*rapid.T){
				"commit and invalidate": func(t *rapid.T) {
					key := rapid.SampledFrom(keys).Draw(t, "key")
					by := rapid.IntRange(0, 1).Draw(t, "process")

					version := write(key)

					if err := processes[by].Invalidate(context.Background(), key); err != nil {
						t.Fatalf("invalidate: %v", err)
					}

					synctest.Wait() // the other process has heard of it

					floor[key] = version
				},
				"commit only": func(t *rapid.T) {
					write(rapid.SampledFrom(keys).Draw(t, "key"))
				},
				"time passes": func(t *rapid.T) {
					time.Sleep(rapid.SampledFrom([]time.Duration{
						time.Second, softTTL / 2, softTTL + softTTL/5, hardTTL / 2, hardTTL + hardTTL/5, 3 * hardTTL,
					}).Draw(t, "for"))
					synctest.Wait() // background refreshes have landed
				},
				"read": func(t *rapid.T) {
					key := rapid.SampledFrom(keys).Draw(t, "key")
					from := rapid.IntRange(0, 1).Draw(t, "process")

					got, err := processes[from].Get(context.Background(), key)
					if err != nil {
						t.Fatalf("a read failed: %v", err)
					}

					if want := record(key, 0, 0).ID; got.ID != want {
						t.Fatalf("asked for %s, got the value of %s", want, got.ID)
					}

					version, err := strconv.Atoi(strings.TrimPrefix(got.CRMID, "v"))
					if err != nil || version < 1 || version > len(history[key]) {
						t.Fatalf("version %q was never committed (latest is v%d)", got.CRMID, len(history[key]))
					}

					if version < floor[key] {
						t.Fatalf("read v%d after v%d was invalidated", version, floor[key])
					}

					if aged := currentAt(history[key], time.Now().Add(-oldest)); version < aged {
						t.Fatalf("read v%d, but v%d was already committed %s ago: older than the hard TTL allows",
							version, aged, oldest)
					}
				},
			})
		})
	})
}

// inBubble hands rapid a test that is not a *testing.T: given one, rapid asks
// for its Deadline, which panics inside a synctest bubble.
type inBubble struct{ testing.TB }

// currentAt is the version that was current at the instant at (0: none yet).
func currentAt(history []commit, at time.Time) int {
	version := 0

	for _, c := range history {
		if !c.at.After(at) {
			version = c.version
		}
	}

	return version
}

// The codec is the cache's wire format: whatever the three strings hold —
// empty, long, arbitrary bytes — decoding what was encoded gives them back,
// appending never disturbs what the buffer already held, and the result
// shares nothing with the buffer it was decoded from.
func TestCredentialCodecRoundTripsAnyValue(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		want := credential.Credential{
			ID:           rapid.String().Draw(t, "id"),
			CRMID:        rapid.String().Draw(t, "crm id"),
			KeyEncrypted: string(rapid.SliceOfN(rapid.Byte(), 0, 4096).Draw(t, "key")),
		}
		prefix := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "what the buffer held")

		encoded, err := credential.Codec{}.Append(append([]byte(nil), prefix...), want)
		if err != nil {
			t.Fatalf("append: %v", err)
		}

		if string(encoded[:len(prefix)]) != string(prefix) {
			t.Fatalf("Append rewrote the bytes before it")
		}

		payload := encoded[len(prefix):]

		got, err := credential.Codec{}.Decode(payload)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		for i := range payload {
			payload[i] = 0xFF // the cache reuses the buffer
		}

		if got != want {
			t.Fatalf("round trip changed the value:\n got %s\nwant %s", describe(got), describe(want))
		}
	})
}

func describe(c credential.Credential) string {
	return fmt.Sprintf("{ID:%q CRMID:%q KeyEncrypted:%d bytes}", c.ID, c.CRMID, len(c.KeyEncrypted))
}
