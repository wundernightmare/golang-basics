package integration_test

// Chaos: each sink taken away, then both, and a restart in the middle of an
// outage. These are the paths the service exists for; none of them is
// trusted without being run against a dependency that really is down.
//
// Kafka is frozen (containers.Pause: the broker stops answering, which is
// what a wedged cluster looks like to a client); the store is cut off with
// Toxiproxy (connection refused). Serial: they break containers the parallel
// tests share, and parallel tests only resume once every serial one is done.

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// proxiedStore wires the service to the store through Toxiproxy.
func proxiedStore(t *testing.T) (backends, *containers.Proxy) {
	t.Helper()

	back := direct(t)
	proxy := containers.Proxied(t, "s3")
	back.s3Endpoint = "http://" + proxy.Addr

	return back, proxy
}

// Kafka stops answering. Batches are still accepted — by the archive, within
// the grace and the urgent linger, not the publish timeout — the circuit
// opens and Kafka is no longer waited on at all; when it is back, it takes
// events again.
//
//nolint:paralleltest // serial: freezes the shared Kafka container
func TestChaos_KafkaFrozen_TheArchiveAnswers(t *testing.T) {
	svc := run(t, direct(t), nil)
	svc.waitReadyz("ready")

	warm := ids(2)
	require.Equal(t, http.StatusOK, svc.post(warm...).status, "the topic exists before the outage")

	unpause := containers.Pause(t, "kafka")

	var during []string

	for i := range 8 {
		batch := ids(3)
		got := svc.post(batch...)
		require.Equal(t, http.StatusOK, got.status, "batch %d during the outage: %s", i, got.body)
		assert.Less(t, got.took, 1800*time.Millisecond, "answered by the archive, not after KAFKA_PUBLISH_TIMEOUT (2s)")

		during = append(during, batch...)
	}

	// Answered means archived: nothing to wait for.
	archived := svc.archived()
	for _, id := range during {
		assert.Equal(t, 1, archived[id], "event %s was answered by the archive, so it is there", id)
	}

	require.Eventually(t, func() bool {
		return svc.metric("ingest_sink_circuit_breaker_state", map[string]string{"target": "kafka"}) >= 1
	}, 20*time.Second, 50*time.Millisecond, "the Kafka circuit opens")

	fast := svc.post(ids(1)...)
	require.Equal(t, http.StatusOK, fast.status)
	assert.Less(t, fast.took, time.Second, "an open circuit goes straight to the urgent archive batch")
	assert.Contains(t, svc.waitReadyz("degraded"), "kafka", "one sink down is degraded, not unready")

	unpause()

	var after []string

	require.Eventually(t, func() bool {
		batch := ids(1)
		if svc.post(batch...).status != http.StatusOK {
			return false
		}

		after = batch

		return svc.metric("ingest_sink_circuit_breaker_state", map[string]string{"target": "kafka"}) == 0
	}, 60*time.Second, 300*time.Millisecond, "the circuit closes once a probe gets through")

	recovered := ids(2)
	require.Equal(t, http.StatusOK, svc.post(recovered...).status)

	seen := svc.published(append(warm, recovered...), 60*time.Second)
	for _, id := range append(warm, recovered...) {
		assert.Contains(t, seen, id, "Kafka has the events from before and after the outage")
	}

	svc.waitArchived(append(after, recovered...))
	svc.waitReadyz("ready")
}

// A pod that starts while Kafka is away — a rollout during the outage, or a
// crash-restart — must come up and serve from the archive.
//
//nolint:paralleltest // serial: freezes the shared Kafka container
func TestChaos_StartsAndStopsWhileKafkaIsFrozen(t *testing.T) {
	back := direct(t)
	containers.Pause(t, "kafka")

	svc := run(t, back, nil) // boots: the producer connects lazily

	batch := ids(4)
	got := svc.post(batch...)
	require.Equal(t, http.StatusOK, got.status, got.body)

	// SIGTERM with Kafka still away: the shutdown does not wait for it
	// beyond its own budgets, and what was answered is in the archive.
	started := time.Now()

	require.NoError(t, svc.stop())
	assert.Less(t, time.Since(started), 15*time.Second)

	archived := svc.archived()
	for _, id := range batch {
		assert.Equal(t, 1, archived[id])
	}
}

// The store refuses connections. Kafka answers every batch at its own speed;
// the archive circuit opens, so batches are not buffered for puts that fail,
// and the events with no archive copy are counted. When the store is back
// the archive resumes.
//
//nolint:paralleltest // serial: takes the shared store's proxy down
func TestChaos_StoreDown_KafkaAnswers(t *testing.T) {
	back, proxy := proxiedStore(t)
	svc := run(t, back, nil)
	svc.waitReadyz("ready")

	proxy.Down()

	var during []string

	require.Eventually(t, func() bool {
		batch := ids(2)
		got := svc.post(batch...)
		require.Equal(t, http.StatusOK, got.status, got.body)
		assert.Less(t, got.took, time.Second, "Kafka answers; the archive is not waited on")

		during = append(during, batch...)

		return svc.metric("ingest_sink_circuit_breaker_state", map[string]string{"target": "archive"}) >= 1
	}, 30*time.Second, 200*time.Millisecond, "the archive circuit opens")

	require.Eventually(t, func() bool {
		svc.post(ids(1)...)
		return svc.metric("ingest_archive_events_total", map[string]string{"result": "refused"}) > 0
	}, 10*time.Second, 100*time.Millisecond, "events with no archive copy are counted")
	assert.Positive(t, svc.metric("ingest_archive_events_total", map[string]string{"result": "failed"}))
	assert.Contains(t, svc.waitReadyz("degraded"), "s3")

	seen := svc.published(during, 60*time.Second)
	for _, id := range during {
		assert.Contains(t, seen, id, "every acknowledged event of the outage is in Kafka")
	}

	proxy.Up()

	var after []string

	require.Eventually(t, func() bool {
		batch := ids(1)
		if svc.post(batch...).status != http.StatusOK {
			return false
		}

		after = batch

		return svc.metric("ingest_sink_circuit_breaker_state", map[string]string{"target": "archive"}) == 0
	}, 60*time.Second, 300*time.Millisecond, "the archive circuit closes once a batch is stored")

	recovered := ids(2)
	require.Equal(t, http.StatusOK, svc.post(recovered...).status)
	svc.waitArchived(recovered)
	svc.waitReadyz("ready")

	_ = after
}

// Nothing can take an event: the client is told to come back (503 with
// Retry-After) and keeps its batch — nothing is acknowledged, so nothing can
// be lost. Readiness goes 503 only now, with both circuits open. Either sink
// returning brings the service back.
//
//nolint:paralleltest // serial: freezes Kafka and takes the store's proxy down
func TestChaos_BothSinksDown_NothingIsAcknowledged(t *testing.T) {
	back, proxy := proxiedStore(t)
	svc := run(t, back, nil)
	svc.waitReadyz("ready")
	require.Equal(t, http.StatusOK, svc.post(ids(1)...).status)

	unpause := containers.Pause(t, "kafka")

	proxy.Down()

	var refused []string

	require.Eventually(t, func() bool {
		batch := ids(2)
		got := svc.post(batch...)
		require.Equal(t, http.StatusServiceUnavailable, got.status, got.body)
		assert.Equal(t, "1", got.header.Get("Retry-After"))
		assert.Contains(t, got.body, "no_sink_available")

		refused = append(refused, batch...)

		return svc.metric("ingest_sink_circuit_breaker_state", map[string]string{"target": "kafka"}) >= 1 &&
			svc.metric("ingest_sink_circuit_breaker_state", map[string]string{"target": "archive"}) >= 1
	}, 60*time.Second, 100*time.Millisecond, "both circuits open")

	assert.Contains(t, svc.waitReadyz("not_ready"), "sinks", "no sink at all is the one thing that takes the pod out of rotation")
	assert.Zero(t, svc.metric("ingest_in_flight_bytes", nil)+0, "refused batches are not held")

	proxy.Up()

	require.Eventually(t, func() bool { return svc.post(ids(1)...).status == http.StatusOK },
		30*time.Second, 300*time.Millisecond, "the store alone is enough to accept again")
	svc.waitReadyz("degraded")

	archived := svc.archived()
	for _, id := range refused {
		assert.Zero(t, archived[id], "a refused batch was not stored behind the client's back by the archive")
	}

	unpause()
	svc.waitReadyz("ready")
}
