//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// consumedMetric is the consumer's per-event success counter
// (services/consumer/internal/worker).
const consumedMetric = "consumer_tasks_consumed_total"

// TestTasksPipeline: a task created through the real tasks binary reaches the
// real consumer binary through the outbox relay and the real broker. The CRUD
// contract, cache and problem bodies are services/tasks' integration suite.
// Skipped unless E2E_DATABASE_URL, E2E_VALKEY_URL and E2E_KAFKA_BROKERS are set.
//
// Not parallel with its own sub-tests: they share the two processes and run
// in order, the shutdown check last.
func TestTasksPipeline(t *testing.T) { //nolint:tparallel // ordered sub-tests over shared processes
	t.Parallel()
	dbURL, valkeyURL, brokers := dataEnv(t)

	// A topic and group of our own: concurrent or earlier runs against the
	// same broker cannot feed (or drain) this run's consumer. The topic does
	// not exist yet: both clients ask the broker to create it (docker/deps.yml
	// allows it; the libs default to off).
	run := time.Now().UTC().Format("20060102t150405.000000000")
	run = strings.ReplaceAll(run, ".", "")
	topic := "e2e.tasks.events." + run

	consumer := start(t, spec{name: "consumer", prefix: "CONSUMER_", env: map[string]string{
		"KAFKA_BROKERS":                   brokers,
		"KAFKA_TOPIC":                     topic,
		"KAFKA_TOPICS":                    topic,
		"KAFKA_GROUP":                     "e2e-" + run,
		"KAFKA_LAG_INTERVAL":              "1s",
		"KAFKA_ALLOW_AUTO_TOPIC_CREATION": "true",
	}})
	tasks := start(t, spec{name: "tasks", prefix: "TASKS_", api: true, env: map[string]string{
		"DATABASE_URL":                    dbURL,
		"VALKEY_URL":                      valkeyURL,
		"KAFKA_BROKERS":                   brokers,
		"KAFKA_TOPIC":                     topic,
		"KAFKA_ALLOW_AUTO_TOPIC_CREATION": "true",
	}})

	t.Run("version carries the build stamp", func(t *testing.T) {
		assertBuildStamp(t, tasks)
		assertBuildStamp(t, consumer)
	})

	t.Run("admin config requires the token", func(t *testing.T) {
		assertAdminGuarded(t, tasks)
		assertAdminGuarded(t, consumer)
	})

	t.Run("created task reaches the consumer", func(t *testing.T) {
		before := consumer.metric(t, consumedMetric)

		title := "e2e " + run
		code, body, err := do(http.MethodPost, tasks.apiURL+"/tasks", "",
			strings.NewReader(fmt.Sprintf(`{"title":%q}`, title)))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, code, "POST /tasks: %s", body)
		var created struct {
			ID    json.RawMessage `json:"id"`
			Title string          `json:"title"`
		}
		require.NoError(t, json.Unmarshal(body, &created), "%s", body)
		require.NotEmpty(t, created.ID, "POST /tasks returned no id: %s", body)
		id := strings.Trim(string(created.ID), `"`)

		code, body, err = tasks.get(tasks.apiURL+"/tasks/"+id, "")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code, "GET /tasks/%s: %s", id, body)
		assert.Contains(t, string(body), title)

		// Asynchronous twice over: the outbox relay publishes, the consumer
		// polls. Poll the counter, never sleep.
		eventually(t, 30*time.Second, consumedMetric+" never advanced", func(c *assert.CollectT) {
			assert.Greater(c, consumer.metric(t, consumedMetric), before)
		})
	})

	t.Run("graceful shutdown", func(t *testing.T) {
		for _, s := range []*service{tasks, consumer} {
			require.True(t, s.terminate(10*time.Second), "%s did not exit within 10s of SIGTERM", s.name)
			assertCleanExit(t, s)
		}
	})
}
