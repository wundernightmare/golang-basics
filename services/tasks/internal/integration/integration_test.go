// Package integration exercises the whole tasks vertical — HTTP → Postgres →
// Valkey → Kafka — against real containers (one of each per test binary, from
// libs/testx), proving the libs compose end to end. It skips under -short or
// when Docker is unavailable, and fails instead when CI is set.
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/contracts/events"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func requireEventDelivered(t testing.TB, brokers []string, topic, wantID string) {
	t.Helper()
	cons, err := kafka.NewConsumer(context.Background(), kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: testx.Unique("verify"),
		ClientID: testx.Unique("verify"), DialTimeout: 10 * time.Second,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer cons.Close()

	runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	found := make(chan struct{})

	go func() {
		_ = cons.Run(runCtx, func(_ context.Context, msg kafka.Message) error {
			var evt events.TaskCreatedEvent
			if json.Unmarshal(msg.Value, &evt) == nil && evt.Id == wantID {
				select {
				case <-found:
				default:
					close(found)
				}
			}
			return nil
		})
	}()

	select {
	case <-found:
	case <-runCtx.Done():
		t.Fatal("task.created event was not delivered to Kafka within the timeout")
	}
}

// --- tiny HTTP helpers -------------------------------------------------------

func postJSON(t testing.TB, ts *httptest.Server, path, body string, wantStatus int) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, wantStatus, resp.StatusCode)
	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))
	return m
}

func get(t testing.TB, ts *httptest.Server, path string) *http.Response {
	t.Helper()
	resp, err := ts.Client().Get(ts.URL + path)
	require.NoError(t, err)
	return resp
}

func do(t testing.TB, ts *httptest.Server, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	return resp
}
