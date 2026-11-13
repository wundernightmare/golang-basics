package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// The whole post-mortem path against the real object store: the service is
// assembled as its binary assembles it (app.Run installs the S3 client as the
// dump sink), the flight recorder is on, and a snapshot the service asks for
// lands in the bucket under diagnostics/<service>/dt=…/<instance>/ — an
// object the Go trace tool reads back.
//
//nolint:paralleltest // the runtime allows one flight recorder per process
func TestDiagnostics_FlightTraceLandsInTheObjectStore(t *testing.T) {
	back := direct(t)
	prefix := testx.Unique("diag")

	svc := run(t, back, map[string]string{
		"INGEST_DIAG_FLIGHT_RECORDER": "true",
		"INGEST_DIAG_KEY_PREFIX":      prefix,
		"INGEST_DIAG_CHECK_INTERVAL":  "50ms",
	})

	// Some work for the trace to show.
	require.Equal(t, 200, svc.post(ids(20)...).status)

	require.True(t, svc.srv.CaptureFlightTrace("integration test"))

	store, err := s3.New(context.Background(), s3.Config{
		Bucket: bucket, Endpoint: back.store.Endpoint, Region: back.store.Region,
		AccessKeyID: back.store.AccessKey, SecretAccessKey: secret.New(back.store.SecretKey),
		UsePathStyle: true, Insecure: true,
	}, nil)
	require.NoError(t, err)

	defer store.Close()

	var key string

	require.Eventually(t, func() bool {
		for obj, err := range store.List(context.Background(), prefix+"/ingest/") {
			if err == nil && strings.HasSuffix(obj.Key, "-flight.trace") {
				key = obj.Key

				return true
			}
		}

		return false
	}, 20*time.Second, 100*time.Millisecond, "the snapshot is in the bucket:\n%s", svc.log.String())

	assert.Contains(t, key, "/dt="+time.Now().UTC().Format(time.DateOnly)+"/", "one listing per service and day finds it")

	body, err := store.Get(context.Background(), key, 64<<20)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "flight.trace")
	require.NoError(t, os.WriteFile(path, body, 0o600))

	out, err := exec.CommandContext(t.Context(), "go", "tool", "trace", "-d=parsed", path).CombinedOutput()
	require.NoError(t, err, "go tool trace rejected the object: %.400s", out)
	assert.Contains(t, string(out), "services/ingest", "the trace shows this service's own goroutines")

	assert.InDelta(t, 1, svc.metric("diag_dumps_total", map[string]string{"kind": "flight", "result": "uploaded"}), 0)
}
