package containers

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// apiVersions sends the broker one ApiVersions v0 request and reports
// whether the reply came within wait. Hand-encoded: a Kafka client would be
// this module's only use of one.
func apiVersions(t *testing.T, broker string, wait time.Duration) bool {
	t.Helper()

	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("tcp", broker)
	require.NoError(t, err, "a frozen broker still accepts: the port is forwarded outside the container")

	defer func() { _ = conn.Close() }()

	// api key 18, version 0, correlation id 1, null client id.
	body := []byte{0, 18, 0, 0, 0, 0, 0, 1, 0xff, 0xff}
	request := binary.BigEndian.AppendUint32(nil, uint32(len(body)))

	require.NoError(t, conn.SetDeadline(time.Now().Add(wait)))

	_, err = conn.Write(append(request, body...))
	require.NoError(t, err)

	_, err = io.ReadFull(conn, make([]byte, 8)) // size + correlation id

	return err == nil
}

func TestPauseFreezesKafkaUntilUnpaused(t *testing.T) {
	t.Parallel()

	broker := Kafka(t)[0]
	require.True(t, apiVersions(t, broker, 10*time.Second), "the broker answers before the freeze")

	unpause := Pause(t, "kafka")
	require.False(t, apiVersions(t, broker, 2*time.Second), "a frozen broker does not answer")

	unpause()
	require.True(t, apiVersions(t, broker, 10*time.Second), "the broker answers again")
}
