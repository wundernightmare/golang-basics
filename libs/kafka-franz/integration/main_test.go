package integration_test

import (
	"os"
	"testing"

	"github.com/wundernightmare/golang-basics/libs/testx/containers"
)

// One Kafka (KRaft) per test binary; every test owns its topics and consumer
// groups by name (testx.Unique).
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }
