package integration_test

import (
	"os"
	"testing"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One Valkey (and one Toxiproxy in front of it) for the whole suite.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }
