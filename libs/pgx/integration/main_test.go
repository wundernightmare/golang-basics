package integration_test

import (
	"os"
	"testing"

	"github.com/wundernightmare/golang-basics/libs/testx/containers"
)

// One Postgres per test binary; tests own their schema by name.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }
