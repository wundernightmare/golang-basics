package integration_test

import (
	"os"
	"testing"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One Valkey per test binary; tests own their keys by prefix.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }
