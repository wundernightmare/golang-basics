package integration_test

import (
	"os"
	"testing"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }
