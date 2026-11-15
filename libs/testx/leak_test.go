package testx_test

import (
	"os"
	"testing"

	"github.com/wundernightmare/golang-basics/libs/testx"
)

func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }
