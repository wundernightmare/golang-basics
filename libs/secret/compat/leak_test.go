package compat_test

import (
	"testing"

	"go.uber.org/goleak"
)

// As testx.Main; this module does not depend on libs/testx.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
