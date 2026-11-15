//go:build e2e

package e2e

import (
	"testing"

	"go.uber.org/goleak"
)

// As testx.Main; this module does not depend on libs/testx.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
