package testx

import (
	"os"
	"testing"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"
	"github.com/ozontech/testo/testoplugin"
)

// T is the test handle every suite and [Run] body receives: testo's T (a
// testing.TB with sub-tests, parameters, hooks) plus the Allure plugin
// (Title, Tags, Step, Attach, Require/Assert that log as steps). It
// implements testing.TB, so testify's require/assert work on it unchanged.
type T = struct {
	*testo.T
	*allure.PluginAllure
}

// Options returns the plugin options every suite in the workspace uses: the
// given tags, and the results directory from ALLURE_RESULTS_DIR when set
// (CI points every module at one directory; locally results land in
// ./allure-results next to the package).
func Options(tags ...string) []testoplugin.Option {
	opts := []testoplugin.Option{}
	if len(tags) > 0 {
		opts = append(opts, allure.WithTags(tags...))
	}
	if dir := os.Getenv("ALLURE_RESULTS_DIR"); dir != "" {
		opts = append(opts, allure.WithOutputDir(dir))
	}
	return opts
}

// Run runs f as one Allure test — the minimal way to make a plain
// `func TestX(t *testing.T)` report to Allure:
//
//	func TestX(t *testing.T) {
//		testx.Run(t, func(t testx.T) { … }, "httpx", "unit")
//	}
//
// Inside, t is a testing.TB (testify keeps working) with testo/Allure on top.
// Sub-tests are testo.Run / allure.Step, not t.Run.
func Run(t *testing.T, f func(t T), tags ...string) {
	t.Helper()
	testo.RunTest(t, f, Options(tags...)...)
}

// Step runs f as a named Allure step (a sub-test whose failure is fatal to
// the parent). Resources that must outlive the step — containers, clients,
// servers — belong on the parent t: a step's t.Cleanup runs when it returns.
func Step(t T, name string, f func(t T)) {
	t.Helper()
	allure.Step(t, name, f)
}
