//go:build e2e

package e2e

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"
	"github.com/ozontech/testo/testoplugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The suite reports to Allure natively, through testo and its Allure plugin,
// instead of through JUnit XML like the layers below it. JUnit carries a
// name, a status and one blob of output per test; here a failure in a
// scenario that drives two or three real processes needs more: which phase
// failed (start, the request, the wait for the consumer, the shutdown), how
// long each took, and each process's own output as a separate attachment.
//
// Everything else is still plain `go test`: the same binary, -run, -shuffle,
// -race, testify. testo is confined to this module — the unit and
// integration layers keep `func TestX(t *testing.T)` and the JUnit report.

// T is what a scenario and its steps receive: testing.T's methods (it
// satisfies testing.TB, so the harness helpers and testify take it as is)
// plus the report's — Step, Attach, labels.
type T struct {
	*testo.T
	*allure.PluginAllure
}

// maxAttachment bounds one attachment. A service at info level logs a few
// kilobytes per scenario; this is the guard against a child stuck in an
// error loop filling the report.
const maxAttachment = 256 << 10

// scenario runs body as one Allure test case of the given feature. Every
// Test function of this package is exactly one call to it
// (TestEveryTestIsAScenario): a test that bypassed it would run and gate the
// build, but be missing from the report.
//
// Inside, testo.Run is a sub-test, as t.Run was: it may be parallel, and a
// failure in it does not stop its siblings. In the report it is a step of
// the scenario. So is allure.Step — and both are sub-tests to `go test`,
// which means `-run` with a sub-test pattern skips the ones it does not
// name. Nothing a later line depends on may live inside one: the harness
// starts and waits for a service outside any step, and only marks it.
func scenario(t *testing.T, feature string, body func(t T)) {
	t.Helper()

	testo.RunTest(t, func(t T) {
		t.Epic("Service binaries")
		t.Feature(feature)
		t.Layer("e2e")
		// The Suites tab groups by this label; the JUnit layers carry their
		// module's path there, so this one does too.
		t.Labels(allure.NewLabel("suite", "e2e"))

		// The suite runs once per Kafka provider in CI against binaries
		// built for it; as a parameter the two runs are two results in one
		// report rather than one overwriting the other.
		if provider := os.Getenv("KAFKA_PROVIDER"); provider != "" {
			t.Parameters(allure.NewParameter("kafka provider", provider))
		}

		body(t)
	}, reportOptions()...)
}

func reportOptions() []testoplugin.Option {
	return []testoplugin.Option{
		allure.WithOutputDir(resultsDir()),
		allure.WithMaxAttachmentSize(maxAttachment),
	}
}

// resultsDir is where the results go: ALLURE_RESULTS_DIR (CI sets it per
// provider), by default <workspace>/.reports/allure-native/e2e, which
// `just allure` picks up.
func resultsDir() string {
	if dir := os.Getenv("ALLURE_RESULTS_DIR"); dir != "" {
		return dir
	}

	return filepath.Join("..", ".reports", "allure-native", "e2e")
}

// attachOutput adds a child's captured output to the report, pass or fail:
// the log of a green run is what a red one is compared against.
func attachOutput(t T, name, output string) {
	// The media type is given, not guessed: guessing makes the plugin write
	// to os.TempDir and rename into the results directory, which fails when
	// /tmp is another filesystem (tmpfs) — testo-allure 1.5.0.
	t.Attach(name+" output", allure.Bytes(output).As(allure.TextPlain))
}

// The report is only complete if every test goes through scenario. This
// reads the package's own sources: a Test function whose body is anything
// but a single scenario(...) call fails here, by name.
func TestEveryTestIsAScenario(t *testing.T) {
	scenario(t, "Test report", func(t T) {
		t.Parallel()

		files, err := filepath.Glob("*_test.go")
		require.NoError(t, err)
		require.NotEmpty(t, files)

		seen := 0

		for _, file := range files {
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
			require.NoError(t, err)

			for _, decl := range parsed.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Name.Name == "TestMain" {
					continue
				}

				seen++

				assert.Truef(t, isOneScenarioCall(fn.Body), "%s: %s must be exactly `scenario(t, feature, func(t T) { … })`",
					file, fn.Name.Name)
			}
		}

		assert.GreaterOrEqual(t, seen, 9, "the sources were read (this test and the eight it started with)")
	})
}

func isOneScenarioCall(body *ast.BlockStmt) bool {
	if body == nil || len(body.List) != 1 {
		return false
	}

	stmt, ok := body.List[0].(*ast.ExprStmt)
	if !ok {
		return false
	}

	call, ok := stmt.X.(*ast.CallExpr)
	if !ok {
		return false
	}

	callee, ok := call.Fun.(*ast.Ident)

	return ok && callee.Name == "scenario"
}
