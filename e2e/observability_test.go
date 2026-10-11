//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// The alert rules and the Grafana dashboard name metrics by hand; a rename in
// a lib silently turns an alert into one that can never fire. These helpers
// let the pipeline test compare every name they reference with what the real
// binaries export.

const (
	rulesFile     = "../docker/observability/rules.yml"
	dashboardFile = "../docker/observability/dashboards/golang-basics.json"
)

// promQLNames returns the metric names a PromQL expression reads: label
// matchers, strings, ranges and grouping clauses removed, function calls and
// keywords skipped. Prometheus' own synthetic series (up) is not exported by
// a service and is left out.
func promQLNames(expr string) []string {
	for _, re := range []*regexp.Regexp{
		regexp.MustCompile(`"(\\.|[^"\\])*"`),
		regexp.MustCompile(`\{[^}]*\}`),
		regexp.MustCompile(`\[[^\]]*\]`),
		regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`),
	} {
		expr = re.ReplaceAllString(expr, " ")
	}
	skip := map[string]bool{"by": true, "without": true, "on": true, "ignoring": true, "and": true,
		"or": true, "unless": true, "bool": true, "offset": true, "group_left": true, "group_right": true, "up": true}
	var out []string
	for _, m := range regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*(\s*\()?`).FindAllString(expr, -1) {
		if strings.HasSuffix(m, "(") || skip[m] {
			continue // a function call or a keyword
		}
		out = append(out, m)
	}
	return out
}

// referencedMetrics lists every metric name the alert rules and the dashboard
// read, with the file that reads it.
func referencedMetrics(t *testing.T) map[string]string {
	t.Helper()
	refs := map[string]string{}

	raw, err := os.ReadFile(rulesFile)
	require.NoError(t, err)
	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &rules))
	for _, g := range rules.Groups {
		for _, r := range g.Rules {
			for _, n := range promQLNames(r.Expr) {
				refs[n] = "rules.yml " + r.Alert
			}
		}
	}

	raw, err = os.ReadFile(dashboardFile)
	require.NoError(t, err)
	var dash any
	require.NoError(t, json.Unmarshal(raw, &dash))
	var walk func(any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, x := range v {
				if s, ok := x.(string); ok && k == "expr" {
					for _, n := range promQLNames(s) {
						refs[n] = "dashboard: " + s
					}
					continue
				}
				walk(x)
			}
		case []any:
			for _, x := range v {
				walk(x)
			}
		}
	}
	walk(dash)
	require.NotEmpty(t, refs)
	return refs
}

// exportedMetrics is the set of sample names (histogram _bucket/_sum/_count
// included) on the admin /metrics of the given services.
func exportedMetrics(t *testing.T, services ...*service) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, s := range services {
		code, body, err := s.get(s.adminURL+"/metrics", "")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code)
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if line == "" || line[0] == '#' {
				continue
			}
			name := line
			if i := strings.IndexAny(line, "{ "); i >= 0 {
				name = line[:i]
			}
			out[name] = true
		}
	}
	return out
}

// assertMetricsExist fails for every name the rules or dashboard reference
// that none of the services exports.
func assertMetricsExist(t *testing.T, services ...*service) {
	t.Helper()
	have := exportedMetrics(t, services...)
	var missing []string
	for name, where := range referencedMetrics(t) {
		if !have[name] {
			missing = append(missing, name+"  ← "+where)
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "metrics referenced by the alerts/dashboard but not exported")
}
