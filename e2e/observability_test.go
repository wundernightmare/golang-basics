//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// The alert rules and the Grafana dashboards name metrics by hand; a rename in
// a lib silently turns an alert into one that can never fire. These helpers
// let the pipeline test compare every name they reference with what the real
// binaries export, and TestDashboards_Lint keeps the dashboards' queries to
// the house rules.

const (
	rulesFile     = "../docker/observability/rules.yml"
	dashboardsDir = "../docker/observability/dashboards"
)

// brokerMetrics are the names the alerts and dashboard read from something
// other than the services — the broker or an exporter — with where they come
// from. The services' /metrics cannot have them by design (docs/METRICS.md:
// what the broker measures better is not collected in the app), so the guard
// skips exactly these and fails on any other name nobody exports. An entry
// nothing references any more fails too, so the list cannot rot.
var brokerMetrics = map[string]string{
	"redpanda_kafka_consumer_group_lag_sum": "Redpanda /public_metrics (enable_consumer_group_metrics, docker/deps.yml)",
}

// errorOnlyMetrics are labelled error counters the dashboards read whose
// series appear with the first error (a vector has no series before its first
// label set), so a healthy e2e run cannot see them. Each name is pinned by the
// lib test that provokes the error instead; an entry nothing references fails,
// like brokerMetrics.
var errorOnlyMetrics = map[string]string{
	"db_query_errors_total":      "libs/pgx/pgx_test.go (a failing query)",
	"cache_command_errors_total": "libs/valkey/valkey_test.go (Valkey down)",
}

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
		regexp.MustCompile(`(?i)\b\d+(\.\d*)?(e[+-]?\d+)?\b`), // 0.95, 1e18
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

	for file, exprs := range dashboardExprs(t) {
		for _, e := range exprs {
			for _, n := range promQLNames(e) {
				refs[n] = file + ": " + e
			}
		}
	}
	require.NotEmpty(t, refs)
	return refs
}

// dashboardExprs returns every PromQL expression ("expr", panel queries and
// template variables' "query"/"definition" aside) of every dashboard file,
// by file name.
func dashboardExprs(t *testing.T) map[string][]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dashboardsDir, "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	out := map[string][]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		var dash any
		require.NoError(t, json.Unmarshal(raw, &dash), f)
		name := filepath.Base(f)
		var walk func(any)
		walk = func(v any) {
			switch v := v.(type) {
			case map[string]any:
				for k, x := range v {
					if s, ok := x.(string); ok && k == "expr" {
						out[name] = append(out[name], s)
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
	}
	return out
}

// TestDashboards_Lint holds every dashboard query to the rules that keep the
// boards correct at any scrape interval and for any set of services:
//   - windows are $__rate_interval (or $__range / $__interval), never a fixed
//     [1m] — a window shorter than two scrapes draws nothing;
//   - every selector of a service metric filters service=~"$service" and
//     instance=~"$instance" (the broker's own metrics, brokerMetrics, aside);
//   - services are named by the scrape's `service` label, never `job`.
//
// Static (no binaries needed), so it never skips.
func TestDashboards_Lint(t *testing.T) {
	fixedWindow := regexp.MustCompile(`\[\d+[smhdwy]\]`)
	jobMatcher := regexp.MustCompile(`\bjob\s*(=|!=|=~|!~)`)
	var problems []string
	for file, exprs := range dashboardExprs(t) {
		for _, e := range exprs {
			if fixedWindow.MatchString(e) {
				problems = append(problems, file+": fixed window, use $__rate_interval: "+e)
			}
			if jobMatcher.MatchString(e) {
				problems = append(problems, file+": selects on job, use service: "+e)
			}
			for _, n := range promQLNames(e) {
				if _, broker := brokerMetrics[n]; broker {
					continue
				}
				sel := regexp.MustCompile(`\b` + regexp.QuoteMeta(n) + `\b(\{[^}]*\})?`)
				for _, m := range sel.FindAllStringSubmatch(e, -1) {
					if !strings.Contains(m[1], `service=~"$service"`) || !strings.Contains(m[1], `instance=~"$instance"`) {
						problems = append(problems, file+": "+n+" without the $service/$instance filter: "+e)
					}
				}
			}
		}
	}
	sort.Strings(problems)
	require.Empty(t, problems)
}

func TestPromQLNames(t *testing.T) {
	require.Equal(t, []string{"go_gc_gomemlimit_bytes"}, promQLNames(`go_gc_gomemlimit_bytes{service=~"$service"} < 1e18`))
	require.Equal(t, []string{"http_request_duration_seconds_bucket"},
		promQLNames(`histogram_quantile(0.95, sum by (service, le) (rate(http_request_duration_seconds_bucket{x="y"}[$__rate_interval])))`))
	require.Equal(t, []string{"process_start_time_seconds"}, promQLNames(`time() - process_start_time_seconds`))
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

// assertMetricsExist fails for every name the rules or dashboards reference
// that none of the services exports, the broker's own (brokerMetrics) and the
// error-only counters (errorOnlyMetrics) aside.
func assertMetricsExist(t *testing.T, services ...*service) {
	t.Helper()
	have := exportedMetrics(t, services...)
	refs := referencedMetrics(t)
	var missing []string
	for name, where := range refs {
		_, broker := brokerMetrics[name]
		_, errorOnly := errorOnlyMetrics[name]
		if !broker && !errorOnly && !have[name] {
			missing = append(missing, name+"  ← "+where)
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "metrics referenced by the alerts/dashboard but not exported")
	for name := range errorOnlyMetrics {
		require.Contains(t, refs, name, "errorOnlyMetrics lists %s, which nothing references", name)
	}
	for name := range brokerMetrics {
		require.Contains(t, refs, name, "brokerMetrics lists %s, which nothing references", name)
		require.False(t, have[name], "%s is the broker's metric; a service must not export it", name)
	}
}
