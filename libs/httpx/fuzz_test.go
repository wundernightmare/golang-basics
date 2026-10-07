package httpx_test

// Fuzz targets for the parsers that take bytes from outside the process:
// `just fuzz` (and the nightly CI job) runs each for a fixed budget; a crash
// lands in testdata/fuzz/<Target>/ and is committed as a regression case.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

type problemKey struct{}

// FuzzProblemJSON: any status/detail/extension the handlers could produce
// marshals to valid JSON that decodes back with the same members and never
// panics; the status in the body always equals the response status, also
// when written through the full server chain.
func FuzzProblemJSON(f *testing.F) {
	f.Add(404, "task 7 does not exist", "code", "task_not_found")
	f.Add(500, "", "", "")
	f.Add(0, "weird status", "trace_id", "abc")
	f.Add(999, "nul\x00 and bytes \xff", "k\"ey", "va\nlue")

	srv, _ := loggedServer(f, httpx.Config{})
	srv.Mux().HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		p, _ := r.Context().Value(problemKey{}).(httpx.Problem)
		httpx.WriteProblem(w, r, p)
	})

	f.Fuzz(func(t *testing.T, status int, detail, extKey, extVal string) {
		p := httpx.NewProblem(status, detail)
		if extKey != "" {
			p.Extensions = map[string]any{extKey: extVal}
		}
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("round trip: %v (%q)", err, b)
		}
		want := p.Status
		if want < 100 || want > 999 {
			want = http.StatusInternalServerError // an invalid status degrades to 500, never a panic
		}
		if got, _ := m["status"].(float64); int(got) != want {
			t.Fatalf("status %v != %d", m["status"], want)
		}
		// encoding/json coerces invalid UTF-8 in keys to U+FFFD, so only a valid
		// key is expected back under its own name.
		if extKey != "" && !isReserved(extKey) && utf8.ValidString(extKey) {
			if _, ok := m[extKey]; !ok {
				t.Fatalf("extension %q lost", extKey)
			}
		}

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(context.WithValue(req.Context(), problemKey{}, p))
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("response status %d != problem status %d", rec.Code, want)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("response body is not JSON: %v (%q)", err, rec.Body.String())
		}
		if got, _ := body["status"].(float64); int(got) != want {
			t.Fatalf("body status %v != response status %d", body["status"], want)
		}
	})
}

func isReserved(k string) bool {
	switch k {
	case "type", "title", "status", "detail", "instance":
		return true
	}
	return false
}

// FuzzLoadYAML: arbitrary bytes in the config file either load or return an
// error — never a panic — and the env overlay still applies on success.
func FuzzLoadYAML(f *testing.F) {
	f.Add("addr: \":9000\"\nworkers: 4\n")
	f.Add("")
	f.Add("addr: [not, a, string]\n")
	f.Add("workers: !!binary abc\n")
	f.Add("- just\n- a list\n")
	f.Add("unknown: key\n")
	f.Add("\xff\xfe")
	f.Fuzz(func(t *testing.T, body string) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Skip()
		}
		t.Setenv("FUZZ_WORKERS", "7")
		var cfg sampleConfig
		if err := httpx.LoadYAML(path, "FUZZ_", &cfg); err != nil {
			return // a parse error is a valid outcome
		}
		if cfg.Workers != 7 {
			t.Fatalf("env overlay lost: workers=%d", cfg.Workers)
		}
	})
}

// FuzzRedact: a config struct holding an arbitrary string — as a URL field,
// as a tagged secret and inside a slice — never panics on the way through
// Redact and json.Marshal. When the string is a URL with a userinfo password,
// the redacted value is that URL with exactly the password replaced: same
// user, password "xxxxx".
func FuzzRedact(f *testing.F) {
	f.Add("postgres://app:hunter2@db:5432/app?sslmode=disable")
	f.Add("plain value")
	f.Add("://@")
	f.Add("http://u:p%zz@h/")
	f.Add("kafka://u:pa%20ss@b:9092")
	f.Add("redis://:only-password@cache:6379/0")
	f.Add("postgres://app:x@db/app?sslpassword=y&token=z")
	f.Add("host=db user=app password=s3cret")
	f.Fuzz(func(t *testing.T, s string) {
		type cfg struct {
			Target string   `yaml:"target"`
			Secret string   `yaml:"secret"`
			List   []string `yaml:"list"`
		}
		b, err := json.Marshal(httpx.Redact(cfg{Target: s, Secret: s, List: []string{s}}))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("round trip: %v (%q)", err, b)
		}
		if s != "" && m["secret"] != "[redacted]" {
			t.Fatalf("secret field leaked: %v", m["secret"])
		}
		list, _ := m["list"].([]any)
		if len(list) != 1 {
			t.Fatalf("list lost its element: %v", m["list"])
		}
		if list[0] != m["target"] {
			t.Fatalf("the same string redacted differently in a slice: %q vs %q", list[0], m["target"])
		}

		if !strings.Contains(s, "://") {
			return
		}
		u, err := url.Parse(s)
		if err != nil || u.User == nil {
			return
		}
		password, has := u.User.Password()
		if !has {
			return
		}
		out, _ := m["target"].(string)
		ru, err := url.Parse(out)
		if err != nil {
			t.Fatalf("redacted URL %q no longer parses: %v", out, err)
		}
		if ru.User == nil {
			t.Fatalf("redacted URL %q lost its userinfo", out)
		}
		if got := ru.User.Username(); got != u.User.Username() {
			t.Fatalf("user changed: %q → %q", u.User.Username(), got)
		}
		if got, ok := ru.User.Password(); !ok || got != "xxxxx" {
			t.Fatalf("password %q redacted to %q (present=%v), want xxxxx", password, got, ok)
		}
	})
}
