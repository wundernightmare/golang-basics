package httpx_test

// Fuzz targets for the parsers that take bytes from outside the process:
// `just fuzz` (and the nightly CI job) runs each for a fixed budget; a crash
// lands in testdata/fuzz/<Target>/ and is committed as a regression case.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// FuzzProblemJSON: any status/detail/extension the handlers could produce
// marshals to valid JSON that decodes back with the same members and never
// panics; the status in the body always equals the response status.
func FuzzProblemJSON(f *testing.F) {
	f.Add(404, "task 7 does not exist", "code", "task_not_found")
	f.Add(500, "", "", "")
	f.Add(0, "weird status", "trace_id", "abc")
	f.Add(999, "nul\x00 and bytes \xff", "k\"ey", "va\nlue")
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

		gin.SetMode(gin.TestMode)
		e := gin.New()
		e.GET("/", func(c *gin.Context) { httpx.AbortProblem(c, p) })
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != want {
			t.Fatalf("response status %d != problem status %d", rec.Code, want)
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

// FuzzRedact: a config struct holding an arbitrary string — as a DSN, as a
// tagged secret and inside a slice — never panics on the way through Redact
// and json.Marshal, and a password given as URL userinfo never survives.
func FuzzRedact(f *testing.F) {
	f.Add("postgres://app:hunter2@db:5432/app?sslmode=disable", "hunter2")
	f.Add("plain value", "")
	f.Add("://@", "")
	f.Add("http://u:p%zz@h/", "p%zz")
	f.Add("kafka://u:pa ss@b:9092,b2:9092", "pa ss")
	f.Fuzz(func(t *testing.T, s, password string) {
		type cfg struct {
			DSN    string   `yaml:"dsn"`
			Secret string   `yaml:"secret"`
			List   []string `yaml:"list"`
		}
		b, err := json.Marshal(httpx.Redact(cfg{DSN: s, Secret: s, List: []string{s}}))
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
		// When s is a URL whose userinfo password is `password`, the password
		// must be gone from the DSN and the list entry alike.
		if u, err := url.Parse(s); err == nil && u.User != nil && password != "" {
			if pw, has := u.User.Password(); has && pw == password {
				for _, v := range []any{m["dsn"], m["list"].([]any)[0]} {
					if strings.Contains(v.(string), password) {
						t.Fatalf("password %q survived in %q", password, v)
					}
				}
			}
		}
	})
}
