package grpcx

import (
	"encoding/json"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/wundernightmare/golang-basics/libs/resilience"
)

func TestParseYAML_OverlaysDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := ParseYAML([]byte(`
targets:
  - name: billing
    address: dns:///billing.internal:443
    timeout: 2s
    retry_methods: [billing.v1.Invoices/Get]
    breaker:
      failure_ratio: 0
  - name: search
    address: dns:///search.internal:443
    insecure: true
    retry_max_attempts: 0
    breaker:
      min_requests: 5
`))
	require.NoError(t, err)

	billing := DefaultTarget("billing", "dns:///billing.internal:443")
	billing.Timeout = 2 * time.Second
	billing.RetryMethods = []string{"billing.v1.Invoices/Get"}
	billing.Breaker.FailureRatio = 0

	search := DefaultTarget("search", "dns:///search.internal:443")
	search.Insecure = true
	search.RetryMaxAttempts = 0
	search.Breaker.MinRequests = 5

	assert.Equal(t, DefaultConfig(billing, search), cfg, "omitted keys keep their defaults; an explicit 0 disables")
}

func TestParseYAML_EmptyIsNoTargets(t *testing.T) {
	t.Parallel()

	cfg, err := ParseYAML(nil)
	require.NoError(t, err)
	assert.Empty(t, cfg.Targets)
}

func TestParseYAML_RejectsUnknownKeysAndInvalid(t *testing.T) {
	t.Parallel()

	_, err := ParseYAML([]byte("targets:\n  - name: a\n    address: x\n    retry_max_atempts: 3\n"))
	require.ErrorContains(t, err, "retry_max_atempts", "a misspelt target key")
	require.ErrorIs(t, err, ErrConfig)

	_, err = ParseYAML([]byte("target: []\n"))
	require.ErrorContains(t, err, "target", "a misspelt top-level key")

	_, err = ParseYAML([]byte("targets:\n  - name: a\n"))
	require.ErrorContains(t, err, `target "a": address is empty`)
	require.ErrorIs(t, err, ErrConfig)
}

func TestDefaultTarget(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig(DefaultTarget("a", "dns:///a:443"))
	require.NoError(t, cfg.Validate())

	tc := cfg.Targets[0]
	assert.False(t, tc.Insecure, "TLS unless the config says otherwise")
	assert.Equal(t, resilience.DefaultBreakerConfig(), tc.Breaker)
	assert.Equal(t, []string{"UNAVAILABLE"}, tc.RetryCodes)
	assert.Zero(t, tc.KeepaliveTime, "keepalive is off unless agreed with the server")
}

func TestValidate(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&Config{}).Validate(), "no targets is valid")

	valid := func() Config {
		return DefaultConfig(DefaultTarget("a", "dns:///a:443"), DefaultTarget("b", "dns:///b:443"))
	}
	noSuchDir := filepath.Join(t.TempDir(), "absent")

	for _, tc := range []struct {
		name string
		mut  func(*TargetConfig)
		msg  string
	}{
		{"empty name", func(t *TargetConfig) { t.Name = "" }, "name is empty"},
		{"duplicate", func(t *TargetConfig) { t.Name = "b" }, "declared twice"},
		{"address", func(t *TargetConfig) { t.Address = "" }, "address is empty"},
		{"address: two slashes put the host into the authority", func(t *TargetConfig) { t.Address = "dns://billing.internal:443" }, "address has no endpoint"},
		{"address: a URL, not a target", func(t *TargetConfig) { t.Address = "https://billing.internal" }, "address has no endpoint"},
		{"address: not host:port", func(t *TargetConfig) { t.Address = "billing.internal:443:443" }, "address is not host:port"},
		{"address: dns endpoint not host:port", func(t *TargetConfig) { t.Address = "dns:///billing.internal:" }, "address is not host:port"},
		{"ca_file without TLS", func(t *TargetConfig) { t.Insecure, t.CAFile = true, "ca.pem" }, "ca_file is set but insecure is true"},
		{"ca_file that cannot be read", func(t *TargetConfig) { t.CAFile = filepath.Join(noSuchDir, "ca.pem") }, "ca_file: open"},
		{"timeout", func(t *TargetConfig) { t.Timeout = -1 }, "timeout must not"},
		{"wait_for_ready without a timeout", func(t *TargetConfig) { t.WaitForReady, t.Timeout = true, 0 }, "timeout must be > 0 with wait_for_ready"},
		{"lb", func(t *TargetConfig) { t.LoadBalancing = "random" }, "load_balancing"},
		{"lb empty", func(t *TargetConfig) { t.LoadBalancing = "" }, "load_balancing"},
		{"keepalive negative", func(t *TargetConfig) { t.KeepaliveTime = -1 }, "keepalive durations"},
		{"keepalive timeout negative", func(t *TargetConfig) { t.KeepaliveTimeout = -1 }, "keepalive durations"},
		{"keepalive too often", func(t *TargetConfig) { t.KeepaliveTime = 10*time.Second - 1 }, "keepalive_time must be 0 or ≥ 10s"},
		{"keepalive without timeout", func(t *TargetConfig) { t.KeepaliveTime, t.KeepaliveTimeout = time.Minute, 0 }, "keepalive_timeout"},
		{"attempts negative", func(t *TargetConfig) { t.RetryMaxAttempts = -1 }, "retry_max_attempts"},
		{"attempts above gRPC's cap", func(t *TargetConfig) { t.RetryMaxAttempts = 6 }, "retry_max_attempts"},
		{"base delay zero", func(t *TargetConfig) { t.RetryBaseDelay = 0 }, "retry delays"},
		{"max < base", func(t *TargetConfig) { t.RetryMaxDelay = t.RetryBaseDelay - 1 }, "retry delays"},
		{"no codes", func(t *TargetConfig) { t.RetryCodes = nil }, "retry_codes must not be empty"},
		{"bad code", func(t *TargetConfig) { t.RetryCodes = []string{"UNAVAILABLE", "unavailable"} }, `retry_codes: "unavailable"`},
		{"numeric code", func(t *TargetConfig) { t.RetryCodes = []string{"14"} }, `retry_codes: "14"`},
		{"OK code", func(t *TargetConfig) { t.RetryCodes = []string{"OK"} }, `retry_codes: "OK"`},
		{"bad method", func(t *TargetConfig) { t.RetryMethods = []string{"a.B/C/D"} }, "retry_methods"},
		{"empty method", func(t *TargetConfig) { t.RetryMethods = []string{"/"} }, "retry_methods"},
		{"method twice", func(t *TargetConfig) { t.RetryMethods = []string{"a.B/C", "/a.B/C"} }, `retry_methods: "/a.B/C" is listed twice`},
		{"service twice", func(t *TargetConfig) { t.RetryMethods = []string{"a.B", "a.B/"} }, `retry_methods: "a.B/" is listed twice`},
		{"tokens negative", func(t *TargetConfig) { t.RetryThrottleMaxTokens = -1 }, "retry_throttle_max_tokens"},
		{"tokens above gRPC's cap", func(t *TargetConfig) { t.RetryThrottleMaxTokens = 1001 }, "retry_throttle_max_tokens"},
		{"token ratio", func(t *TargetConfig) { t.RetryThrottleTokenRatio = 0 }, "retry_throttle_token_ratio"},
		{"token ratio NaN", func(t *TargetConfig) { t.RetryThrottleTokenRatio = math.NaN() }, "retry_throttle_token_ratio"},
		{"token ratio infinite", func(t *TargetConfig) { t.RetryThrottleTokenRatio = math.Inf(1) }, "retry_throttle_token_ratio"},
		{"breaker", func(t *TargetConfig) { t.Breaker.MinRequests = 0 }, "breaker: min_requests"},
	} {
		c := valid()
		tc.mut(&c.Targets[0])

		err := c.Validate()
		if assert.Error(t, err, tc.name) {
			assert.Contains(t, err.Error(), tc.msg, tc.name)
			assert.Equal(t, 1, strings.Count(err.Error(), "\n")+1, "%s: exactly one problem: %v", tc.name, err)
		}
	}

	// Bounds are inclusive where the doc says so.
	cfg := valid()
	edge := &cfg.Targets[0]
	edge.Timeout = 0
	edge.KeepaliveTime = 10 * time.Second
	edge.RetryMaxAttempts = 5
	edge.RetryMaxDelay = edge.RetryBaseDelay
	edge.RetryThrottleMaxTokens = 1000
	edge.RetryMethods = []string{"a.B", "a.B/C", "/a.B/D"}

	require.NoError(t, cfg.Validate())

	// Every form of target gRPC resolves itself, and a bare host.
	for _, address := range []string{
		"billing.internal:443", "billing.internal", "127.0.0.1:443", "[::1]:443", ":443",
		"dns:///billing.internal:443", "dns:billing.internal", "dns://10.0.0.53/billing.internal:443",
		"passthrough:///anything at all", "unix:///run/billing.sock", "unix:billing.sock",
		"mesh:///billing", // a scheme whose resolver comes with grpc.WithResolvers
	} {
		cfg = valid()
		cfg.Targets[0].Address = address
		require.NoError(t, cfg.Validate(), address)
	}

	// Zero means disabled, never invalid: without retries their knobs are free.
	cfg = DefaultConfig(TargetConfig{Name: "a", Address: "x", LoadBalancing: PickFirst})
	require.NoError(t, cfg.Validate())

	cfg = valid()
	cfg.Targets[0].RetryMaxAttempts = 1
	cfg.Targets[0].RetryBaseDelay, cfg.Targets[0].RetryCodes = 0, nil
	require.NoError(t, cfg.Validate())

	// Throttling off: its ratio is free.
	cfg = valid()
	cfg.Targets[0].RetryThrottleMaxTokens, cfg.Targets[0].RetryThrottleTokenRatio = 0, 0
	require.NoError(t, cfg.Validate())

	// Every problem is reported, not just the first.
	cfg = valid()
	cfg.Targets[0].Timeout = -1
	cfg.Targets[1].Address = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Equal(t, 2, strings.Count(err.Error(), "\n")+1)
}

func TestServiceConfigJSON(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mut  func(*TargetConfig)
		want string
	}{
		{
			"defaults", nil,
			`{"loadBalancingConfig":[{"round_robin":{}}],
			  "methodConfig":[{"name":[{}],"retryPolicy":{
			    "maxAttempts":3,"initialBackoff":"0.1s","maxBackoff":"5s","backoffMultiplier":2,"retryableStatusCodes":["UNAVAILABLE"]}}],
			  "retryThrottling":{"maxTokens":10,"tokenRatio":0.1}}`,
		},
		{
			"everything off",
			func(t *TargetConfig) {
				t.Timeout, t.RetryMaxAttempts, t.LoadBalancing = 0, 0, PickFirst
			},
			`{"loadBalancingConfig":[{"pick_first":{}}],"methodConfig":[{"name":[{}]}]}`,
		},
		{
			"one attempt is no retry policy, and no throttling",
			func(t *TargetConfig) { t.RetryMaxAttempts, t.WaitForReady = 1, true },
			`{"loadBalancingConfig":[{"round_robin":{}}],"methodConfig":[{"name":[{}],"waitForReady":true}]}`,
		},
		{
			"no throttling",
			func(t *TargetConfig) { t.RetryThrottleMaxTokens, t.RetryMaxAttempts = 0, 2 },
			`{"loadBalancingConfig":[{"round_robin":{}}],
			  "methodConfig":[{"name":[{}],"retryPolicy":{
			    "maxAttempts":2,"initialBackoff":"0.1s","maxBackoff":"5s","backoffMultiplier":2,"retryableStatusCodes":["UNAVAILABLE"]}}]}`,
		},
		{
			"retry methods get their own entry",
			func(t *TargetConfig) {
				t.RetryMethods = []string{"a.B", "/a.C/Get"}
				t.RetryCodes = []string{"UNAVAILABLE", "ABORTED"}
				t.RetryThrottleMaxTokens, t.RetryThrottleTokenRatio = 100, 0.5
			},
			`{"loadBalancingConfig":[{"round_robin":{}}],
			  "methodConfig":[
			    {"name":[{}]},
			    {"name":[{"service":"a.B"},{"service":"a.C","method":"Get"}],"retryPolicy":{
			      "maxAttempts":3,"initialBackoff":"0.1s","maxBackoff":"5s","backoffMultiplier":2,"retryableStatusCodes":["UNAVAILABLE","ABORTED"]}}],
			  "retryThrottling":{"maxTokens":100,"tokenRatio":0.5}}`,
		},
	} {
		target := DefaultTarget("a", "dns:///a:443")
		if tc.mut != nil {
			tc.mut(&target)
		}

		got, err := serviceConfigJSON(&target)
		require.NoError(t, err, tc.name)
		assert.JSONEq(t, tc.want, got, tc.name)
		requireAccepted(t, got)
	}
}

// requireAccepted fails unless grpc-go takes serviceConfig as a channel's
// default: it parses the document when the channel is built.
func requireAccepted(t *testing.T, serviceConfig string) {
	t.Helper()

	conn, err := grpc.NewClient("passthrough:///unused",
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(serviceConfig))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestKeepalive(t *testing.T) {
	t.Parallel()

	off := DefaultTarget("a", "x")
	_, ok := off.keepalive()
	assert.False(t, ok, "keepalive_time 0 is off: no pings, not gRPC's 10s floor")

	on := DefaultTarget("a", "x")
	on.KeepaliveTime, on.KeepaliveTimeout = time.Minute, 3*time.Second
	params, ok := on.keepalive()
	require.True(t, ok)
	assert.Equal(t, keepalive.ClientParameters{Time: time.Minute, Timeout: 3 * time.Second}, params)
	assert.False(t, params.PermitWithoutStream, "an idle connection is not pinged")
}

func TestProtoDuration(t *testing.T) {
	t.Parallel()

	for d, want := range map[time.Duration]string{
		5 * time.Second:         "5s",
		100 * time.Millisecond:  "0.1s",
		time.Millisecond:        "0.001s",
		4544 * time.Millisecond: "4.544s",
		90 * time.Minute:        "5400s",
		time.Nanosecond:         "0.000000001s",
		math.MaxInt64:           "9223372036.854775807s",
	} {
		assert.Equal(t, want, protoDuration(d))
	}
}

// grpc-go refuses a duration with more than nine fractional digits, and a
// config is free to name any: every whole millisecond up to a minute goes
// through its parser, one method config each.
func TestProtoDuration_EveryMillisecondIsAcceptedByGRPC(t *testing.T) {
	t.Parallel()

	var methods []methodConfig

	for d := time.Millisecond; d <= time.Minute; d += time.Millisecond {
		target := DefaultTarget("a", "x")
		target.RetryBaseDelay, target.RetryMaxDelay = d, d

		methods = append(methods, methodConfig{
			Name:        []methodName{{Service: "s" + strconv.FormatInt(int64(d), 10)}},
			RetryPolicy: target.retryPolicy(),
		})
	}

	raw, err := json.Marshal(serviceConfig{MethodConfig: methods})
	require.NoError(t, err)
	requireAccepted(t, string(raw))
}

func TestSplitMethod(t *testing.T) {
	t.Parallel()

	for input, want := range map[string][2]string{
		"a.B":     {"a.B", ""},
		"a.B/C":   {"a.B", "C"},
		"/a.B/C":  {"a.B", "C"},
		"a.B/":    {"a.B", ""},
		"":        {},
		"/":       {},
		"/C":      {"C", ""},
		"a.B/C/D": {},
	} {
		service, method, ok := splitMethod(input)
		if want[0] == "" {
			assert.False(t, ok, input)

			continue
		}

		assert.True(t, ok, input)
		assert.Equal(t, want, [2]string{service, method}, input)
	}
}

func TestCodeOutcome(t *testing.T) {
	t.Parallel()

	// Every code gRPC defines has its canonical name, the one retry_codes
	// takes — but CANCELLED, which is spelt like the HTTP client's outcome.
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		name := strings.ToUpper(codeOutcome(c))
		if c == codes.Canceled {
			require.Equal(t, "CANCELED", name)

			name = "CANCELLED"
		}

		parsed, ok := parseCode(name)
		require.True(t, ok, c.String())
		assert.Equal(t, c, parsed)
	}

	assert.Equal(t, "ok", codeOutcome(codes.OK))
	assert.Equal(t, "unavailable", codeOutcome(codes.Unavailable))
	assert.Equal(t, "deadline_exceeded", codeOutcome(codes.DeadlineExceeded))
	assert.Equal(t, "unauthenticated", codeOutcome(codes.Unauthenticated), "the last code gRPC defines")
	assert.Equal(t, "unknown", codeOutcome(codes.Code(17)), "a code gRPC does not define never becomes a label value")
	assert.Equal(t, "unknown", codeOutcome(codes.Code(999)))
}
