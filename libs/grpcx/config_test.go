package grpcx

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

func TestLoadConfig_OverlaysDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig([]byte(`
targets:
  - name: billing
    address: dns:///billing.internal:443
    timeout: 2s
    retry_methods: [billing.v1.Invoices/Get]
    breaker_failure_ratio: 0
  - name: search
    address: dns:///search.internal:443
    insecure: true
    retry_max_attempts: 0
    breaker_min_requests: 5
`))
	require.NoError(t, err)

	billing := DefaultTarget("billing", "dns:///billing.internal:443")
	billing.Timeout = 2 * time.Second
	billing.RetryMethods = []string{"billing.v1.Invoices/Get"}
	billing.FailureRatio = 0

	search := DefaultTarget("search", "dns:///search.internal:443")
	search.Insecure = true
	search.RetryMaxAttempts = 0
	search.MinRequests = 5

	assert.Equal(t, DefaultConfig(billing, search), cfg, "omitted keys keep their defaults; an explicit 0 disables")
}

func TestLoadConfig_EmptyIsNoTargets(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig(nil)
	require.NoError(t, err)
	assert.Empty(t, cfg.Targets)
}

func TestLoadConfig_RejectsUnknownKeysAndInvalid(t *testing.T) {
	t.Parallel()

	_, err := LoadConfig([]byte("targets:\n  - name: a\n    address: x\n    retry_max_atempts: 3\n"))
	require.ErrorContains(t, err, "retry_max_atempts", "a misspelt target key")

	_, err = LoadConfig([]byte("target: []\n"))
	require.ErrorContains(t, err, "target", "a misspelt top-level key")

	_, err = LoadConfig([]byte("targets:\n  - name: a\n"))
	require.ErrorContains(t, err, `target "a": address is empty`)
}

func TestDefaultTarget(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig(DefaultTarget("a", "dns:///a:443"))
	require.NoError(t, cfg.Validate())

	tc := cfg.Targets[0]
	assert.False(t, tc.Insecure, "TLS unless the config says otherwise")
	assert.Equal(t, resilience.DefaultBreakerConfig(), tc.BreakerConfig)
	assert.Equal(t, []string{"UNAVAILABLE"}, tc.RetryCodes)
	assert.Zero(t, tc.KeepaliveTime, "keepalive is off unless agreed with the server")
}

func TestValidate(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&Config{}).Validate(), "no targets is valid")

	valid := func() Config {
		return DefaultConfig(DefaultTarget("a", "dns:///a:443"), DefaultTarget("b", "dns:///b:443"))
	}

	for _, tc := range []struct {
		name string
		mut  func(*TargetConfig)
		msg  string
	}{
		{"empty name", func(t *TargetConfig) { t.Name = "" }, "name is empty"},
		{"duplicate", func(t *TargetConfig) { t.Name = "b" }, "declared twice"},
		{"address", func(t *TargetConfig) { t.Address = "" }, "address is empty"},
		{"timeout", func(t *TargetConfig) { t.Timeout = -1 }, "timeout must not"},
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
		{"tokens negative", func(t *TargetConfig) { t.RetryThrottleMaxTokens = -1 }, "retry_throttle_max_tokens"},
		{"tokens above gRPC's cap", func(t *TargetConfig) { t.RetryThrottleMaxTokens = 1001 }, "retry_throttle_max_tokens"},
		{"token ratio", func(t *TargetConfig) { t.RetryThrottleTokenRatio = 0 }, "retry_throttle_token_ratio"},
		{"breaker", func(t *TargetConfig) { t.MinRequests = 0 }, "breaker_min_requests"},
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
	edge.RetryMethods = []string{"a.B", "a.B/C", "/a.B/C"}

	require.NoError(t, cfg.Validate())

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
			  "methodConfig":[{"name":[{}],"timeout":"5s","retryPolicy":{
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
			`{"loadBalancingConfig":[{"round_robin":{}}],"methodConfig":[{"name":[{}],"timeout":"5s","waitForReady":true}]}`,
		},
		{
			"no throttling",
			func(t *TargetConfig) { t.RetryThrottleMaxTokens, t.RetryMaxAttempts = 0, 2 },
			`{"loadBalancingConfig":[{"round_robin":{}}],
			  "methodConfig":[{"name":[{}],"timeout":"5s","retryPolicy":{
			    "maxAttempts":2,"initialBackoff":"0.1s","maxBackoff":"5s","backoffMultiplier":2,"retryableStatusCodes":["UNAVAILABLE"]}}]}`,
		},
		{
			"retry methods get their own entry; the default one keeps the timeout",
			func(t *TargetConfig) {
				t.Timeout = 1500 * time.Millisecond
				t.RetryMethods = []string{"a.B", "/a.C/Get"}
				t.RetryCodes = []string{"UNAVAILABLE", "ABORTED"}
				t.RetryThrottleMaxTokens, t.RetryThrottleTokenRatio = 100, 0.5
			},
			`{"loadBalancingConfig":[{"round_robin":{}}],
			  "methodConfig":[
			    {"name":[{}],"timeout":"1.5s"},
			    {"name":[{"service":"a.B"},{"service":"a.C","method":"Get"}],"timeout":"1.5s","retryPolicy":{
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
	}
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
		5 * time.Second:        "5s",
		100 * time.Millisecond: "0.1s",
		time.Millisecond:       "0.001s",
		90 * time.Minute:       "5400s",
		time.Nanosecond:        "0.000000001s",
	} {
		assert.Equal(t, want, protoDuration(d))
	}
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
