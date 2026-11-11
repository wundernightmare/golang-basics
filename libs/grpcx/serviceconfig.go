package grpcx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// The gRPC service config, as JSON
// (https://github.com/grpc/grpc/blob/master/doc/service_config.md): the part
// of a target's policy that grpc-go implements itself.
type (
	serviceConfig struct {
		LoadBalancingConfig []map[string]struct{} `json:"loadBalancingConfig"`
		MethodConfig        []methodConfig        `json:"methodConfig"`
		RetryThrottling     *retryThrottling      `json:"retryThrottling,omitempty"`
	}

	methodConfig struct {
		Name         []methodName `json:"name"`
		Timeout      string       `json:"timeout,omitempty"`
		WaitForReady bool         `json:"waitForReady,omitempty"`
		RetryPolicy  *retryPolicy `json:"retryPolicy,omitempty"`
	}

	methodName struct {
		Service string `json:"service,omitempty"`
		Method  string `json:"method,omitempty"`
	}

	retryPolicy struct {
		MaxAttempts          int      `json:"maxAttempts"`
		InitialBackoff       string   `json:"initialBackoff"`
		MaxBackoff           string   `json:"maxBackoff"`
		BackoffMultiplier    float64  `json:"backoffMultiplier"`
		RetryableStatusCodes []string `json:"retryableStatusCodes"`
	}

	retryThrottling struct {
		MaxTokens  int     `json:"maxTokens"`
		TokenRatio float64 `json:"tokenRatio"`
	}
)

// backoffMultiplier doubles the backoff per retry, as the HTTP client does.
const backoffMultiplier = 2

// serviceConfigJSON renders t's service config. The entry named {} is the
// default for every method; with retry_methods a second entry, which gRPC
// prefers for the methods it names, carries the retry policy.
func serviceConfigJSON(target *TargetConfig) (string, error) {
	base := methodConfig{Name: []methodName{{}}, WaitForReady: target.WaitForReady}
	if target.Timeout > 0 {
		base.Timeout = protoDuration(target.Timeout)
	}

	config := serviceConfig{LoadBalancingConfig: []map[string]struct{}{{target.LoadBalancing: {}}}}

	config.MethodConfig = []methodConfig{base}

	if target.retries() && len(target.RetryMethods) == 0 {
		config.MethodConfig[0].RetryPolicy = target.retryPolicy()
	}

	if target.retries() && len(target.RetryMethods) > 0 {
		retried := base
		retried.Name = nil
		retried.RetryPolicy = target.retryPolicy()

		for _, m := range target.RetryMethods {
			service, method, _ := splitMethod(m) // validated
			retried.Name = append(retried.Name, methodName{Service: service, Method: method})
		}

		config.MethodConfig = append(config.MethodConfig, retried)
	}

	if target.retries() && target.RetryThrottleMaxTokens > 0 {
		config.RetryThrottling = &retryThrottling{MaxTokens: target.RetryThrottleMaxTokens, TokenRatio: target.RetryThrottleTokenRatio}
	}

	raw, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("grpcx: service config of %q: %w", target.Name, err)
	}

	return string(raw), nil
}

func (t *TargetConfig) retryPolicy() *retryPolicy {
	return &retryPolicy{
		MaxAttempts:          t.RetryMaxAttempts,
		InitialBackoff:       protoDuration(t.RetryBaseDelay),
		MaxBackoff:           protoDuration(t.RetryMaxDelay),
		BackoffMultiplier:    backoffMultiplier,
		RetryableStatusCodes: t.RetryCodes,
	}
}

// protoDuration is d in protobuf's JSON form: decimal seconds, "0.1s".
func protoDuration(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}
