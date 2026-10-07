module github.com/tracehubmmp/golang-basics/libs/resilient-http-client

go 1.27.0

require (
	github.com/prometheus/client_golang v1.24.1
	github.com/stretchr/testify v1.12.1
	github.com/tracehubmmp/golang-basics/libs/testx v0.0.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.71.0
	go.uber.org/goleak v1.3.0
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/time v0.16.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.71.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/sdk v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/tracehubmmp/golang-basics/libs/testx => ../testx
