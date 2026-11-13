module github.com/tracehubmmp/golang-basics/libs/grpcx

go 1.27.0

require (
	github.com/prometheus/client_golang v1.25.0
	github.com/stretchr/testify v1.12.1
	github.com/tracehubmmp/golang-basics/libs/httpx v0.0.0
	github.com/tracehubmmp/golang-basics/libs/resilience v0.0.0
	github.com/tracehubmmp/golang-basics/libs/testx v0.0.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
	go.uber.org/goleak v1.3.0
	go.yaml.in/yaml/v3 v3.0.5
	google.golang.org/grpc v1.84.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/caarlos0/env/v11 v11.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/tracehubmmp/golang-basics/libs/secret v0.0.0-00010101000000-000000000000 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260928230214-8a89bd6388cc // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/tracehubmmp/golang-basics/libs/resilience => ../resilience

replace github.com/tracehubmmp/golang-basics/libs/testx => ../testx

replace github.com/tracehubmmp/golang-basics/libs/httpx => ../httpx

replace github.com/tracehubmmp/golang-basics/libs/secret => ../secret
