module github.com/tracehubmmp/golang-basics/libs/kafka-franz

go 1.27.0

require (
	github.com/stretchr/testify v1.12.1
	github.com/tracehubmmp/golang-basics/libs/kafka v0.0.0
	github.com/tracehubmmp/golang-basics/libs/secret v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.22.3
	github.com/twmb/franz-go/pkg/kmsg v1.15.1
	go.uber.org/goleak v1.3.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/caarlos0/env/v11 v11.4.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/tracehubmmp/golang-basics/libs/httpx v0.0.0 // indirect
	github.com/tracehubmmp/golang-basics/libs/resilience v0.0.0-00010101000000-000000000000 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/tracehubmmp/golang-basics/libs/kafka => ../kafka

replace github.com/tracehubmmp/golang-basics/libs/httpx => ../httpx

replace github.com/tracehubmmp/golang-basics/libs/secret => ../secret

replace github.com/tracehubmmp/golang-basics/libs/resilience => ../resilience

replace github.com/tracehubmmp/golang-basics/libs/testx => ../testx
