module github.com/tracehubmmp/golang-basics/services/ping

go 1.27.0

require (
	github.com/stretchr/testify v1.12.1
	github.com/tracehubmmp/golang-basics/libs/contracts v0.0.0
	github.com/tracehubmmp/golang-basics/libs/httpx v0.0.0
	github.com/tracehubmmp/golang-basics/libs/testx/contract v0.0.0-00010101000000-000000000000
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/caarlos0/env/v11 v11.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/getkin/kin-openapi v0.149.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v1.0.1 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.71.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3 // indirect
	github.com/tracehubmmp/golang-basics/libs/testx v0.0.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// Resolved to the local module by go.work; this replace keeps `go build`
// working outside the workspace too (e.g. inside the per-service Docker build).
replace github.com/tracehubmmp/golang-basics/libs/httpx => ../../libs/httpx

replace github.com/tracehubmmp/golang-basics/libs/testx => ../../libs/testx

replace github.com/tracehubmmp/golang-basics/libs/testx/contract => ../../libs/testx/contract

replace github.com/tracehubmmp/golang-basics/libs/contracts => ../../libs/contracts
