module github.com/tracehubmmp/golang-basics/libs/tiercache/onheap

go 1.27.0

replace github.com/tracehubmmp/golang-basics/libs/tiercache => ../

replace github.com/tracehubmmp/golang-basics/libs/resilience => ../../resilience

require (
	github.com/coocood/freecache v1.2.7
	github.com/stretchr/testify v1.12.1
	github.com/tracehubmmp/golang-basics/libs/tiercache v0.0.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/tracehubmmp/golang-basics/libs/resilience v0.0.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
