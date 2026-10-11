//go:build !kafka_franz

// Package kafkaprovider links the Kafka client library this binary is
// built with — exactly one, chosen at build time (KAFKA_PROVIDER in
// scripts/build-service.sh and the Dockerfile):
//
//	(default)          librdkafka via libs/kafka-rdkafka — cgo, a static musl binary in the image
//	-tags kafka_franz  franz-go via libs/kafka-franz — pure Go, CGO_ENABLED=0
//
// main and the integration tests import it for its side effect; everything
// else uses libs/kafka, which does not know which one is linked.
package kafkaprovider

import _ "github.com/tracehubmmp/golang-basics/libs/kafka-rdkafka" // the librdkafka provider
