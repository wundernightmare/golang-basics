// Package integration is the integration suite of libs/kafka-rdkafka: the
// libs/kafka conformance suite (libs/kafka/kafkatest) and the librdkafka
// provider's own broker-backed cases, against a real Kafka.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client — stays out of the
// provider's go.mod.
package integration
