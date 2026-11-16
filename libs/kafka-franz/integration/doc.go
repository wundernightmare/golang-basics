// Package integration is the integration suite of libs/kafka-franz: the
// libs/kafka conformance suite (libs/kafka/kafkatest) against a real Kafka,
// with the franz-go provider linked.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client — stays out of the
// provider's go.mod.
package integration
