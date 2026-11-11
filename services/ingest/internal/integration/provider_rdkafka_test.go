//go:build !kafka_franz

package integration_test

// buildTags are the tags the child binary is built with: the same Kafka
// provider this test binary links.
const buildTags = ""
