package kafkatest

import "github.com/tracehubmmp/golang-basics/libs/kafka"

// BrokerSendErrorKinds is the README "Errors" table for the errors a broker
// answers with: Kafka protocol error code → [kafka.SendError] kind. Each
// provider's unit test holds its own code map to it, so a broker error is
// the same kind whichever client is linked. A code that is not here is
// [kafka.KindUnknown] in both.
func BrokerSendErrorKinds() map[int16]string {
	return map[int16]string{
		5:  kafka.KindUnavailable, // LEADER_NOT_AVAILABLE
		6:  kafka.KindUnavailable, // NOT_LEADER_OR_FOLLOWER
		7:  kafka.KindUnavailable, // REQUEST_TIMED_OUT
		13: kafka.KindUnavailable, // NETWORK_EXCEPTION
		19: kafka.KindUnavailable, // NOT_ENOUGH_REPLICAS
		20: kafka.KindUnavailable, // NOT_ENOUGH_REPLICAS_AFTER_APPEND
		56: kafka.KindUnavailable, // KAFKA_STORAGE_ERROR
		89: kafka.KindUnavailable, // THROTTLING_QUOTA_EXCEEDED

		3:  kafka.KindNotProvisioned, // UNKNOWN_TOPIC_OR_PARTITION
		29: kafka.KindNotProvisioned, // TOPIC_AUTHORIZATION_FAILED
		31: kafka.KindNotProvisioned, // CLUSTER_AUTHORIZATION_FAILED
		33: kafka.KindNotProvisioned, // UNSUPPORTED_SASL_MECHANISM
		34: kafka.KindNotProvisioned, // ILLEGAL_SASL_STATE
		53: kafka.KindNotProvisioned, // TRANSACTIONAL_ID_AUTHORIZATION_FAILED
		58: kafka.KindNotProvisioned, // SASL_AUTHENTICATION_FAILED

		2:  kafka.KindRejected, // CORRUPT_MESSAGE
		10: kafka.KindRejected, // MESSAGE_TOO_LARGE
		17: kafka.KindRejected, // INVALID_TOPIC_EXCEPTION
		18: kafka.KindRejected, // RECORD_LIST_TOO_LARGE
		44: kafka.KindRejected, // POLICY_VIOLATION
		87: kafka.KindRejected, // INVALID_RECORD
	}
}
