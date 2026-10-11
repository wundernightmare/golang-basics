# Kafka over SASL_SSL with your own CA

How a service connects to a cluster that wants SCRAM credentials over TLS
signed by a private CA — the usual shape of a managed or platform-run Kafka —
and a local broker to try it against. Both Kafka providers (librdkafka and
franz-go, see [libs/kafka](../../libs/kafka/README.md)) are tested against it.

| File | What it is |
|---|---|
| `gen-certs.sh` | a CA, the broker's certificate (SAN `localhost`, `redpanda`, `127.0.0.1`), a client certificate for mTLS, the app user's password file → `certs/` (gitignored) |
| `compose.yml` | Redpanda with one SASL_SSL listener on `localhost:29094`, users `app` / `admin`, ACLs for `app` |
| `configmap.yaml` | the same client settings on Kubernetes: a ConfigMap with the service's `config.yaml`, the password and `ca.crt` from a Secret mounted as files |

```sh
just kafka-sasl-ssl-up        # certificates, broker, users and ACLs
just kafka-sasl-ssl-test      # TestSASLSSL of both providers against it
just kafka-sasl-ssl-down
```

The client settings — env or `config.yaml`, the same keys (`<service>
--config-reference` lists them all; `CONSUMER_` is the consumer's prefix):

```sh
CONSUMER_KAFKA_BROKERS=localhost:29094
CONSUMER_KAFKA_SECURITY_PROTOCOL=sasl_ssl
CONSUMER_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
CONSUMER_KAFKA_SASL_USERNAME=app
CONSUMER_KAFKA_SASL_PASSWORD_FILE=examples/kafka-sasl-ssl/certs/app.password  # or …_SASL_PASSWORD
CONSUMER_KAFKA_SSL_CA_LOCATION=examples/kafka-sasl-ssl/certs/ca.crt
```

- **Files, not values.** The password and the CA come from a mounted Secret;
  the password file is read once — a new password is a rollout restart.
- **mTLS.** When the broker also asks for a client certificate, set
  `KAFKA_SSL_CERTIFICATE_LOCATION` / `KAFKA_SSL_KEY_LOCATION` (commented out
  in `configmap.yaml`); the compose broker does not require one
  (`require_client_auth: false`). Both providers map them.
- **Hostname check.** Both providers verify the broker's name against its
  certificate (`KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https`, the
  default): the certificate must carry the advertised host names. `none`
  skips only that check — the chain is still verified against the CA.
- **Any other librdkafka property** goes through `KAFKA_EXTRA_PROPERTIES`
  (every client), `KAFKA_PRODUCER_EXTRA_PROPERTIES`,
  `KAFKA_CONSUMER_EXTRA_PROPERTIES` — a JSON object in env, a mapping in YAML.
  Buffer and message sizes are not among them: they have their own keys and
  are always set (libs/kafka README, "Memory"). A franz-go build has no
  property passthrough and refuses them.
- **ACLs.** `app` may do anything on any topic and group here; a real
  cluster scopes it to the service's topics and groups. Idempotent producers
  (always on) also need `IDEMPOTENT_WRITE` on the cluster.
