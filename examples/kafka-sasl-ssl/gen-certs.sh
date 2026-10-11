#!/usr/bin/env bash
# gen-certs.sh — a private CA and the certificates the SASL_SSL example runs on.
#
#   examples/kafka-sasl-ssl/gen-certs.sh     # → examples/kafka-sasl-ssl/certs/ (gitignored)
#
# ca.crt      the CA clients trust: KAFKA_SSL_CA_LOCATION (a file, mounted from a Secret)
# broker.*    the broker's certificate, SAN localhost + redpanda (the compose service)
# client.*    a client certificate, for brokers that also require mTLS
# app.password the app user's password as a file — how a mounted Secret
#             hands it over (KAFKA_SASL_PASSWORD_FILE)
#
# Re-running keeps the CA and re-issues the leaf certificates. A real cluster
# gets its certificates from the platform's CA (cert-manager, Vault, …); only
# ca.crt — or nothing, for a publicly trusted CA — reaches the app.
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
mkdir -p "$dir"
cd "$dir"

if [ ! -f ca.key ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
    -keyout ca.key -out ca.crt -subj "/CN=kafka-sasl-ssl-example-ca" 2>/dev/null
fi

# issue <name> <subjectAltName> <extendedKeyUsage>
issue() {
  openssl req -newkey rsa:2048 -nodes -sha256 -keyout "$1.key" -out "$1.csr" -subj "/CN=$1" 2>/dev/null
  openssl x509 -req -sha256 -days 825 -in "$1.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$1.crt" -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=%s\n' "$2" "$3") 2>/dev/null
  rm -f "$1.csr"
}
issue broker "DNS:localhost,DNS:redpanda,IP:127.0.0.1" serverAuth
issue client "DNS:app" clientAuth
printf %s app-secret > app.password

# The broker runs as another user inside its container. Example certificates
# only: a real key is never world-readable.
chmod 0644 ./*.crt ./*.key ./app.password
echo "certificates in $dir"
