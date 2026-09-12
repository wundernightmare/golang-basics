#!/bin/sh
# Prints every tool pin from mise.toml as KEY=value lines — the single source
# of truth both CI pipelines read instead of hard-coding versions.
#
#   GitLab:  sh scripts/mise-pins.sh | tee versions.env   (dotenv artifact)
#   GitHub:  scripts/mise-pins.sh >> "$GITHUB_OUTPUT"      (job outputs)
#
# POSIX sh on purpose: the GitLab `versions` job runs it in a bare alpine
# image. Exits non-zero if any expected pin is missing, so a typo in mise.toml
# fails here instead of surfacing later as a broken `image:` tag.
set -eu

mt="${1:-mise.toml}"

pin() {
  grep -E "^\"?$1\"?[[:space:]]*=" "$mt" | head -1 \
    | sed -E 's/.*=[[:space:]]*"([^"]+)".*/\1/'
}

emit() {
  v="$(pin "$2")"
  if [ -z "$v" ]; then
    echo "mise-pins: no pin for '$2' in $mt" >&2
    exit 1
  fi
  echo "$1=$v"
}

emit GO_VERSION          go
emit NODE_VERSION        node
emit GOLANGCI_VERSION    golangci-lint
emit K6_VERSION          k6
emit OSV_VERSION         osv-scanner
emit SYFT_VERSION        syft
emit GRYPE_VERSION       grype
emit GITLEAKS_VERSION    gitleaks
emit SEMGREP_VERSION     semgrep
emit HADOLINT_VERSION    hadolint
emit COSIGN_VERSION      cosign
emit GOTESTSUM_VERSION   go:gotest.tools/gotestsum
emit COBERTURA_VERSION   go:github.com/boumenot/gocover-cobertura
emit GOVULNCHECK_VERSION go:golang.org/x/vuln/cmd/govulncheck
emit GOTESTCOV_VERSION   go:github.com/vladopajic/go-test-coverage/v2
emit GREMLINS_VERSION    go:github.com/go-gremlins/gremlins/cmd/gremlins
emit OAPICODEGEN_VERSION go:github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
emit GOJSONSCHEMA_VERSION go:github.com/atombender/go-jsonschema
emit OASDIFF_VERSION     go:github.com/oasdiff/oasdiff
