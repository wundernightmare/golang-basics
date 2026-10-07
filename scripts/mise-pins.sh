#!/bin/sh
# Prints every tool pin from the mise config files as KEY=value lines — the
# single source of truth both CI pipelines read instead of hard-coding versions.
#
#   GitLab:  sh scripts/mise-pins.sh | tee versions.env   (dotenv artifact)
#   GitHub:  scripts/mise-pins.sh >> "$GITHUB_OUTPUT"      (job outputs)
#
# Reads the core mise.toml AND every profile file (mise.appsec.toml,
# mise.ide.toml, mise.perf.toml, mise.report.toml — see mise.toml), because CI
# needs tools from all of them without ever setting MISE_ENV. mise.local.toml
# (personal, gitignored) is never read.
#
# POSIX sh on purpose: the GitLab `versions` job runs it in a bare alpine
# image. Exits non-zero if any expected pin is missing, so a typo in a mise
# file fails here instead of surfacing later as a broken `image:` tag.
set -eu

dir="${1:-$(cd "$(dirname "$0")/.." && pwd)}"

files=""
for f in "$dir"/mise.toml "$dir"/mise.*.toml; do
  case "$f" in */mise.local.toml|*'*'*) continue ;; esac
  [ -f "$f" ] && files="$files $f"
done
[ -n "$files" ] || { echo "mise-pins: no mise*.toml under $dir" >&2; exit 1; }

pin() {
  # shellcheck disable=SC2086 # $files is a list of paths without spaces
  grep -hE "^\"?$1\"?[[:space:]]*=" $files | head -1 \
    | sed -E 's/^[^=]*=[[:space:]]*"([^"]+)".*/\1/'
}

emit() {
  v="$(pin "$2")"
  if [ -z "$v" ]; then
    echo "mise-pins: no pin for '$2' in$files" >&2
    exit 1
  fi
  echo "$1=$v"
}

# core (mise.toml)
emit GO_VERSION           go
emit GOLANGCI_VERSION     golangci-lint
emit JUST_VERSION         just
emit SHELLCHECK_VERSION   shellcheck
emit NODE_VERSION         node
emit PNPM_VERSION         pnpm
emit GOTESTSUM_VERSION    go:gotest.tools/gotestsum
emit OAPICODEGEN_VERSION  go:github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
emit GOJSONSCHEMA_VERSION go:github.com/atombender/go-jsonschema
emit OASDIFF_VERSION      go:github.com/oasdiff/oasdiff
emit SCHEMATHESIS_VERSION SCHEMATHESIS_VERSION
# appsec (mise.appsec.toml)
emit SEMGREP_VERSION      semgrep
emit GITLEAKS_VERSION     gitleaks
emit OSV_VERSION          osv-scanner
emit HADOLINT_VERSION     hadolint
emit SYFT_VERSION         syft
emit GRYPE_VERSION        grype
emit COSIGN_VERSION       cosign
emit GOVULNCHECK_VERSION  go:golang.org/x/vuln/cmd/govulncheck
# perf (mise.perf.toml)
emit K6_VERSION           k6
emit BENCHSTAT_VERSION    go:golang.org/x/perf/cmd/benchstat
emit GREMLINS_VERSION     go:github.com/go-gremlins/gremlins/cmd/gremlins
emit GOTESTCOV_VERSION    go:github.com/vladopajic/go-test-coverage/v2
emit COBERTURA_VERSION    go:github.com/boumenot/gocover-cobertura
# report (mise.report.toml) — "temurin-25": CI uses ${JAVA_VERSION#*-} as the
# major version for setup-java / the eclipse-temurin image tag.
emit JAVA_VERSION         java
