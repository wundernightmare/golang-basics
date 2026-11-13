package httpx

import (
	"crypto/fips140"
	"os"
	"path/filepath"
	"runtime/debug"
)

// Version is the service version reported in build_info, /version and the
// OpenTelemetry service.version resource. It is "dev" unless the build
// overrides it:
//
//	go build -ldflags "-X github.com/tracehubmmp/golang-basics/libs/httpx.Version=v1.4.2"
//
// (scripts/build-service.sh and the Dockerfiles do this from VERSION.)
var Version = "dev" //nolint:gochecknoglobals // set by -ldflags -X, which needs a variable

// Revision and BuildTime are the VCS revision and build timestamp. They come
// from the toolchain's embedded VCS info when the binary is built inside a
// checkout, and from -ldflags (-X …httpx.Revision=<sha>) in a Docker build,
// whose context has no .git.
//
//nolint:gochecknoglobals // set by -ldflags -X / embedded VCS info, which needs variables
var (
	Revision  = ""
	BuildTime = ""
)

// BuildInfo is the identity of the running binary — what the /version endpoint
// returns and what the build_info metric carries as labels.
type BuildInfo struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	Revision  string `json:"revision"`   // vcs.revision or the injected Revision, else "unknown"
	BuildTime string `json:"build_time"` // vcs.time or the injected BuildTime, RFC 3339, when known
	Modified  bool   `json:"modified"`   // vcs.modified: built from a dirty tree
	GoVersion string `json:"go_version"`
	FIPS140   bool   `json:"fips140"` // the Go Cryptographic Module runs in FIPS 140-3 mode, see [FIPS140]
}

// FIPS140 reports whether the Go Cryptographic Module runs in FIPS 140-3 mode
// ([fips140.Enabled]): the binary was built with GOFIPS140 (FIPS=1 in
// scripts/build-service.sh, which makes fips140=on the default GODEBUG) or was
// started with GODEBUG=fips140=on. It covers Go's own crypto (TLS, hashes,
// signatures) and nothing linked through cgo — docs/RUNTIME.md "FIPS 140-3".
func FIPS140() bool { return fips140.Enabled() }

// Build reads the toolchain-embedded build metadata for service. Everything
// but Service and Version comes from [debug.ReadBuildInfo], which is populated
// when the binary is built inside a git checkout (`-buildvcs`, on by default),
// with the injected Revision / BuildTime as the fallback.
func Build(service string) BuildInfo {
	build := BuildInfo{Service: service, Version: Version, Revision: Revision, BuildTime: BuildTime, FIPS140: FIPS140()}
	if build.Revision == "" {
		build.Revision = "unknown"
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return build
	}

	build.GoVersion = info.GoVersion
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			build.Revision = s.Value
		case "vcs.time":
			build.BuildTime = s.Value
		case "vcs.modified":
			build.Modified = s.Value == "true"
		}
	}

	return build
}

// defaultServiceName is what Config.Service falls back to: the executable's
// base name, which is the service name for every binary in this workspace.
func defaultServiceName() string {
	exe, err := os.Executable()
	if err != nil {
		return "service"
	}

	return filepath.Base(exe)
}
