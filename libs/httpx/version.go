package httpx

import (
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
var Version = "dev"

// BuildInfo is the identity of the running binary — what the /version endpoint
// returns and what the build_info metric carries as labels.
type BuildInfo struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	Revision  string `json:"revision"`   // vcs.revision when built from a checkout, else "unknown"
	BuildTime string `json:"build_time"` // vcs.time, RFC 3339, when known
	Modified  bool   `json:"modified"`   // vcs.modified: built from a dirty tree
	GoVersion string `json:"go_version"`
}

// Build reads the toolchain-embedded build metadata for service. Everything
// but Service and Version comes from [debug.ReadBuildInfo], which is populated
// when the binary is built inside a git checkout (`-buildvcs`, on by default).
// A Docker build has no .git in its context, which is why Version is injected
// separately via -ldflags.
func Build(service string) BuildInfo {
	bi := BuildInfo{Service: service, Version: Version, Revision: "unknown"}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return bi
	}
	bi.GoVersion = info.GoVersion
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			bi.Revision = s.Value
		case "vcs.time":
			bi.BuildTime = s.Value
		case "vcs.modified":
			bi.Modified = s.Value == "true"
		}
	}
	return bi
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
