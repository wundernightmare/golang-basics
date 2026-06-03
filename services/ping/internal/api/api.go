// Package api holds the ping service's HTTP routes. It is deliberately thin:
// all cross-cutting concerns (logging, metrics, health, shutdown) live in the
// shared libs/httpx package, so this file is only the service's own surface.
package api

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// PongResponse is the body returned by GET /ping.
type PongResponse struct {
	Message string `json:"message"`
	Echo    string `json:"echo,omitempty"`
}

// VersionResponse is the body returned by GET /version.
type VersionResponse struct {
	Service  string `json:"service"`
	Revision string `json:"revision"`
	GoVer    string `json:"go_version"`
	Time     string `json:"time"`
}

// Register attaches the ping service's routes to the shared server engine.
func Register(srv *httpx.Server) {
	e := srv.Engine()
	e.GET("/ping", pong)
	e.GET("/version", version)
}

// pong answers GET /ping with {"message":"pong"}, echoing an optional ?msg=.
func pong(c *gin.Context) {
	resp := PongResponse{Message: "pong"}
	if msg := c.Query("msg"); msg != "" {
		resp.Echo = msg
	}
	c.JSON(http.StatusOK, resp)
}

// version reports build metadata embedded by the Go toolchain (VCS revision
// when built from a git checkout; "unknown" otherwise).
func version(c *gin.Context) {
	c.JSON(http.StatusOK, VersionResponse{
		Service:  "ping",
		Revision: vcsRevision(),
		GoVer:    runtimeVersion(),
		Time:     time.Now().UTC().Format(time.RFC3339),
	})
}

func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}
	return "unknown"
}

func runtimeVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return info.GoVersion
}
