// Package api holds the ping service's HTTP routes. It is deliberately thin:
// all cross-cutting concerns (logging, metrics, health, shutdown) live in the
// shared libs/httpx package, so this file is only the service's own surface.
package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// PongResponse is the body returned by GET /ping.
type PongResponse struct {
	Message string `json:"message"`
	Echo    string `json:"echo,omitempty"`
}

// VersionResponse is the body returned by GET /version — the same build
// identity the admin listener serves, exposed here on the API port as an
// example of a public "what am I talking to" endpoint.
type VersionResponse = httpx.BuildInfo

// Register attaches the ping service's routes to the shared server engine.
func Register(srv *httpx.Server) {
	e := srv.Engine()
	e.GET("/ping", pong)
	e.GET("/version", func(c *gin.Context) { c.JSON(http.StatusOK, srv.Build) })
}

// pong answers GET /ping with {"message":"pong"}, echoing an optional ?msg=.
func pong(c *gin.Context) {
	resp := PongResponse{Message: "pong"}
	if msg := c.Query("msg"); msg != "" {
		resp.Echo = msg
	}
	c.JSON(http.StatusOK, resp)
}
