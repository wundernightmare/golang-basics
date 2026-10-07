// Package api holds the ping service's HTTP routes. It is deliberately thin:
// all cross-cutting concerns (logging, metrics, tracing, health, shutdown)
// live in the shared libs/httpx package, so this file is only the service's
// own surface.
package api

import (
	"net/http"

	"github.com/tracehubmmp/golang-basics/libs/contracts/pingapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// PongResponse is the body returned by GET /ping — the generated contract
// type (api/tsp/ping.tsp).
type PongResponse = pingapi.PongResponse

// VersionResponse is the body returned by GET /version — the same build
// identity the admin listener serves, exposed here on the API port as an
// example of a public "what am I talking to" endpoint.
type VersionResponse = httpx.BuildInfo

// Register attaches the ping service's routes to the server's API mux.
func Register(srv *httpx.Server) {
	mux := srv.Mux()
	mux.HandleFunc("GET /ping", pong)
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, srv.Build)
	})
}

// pong answers GET /ping with {"message":"pong"}, echoing an optional ?msg=.
func pong(w http.ResponseWriter, r *http.Request) {
	resp := PongResponse{Message: pingapi.Pong}
	if msg := r.URL.Query().Get("msg"); msg != "" {
		resp.Echo = &msg
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}
