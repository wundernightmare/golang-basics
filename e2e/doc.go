// Package e2e drives the real service binaries as child processes over HTTP.
//
// Every test file carries the `e2e` build tag, so `go test ./...` without the
// tag compiles only this file and runs nothing. Run the suite with
//
//	go test -tags e2e -count=1 ./...
//
// against binaries in E2E_BIN_DIR (default ../.build). See README.md.
package e2e
