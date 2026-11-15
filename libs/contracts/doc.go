// Package contracts holds the Go side of the workspace's contracts — generated,
// never edited:
//
//   - one <service>api package per HTTP API (tasksapi, …): its request and
//     response types, from api/openapi3/<service>.openapi.yaml (oapi-codegen,
//     types only);
//   - events: the Kafka event payloads, from api/jsonschema/*.json
//     (go-jsonschema), shared by the producer (services/tasks) and the
//     consumer (services/consumer) so the wire shape is one type, not two.
//
// The source of all of them is TypeSpec under api/tsp. `just contracts`
// regenerates everything; `just contracts-check` fails when the generated
// files are stale or the change is breaking (oasdiff against master).
package contracts
