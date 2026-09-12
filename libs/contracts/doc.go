// Package contracts holds the Go side of the workspace's contracts — generated,
// never edited:
//
//   - tasksapi: request/response types of the tasks HTTP API, from
//     api/openapi3/tasks.openapi.yaml (oapi-codegen, types only);
//   - events: the Kafka event payloads, from api/jsonschema/*.json
//     (go-jsonschema), shared by the producer (services/tasks) and the
//     consumer (services/consumer) so the wire shape is one type, not two.
//
// The source of both is TypeSpec under api/tsp. `just contracts` regenerates
// everything; `just contracts-check` fails when the generated files are stale
// or the change is breaking (oasdiff against master).
package contracts
