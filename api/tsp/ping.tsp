import "@typespec/http";
import "@typespec/openapi";

using TypeSpec.Http;

/**
 * Ping — the HTTP contract of services/ping.
 *
 * Small on purpose (the service is the "hello world" of the workspace), but a
 * contract all the same: the Go types (libs/contracts/pingapi), the response
 * validation in the api tests and the Schemathesis run are generated from it.
 */
@service(#{ title: "Ping" })
@server("http://localhost:8080", "local")
@OpenAPI.info(#{ version: "1.0.0" })
namespace Ping;

/** Body of GET /ping. */
model PongResponse {
  /** Always "pong". */
  message: "pong";
  /** The `msg` query parameter, echoed back; absent when it was not given. */
  echo?: string;
}

/**
 * Build identity of the running binary — the same document the admin
 * listener serves on /version (libs/httpx.BuildInfo).
 */
model VersionResponse {
  /** Service name. */
  service: string;
  /** Version stamped at build time (libs/httpx.Version); "dev" locally. */
  version: string;
  /** VCS revision when built from a checkout, else "unknown". */
  revision: string;
  /** vcs.time, RFC 3339, empty when unknown. */
  build_time: string;
  /** Built from a dirty tree. */
  modified: boolean;
  /** Go toolchain version. */
  go_version: string;
}

interface PingOps {
  /** Answer pong, echoing `msg` when given. */
  @route("/ping")
  @get
  ping(@query msg?: string): PongResponse;

  /** Report the build identity on the API port (public on purpose). */
  @route("/version")
  @get
  version(): VersionResponse;
}
