import "@typespec/http";
import "@typespec/openapi";

using TypeSpec.Http;

/**
 * Tasks — the HTTP contract of services/tasks.
 *
 * This file is the source of truth: the OpenAPI document (api/openapi3), the Go
 * request/response types (libs/contracts/tasksapi) and the response validation
 * in the tests are generated from it. Change the contract here, run
 * `just contracts`, and the compiler tells you which handler no longer fits.
 *
 * Errors are RFC 9457 problem details (application/problem+json); every error
 * response is a `Problem`, and business errors carry a stable `code`.
 */
@service(#{ title: "Tasks" })
@server("http://localhost:8082", "local")
@OpenAPI.info(#{ version: "1.0.0" })
namespace Tasks;

/** A single to-do item. */
model Task {
  /** Server-assigned identifier (UUID). */
  id: string;
  /** Human-readable title; never empty, never containing NUL (PostgreSQL TEXT rejects it). */
  @minLength(1)
  @pattern("^[^\\u0000]+$")
  title: string;
  /** Completion flag. */
  done: boolean;
  /** Creation time, RFC 3339 UTC. */
  created_at: utcDateTime;
}

/** Body of POST /tasks. */
model CreateTaskRequest {
  /**
   * Title of the task. Empty, or containing NUL, is rejected with a 400
   * problem — the pattern states what the store can hold, so a generated
   * schema-compliant request is always accepted (found by Schemathesis).
   */
  @minLength(1)
  @pattern("^[^\\u0000]+$")
  title: string;
}

/** Body of GET /tasks: newest first, at most 100 entries. */
model TaskList {
  tasks: Task[];
}

/** RFC 9457 problem details. Extension members are inlined (RFC 9457 §3.2). */
@error
model Problem {
  /** URI reference identifying the problem type; "about:blank" when generic. */
  type: string;
  /** Short, human-readable summary of the problem type. */
  title: string;
  /** HTTP status of this occurrence — always equals the response status. */
  @minValue(100)
  @maxValue(599)
  status: int32;
  /** Human-readable explanation specific to this occurrence. */
  detail?: string;
  /** URI reference identifying this occurrence (the request path). */
  instance?: string;
  /** Stable machine-readable code for business errors (e.g. task_not_found). */
  code?: string;
  /**
   * The request's correlation id — the same value as the X-Request-Id
   * response header and the request_id field of the service's log lines.
   */
  request_id?: string;
}

/** A problem response with the given status and content type. */
model ProblemResponse<Status> {
  @statusCode _: Status;
  @header contentType: "application/problem+json";
  @body body: Problem;
}

@route("/tasks")
interface TasksOps {
  /** Create a task. Publishes a `task.created` event (see events.tsp) best-effort. */
  @post
  create(@body body: CreateTaskRequest): {
    @statusCode _: 201;
    @body task: Task;
  } | ProblemResponse<400>;

  /** List tasks, newest first. */
  @get
  list(): TaskList;

  /** Fetch one task; served from the cache when warm (X-Cache: hit|miss). */
  @get
  get(@path id: string): {
    @statusCode _: 200;
    @header("X-Cache") xCache?: "hit" | "miss";
    @body task: Task;
  } | ProblemResponse<404>;

  /** Delete a task and evict its cache entry. */
  @delete
  delete(@path id: string): {
    @statusCode _: 204;
  } | ProblemResponse<404>;
}
