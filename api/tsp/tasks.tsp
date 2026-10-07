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
  /**
   * Human-readable title, as stored: surrounding whitespace trimmed, 1–200
   * characters, at least one of them not whitespace, no control characters
   * other than tab (NUL in particular — PostgreSQL TEXT rejects it; found by
   * Schemathesis). The pattern states the rules so that a generated,
   * schema-compliant request is always accepted.
   */
  @minLength(1)
  @maxLength(200)
  @pattern("^[^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*[^\\s\\u0000-\\u001F\\u007F][^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*$")
  title: string;
  /** Completion flag. */
  done: boolean;
  /**
   * Starts at 1 and grows by one with every change. The ETag of the task is
   * this number in quotes; send it back in If-Match to update or delete only
   * the version you read.
   */
  @minValue(1)
  version: int64;
  /** Creation time, RFC 3339 UTC, assigned by the database. */
  created_at: utcDateTime;
}

/** Body of POST /tasks. */
model CreateTaskRequest {
  /** Title of the task; surrounding whitespace is trimmed (rules as Task.title). */
  @minLength(1)
  @maxLength(200)
  @pattern("^[^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*[^\\s\\u0000-\\u001F\\u007F][^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*$")
  title: string;
}

/** Body of PATCH /tasks/{id}: the fields to change — at least one, or 400. */
model UpdateTaskRequest {
  /** New title; surrounding whitespace is trimmed (rules as Task.title). */
  @minLength(1)
  @maxLength(200)
  @pattern("^[^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*[^\\s\\u0000-\\u001F\\u007F][^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*$")
  title?: string;
  /** New completion flag. */
  done?: boolean;
}

/** Body of GET /tasks: one page, newest first. */
model TaskList {
  tasks: Task[];
  /**
   * Opaque cursor of the next page — pass it as ?cursor= — absent on the
   * last page.
   */
  next_cursor?: string;
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
  /**
   * Create a task. The task and its `task.created` event (see events.tsp)
   * are committed together (transactional outbox); the event reaches Kafka
   * shortly after, even if the broker is down at the time of the request.
   *
   * With an Idempotency-Key, a retry of the same request returns the task the
   * first one created (200, Idempotent-Replayed: true) instead of creating
   * another; the same key with a different body is a 422. Keys are kept for
   * 24 hours.
   */
  @post
  create(
    @header("Idempotency-Key") @minLength(1) @maxLength(255) idempotencyKey?: string,
    @body body: CreateTaskRequest,
  ): {
    @statusCode _: 201;
    @header("ETag") etag: string;
    @header("Location") location: string;
    @body task: Task;
  } | {
    @statusCode _: 200;
    @header("ETag") etag: string;
    @header("Idempotent-Replayed") replayed: "true";
    @body task: Task;
  } | ProblemResponse<400> | ProblemResponse<422>;

  /**
   * List tasks, newest first, one page at a time (keyset pagination: pages
   * do not shift when tasks are added or removed meanwhile).
   */
  @get
  list(
    /** Page size. */
    @query @minValue(1) @maxValue(200) limit?: int32 = 50,
    /** The next_cursor of the previous page. */
    @query cursor?: string,
  ): TaskList | ProblemResponse<400>;

  /** Fetch one task; served from the cache when warm (X-Cache: hit|miss). */
  @get
  get(@path id: string): {
    @statusCode _: 200;
    @header("ETag") etag: string;
    @header("X-Cache") xCache?: "hit" | "miss";
    @body task: Task;
  } | ProblemResponse<404>;

  /**
   * Change the title and/or the done flag. With If-Match: "<version>" the
   * change applies only if the task is still at that version — otherwise 412,
   * re-read and retry. Emits `task.updated`.
   */
  @patch
  update(
    @path id: string,
    @header("If-Match") ifMatch?: string,
    @body body: UpdateTaskRequest,
  ): {
    @statusCode _: 200;
    @header("ETag") etag: string;
    @body task: Task;
  } | ProblemResponse<400> | ProblemResponse<404> | ProblemResponse<412>;

  /**
   * Delete a task (and invalidate its cache entry); If-Match as for PATCH.
   * Emits `task.deleted`.
   */
  @delete
  delete(@path id: string, @header("If-Match") ifMatch?: string): {
    @statusCode _: 204;
  } | ProblemResponse<400> | ProblemResponse<404> | ProblemResponse<412>;
}
