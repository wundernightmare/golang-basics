import "@typespec/http";
import "@typespec/openapi";

using TypeSpec.Http;

/**
 * Ingest — the HTTP contract of services/ingest.
 *
 * One operation: a client hands over a batch of events and gets an answer
 * only once every event of the batch is durable in at least one of the two
 * sinks (Kafka, the S3 archive). The answer is all-or-nothing; anything but
 * 200 means "send the same batch again" — the event ids make that safe.
 *
 * Errors are RFC 9457 problem details (application/problem+json).
 */
@service(#{ title: "Ingest" })
@server("http://localhost:8084", "local")
@OpenAPI.info(#{ version: "1.0.0" })
namespace Ingest;

/** One event, stored and published byte for byte as it was sent. */
@OpenAPI.extension("additionalProperties", false)
model Event {
  /**
   * Client-generated unique id (UUIDv7 recommended), assigned once and kept
   * across retries: it is the deduplication key everywhere downstream — the
   * archive compaction, the Kafka consumers, a replay.
   */
  @pattern("^[A-Za-z0-9_-]{1,64}$")
  id: string;
  /** Optional event type, e.g. "page.viewed"; travels as the event_type record header. */
  @pattern("^[a-z0-9._-]{1,64}$")
  type?: string;
  /** The event itself: any JSON object. Opaque to the service. */
  payload: Record<unknown>;
}

/** Body of POST /events. */
@OpenAPI.extension("additionalProperties", false)
model IngestRequest {
  /**
   * The batch. A client should collect events and send them together: while
   * Kafka is unavailable an answer waits for the next archive flush, so one
   * event per request means one event per flush interval.
   */
  @minItems(1)
  @maxItems(1000)
  events: Event[];
}

/** Body of a 200: every event of the request is durable. */
model IngestResponse {
  /** Number of events accepted — the size of the batch sent. */
  @minValue(1)
  accepted: int32;
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
  /** Stable machine-readable code (e.g. invalid_event, overloaded, no_sink_available). */
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

/** A problem the client answers by sending the same batch again, later. */
model RetryableProblemResponse<Status> {
  @statusCode _: Status;
  @header contentType: "application/problem+json";
  /** Seconds to wait before the retry. */
  @header("Retry-After") retryAfter: string;
  @body body: Problem;
}

@route("/events")
interface IngestOps {
  /**
   * Accept a batch of events.
   *
   * 200 — every event is durable in Kafka or in the archive (usually both).
   * 400 — the body is not the documented shape; do not retry it unchanged.
   * 413 — the body, or one event, is larger than the service accepts.
   * 429 — the service is holding as much unconfirmed data as it may; retry.
   * 503 — neither sink confirmed the batch; retry.
   *
   * A retry may store an event twice (the first attempt may have succeeded
   * before its answer was lost): consumers deduplicate by event id.
   */
  @post
  ingest(@body body: IngestRequest):
    | IngestResponse
    | ProblemResponse<400>
    | ProblemResponse<413>
    | RetryableProblemResponse<429>
    | RetryableProblemResponse<503>;
}
