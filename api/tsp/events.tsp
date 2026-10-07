import "@typespec/json-schema";

using TypeSpec.JsonSchema;

/**
 * Events — the Kafka contract between services/tasks (producer) and
 * services/consumer (consumer). JSON on the wire, one schema per event,
 * generated Go types in libs/contracts/events shared by both sides, so the
 * shape can no longer drift as two hand-maintained structs.
 *
 * Compatibility rule: a field may be added as optional; a field may never be
 * removed or change type. `oasdiff`-style breaking-change detection is done
 * on the emitted schemas in CI (`just contracts-check`).
 */
@jsonSchema
namespace TasksEvents;

/*
 * Every event goes to the `tasks.events` topic with the task id as the record
 * key (so one task's events stay in order on one partition) and these record
 * headers: event_id (= the payload's event_id), event_type (task.created |
 * task.updated | task.deleted — what a consumer dispatches on),
 * content_type: application/json, and traceparent (the trace of the request
 * that made the change). Delivery is at least once (transactional outbox):
 * de-duplicate on event_id.
 */

/** Published when a task is created (event_type task.created). */
// The argument is the schema's $id (and file name): go-jsonschema names the Go
// type after it, so it is the type name, not a kebab-case file name.
@jsonSchema("TaskCreatedEvent")
model TaskCreatedEvent {
  /** Task identifier (UUID), also the record key. */
  id: string;
  /** Task title at creation time. */
  title: string;
  /** Creation time, RFC 3339 UTC. */
  created_at: utcDateTime;
  /**
   * Unique id of this event (UUID), also the event_id header — the
   * consumer's de-duplication key. Optional for compatibility with events
   * published before it existed.
   */
  event_id?: string;
  /** The task's version after the change (1 for a new task). Optional, as event_id. */
  version?: safeint;
}

/** Published when a task's title or done flag changes (event_type task.updated). */
@jsonSchema("TaskUpdatedEvent")
model TaskUpdatedEvent {
  /** Unique id of this event (UUID), also the event_id header. */
  event_id: string;
  /** Task identifier (UUID), also the record key. */
  id: string;
  /** Title after the change. */
  title: string;
  /** Completion flag after the change. */
  done: boolean;
  /** The task's version after the change; a consumer can drop an older one than it has seen. */
  version: safeint;
}

/** Published when a task is deleted (event_type task.deleted). */
@jsonSchema("TaskDeletedEvent")
model TaskDeletedEvent {
  /** Unique id of this event (UUID), also the event_id header. */
  event_id: string;
  /** Task identifier (UUID), also the record key. */
  id: string;
  /** The last version the task had. */
  version: safeint;
}
