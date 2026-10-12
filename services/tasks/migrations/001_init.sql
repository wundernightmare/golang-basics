-- The tasks service's schema. Later changes go in 002_…, 003_…: a shipped
-- file is never edited (safety_test.go reviews each new one).

-- Tasks. version is the optimistic-concurrency counter: every change bumps
-- it; If-Match / ETag carry it.
CREATE TABLE tasks (
    id         TEXT PRIMARY KEY,
    title      TEXT        NOT NULL,
    done       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version    BIGINT      NOT NULL DEFAULT 1
);

-- Keyset pagination walks (created_at DESC, id DESC): the index matches the
-- ORDER BY exactly, so every page is an index range scan.
CREATE INDEX tasks_created_at_id_idx ON tasks (created_at DESC, id DESC);

-- Transactional outbox: an event row is written in the same transaction as
-- the change it describes and relayed to Kafka afterwards (internal/outbox).
-- A row is deleted once the broker has acknowledged it, so the table only
-- ever holds the backlog.
CREATE TABLE outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- relay order
    event_id        UUID        NOT NULL UNIQUE,                     -- the consumer's idempotency key
    aggregate_id    TEXT        NOT NULL,                            -- the task id; the record key
    event_type      TEXT        NOT NULL,                            -- task.created | task.updated | task.deleted
    payload         JSONB       NOT NULL,
    headers         JSONB       NOT NULL DEFAULT '{}'::jsonb,        -- trace context captured at insert
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- times the broker refused the record itself (too large, malformed);
    -- OUTBOX_MAX_ATTEMPTS of them make the row dead. Outages do not count.
    rejections      INT         NOT NULL DEFAULT 0,
    -- a rejected row is not retried before this (exponential backoff);
    -- NULL = due now.
    next_attempt_at TIMESTAMPTZ,
    last_error      TEXT
);

-- Idempotency-Key on POST /tasks: key → the task the first request created.
-- ON DELETE CASCADE: once the task is gone the key has nothing to replay and
-- is released. Rows older than the TTL are purged by the service.
CREATE TABLE idempotency_keys (
    key          TEXT PRIMARY KEY,
    request_hash TEXT        NOT NULL,
    task_id      TEXT        NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);
CREATE INDEX idempotency_keys_task_id_idx ON idempotency_keys (task_id);
