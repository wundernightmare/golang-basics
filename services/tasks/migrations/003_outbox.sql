-- Transactional outbox: an event row is written in the same transaction as
-- the change it describes and relayed to Kafka afterwards (internal/outbox).
-- A row is deleted once the broker has acknowledged it, so the table only
-- ever holds the backlog.
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- relay order
    event_id      UUID        NOT NULL UNIQUE,                     -- the consumer's idempotency key
    aggregate_id  TEXT        NOT NULL,                            -- the task id; the record key
    event_type    TEXT        NOT NULL,                            -- task.created | task.updated | task.deleted
    payload       JSONB       NOT NULL,
    headers       JSONB       NOT NULL DEFAULT '{}'::jsonb,        -- trace context captured at insert
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INT         NOT NULL DEFAULT 0,
    last_error    TEXT
);
