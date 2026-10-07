-- Idempotency-Key on POST /tasks: key → the task the first request created.
-- ON DELETE CASCADE: once the task is gone the key has nothing to replay and
-- is released. Rows older than the TTL are purged by the service.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          TEXT PRIMARY KEY,
    request_hash TEXT        NOT NULL,
    task_id      TEXT        NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idempotency_keys_created_at_idx ON idempotency_keys (created_at);
CREATE INDEX IF NOT EXISTS idempotency_keys_task_id_idx ON idempotency_keys (task_id);
