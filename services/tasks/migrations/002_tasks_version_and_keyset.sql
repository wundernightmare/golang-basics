-- Optimistic concurrency: every change bumps version; If-Match / ETag carry it.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

-- Keyset pagination walks (created_at DESC, id DESC): the index matches the
-- ORDER BY exactly, so every page is an index range scan.
CREATE INDEX IF NOT EXISTS tasks_created_at_id_idx ON tasks (created_at DESC, id DESC);
