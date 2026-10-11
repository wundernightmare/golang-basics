-- migration-safety: reviewed — the index below is built inside the
-- migration's transaction (writes to tasks wait for it); it shipped while the
-- table was small. On a large table build it CONCURRENTLY by hand first.
-- Optimistic concurrency: every change bumps version; If-Match / ETag carry it.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

-- Keyset pagination walks (created_at DESC, id DESC): the index matches the
-- ORDER BY exactly, so every page is an index range scan.
CREATE INDEX IF NOT EXISTS tasks_created_at_id_idx ON tasks (created_at DESC, id DESC);
