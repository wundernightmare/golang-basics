-- The original table. IF NOT EXISTS: databases that predate versioned
-- migrations already have it (created by the old boot-time helper), and this
-- file must adopt them rather than fail.
CREATE TABLE IF NOT EXISTS tasks (
    id         TEXT PRIMARY KEY,
    title      TEXT        NOT NULL,
    done       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
