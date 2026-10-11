-- The relay's poison-row counter and its retry clock.
--
-- Releases before this one counted every failed publish in outbox.attempts,
-- broker outages included, so a backlog that sat through an outage can hold
-- attempts far above OUTBOX_MAX_ATTEMPTS without any record being at fault.
-- Reusing that column would declare those rows dead on upgrade (and the old
-- release, still running during a rolling deploy, keeps counting outages in
-- it). The relay counts rejections in a fresh column instead and leaves
-- attempts alone; it is dropped once no release reads it.
--
-- rejections       times the broker refused the record itself (too large,
--                  malformed); OUTBOX_MAX_ATTEMPTS of them make the row dead.
-- next_attempt_at  a rejected row is not retried before this (exponential
--                  backoff); NULL = due now.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS rejections INT NOT NULL DEFAULT 0;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
