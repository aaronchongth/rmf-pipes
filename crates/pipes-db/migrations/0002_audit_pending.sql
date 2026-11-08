-- A write is recorded as `pending` before it is attempted, then completed by
-- a second entry (`ok` or `error`) that references it in detail.intent.
ALTER TABLE pipes.audit_log DROP CONSTRAINT audit_log_outcome_check;
ALTER TABLE pipes.audit_log ADD CONSTRAINT audit_log_outcome_check
    CHECK (outcome IN ('ok', 'denied', 'error', 'pending'));
