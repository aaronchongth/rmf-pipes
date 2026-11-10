-- Recordings: what recording rules observed on the bus (topic messages,
-- service requests, action goals, feedback, statuses and results), decoded
-- to JSON. Append-only; old rows are purged by `--record-retention`.
CREATE TABLE pipes.records (
    id        bigserial PRIMARY KEY,
    at        timestamptz NOT NULL,
    rule      text NOT NULL,
    kind      text NOT NULL CHECK (kind IN ('topic', 'service', 'action')),
    name      text NOT NULL,
    part      text NOT NULL CHECK (part IN ('message', 'request', 'goal', 'feedback', 'status', 'result')),
    type_name text NOT NULL,
    goal_id   uuid,
    data      jsonb
);
CREATE INDEX records_name_at ON pipes.records (name, at DESC);
CREATE INDEX records_goal_id ON pipes.records (goal_id) WHERE goal_id IS NOT NULL;
-- Cheap time-range scans and retention deletes on an append-only table.
CREATE INDEX records_at ON pipes.records USING brin (at);
