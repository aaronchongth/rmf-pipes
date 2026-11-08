-- rmf-pipes' own state lives in the `pipes` schema. The zenoh storage backend
-- (phase 5) will use its own schema and role.
CREATE SCHEMA IF NOT EXISTS pipes;

CREATE TABLE pipes.users (
    id            BIGSERIAL PRIMARY KEY,
    username      TEXT NOT NULL,
    -- PHC string (argon2id), from the password-auth crate.
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('viewer', 'operator', 'admin')),
    disabled      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Usernames are case-insensitive for login and uniqueness.
CREATE UNIQUE INDEX users_username_lower ON pipes.users (lower(username));

-- Browser sessions. Only a SHA-256 of the random cookie value is stored.
CREATE TABLE pipes.sessions (
    token_hash   BYTEA PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES pipes.users (id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX sessions_user ON pipes.sessions (user_id);
CREATE INDEX sessions_expires ON pipes.sessions (expires_at);

-- API tokens for scripts and agents. Only a SHA-256 of the token is stored;
-- the plaintext is shown once, at creation.
CREATE TABLE pipes.api_tokens (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES pipes.users (id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    token_hash   BYTEA NOT NULL UNIQUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    expires_at   TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ
);
CREATE INDEX api_tokens_user ON pipes.api_tokens (user_id);

-- Append-only record of security-relevant actions and every write to the
-- robot bus. `actor` is denormalised so entries survive user deletion.
CREATE TABLE pipes.audit_log (
    id          BIGSERIAL PRIMARY KEY,
    at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor_id    BIGINT REFERENCES pipes.users (id) ON DELETE SET NULL,
    actor       TEXT NOT NULL,
    auth_method TEXT NOT NULL,
    action      TEXT NOT NULL,
    target      TEXT NOT NULL DEFAULT '',
    outcome     TEXT NOT NULL CHECK (outcome IN ('ok', 'denied', 'error')),
    detail      JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX audit_log_at ON pipes.audit_log (at DESC);
CREATE INDEX audit_log_action ON pipes.audit_log (action, at DESC);
