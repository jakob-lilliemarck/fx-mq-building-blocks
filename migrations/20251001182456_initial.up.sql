-- Messages that have not yet been attempted
CREATE TABLE messages_unattempted (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL
);

-- Messages that have been attempted at-least once.
-- Kept here to keep unattempted messages table lean
CREATE TABLE messages_attempted (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL
);

-- Leases for messages. Workers may hold messages for the duration of the lease.
CREATE TABLE leases (
    message_id UUID NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    acquired_by UUID NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (message_id, expires_at)
);

CREATE TABLE attempts_failed (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL REFERENCES messages_attempted(id),
    failed_at TIMESTAMPTZ NOT NULL,
    attempted INTEGER NOT NULL, -- the index of this attempt
    retry_earliest_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE attempts_succeeded (
    message_id UUID PRIMARY KEY REFERENCES messages_attempted(id),
    succeeded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE attempts_dead (
    message_id UUID PRIMARY KEY REFERENCES messages_attempted(id),
    dead_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE errors (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL REFERENCES messages_attempted(id),
    reported_at TIMESTAMPTZ NOT NULL,
    error TEXT NOT NULL
)
