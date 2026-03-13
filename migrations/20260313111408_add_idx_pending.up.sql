-- GIN indexes for JSONB containment (@>) on payload, used by search_pending.
-- The name column is cheap to filter with Postgres's bitmap AND after the GIN scan,
-- so a separate btree index on name is added to support combined selectivity.
CREATE INDEX idx_messages_unattempted_name ON messages_unattempted (name);
CREATE INDEX idx_messages_unattempted_payload ON messages_unattempted USING GIN (payload);

CREATE INDEX idx_messages_attempted_name ON messages_attempted (name);
CREATE INDEX idx_messages_attempted_payload ON messages_attempted USING GIN (payload);
