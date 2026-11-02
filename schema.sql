-- The server needs wal_level = logical, and the relay's role needs REPLICATION.
CREATE TABLE outbox (
    id         bigserial PRIMARY KEY,
    topic      text NOT NULL CHECK (topic ~ '^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$'),
    key        text NOT NULL,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE PUBLICATION outbox_relay FOR TABLE outbox WITH (publish = 'insert');
