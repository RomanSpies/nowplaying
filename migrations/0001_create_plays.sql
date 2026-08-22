CREATE TABLE plays (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    track_id    TEXT NOT NULL,
    title       TEXT NOT NULL,
    album       TEXT NOT NULL,
    artists     TEXT[] NOT NULL,
    cover_url   TEXT,
    duration_ms INTEGER NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (track_id, started_at)
);

CREATE INDEX plays_started_at_idx ON plays (started_at DESC);
