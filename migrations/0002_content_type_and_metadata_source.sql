ALTER TABLE plays
    ADD COLUMN content_type TEXT NOT NULL DEFAULT 'track'
        CHECK (content_type IN ('track', 'episode')),
    ADD COLUMN metadata_source TEXT NOT NULL DEFAULT 'fetch'
        CHECK (metadata_source IN ('fetch', 'cluster_map', 'unresolvable'));

UPDATE plays SET metadata_source = 'cluster_map' WHERE cardinality(artists) = 0;

CREATE INDEX plays_degraded_idx ON plays (track_id) WHERE metadata_source = 'cluster_map';
