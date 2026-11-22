-- RAW · one row per attempt of fetch_batch_from_api, whatever its outcome.
--
-- raw.batches only holds responses that carried data. The project treats every
-- request to the Data API as a data event, so the runs that got nothing new
-- (end of data, empty batch, a batch already ingested, an API error) and the
-- runs that resumed an unfinished batch without calling the API are recorded
-- here too. Safe to re-run.

CREATE TABLE IF NOT EXISTS raw.api_requests (
    request_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_run_id TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- false when the run resumed an unfinished batch instead of calling the API.
    api_called BOOLEAN NOT NULL,
    request_url TEXT,
    group_number INTEGER NOT NULL,
    http_status INTEGER,
    duration_ms DOUBLE PRECISION CHECK (duration_ms IS NULL OR duration_ms >= 0),
    -- new | resumed | duplicate | empty | end_of_data | error
    outcome TEXT NOT NULL
        CHECK (outcome IN ('new', 'resumed', 'duplicate', 'empty', 'end_of_data', 'error')),
    api_batch_number INTEGER,
    batch_id UUID REFERENCES raw.batches (batch_id),
    response_sha256 CHAR(64),
    record_count INTEGER CHECK (record_count IS NULL OR record_count >= 0),
    detail TEXT
);

CREATE INDEX IF NOT EXISTS ix_api_requests_requested_at
    ON raw.api_requests (requested_at DESC);
