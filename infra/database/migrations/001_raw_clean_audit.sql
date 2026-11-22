-- Data domains of the real estate pipeline (training database).
--
--   raw    batches exactly as the Data API returned them, plus one row per
--          listing with its validation status. Never modified by preprocessing.
--   clean  typed, model-ready listings with lineage to RAW, training
--          eligibility and data split.
--   audit  one row per batch with every check, decision and MLflow identifier;
--          it is the training/promotion history shown in Streamlit.
--
-- Safe to re-run (IF NOT EXISTS everywhere).

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS audit;

-- --------------------------------------------------------------------------
-- RAW · one row per Data API response (= one batch)
-- --------------------------------------------------------------------------
-- The Data API is stateful: every GET /data hands out the NEXT batch, so a
-- response that is lost after the call cannot be requested again. The body is
-- therefore persisted (gzip, byte-for-byte) by the fetch task before anything
-- else happens; the later tasks only read it from here.
CREATE TABLE IF NOT EXISTS raw.batches (
    batch_id UUID PRIMARY KEY,
    group_number INTEGER NOT NULL,
    api_batch_number INTEGER NOT NULL,
    request_url TEXT NOT NULL,
    http_status INTEGER NOT NULL,
    response_sha256 CHAR(64) NOT NULL,
    response_bytes BIGINT NOT NULL CHECK (response_bytes > 0),
    payload_gzip BYTEA NOT NULL,
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    -- Column -> JSON types observed in the response (validate_schema input).
    received_schema JSONB,
    -- landed: body persisted · stored: rows exploded into raw.records
    -- failed: the rows could not be stored (error_message)
    status TEXT NOT NULL DEFAULT 'landed'
        CHECK (status IN ('landed', 'stored', 'failed')),
    -- Lifecycle of the whole DAG for this batch. A batch that is not
    -- `completed` is resumed by the next run before the API is called again.
    pipeline_status TEXT NOT NULL DEFAULT 'in_progress'
        CHECK (pipeline_status IN ('in_progress', 'completed', 'failed')),
    dag_run_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stored_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    -- The same response twice (e.g. after the API restarted its counter) is the
    -- same batch: it is detected and never reprocessed.
    CONSTRAINT uq_batches_response UNIQUE (group_number, api_batch_number, response_sha256)
);

CREATE INDEX IF NOT EXISTS ix_batches_pipeline_status
    ON raw.batches (pipeline_status, received_at);

-- --------------------------------------------------------------------------
-- RAW · one row per listing of a batch, unmodified
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.records (
    raw_record_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES raw.batches (batch_id),
    row_number INTEGER NOT NULL CHECK (row_number > 0),
    -- Content hash (contract.record_key): finds exact duplicates across batches.
    record_key CHAR(64) NOT NULL,
    record_data JSONB NOT NULL CHECK (jsonb_typeof(record_data) = 'object'),
    status TEXT NOT NULL DEFAULT 'loaded'
        CHECK (status IN ('loaded', 'validated', 'rejected')),
    status_reason TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMPTZ,
    CONSTRAINT uq_raw_records_batch_row UNIQUE (batch_id, row_number),
    CONSTRAINT ck_raw_records_rejection_reason
        CHECK (status <> 'rejected' OR status_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_raw_records_record_key ON raw.records (record_key);
CREATE INDEX IF NOT EXISTS ix_raw_records_status ON raw.records (status);

-- --------------------------------------------------------------------------
-- CLEAN · typed listings derived from validated RAW records
-- --------------------------------------------------------------------------
-- Columns hold the model INPUT contract (real_estate_model.contract) already
-- normalised: codes as text (ZIP with its leading zeros), numbers as double,
-- dates as date. Feature engineering happens inside the served model, so
-- training and inference share it. `street` is kept only for lineage/splits.
CREATE TABLE IF NOT EXISTS clean.records (
    clean_record_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_record_id BIGINT NOT NULL REFERENCES raw.records (raw_record_id),
    batch_id UUID NOT NULL REFERENCES raw.batches (batch_id),
    transformation_version TEXT NOT NULL,
    brokered_by TEXT,
    status TEXT,
    bed DOUBLE PRECISION,
    bath DOUBLE PRECISION,
    acre_lot DOUBLE PRECISION,
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    house_size DOUBLE PRECISION,
    prev_sold_date DATE,
    price DOUBLE PRECISION NOT NULL CHECK (price > 0),
    -- Valid but not usable for training (e.g. out_of_range_price); such rows
    -- never get a split.
    exclusion_reason TEXT,
    data_split TEXT CHECK (data_split IN ('train', 'validation', 'test')),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_clean_records_raw_version UNIQUE (raw_record_id, transformation_version),
    CONSTRAINT ck_clean_records_split
        CHECK ((exclusion_reason IS NULL) = (data_split IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ix_clean_records_version_split
    ON clean.records (transformation_version, data_split);
CREATE INDEX IF NOT EXISTS ix_clean_records_batch ON clean.records (batch_id);

-- --------------------------------------------------------------------------
-- AUDIT · training and promotion history, one row per batch
-- --------------------------------------------------------------------------
-- Each DAG task fills its own columns as it finishes, so a failed run still
-- shows how far the batch got and why it stopped.
CREATE TABLE IF NOT EXISTS audit.batch_history (
    batch_id UUID PRIMARY KEY REFERENCES raw.batches (batch_id),
    group_number INTEGER NOT NULL,
    api_batch_number INTEGER NOT NULL,
    dag_run_id TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    schema_report JSONB,
    quality_report JSONB,
    new_categories_report JSONB,
    drift_report JSONB,
    preprocessing_report JSONB,
    training_decision TEXT CHECK (training_decision IN ('train', 'skip')),
    training_reason TEXT,
    training_rules JSONB,
    mlflow_experiment_id TEXT,
    mlflow_parent_run_id TEXT,
    mlflow_candidate_run_id TEXT,
    candidate_model TEXT,
    candidate_version TEXT,
    candidate_metrics JSONB,
    production_version_before TEXT,
    production_metrics JSONB,
    metric_deltas JSONB,
    promotion_decision TEXT CHECK (promotion_decision IN ('promoted', 'rejected')),
    promotion_reason TEXT,
    production_version_after TEXT,
    final_status TEXT NOT NULL DEFAULT 'in_progress'
        CHECK (final_status IN ('in_progress', 'completed', 'failed')),
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_batch_history_received
    ON audit.batch_history (received_at DESC);

-- Readable history for Streamlit: one line per batch with the key outcomes.
CREATE OR REPLACE VIEW audit.batch_history_summary AS
SELECT
    h.api_batch_number,
    h.group_number,
    h.received_at,
    h.record_count,
    (h.quality_report ->> 'validated_rows')::INTEGER AS validated_rows,
    (h.preprocessing_report ->> 'eligible_rows')::INTEGER AS eligible_rows,
    h.training_decision,
    h.training_reason,
    h.candidate_model,
    h.candidate_version,
    h.promotion_decision,
    h.promotion_reason,
    h.production_version_before,
    h.production_version_after,
    h.metric_deltas,
    h.mlflow_experiment_id,
    h.mlflow_parent_run_id,
    h.mlflow_candidate_run_id,
    h.final_status,
    h.error_message,
    h.dag_run_id,
    h.batch_id,
    h.finished_at
FROM audit.batch_history h
ORDER BY h.received_at;
