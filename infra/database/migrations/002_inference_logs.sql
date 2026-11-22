-- INFERENCE · one auditable event per request to the inference API (RF8).
-- Safe to re-run.

CREATE SCHEMA IF NOT EXISTS inference;

CREATE TABLE IF NOT EXISTS inference.logs (
    request_id UUID PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    input_data JSONB NOT NULL CHECK (jsonb_typeof(input_data) = 'object'),
    prediction JSONB,
    probabilities JSONB
        CHECK (probabilities IS NULL OR jsonb_typeof(probabilities) = 'object'),
    model_name TEXT,
    model_version TEXT,
    model_alias TEXT,
    processing_time_ms DOUBLE PRECISION
        CHECK (processing_time_ms IS NULL OR processing_time_ms >= 0),
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
    error_code TEXT,
    error_message TEXT,
    CONSTRAINT ck_inference_log_result
        CHECK (
            (status = 'succeeded' AND prediction IS NOT NULL
                AND model_name IS NOT NULL AND model_version IS NOT NULL
                AND processing_time_ms IS NOT NULL AND error_code IS NULL)
            OR (status = 'failed' AND error_code IS NOT NULL)
        )
);

CREATE INDEX IF NOT EXISTS ix_inference_logs_requested_at
    ON inference.logs (requested_at DESC);
CREATE INDEX IF NOT EXISTS ix_inference_logs_model
    ON inference.logs (model_name, model_version);
