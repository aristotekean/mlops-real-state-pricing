-- INFERENCE LOGS · link from a logged request to its distributed trace.
--
-- trace_id is the W3C trace id (32 lowercase hex characters) of the request,
-- as shown by Jaeger. It is only set when the trace was sampled, so a non-null
-- value always points to a trace that was exported. Rows written before this
-- migration, with tracing off, or for an unsampled request keep it null.
-- Safe to re-run.

ALTER TABLE inference.logs
    ADD COLUMN IF NOT EXISTS trace_id TEXT
        CONSTRAINT ck_inference_log_trace_id
        CHECK (trace_id IS NULL OR trace_id ~ '^[0-9a-f]{32}$');

CREATE INDEX IF NOT EXISTS ix_inference_logs_trace_id
    ON inference.logs (trace_id)
    WHERE trace_id IS NOT NULL;
