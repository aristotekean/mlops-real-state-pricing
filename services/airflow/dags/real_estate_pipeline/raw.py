"""RAW layer: land each Data API response, then explode it into raw.records."""

import gzip
import json
import math
import time
import uuid
from collections import defaultdict
from typing import Any

from sqlalchemy import text

from real_estate_model.contract import record_key
from real_estate_pipeline import history, settings
from real_estate_pipeline.api_client import DataApiError, DataApiUnavailable, fetch_batch
from real_estate_pipeline.db import copy_rows, engine_scope

BATCH_NAMESPACE = uuid.UUID("5b0f8c51-6f0e-4a59-9a43-0d0c7c4f1e26")

BATCH_COLUMNS = """
    batch_id::text AS batch_id, group_number, api_batch_number, record_count,
    received_at::text AS received_at, dag_run_id, status, pipeline_status
"""


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    return type(value).__name__


def observed_schema(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Column -> sorted JSON types seen across every record of the response."""
    types: dict[str, set[str]] = defaultdict(set)
    for record in records:
        for column, value in record.items():
            types[column].add(_json_type(value))
    return {column: sorted(kinds) for column, kinds in sorted(types.items())}


def _log_request(connection: Any, **fields: Any) -> None:
    """One raw.api_requests row per fetch attempt (every request is a data event)."""
    row = {
        "dag_run_id": None, "api_called": True, "request_url": None,
        "group_number": settings.DATA_API_GROUP_NUMBER, "http_status": None, "duration_ms": None,
        "outcome": None, "api_batch_number": None, "batch_id": None, "response_sha256": None,
        "record_count": None, "detail": None,
    } | fields
    connection.execute(
        text(
            """
            INSERT INTO raw.api_requests (
                dag_run_id, api_called, request_url, group_number, http_status, duration_ms,
                outcome, api_batch_number, batch_id, response_sha256, record_count, detail
            )
            VALUES (
                :dag_run_id, :api_called, :request_url, :group_number, :http_status, :duration_ms,
                :outcome, :api_batch_number, :batch_id, :response_sha256, :record_count, :detail
            )
            """
        ),
        row,
    )


def _context(row: Any, outcome: str, **extra: Any) -> dict[str, Any]:
    return {"outcome": outcome, **dict(row), **extra}


def fetch_and_land(dag_run_id: str) -> dict[str, Any]:
    """fetch_batch_from_api: get ONE new batch and persist the response first.

    1. A batch left unfinished by an earlier run is resumed instead of calling
       the API: the API is stateful and that batch can never be requested again.
    2. Otherwise GET /data once. The body is stored gzip-compressed in
       raw.batches before any processing, so a later failure loses nothing.
    3. A response identical to one already landed (same group, API batch
       number and SHA-256) is a duplicate and is not reprocessed.
    """
    with engine_scope() as engine:
        with engine.begin() as connection:
            pending = connection.execute(
                text(
                    f"""
                    SELECT {BATCH_COLUMNS} FROM raw.batches
                    WHERE pipeline_status <> 'completed'
                    ORDER BY received_at LIMIT 1
                    """
                )
            ).mappings().first()
            if pending is not None:
                connection.execute(
                    text("UPDATE raw.batches SET dag_run_id = :run, pipeline_status = 'in_progress' WHERE batch_id = :id"),
                    {"run": dag_run_id, "id": pending["batch_id"]},
                )
                history.start_batch(
                    connection,
                    {
                        "batch_id": pending["batch_id"],
                        "group_number": pending["group_number"],
                        "api_batch_number": pending["api_batch_number"],
                        "dag_run_id": dag_run_id,
                        "record_count": pending["record_count"],
                        "received_at": pending["received_at"],
                    },
                )
                _log_request(
                    connection, dag_run_id=dag_run_id, api_called=False, outcome="resumed",
                    api_batch_number=pending["api_batch_number"], batch_id=pending["batch_id"],
                    record_count=pending["record_count"],
                    detail=f"previous pipeline_status={pending['pipeline_status']}",
                )
                print(
                    f"Resuming unfinished batch {pending['api_batch_number']} "
                    f"({pending['batch_id']}, previous status {pending['pipeline_status']}); "
                    "the Data API is not called in this run."
                )
                return _context(pending, "resumed", dag_run_id=dag_run_id)

        started = time.perf_counter()
        try:
            result = fetch_batch()
        except (DataApiError, DataApiUnavailable) as exc:
            with engine.begin() as connection:
                _log_request(
                    connection, dag_run_id=dag_run_id, outcome="error",
                    request_url=f"{settings.DATA_API_URL}/data",
                    duration_ms=(time.perf_counter() - started) * 1000, detail=str(exc)[:4000],
                )
            raise
        elapsed_ms = (time.perf_counter() - started) * 1000
        request_log = {
            "dag_run_id": dag_run_id, "request_url": result.request_url,
            "http_status": result.http_status, "duration_ms": elapsed_ms,
            "api_batch_number": result.api_batch_number,
        }
        if result.kind in ("end_of_data", "empty"):
            with engine.begin() as connection:
                _log_request(connection, **request_log, outcome=result.kind,
                             record_count=0 if result.kind == "empty" else None, detail=result.detail)
            if result.kind == "end_of_data":
                print(f"Data API reports end of data: {result.detail}")
                return {"outcome": "end_of_data", "detail": result.detail}
            print(f"Data API returned batch {result.api_batch_number} with no rows.")
            return {"outcome": "empty", "api_batch_number": result.api_batch_number}

        sha256 = result.sha256
        batch_id = str(
            uuid.uuid5(BATCH_NAMESPACE, f"{result.group_number}:{result.api_batch_number}:{sha256}")
        )
        schema = observed_schema(result.records)
        with engine.begin() as connection:
            previous = connection.execute(
                text("SELECT MAX(api_batch_number) FROM raw.batches WHERE group_number = :group"),
                {"group": result.group_number},
            ).scalar_one()
            inserted = connection.execute(
                text(
                    """
                    INSERT INTO raw.batches (
                        batch_id, group_number, api_batch_number, request_url,
                        http_status, response_sha256, response_bytes, payload_gzip,
                        record_count, received_schema, dag_run_id
                    )
                    VALUES (
                        :batch_id, :group_number, :api_batch_number, :request_url,
                        :http_status, :sha256, :response_bytes, :payload_gzip,
                        :record_count, CAST(:received_schema AS jsonb), :dag_run_id
                    )
                    ON CONFLICT (group_number, api_batch_number, response_sha256) DO NOTHING
                    RETURNING batch_id
                    """
                ),
                {
                    "batch_id": batch_id,
                    "group_number": result.group_number,
                    "api_batch_number": result.api_batch_number,
                    "request_url": result.request_url,
                    "http_status": result.http_status,
                    "sha256": sha256,
                    "response_bytes": len(result.body),
                    "payload_gzip": gzip.compress(result.body, compresslevel=6),
                    "record_count": len(result.records),
                    "received_schema": json.dumps(schema),
                    "dag_run_id": dag_run_id,
                },
            ).first()
            request_log |= {"batch_id": batch_id, "response_sha256": sha256,
                            "record_count": len(result.records)}
            if inserted is None:
                _log_request(connection, **request_log, outcome="duplicate",
                             detail="same group, API batch number and SHA-256 as an ingested batch")
                print(
                    f"Duplicate response: API batch {result.api_batch_number} with SHA-256 "
                    f"{sha256[:12]} was already ingested; it is not reprocessed."
                )
                return {
                    "outcome": "duplicate",
                    "api_batch_number": result.api_batch_number,
                    "batch_id": batch_id,
                }
            row = connection.execute(
                text(f"SELECT {BATCH_COLUMNS} FROM raw.batches WHERE batch_id = :id"),
                {"id": batch_id},
            ).mappings().one()
            _log_request(connection, **request_log, outcome="new")
            history.start_batch(
                connection,
                {
                    "batch_id": batch_id,
                    "group_number": row["group_number"],
                    "api_batch_number": row["api_batch_number"],
                    "dag_run_id": dag_run_id,
                    "record_count": row["record_count"],
                    "received_at": row["received_at"],
                },
            )

    # The API hands out batches in order; a jump means a response was lost
    # (e.g. a timeout after the server had already advanced its counter).
    expected = 0 if previous is None else previous + 1
    gap = list(range(expected, result.api_batch_number)) if result.api_batch_number > expected else []
    print(
        f"Landed API batch {result.api_batch_number}: {len(result.records)} rows, "
        f"{len(result.body) / 1e6:.1f} MB, sha256 {sha256[:12]}"
        + (f"; WARNING missing API batches {gap}" if gap else "")
    )
    return _context(row, "new", dag_run_id=dag_run_id, missing_api_batches=gap)


def load_payload(batch_id: str) -> list[dict[str, Any]]:
    """Decompress and parse the landed response of a batch."""
    with engine_scope() as engine, engine.connect() as connection:
        payload = connection.execute(
            text("SELECT payload_gzip FROM raw.batches WHERE batch_id = :id"),
            {"id": batch_id},
        ).scalar_one()
    return json.loads(gzip.decompress(bytes(payload)))["data"]


def _jsonb_safe(record: dict[str, Any]) -> dict[str, Any]:
    """jsonb cannot hold NaN/Infinity: those (non-standard JSON) become null."""
    return {
        key: None if isinstance(value, float) and not math.isfinite(value) else value
        for key, value in record.items()
    }


def store_raw_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """store_raw_batch: one raw.records row per listing, exactly as received.

    Idempotent: a batch already stored is left untouched; a partial load is
    replaced inside a single transaction.
    """
    batch_id = batch["batch_id"]
    with engine_scope() as engine:
        with engine.connect() as connection:
            stored = connection.execute(
                text("SELECT COUNT(*) FROM raw.records WHERE batch_id = :id"),
                {"id": batch_id},
            ).scalar_one()
        if stored == batch["record_count"]:
            print(f"Batch {batch['api_batch_number']} already stored ({stored} rows).")
            return {"stored_rows": stored, "already_stored": True}

        records = load_payload(batch_id)
        if len(records) != batch["record_count"]:
            raise RuntimeError(
                f"Landed payload has {len(records)} rows, raw.batches says {batch['record_count']}."
            )
        try:
            with engine.begin() as connection:
                connection.execute(
                    text("DELETE FROM raw.records WHERE batch_id = :id"), {"id": batch_id}
                )
                copy_rows(
                    connection,
                    "raw.records",
                    ("batch_id", "row_number", "record_key", "record_data"),
                    (
                        (batch_id, number, record_key(record), _jsonb_safe(record))
                        for number, record in enumerate(records, start=1)
                    ),
                )
                connection.execute(
                    text(
                        """
                        UPDATE raw.batches
                        SET status = 'stored', stored_at = CURRENT_TIMESTAMP, error_message = NULL
                        WHERE batch_id = :id
                        """
                    ),
                    {"id": batch_id},
                )
        except Exception as exc:
            with engine.begin() as connection:
                connection.execute(
                    text("UPDATE raw.batches SET status = 'failed', error_message = :error WHERE batch_id = :id"),
                    {"id": batch_id, "error": str(exc)[:4000]},
                )
            raise
    print(f"Stored {len(records)} RAW records of API batch {batch['api_batch_number']}.")
    return {"stored_rows": len(records), "already_stored": False}
