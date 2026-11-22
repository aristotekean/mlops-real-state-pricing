"""audit.batch_history: the per-batch record of checks, decisions and results.

Every task writes its own columns as soon as it finishes, so the history shows
how far a batch got even when a later task fails. Streamlit reads this table
(or the audit.batch_history_summary view) to show the training/promotion log.
"""

import json
from typing import Any

from sqlalchemy import text

from real_estate_pipeline.db import engine_scope

JSON_COLUMNS = frozenset(
    {
        "schema_report",
        "quality_report",
        "new_categories_report",
        "drift_report",
        "preprocessing_report",
        "training_rules",
        "candidate_metrics",
        "production_metrics",
        "metric_deltas",
    }
)
TEXT_COLUMNS = frozenset(
    {
        "training_decision",
        "training_reason",
        "mlflow_experiment_id",
        "mlflow_parent_run_id",
        "mlflow_candidate_run_id",
        "candidate_model",
        "candidate_version",
        "production_version_before",
        "promotion_decision",
        "promotion_reason",
        "production_version_after",
        "final_status",
        "error_message",
        "dag_run_id",
    }
)


def start_batch(connection: Any, batch: dict[str, Any]) -> None:
    """Create (or reset, when a failed batch is resumed) the history row."""
    connection.execute(
        text(
            """
            INSERT INTO audit.batch_history (
                batch_id, group_number, api_batch_number, dag_run_id,
                record_count, received_at
            )
            VALUES (
                :batch_id, :group_number, :api_batch_number, :dag_run_id,
                :record_count, :received_at
            )
            ON CONFLICT (batch_id) DO UPDATE SET
                dag_run_id = EXCLUDED.dag_run_id,
                final_status = 'in_progress',
                error_message = NULL,
                finished_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        batch,
    )


def update(batch_id: str, **values: Any) -> None:
    """Set the given history columns of a batch (JSON reports are serialized)."""
    unknown = set(values) - JSON_COLUMNS - TEXT_COLUMNS
    if unknown:
        raise ValueError(f"Unknown audit.batch_history columns: {sorted(unknown)}")
    assignments = []
    params: dict[str, Any] = {"batch_id": batch_id}
    for column, value in values.items():
        if column in JSON_COLUMNS:
            assignments.append(f"{column} = CAST(:{column} AS jsonb)")
            params[column] = None if value is None else json.dumps(value, default=str)
        else:
            assignments.append(f"{column} = :{column}")
            params[column] = None if value is None else str(value)
    with engine_scope() as engine, engine.begin() as connection:
        connection.execute(
            text(
                f"""
                UPDATE audit.batch_history
                SET {", ".join(assignments)}, updated_at = CURRENT_TIMESTAMP
                WHERE batch_id = :batch_id
                """
            ),
            params,
        )


def finish(batch_id: str) -> dict[str, Any]:
    """Close a batch: history `completed` and RAW pipeline_status `completed`."""
    with engine_scope() as engine, engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE audit.batch_history
                SET final_status = 'completed', error_message = NULL,
                    finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE batch_id = :batch_id
                """
            ),
            {"batch_id": batch_id},
        )
        connection.execute(
            text(
                """
                UPDATE raw.batches
                SET pipeline_status = 'completed', completed_at = CURRENT_TIMESTAMP,
                    error_message = NULL
                WHERE batch_id = :batch_id
                """
            ),
            {"batch_id": batch_id},
        )
        row = connection.execute(
            text("SELECT * FROM audit.batch_history_summary WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        ).mappings().one()
    return {key: (value if isinstance(value, (int, float, type(None), dict, list)) else str(value)) for key, value in row.items()}


def mark_failed(batch_id: str, error: str) -> None:
    """Record a failed run: the batch stays resumable and the error is visible."""
    with engine_scope() as engine, engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE audit.batch_history
                SET final_status = 'failed', error_message = :error,
                    finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE batch_id = :batch_id
                """
            ),
            {"batch_id": batch_id, "error": error[:4000]},
        )
        connection.execute(
            text(
                """
                UPDATE raw.batches
                SET pipeline_status = 'failed', error_message = :error
                WHERE batch_id = :batch_id AND pipeline_status <> 'completed'
                """
            ),
            {"batch_id": batch_id, "error": error[:4000]},
        )
