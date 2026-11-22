"""Real estate pricing: one Data API batch per run, from ingestion to MLflow.

start -> fetch_batch_from_api -> check_new_batch
    -> no_new_batch -> end                       (end of data, empty or duplicate response)
    -> store_raw_batch -> validate_schema -> validate_data_quality
       -> detect_new_categories -> detect_data_drift -> preprocess_data
       -> decide_training
            -> skip_training ---------------------------------------------+
            -> train_candidate_model -> evaluate_candidate_model           |
               -> register_candidate_in_mlflow -> compare_with_production  |
               -> decide_promotion -> promote_model | reject_model --------+
       -> notify_or_log_result -> end

Two tasks extend the reference design: `check_new_batch` / `no_new_batch`
route the runs that receive no new data (the API reports end of data, or
returns a batch already ingested) so they finish cleanly without touching
RAW. Each task has a single responsibility, keeps its logic in the
`real_estate_pipeline` package and records its result in
audit.batch_history. A task that fails after its retries marks the batch
`failed` in that table and the run fails: errors are never hidden.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, get_current_context, task

FETCH_TASK = "fetch_batch_from_api"
# Cron from the environment; empty or "none" = manual triggers only.
SCHEDULE = os.environ.get("REAL_ESTATE_TRAINING_SCHEDULE", "").strip()
SCHEDULE = None if SCHEDULE.lower() in ("", "none") else SCHEDULE


def _run_id() -> str:
    return get_current_context()["run_id"]


def _mark_batch_failed(context: dict[str, Any]) -> None:
    """on_failure_callback: leave the failure visible in audit.batch_history."""
    try:
        batch = context["ti"].xcom_pull(task_ids=FETCH_TASK)
        if not batch or not batch.get("batch_id"):
            return
        from real_estate_pipeline.history import mark_failed

        exception = context.get("exception")
        mark_failed(batch["batch_id"], f"{context['ti'].task_id}: {exception!r}")
    except Exception as exc:  # the callback must never mask the original error
        print(f"Could not record the failure in audit.batch_history: {exc!r}")


@dag(
    dag_id="real_estate_incremental_training",
    description=(
        "Data API batch -> RAW -> validations -> CLEAN -> training decision -> "
        "MLflow candidate -> comparison with production -> promotion."
    ),
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # The Data API hands out batches in order and the pipeline resumes
    # unfinished batches first: runs must never overlap.
    max_active_runs=1,
    default_args={
        "owner": "mlops",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=45),
        "on_failure_callback": _mark_batch_failed,
    },
    tags=["real_estate", "data-api", "training", "mlflow"],
)
def real_estate_incremental_training() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # The fetch itself retries transient HTTP errors internally (with
    # backoff); Airflow retries cover database errors when landing.
    @task(task_id=FETCH_TASK, retries=3)
    def fetch_batch_from_api() -> dict[str, Any]:
        from real_estate_pipeline.raw import fetch_and_land

        return fetch_and_land(_run_id())

    @task.branch
    def check_new_batch(batch: dict[str, Any]) -> str:
        if batch["outcome"] in ("new", "resumed"):
            return "store_raw_batch"
        return "no_new_batch"

    @task
    def no_new_batch(batch: dict[str, Any]) -> dict[str, Any]:
        messages = {
            "end_of_data": "La API de datos ya entregó todos los lotes del grupo.",
            "empty": "La API devolvió un lote sin registros.",
            "duplicate": "El lote recibido ya había sido ingerido (mismo número y hash); no se reprocesa.",
        }
        print(messages.get(batch["outcome"], batch["outcome"]), batch)
        return batch

    @task
    def store_raw_batch(batch: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.raw import store_raw_batch as store

        return store(batch)

    @task
    def validate_schema(batch: dict[str, Any], stored: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.validation import validate_schema as run

        return run(batch)

    @task
    def validate_data_quality(batch: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.validation import validate_data_quality as run

        return run(batch)

    @task
    def detect_new_categories(batch: dict[str, Any], quality: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.validation import detect_new_categories as run

        return run(batch)

    @task
    def detect_data_drift(batch: dict[str, Any], categories: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.validation import detect_data_drift as run

        return run(batch)

    @task
    def preprocess_data(batch: dict[str, Any], drift: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.clean import preprocess_data as run

        return run(batch)

    @task.branch
    def decide_training(batch: dict[str, Any], processed: dict[str, Any]) -> str:
        from real_estate_pipeline.decision import decide_training as run

        return run(batch)

    @task
    def skip_training(batch: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.decision import skip_training as run

        return run(batch)

    @task(execution_timeout=timedelta(minutes=90))
    def train_candidate_model(batch: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.modeling import train_candidate_model as run

        return run(batch, _run_id())

    @task
    def evaluate_candidate_model(batch: dict[str, Any], trained: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.modeling import evaluate_candidate_model as run

        return run(batch, trained)

    @task
    def register_candidate_in_mlflow(
        batch: dict[str, Any], trained: dict[str, Any], evaluated: dict[str, Any]
    ) -> dict[str, Any]:
        from real_estate_pipeline.modeling import register_candidate_in_mlflow as run

        return run(batch, trained, evaluated)

    @task
    def compare_with_production(
        batch: dict[str, Any], trained: dict[str, Any], evaluated: dict[str, Any],
        registered: dict[str, Any],
    ) -> dict[str, Any]:
        from real_estate_pipeline.modeling import compare_with_production as run

        return run(batch, trained, evaluated, registered)

    @task.branch
    def decide_promotion(comparison: dict[str, Any]) -> str:
        return "promote_model" if comparison["promote"] else "reject_model"

    @task
    def promote_model(batch: dict[str, Any], trained: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.modeling import promote_model as run

        return run(batch, trained, comparison)

    @task
    def reject_model(batch: dict[str, Any], trained: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
        from real_estate_pipeline.modeling import reject_model as run

        return run(batch, trained, comparison)

    @task(trigger_rule="none_failed_min_one_success")
    def notify_or_log_result(batch: dict[str, Any]) -> dict[str, Any]:
        # Its upstream also includes the fetch task, which succeeds on the
        # no-new-batch route too: there is no batch to close then.
        if batch.get("outcome") not in ("new", "resumed"):
            from airflow.sdk.exceptions import AirflowSkipException

            raise AirflowSkipException("No batch was processed in this run.")
        from real_estate_pipeline.history import finish

        summary = finish(batch["batch_id"])
        print(
            f"Batch {summary['api_batch_number']}: training={summary['training_decision']} "
            f"({summary['training_reason']}) promotion={summary['promotion_decision']} "
            f"({summary['promotion_reason']}) production v{summary['production_version_before']} "
            f"-> v{summary['production_version_after']}"
        )
        return summary

    batch = fetch_batch_from_api()
    route = check_new_batch(batch)
    start >> batch
    route >> no_new_batch(batch) >> end

    stored = store_raw_batch(batch)
    route >> stored
    schema = validate_schema(batch, stored)
    quality = validate_data_quality(batch, schema)
    categories = detect_new_categories(batch, quality)
    drift = detect_data_drift(batch, categories)
    processed = preprocess_data(batch, drift)
    training_branch = decide_training(batch, processed)

    skipped = skip_training(batch)
    trained = train_candidate_model(batch)
    training_branch >> [skipped, trained]
    evaluated = evaluate_candidate_model(batch, trained)
    registered = register_candidate_in_mlflow(batch, trained, evaluated)
    comparison = compare_with_production(batch, trained, evaluated, registered)
    promotion_branch = decide_promotion(comparison)
    promoted = promote_model(batch, trained, comparison)
    rejected = reject_model(batch, trained, comparison)
    promotion_branch >> [promoted, rejected]

    result = notify_or_log_result(batch)
    [skipped, promoted, rejected] >> result >> end


real_estate_training_dag = real_estate_incremental_training()
