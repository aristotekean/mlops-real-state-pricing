"""decide_training: turn the batch reports into an explicit train / skip decision.

Rules, evaluated in order (the first two veto, the third forces a baseline):

1. Invalid batch: blocking schema change or rejected share above
   MAX_REJECTED_FRACTION                                        -> skip
2. Insufficient data: accumulated train/validation/test rows below
   MIN_TRAIN_ROWS / MIN_EVALUATION_ROWS                         -> skip
3. No production model yet                                      -> train (baseline)
4. Small batch: eligible rows below MIN_BATCH_ROWS              -> skip
5. Otherwise train if AT LEAST ONE trigger holds, else skip:
   a. volume growth since the production model's data >= MIN_VOLUME_GROWTH
   b. drift: PSI >= DRIFT_PSI_THRESHOLD in some DRIFT_FEATURE
   c. new categories covering >= NEW_CATEGORY_MIN_SHARE of the batch
   d. degradation: the production model on the new labelled batch is worse
      than on its own validation set by >= DEGRADATION_THRESHOLD

Periodicity or the raw number of batches is never a reason by itself. Every
rule's inputs and outcome are stored in audit.batch_history.training_rules.
"""

from typing import Any

import numpy as np
from sqlalchemy import text

from real_estate_model.models import evaluate_predictions, relative_improvement
from real_estate_pipeline import history, rules, settings
from real_estate_pipeline.db import engine_scope
from real_estate_pipeline.modeling import load_clean, mlflow_client, model_inputs, production_version

TRAIN_TASK = "train_candidate_model"
SKIP_TASK = "skip_training"


def _reports(batch_id: str) -> dict[str, Any]:
    with engine_scope() as engine, engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    """
                    SELECT schema_report, quality_report, new_categories_report,
                           drift_report, preprocessing_report
                    FROM audit.batch_history WHERE batch_id = :id
                    """
                ),
                {"id": batch_id},
            ).mappings().one()
        )


def _degradation(batch_id: str, champion: Any) -> dict[str, Any]:
    """Production model scored on the new batch vs its registered validation score."""
    import mlflow

    metric = settings.DEGRADATION_METRIC
    reference_tag = champion.tags.get(f"validation_{metric}")
    if reference_tag is None:
        return {"available": False, "reason": f"la versión productiva no tiene el tag validation_{metric}"}
    frame = load_clean(batch_id=batch_id, limit=None)
    if frame.empty:
        return {"available": False, "reason": "lote sin filas elegibles"}
    if len(frame) > settings.DEGRADATION_SAMPLE_ROWS:
        frame = frame.sample(n=settings.DEGRADATION_SAMPLE_ROWS, random_state=42)
    model = mlflow.pyfunc.load_model(f"models:/{settings.MODEL_NAME}/{champion.version}")
    predictions = np.asarray(model.predict(model_inputs(frame)), dtype="float64")
    batch_metrics = evaluate_predictions(frame["price"].reset_index(drop=True), predictions, prefix="batch")
    reference = float(reference_tag)
    current = batch_metrics[f"batch_{metric}"]
    # relative_improvement > 0 means better; degradation is its negative.
    degradation = -relative_improvement(current, reference, metric)
    return {
        "available": True,
        "metric": metric,
        "production_version": champion.version,
        "reference_validation": reference,
        "batch_value": current,
        "batch_rows_scored": int(len(frame)),
        "degradation": round(degradation, 6),
        "batch_metrics": batch_metrics,
    }


def decide_training(batch: dict[str, Any]) -> str:
    """Gather the batch reports and the production model state, then apply rules.decide_training."""
    batch_id = batch["batch_id"]
    reports = _reports(batch_id)
    client = mlflow_client()
    champion = production_version(client)
    eligible = reports["preprocessing_report"]["eligible_rows"]
    if champion is not None and eligible > 0:
        degradation = _degradation(batch_id, champion)
    else:
        degradation = {"available": False,
                       "reason": "sin modelo productivo" if champion is None else "lote sin filas elegibles"}

    outcome = rules.decide_training(
        schema=reports["schema_report"],
        quality=reports["quality_report"],
        categories=reports["new_categories_report"],
        drift=reports["drift_report"],
        preprocessing=reports["preprocessing_report"],
        has_production=champion is not None,
        production_trained_rows=int(champion.tags.get("eligible_rows", "0")) if champion else 0,
        degradation=degradation,
        thresholds=rules.Thresholds.from_settings(),
    )
    history.update(
        batch_id,
        training_decision=outcome.decision,
        training_reason=outcome.reason,
        training_rules={**outcome.rules, "settings": settings.snapshot()},
        production_version_before=None if champion is None else champion.version,
    )
    print(outcome.reason)
    return TRAIN_TASK if outcome.decision == "train" else SKIP_TASK


def skip_training(batch: dict[str, Any]) -> dict[str, Any]:
    """Record the outcome of a batch that is not trained on."""
    with engine_scope() as engine, engine.connect() as connection:
        row = connection.execute(
            text("SELECT training_reason, production_version_before FROM audit.batch_history WHERE batch_id = :id"),
            {"id": batch["batch_id"]},
        ).mappings().one()
    history.update(batch["batch_id"], production_version_after=row["production_version_before"])
    print(f"Training skipped for API batch {batch['api_batch_number']}: {row['training_reason']}")
    return {"skipped": True, "reason": row["training_reason"]}
