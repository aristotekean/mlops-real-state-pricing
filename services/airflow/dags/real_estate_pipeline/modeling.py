"""Training, evaluation, MLflow registration, comparison and promotion.

Data flow between tasks goes through MLflow and PostgreSQL, never through
XCom: the trained model is an MLflow logged model, the decisions live in
audit.batch_history, and XComs only carry identifiers and small summaries.
"""

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from airflow.sdk.exceptions import AirflowFailException
from sqlalchemy import text

import real_estate_model
from real_estate_model.contract import (
    CATEGORICAL_INPUTS,
    DATASET_SCHEMA_VERSION,
    DATE_INPUTS,
    INPUT_COLUMNS,
    NUMERIC_INPUTS,
)
from real_estate_model.features import FEATURE_COLUMNS, TRANSFORMATION_VERSION, engineer_features
from real_estate_model.models import (
    build_candidate,
    build_serving_pipeline,
    evaluate_predictions,
    metric_greater_is_better,
)
from real_estate_model.splits import SPLIT_NAMES, SPLIT_SALT
from real_estate_pipeline import history, rules, settings
from real_estate_pipeline.db import engine_scope

# Types skops may deserialize from our models; anything else is refused.
# HistGradientBoosting stores functools.partial(check_array); skops vets the
# wrapped function separately, so the partial itself is safe to allow.
TRUSTED_TYPE_PREFIXES = ("builtins.", "numpy.", "scipy.", "sklearn.", "real_estate_model.")
TRUSTED_TYPES = frozenset({"functools.partial"})
# Serving the registered artifact must reproduce the training-time score.
SKEW_RELATIVE_TOLERANCE = 1e-6


# --------------------------------------------------------------------------- #
# MLflow and data helpers
# --------------------------------------------------------------------------- #
def mlflow_client() -> Any:
    import mlflow
    from mlflow.tracking import MlflowClient

    tracking_uri = settings.mlflow_tracking_uri()
    mlflow.set_tracking_uri(tracking_uri)
    return MlflowClient(tracking_uri=tracking_uri)


def production_version(client: Any) -> Any | None:
    """Current model version behind the production alias, or None.

    Only "the registered model does not exist" means there is no production
    model. Any other error (MLflow unreachable, 5xx) must fail the task:
    reading it as "no model" would train a baseline and promote it without a
    comparison.
    """
    from mlflow.exceptions import MlflowException
    from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST, ErrorCode

    try:
        aliases = client.get_registered_model(settings.MODEL_NAME).aliases
    except MlflowException as exc:
        if exc.error_code == ErrorCode.Name(RESOURCE_DOES_NOT_EXIST):
            return None
        raise
    version = aliases.get(settings.MODEL_ALIAS)
    return None if version is None else client.get_model_version(settings.MODEL_NAME, version)


def load_clean(
    *, splits: tuple[str, ...] = SPLIT_NAMES, max_clean_record_id: int | None = None,
    batch_id: str | None = None, batch_ids: list[str] | None = None, limit: int | None = None,
    version: str | None = None,
) -> pd.DataFrame:
    """Training-eligible CLEAN rows as a typed frame (inputs + price + split).

    A training set is reproducible as (CLEAN version, batch ids): exactly what
    the MLflow snapshot records. ``max_clean_record_id`` additionally pins the
    rows read within one DAG run.
    """
    filters = ["transformation_version = :version", "exclusion_reason IS NULL", "data_split IN :splits"]
    params: dict[str, Any] = {"version": version or settings.CLEAN_VERSION, "splits": tuple(splits)}
    if max_clean_record_id is not None:
        filters.append("clean_record_id <= :max_id")
        params["max_id"] = max_clean_record_id
    if batch_id is not None:
        filters.append("batch_id = :batch_id")
        params["batch_id"] = batch_id
    if batch_ids is not None:
        filters.append("batch_id::text IN :batch_ids")
        params["batch_ids"] = tuple(batch_ids)
    columns = ", ".join(column for column in INPUT_COLUMNS if column != "prev_sold_date")
    sql = f"""
        SELECT clean_record_id, batch_id::text AS batch_id, data_split, price,
               {columns}, prev_sold_date::text AS prev_sold_date
        FROM clean.records WHERE {" AND ".join(filters)}
        ORDER BY clean_record_id
    """
    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit
    from sqlalchemy import bindparam

    statement = text(sql).bindparams(bindparam("splits", expanding=True))
    if batch_ids is not None:
        statement = statement.bindparams(bindparam("batch_ids", expanding=True))
    with engine_scope() as engine, engine.connect() as connection:
        return pd.read_sql_query(statement, connection, params=params)


def model_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    """The served model's input contract, with the dtypes of contract.input_frame."""
    inputs = frame.loc[:, list(INPUT_COLUMNS)].copy()
    for column in NUMERIC_INPUTS:
        inputs[column] = pd.to_numeric(inputs[column], errors="raise").astype("float64")
    for column in (*CATEGORICAL_INPUTS, *DATE_INPUTS):
        inputs[column] = inputs[column].astype(object).where(inputs[column].notna(), None)
    return inputs.reset_index(drop=True)


def clean_snapshot(frame: pd.DataFrame) -> dict[str, Any]:
    """Identity of the data a model is trained on: rows, splits and batches."""
    with engine_scope() as engine, engine.connect() as connection:
        batches = connection.execute(
            text(
                """
                SELECT b.batch_id::text AS batch_id, b.api_batch_number
                FROM raw.batches b
                WHERE b.batch_id IN (SELECT DISTINCT batch_id FROM clean.records)
                ORDER BY b.received_at
                """
            )
        ).mappings().all()
    used = set(frame["batch_id"].unique())
    snapshot = {
        "transformation_version": settings.CLEAN_VERSION,
        "feature_version": TRANSFORMATION_VERSION,
        "split_salt": SPLIT_SALT,
        "eligible_rows": int(len(frame)),
        "max_clean_record_id": int(frame["clean_record_id"].max()),
        "split_rows": {split: int((frame["data_split"] == split).sum()) for split in SPLIT_NAMES},
        "batches": [dict(row) for row in batches if row["batch_id"] in used],
    }
    snapshot["fingerprint"] = hashlib.sha256(
        json.dumps(snapshot, sort_keys=True).encode()
    ).hexdigest()[:16]
    return snapshot


def _snapshot_filter(snapshot: dict[str, Any]) -> dict[str, Any]:
    """load_clean arguments that read back exactly the rows of a snapshot."""
    return {
        "version": snapshot["transformation_version"],
        "batch_ids": [item["batch_id"] for item in snapshot["batches"]],
        "max_clean_record_id": snapshot["max_clean_record_id"],
    }


def _signature() -> Any:
    from mlflow.models import ModelSignature
    from mlflow.types.schema import ColSpec, Schema

    return ModelSignature(
        inputs=Schema(
            [
                ColSpec("double" if column in NUMERIC_INPUTS else "string", column, required=False)
                for column in INPUT_COLUMNS
            ]
        ),
        outputs=Schema([ColSpec("double", "price")]),
    )


def _skops_trusted_types(model: Any) -> list[str]:
    import skops.io

    untrusted = skops.io.get_untrusted_types(data=skops.io.dumps(model))
    unexpected = [
        name for name in untrusted
        if name not in TRUSTED_TYPES and not name.startswith(TRUSTED_TYPE_PREFIXES)
    ]
    if unexpected:
        raise RuntimeError(f"Refusing to trust unexpected serialized types: {unexpected}")
    return untrusted


def _history_row(batch_id: str) -> dict[str, Any]:
    with engine_scope() as engine, engine.connect() as connection:
        return dict(
            connection.execute(
                text("SELECT * FROM audit.batch_history WHERE batch_id = :id"), {"id": batch_id}
            ).mappings().one()
        )


def _metric(split: str, metric: str) -> str:
    return f"{split}_{metric}"


def _regressor(candidate: Any) -> Any:
    return candidate.named_steps["model"].regressor.named_steps["regressor"]


# --------------------------------------------------------------------------- #
# train_candidate_model
# --------------------------------------------------------------------------- #
def train_candidate_model(batch: dict[str, Any], dag_run_id: str) -> dict[str, Any]:
    """Fit every configured candidate on the accumulated train split.

    Each candidate is a nested MLflow run (params, validation metrics, model
    in MLflow Models format). The parent run carries the batch, the data
    snapshot, the code version, the settings and the reason to train. The
    candidate with the best validation PRIMARY_METRIC is THE candidate.
    """
    import mlflow
    import mlflow.sklearn

    mlflow_client()
    experiment = mlflow.set_experiment(settings.EXPERIMENT_NAME)
    decision = _history_row(batch["batch_id"])
    data = load_clean()
    snapshot = clean_snapshot(data)
    train = data[data["data_split"] == "train"]
    if settings.TRAINING_MAX_ROWS and len(train) > settings.TRAINING_MAX_ROWS:
        train = train.sample(n=settings.TRAINING_MAX_ROWS, random_state=42).sort_values("clean_record_id")
    validation = data[data["data_split"] == "validation"]
    train_inputs, validation_inputs = model_inputs(train), model_inputs(validation)
    train_features = engineer_features(train_inputs)
    validation_features = engineer_features(validation_inputs)
    signature = _signature()
    package_path = str(Path(real_estate_model.__file__).parent)
    primary = _metric("validation", settings.PRIMARY_METRIC)
    batches_used = ",".join(str(item["api_batch_number"]) for item in snapshot["batches"])

    common_tags = {
        "batch_id": batch["batch_id"],
        "api_batch_number": str(batch["api_batch_number"]),
        "group_number": str(batch["group_number"]),
        "dag_run_id": dag_run_id,
        "clean_snapshot": snapshot["fingerprint"],
        "transformation_version": settings.CLEAN_VERSION,
        "primary_metric": primary,
        "mlflow.source.git.commit": settings.GIT_COMMIT,
        "git_commit": settings.GIT_COMMIT,
    }
    common_params = {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "transformation_version": settings.CLEAN_VERSION,
        "feature_version": TRANSFORMATION_VERSION,
        "split_strategy": "sha256(street,city,state,zip) 70/15/15",
        "split_salt": SPLIT_SALT,
        "batches_used": batches_used,
        "batch_count": len(snapshot["batches"]),
        "rows_train": len(train),
        "rows_train_available": snapshot["split_rows"]["train"],
        "rows_validation": snapshot["split_rows"]["validation"],
        "rows_test": snapshot["split_rows"]["test"],
        "input_columns": ",".join(INPUT_COLUMNS),
        "feature_count": len(FEATURE_COLUMNS),
        "target_transform": "log1p(price), inverse expm1 clipped",
        "training_bounds": json.dumps(settings.TRAINING_BOUNDS),
        "training_max_rows": settings.TRAINING_MAX_ROWS,
    }

    candidates = []
    with mlflow.start_run(run_name=f"batch-{batch['api_batch_number']}-{dag_run_id}") as parent:
        mlflow.set_tags({**common_tags, "run_role": "training", "training_reason": (decision["training_reason"] or "")[:5000]})
        mlflow.log_params(common_params)
        mlflow.log_dict(snapshot, "data/clean_snapshot.json")
        mlflow.log_dict(settings.snapshot(), "config/pipeline_settings.json")
        mlflow.log_dict(
            {
                "training_decision": decision["training_decision"],
                "training_reason": decision["training_reason"],
                "rules": decision["training_rules"],
                "schema_report": decision["schema_report"],
                "quality_report": decision["quality_report"],
                "new_categories_report": decision["new_categories_report"],
                "drift_report": decision["drift_report"],
            },
            "decision/training_decision.json",
        )
        mlflow.log_text(decision["training_reason"] or "", "decision/training_reason.txt")

        for name in settings.TRAINING_CANDIDATES:
            with mlflow.start_run(run_name=name, nested=True) as child:
                started = time.perf_counter()
                fitted = build_candidate(name).fit(train_features, train["price"].to_numpy())
                fit_seconds = time.perf_counter() - started
                metrics = evaluate_predictions(
                    validation["price"], fitted.predict(validation_features), prefix="validation"
                )
                regressor = _regressor(fitted)
                mlflow.set_tags({**common_tags, "run_role": "candidate", "candidate": name})
                mlflow.log_params(
                    {
                        **common_params,
                        "candidate": name,
                        "regressor": type(regressor).__name__,
                        "preprocessing": json.dumps(
                            {step: type(transformer).__name__ for step, transformer, _ in
                             fitted.named_steps["model"].regressor.named_steps["preprocessor"].transformers}
                        ),
                        **{
                            f"regressor.{key}": value
                            for key, value in regressor.get_params().items()
                            if key != "categorical_features"
                        },
                    }
                )
                mlflow.log_metrics({**metrics, "fit_seconds": fit_seconds})
                serving = build_serving_pipeline(fitted)
                model_info = mlflow.sklearn.log_model(
                    sk_model=serving,
                    name="model",
                    signature=signature,
                    input_example=train_inputs.head(5),
                    code_paths=[package_path],
                    skops_trusted_types=_skops_trusted_types(serving),
                )
                candidates.append(
                    {
                        "candidate": name,
                        "run_id": child.info.run_id,
                        "model_uri": model_info.model_uri,
                        "fit_seconds": round(fit_seconds, 2),
                        **{key: float(value) for key, value in metrics.items()},
                    }
                )
                print(f"{name}: {primary}={metrics[primary]:.4f} ({fit_seconds:.1f}s)")

        choose = max if metric_greater_is_better(primary) else min
        best = choose(candidates, key=lambda item: item[primary])
        mlflow.log_dict({"selection_metric": primary, "candidates": candidates}, "candidates_comparison.json")
        mlflow.log_metric(f"best_{primary}", best[primary])
        mlflow.set_tags({"best_candidate": best["candidate"], "best_candidate_run_id": best["run_id"]})

    history.update(
        batch["batch_id"],
        mlflow_experiment_id=experiment.experiment_id,
        mlflow_parent_run_id=parent.info.run_id,
        mlflow_candidate_run_id=best["run_id"],
        candidate_model=best["candidate"],
    )
    print(f"Candidate: {best['candidate']} ({primary}={best[primary]:.4f}) run {best['run_id']}")
    return {
        "experiment_id": experiment.experiment_id,
        "parent_run_id": parent.info.run_id,
        "candidate_run_id": best["run_id"],
        "candidate": best["candidate"],
        "model_uri": best["model_uri"],
        "snapshot": snapshot,
        "selection_metric": primary,
        "candidates": [{k: v for k, v in item.items() if k != "model_uri"} for item in candidates],
    }


# --------------------------------------------------------------------------- #
# evaluate_candidate_model
# --------------------------------------------------------------------------- #
def _log_evaluation_artifacts(model: Any, frame: pd.DataFrame, inputs: pd.DataFrame, predictions: np.ndarray) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import mlflow
    from sklearn.inspection import permutation_importance

    actual = frame["price"].to_numpy()
    figure, axis = plt.subplots(figsize=(6, 5))
    axis.hexbin(actual, predictions, gridsize=60, bins="log", xscale="log", yscale="log", cmap="viridis")
    low, high = np.nanmin(actual), np.nanmax(actual)
    axis.plot([low, high], [low, high], color="red", linewidth=1)
    axis.set(xlabel="Precio real (USD)", ylabel="Precio predicho (USD)", title="Validación: predicho vs real")
    figure.tight_layout()
    mlflow.log_figure(figure, "evaluation/predicted_vs_actual_validation.png")
    plt.close(figure)

    log_error = np.log1p(np.clip(predictions, 0, None)) - np.log1p(actual)
    figure, axis = plt.subplots(figsize=(6, 4))
    axis.hist(np.clip(log_error, -3, 3), bins=80, color="steelblue")
    axis.set(xlabel="log1p(pred) - log1p(real)", ylabel="Registros", title="Validación: distribución de residuos (log)")
    figure.tight_layout()
    mlflow.log_figure(figure, "evaluation/residuals_validation.png")
    plt.close(figure)

    errors = pd.DataFrame({"actual": actual, "predicted": predictions, "state": frame["state"].to_numpy()})
    errors["abs_error"] = (errors["predicted"] - errors["actual"]).abs()
    errors["ape"] = errors["abs_error"] / errors["actual"]
    errors["decile"] = pd.qcut(errors["actual"], 10, labels=False, duplicates="drop")
    by_decile = errors.groupby("decile").agg(
        price_min=("actual", "min"), price_max=("actual", "max"), rows=("actual", "size"),
        mae=("abs_error", "mean"), mape=("ape", "mean"),
    ).reset_index()
    mlflow.log_dict(by_decile.to_dict(orient="records"), "evaluation/error_by_price_decile_validation.json")
    figure, axis = plt.subplots(figsize=(7, 4))
    axis.bar(by_decile["decile"].astype(str), by_decile["mape"], color="darkorange")
    axis.set(xlabel="Decil de precio real", ylabel="MAPE", title="Validación: error relativo por decil de precio")
    figure.tight_layout()
    mlflow.log_figure(figure, "evaluation/error_by_price_decile_validation.png")
    plt.close(figure)

    by_state = errors.groupby("state").agg(rows=("actual", "size"), mae=("abs_error", "mean"), mape=("ape", "mean"))
    mlflow.log_dict(
        by_state.sort_values("rows", ascending=False).head(25).reset_index().to_dict(orient="records"),
        "evaluation/error_by_state_validation.json",
    )

    # Interpretability: permutation importance of each INPUT field of the
    # served pipeline (what the API receives), measured on validation.
    sample = inputs.sample(n=min(settings.IMPORTANCE_SAMPLE_ROWS, len(inputs)), random_state=42)
    target = frame["price"].reset_index(drop=True).loc[sample.index]
    importance = permutation_importance(
        model, sample, target, scoring="neg_mean_absolute_error", n_repeats=3, random_state=42
    )
    ranking = (
        pd.DataFrame({"feature": sample.columns, "mae_increase": importance.importances_mean,
                      "std": importance.importances_std})
        .sort_values("mae_increase", ascending=False)
    )
    mlflow.log_dict(ranking.to_dict(orient="records"), "interpretability/permutation_importance.json")
    figure, axis = plt.subplots(figsize=(7, 4))
    axis.barh(ranking["feature"][::-1], ranking["mae_increase"][::-1], color="seagreen")
    axis.set(xlabel="Aumento del MAE al permutar (USD)", title="Importancia por permutación (validación)")
    figure.tight_layout()
    mlflow.log_figure(figure, "interpretability/permutation_importance.png")
    plt.close(figure)


def evaluate_candidate_model(batch: dict[str, Any], trained: dict[str, Any]) -> dict[str, Any]:
    """Score the logged candidate on train/validation/test and log the artifacts."""
    import mlflow
    import mlflow.sklearn

    client = mlflow_client()
    model = mlflow.sklearn.load_model(trained["model_uri"])
    data = load_clean(**_snapshot_filter(trained["snapshot"]))
    metrics: dict[str, float] = {}
    validation_frame = validation_inputs = validation_predictions = None
    for split in SPLIT_NAMES:
        frame = data[data["data_split"] == split].reset_index(drop=True)
        inputs = model_inputs(frame)
        predictions = np.asarray(model.predict(inputs), dtype="float64")
        metrics.update(evaluate_predictions(frame["price"], predictions, prefix=split))
        if split == "validation":
            validation_frame, validation_inputs, validation_predictions = frame, inputs, predictions

    with mlflow.start_run(run_id=trained["candidate_run_id"]):
        mlflow.log_metrics(metrics)
        mlflow.log_dict(metrics, "evaluation/metrics.json")
        _log_evaluation_artifacts(model, validation_frame, validation_inputs, validation_predictions)
    for key, value in metrics.items():
        client.log_metric(trained["parent_run_id"], f"candidate_{key}", value)

    history.update(batch["batch_id"], candidate_metrics=metrics)
    print("Candidate metrics: " + ", ".join(f"{k}={v:,.4f}" for k, v in sorted(metrics.items())))
    return {"metrics": metrics}


# --------------------------------------------------------------------------- #
# register_candidate_in_mlflow
# --------------------------------------------------------------------------- #
def register_candidate_in_mlflow(batch: dict[str, Any], trained: dict[str, Any], evaluated: dict[str, Any]) -> dict[str, Any]:
    """Register the candidate as a new model version (once per training run)."""
    import mlflow

    client = mlflow_client()
    metrics = evaluated["metrics"]
    decision = _history_row(batch["batch_id"])
    existing = [
        version
        for version in client.search_model_versions(f"name = '{settings.MODEL_NAME}'")
        if version.tags.get("training_run_id") == trained["candidate_run_id"]
    ]
    if existing:
        version = existing[0]
        print(f"Candidate run already registered as version {version.version}.")
    else:
        snapshot = trained["snapshot"]
        version = mlflow.register_model(
            trained["model_uri"],
            settings.MODEL_NAME,
            tags={
                "source_batch_id": batch["batch_id"],
                "api_batch_number": str(batch["api_batch_number"]),
                "batches_used": ",".join(str(item["api_batch_number"]) for item in snapshot["batches"]),
                "candidate": trained["candidate"],
                "training_run_id": trained["candidate_run_id"],
                "parent_run_id": trained["parent_run_id"],
                "clean_snapshot": snapshot["fingerprint"],
                "eligible_rows": str(snapshot["eligible_rows"]),
                "transformation_version": settings.CLEAN_VERSION,
                "git_commit": settings.GIT_COMMIT,
                "training_reason": (decision["training_reason"] or "")[:4900],
                "selection_metric": trained["selection_metric"],
                **{key: f"{value:.6f}" for key, value in metrics.items()},
            },
        )
        client.update_model_version(
            settings.MODEL_NAME,
            version.version,
            description=(
                f"{trained['candidate']} entrenado con los lotes {','.join(str(b['api_batch_number']) for b in snapshot['batches'])} "
                f"({snapshot['eligible_rows']} filas elegibles, snapshot {snapshot['fingerprint']}). "
                f"Validación: MAE {metrics['validation_mae']:,.0f}, RMSE {metrics['validation_rmse']:,.0f}, "
                f"RMSLE {metrics['validation_rmsle']:.4f}, R2 {metrics['validation_r2']:.4f}. "
                f"Motivo: {decision['training_reason']}"
            )[:5000],
        )
    client.set_tag(trained["parent_run_id"], "registered_model_version", version.version)
    history.update(batch["batch_id"], candidate_version=version.version)
    print(f"Registered {settings.MODEL_NAME} version {version.version}")
    return {"version": version.version}


# --------------------------------------------------------------------------- #
# compare_with_production
# --------------------------------------------------------------------------- #
def compare_with_production(batch: dict[str, Any], trained: dict[str, Any], evaluated: dict[str, Any], registered: dict[str, Any]) -> dict[str, Any]:
    """Score candidate and production on the SAME validation rows and apply the rule.

    Both models are loaded from the registry and fed the input contract, as
    the API will call them. The candidate must reproduce its evaluation score
    (training/serving skew guard). Promotion rule:
        gain(PRIMARY_METRIC) >= PROMOTION_MIN_IMPROVEMENT and
        gain(GUARD_METRIC)   >= -PROMOTION_MAX_GUARD_DEGRADATION
    where gain is the signed relative improvement (positive = better).
    """
    import mlflow

    client = mlflow_client()
    version = registered["version"]
    frame = load_clean(splits=("validation",), **_snapshot_filter(trained["snapshot"]))
    inputs, actual = model_inputs(frame), frame["price"].reset_index(drop=True)
    candidate_model = mlflow.pyfunc.load_model(f"models:/{settings.MODEL_NAME}/{version}")
    candidate = evaluate_predictions(actual, np.asarray(candidate_model.predict(inputs), dtype="float64"), prefix="validation")

    primary = _metric("validation", settings.PRIMARY_METRIC)
    expected = evaluated["metrics"][primary]
    if abs(candidate[primary] - expected) > SKEW_RELATIVE_TOLERANCE * max(1.0, abs(expected)):
        raise AirflowFailException(
            f"Training/serving skew: version {version} scores {candidate[primary]:.6f} from the "
            f"registry but {expected:.6f} at evaluation time."
        )

    champion = production_version(client)
    production: dict[str, float] | None = None
    if champion is not None and champion.version != version:
        champion_model = mlflow.pyfunc.load_model(f"models:/{settings.MODEL_NAME}/{champion.version}")
        production = evaluate_predictions(
            actual, np.asarray(champion_model.predict(inputs), dtype="float64"), prefix="validation"
        )
    elif champion is not None:
        production = candidate
    outcome = rules.decide_promotion(
        candidate=candidate,
        production=production,
        candidate_version=version,
        production_version=None if champion is None else champion.version,
        thresholds=rules.Thresholds.from_settings(),
    )
    promote, reason, deltas = outcome.promote, outcome.reason, outcome.deltas

    for key, value in candidate.items():
        client.log_metric(trained["parent_run_id"], f"candidate_registry_{key}", value)
    for key, value in (production or {}).items():
        client.log_metric(trained["parent_run_id"], f"production_{key}", value)
    for key, value in deltas.items():
        client.log_metric(trained["parent_run_id"], f"relative_gain_{key}", value)
    history.update(
        batch["batch_id"],
        production_version_before=None if champion is None else champion.version,
        production_metrics=production,
        metric_deltas={
            "relative_gain": deltas,
            "rule": {
                "primary_metric": settings.PRIMARY_METRIC,
                "min_improvement": settings.PROMOTION_MIN_IMPROVEMENT,
                "guard_metric": settings.GUARD_METRIC,
                "max_guard_degradation": settings.PROMOTION_MAX_GUARD_DEGRADATION,
            },
            "validation_rows": int(len(actual)),
            "candidate": candidate,
        },
        promotion_reason=reason,
    )
    print(reason)
    return {
        "version": version,
        "promote": promote,
        "reason": reason,
        "production_version": None if champion is None else champion.version,
    }


# --------------------------------------------------------------------------- #
# promote_model / reject_model
# --------------------------------------------------------------------------- #
def promote_model(batch: dict[str, Any], trained: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    client = mlflow_client()
    version = comparison["version"]
    client.set_registered_model_alias(settings.MODEL_NAME, settings.MODEL_ALIAS, version)
    client.set_model_version_tag(settings.MODEL_NAME, version, "promotion_decision", "promoted")
    client.set_model_version_tag(settings.MODEL_NAME, version, "promotion_reason", comparison["reason"][:4900])
    client.set_model_version_tag(settings.MODEL_NAME, version, "compared_with_version", comparison["production_version"] or "none")
    client.set_tag(trained["parent_run_id"], "promotion_decision", "promoted")
    client.log_metric(trained["parent_run_id"], "promoted_to_production", 1.0)
    current = production_version(client)
    history.update(
        batch["batch_id"],
        promotion_decision="promoted",
        promotion_reason=comparison["reason"],
        production_version_after=current.version,
    )
    print(f"Alias {settings.MODEL_ALIAS} -> {settings.MODEL_NAME} v{current.version}")
    return {"production_version": current.version}


def reject_model(batch: dict[str, Any], trained: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    client = mlflow_client()
    version = comparison["version"]
    client.set_model_version_tag(settings.MODEL_NAME, version, "promotion_decision", "rejected")
    client.set_model_version_tag(settings.MODEL_NAME, version, "promotion_reason", comparison["reason"][:4900])
    client.set_model_version_tag(settings.MODEL_NAME, version, "compared_with_version", comparison["production_version"] or "none")
    client.set_tag(trained["parent_run_id"], "promotion_decision", "rejected")
    client.log_metric(trained["parent_run_id"], "promoted_to_production", 0.0)
    history.update(
        batch["batch_id"],
        promotion_decision="rejected",
        promotion_reason=comparison["reason"],
        production_version_after=comparison["production_version"],
    )
    print(f"Version {version} rejected; production stays at v{comparison['production_version']}")
    return {"production_version": comparison["production_version"]}
