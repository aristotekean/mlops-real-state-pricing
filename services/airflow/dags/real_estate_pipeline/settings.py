"""Runtime configuration of the pipeline, read from the deployment environment.

Every tunable of the pipeline lives here with its default. In Kubernetes the
values come from the `pipeline-config` ConfigMap (infra/k8s/base/airflow), so
changing a threshold is a versioned Git change synced by Argo CD, not a code
change. See docs/pipeline.md for the meaning and justification of each one.
"""

import hashlib
import json
import os
from typing import Any

from real_estate_model.contract import DEFAULT_TRAINING_BOUNDS
from real_estate_model.features import TRANSFORMATION_VERSION


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_list(name: str, default: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in os.environ.get(name, default).split(",") if item.strip())


# --- Data API (external source, one batch per call) --------------------------
DATA_API_URL = os.environ.get("DATA_API_URL", "http://data-api:80").rstrip("/")
DATA_API_GROUP_NUMBER = _env_int("DATA_API_GROUP_NUMBER", 6)
# Optional `day` query parameter of the API (Tuesday/Wednesday); empty = omit.
DATA_API_DAY = os.environ.get("DATA_API_DAY", "").strip() or None
# The largest batch is ~360k rows (~90 MB of JSON): generous read timeout.
DATA_API_CONNECT_TIMEOUT_SECONDS = _env_float("DATA_API_CONNECT_TIMEOUT_SECONDS", 10)
DATA_API_READ_TIMEOUT_SECONDS = _env_float("DATA_API_READ_TIMEOUT_SECONDS", 300)
# In-task attempts for transient failures (connection errors, 5xx), with
# exponential backoff. Airflow task retries come on top of these.
DATA_API_MAX_ATTEMPTS = _env_int("DATA_API_MAX_ATTEMPTS", 4)
DATA_API_BACKOFF_SECONDS = _env_float("DATA_API_BACKOFF_SECONDS", 5)

# --- Validation ----------------------------------------------------------------
# A batch whose share of rejected rows (invalid values, duplicates) exceeds this
# is considered invalid: it stays in RAW for audit and is never trained on.
MAX_REJECTED_FRACTION = _env_float("MAX_REJECTED_FRACTION", 0.05)
# Plausible ranges for training (outliers are kept but excluded from training).
TRAINING_BOUNDS: dict[str, tuple[float | None, float | None]] = (
    {key: tuple(value) for key, value in json.loads(os.environ["TRAINING_BOUNDS_JSON"]).items()}
    if os.environ.get("TRAINING_BOUNDS_JSON")
    else dict(DEFAULT_TRAINING_BOUNDS)
)
# Version of the CLEAN layer: the code's transformation version plus a hash of
# the training bounds, which decide `exclusion_reason`. Changing the bounds in
# the ConfigMap therefore creates a NEW version instead of silently mixing two
# rules under one name; preprocess_data then rebuilds every earlier batch under
# it, so a training set never combines rows cleaned with different rules.
CLEAN_VERSION = (
    TRANSFORMATION_VERSION
    + "+bounds-"
    + hashlib.sha256(json.dumps(TRAINING_BOUNDS, sort_keys=True).encode()).hexdigest()[:8]
)
# Columns whose unseen values are reported as new categories, and the share of
# the batch rows they must cover to count as a reason to retrain.
NEW_CATEGORY_COLUMNS = _env_list("NEW_CATEGORY_COLUMNS", "status,state,city")
NEW_CATEGORY_MIN_SHARE = _env_float("NEW_CATEGORY_MIN_SHARE", 0.01)
# Drift: Population Stability Index of the batch against the accumulated CLEAN
# history. PSI >= 0.2 is the usual "significant shift" threshold.
DRIFT_FEATURES = _env_list("DRIFT_FEATURES", "price,house_size,bed,bath,acre_lot,state,status")
DRIFT_PSI_THRESHOLD = _env_float("DRIFT_PSI_THRESHOLD", 0.20)
DRIFT_BINS = _env_int("DRIFT_BINS", 10)

# --- Training decision ----------------------------------------------------------
# Below this many training-eligible rows a batch never triggers training on its
# own (its rows stay in CLEAN and are used by the next training).
MIN_BATCH_ROWS = _env_int("MIN_BATCH_ROWS", 5000)
# Growth of eligible CLEAN rows since the production model was trained.
MIN_VOLUME_GROWTH = _env_float("MIN_VOLUME_GROWTH", 0.20)
# Relative worsening of the production model on the new (labelled) batch
# compared with its own validation score, measured with DEGRADATION_METRIC.
DEGRADATION_METRIC = os.environ.get("DEGRADATION_METRIC", "rmsle")
DEGRADATION_THRESHOLD = _env_float("DEGRADATION_THRESHOLD", 0.10)
DEGRADATION_SAMPLE_ROWS = _env_int("DEGRADATION_SAMPLE_ROWS", 20000)
# Data sufficiency: minimum rows per split to train and evaluate at all.
MIN_TRAIN_ROWS = _env_int("MIN_TRAIN_ROWS", 1000)
MIN_EVALUATION_ROWS = _env_int("MIN_EVALUATION_ROWS", 200)

# --- Training -------------------------------------------------------------------
TRAINING_CANDIDATES = _env_list("TRAINING_CANDIDATES", "ridge,hist_gradient_boosting")
# 0 = train on every eligible training row; otherwise a deterministic sample.
TRAINING_MAX_ROWS = _env_int("TRAINING_MAX_ROWS", 1_000_000)
# Rows used for permutation importance (interpretability artifact).
IMPORTANCE_SAMPLE_ROWS = _env_int("IMPORTANCE_SAMPLE_ROWS", 5000)

# --- Selection and promotion ------------------------------------------------------
# Base metric names of real_estate_model.models.evaluate_predictions:
# mae, rmse, rmsle, median_ae, mape, r2.
PRIMARY_METRIC = os.environ.get("PRIMARY_METRIC", "mae")
GUARD_METRIC = os.environ.get("GUARD_METRIC", "rmse")
# Promote only if the primary metric improves by at least this fraction AND the
# guard metric does not worsen by more than PROMOTION_MAX_GUARD_DEGRADATION.
PROMOTION_MIN_IMPROVEMENT = _env_float("PROMOTION_MIN_IMPROVEMENT", 0.03)
PROMOTION_MAX_GUARD_DEGRADATION = _env_float("PROMOTION_MAX_GUARD_DEGRADATION", 0.01)

# --- MLflow ---------------------------------------------------------------------
MODEL_NAME = os.environ.get("MODEL_NAME", "real_estate")
MODEL_ALIAS = os.environ.get("MODEL_ALIAS", "production")
EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "real-estate-pricing")
# Baked into the image by CI (docker build --build-arg GIT_COMMIT=<sha>).
GIT_COMMIT = os.environ.get("GIT_COMMIT", "unknown")


def snapshot() -> dict[str, Any]:
    """Every effective setting, logged with each batch decision for traceability."""
    return {
        "data_api_url": DATA_API_URL,
        "data_api_group_number": DATA_API_GROUP_NUMBER,
        "data_api_day": DATA_API_DAY,
        "max_rejected_fraction": MAX_REJECTED_FRACTION,
        "training_bounds": TRAINING_BOUNDS,
        "clean_version": CLEAN_VERSION,
        "new_category_columns": NEW_CATEGORY_COLUMNS,
        "new_category_min_share": NEW_CATEGORY_MIN_SHARE,
        "drift_features": DRIFT_FEATURES,
        "drift_psi_threshold": DRIFT_PSI_THRESHOLD,
        "min_batch_rows": MIN_BATCH_ROWS,
        "min_volume_growth": MIN_VOLUME_GROWTH,
        "degradation_metric": DEGRADATION_METRIC,
        "degradation_threshold": DEGRADATION_THRESHOLD,
        "min_train_rows": MIN_TRAIN_ROWS,
        "min_evaluation_rows": MIN_EVALUATION_ROWS,
        "training_candidates": TRAINING_CANDIDATES,
        "training_max_rows": TRAINING_MAX_ROWS,
        "primary_metric": PRIMARY_METRIC,
        "guard_metric": GUARD_METRIC,
        "promotion_min_improvement": PROMOTION_MIN_IMPROVEMENT,
        "promotion_max_guard_degradation": PROMOTION_MAX_GUARD_DEGRADATION,
        "git_commit": GIT_COMMIT,
    }


def database_engine() -> Any:
    """SQLAlchemy engine for the training database (raw, clean, audit)."""
    from sqlalchemy import URL, create_engine

    database_url = os.environ.get("TRAIN_DATABASE_URL")
    if database_url:
        return create_engine(database_url, pool_pre_ping=True)

    required = ("TRAIN_DB_HOST", "TRAIN_DB_NAME", "TRAIN_DB_USER", "TRAIN_DB_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Training database configuration is missing: " + ", ".join(missing))
    return create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=os.environ["TRAIN_DB_USER"],
            password=os.environ["TRAIN_DB_PASSWORD"],
            host=os.environ["TRAIN_DB_HOST"],
            port=int(os.environ.get("TRAIN_DB_PORT", "5432")),
            database=os.environ["TRAIN_DB_NAME"],
        ),
        pool_pre_ping=True,
    )


def mlflow_tracking_uri() -> str:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise RuntimeError("MLFLOW_TRACKING_URI must be configured.")
    return tracking_uri
