"""Batch checks: schema, data quality, new categories and distribution drift.

Each check writes its report to audit.batch_history and returns a short
summary for the DAG. A check never fails the DAG because the DATA is bad: an
unusable batch is flagged ``blocking`` and the training decision turns that
into "do not train". Only infrastructure errors (database, MLflow) raise.
"""

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from real_estate_model.contract import (
    COLUMN_TYPES,
    NUMERIC_TYPES,
    REQUIRED_COLUMNS,
    SOURCE_COLUMNS,
    TARGET_COLUMN,
    exclusion_reason,
    is_missing,
    normalize_value,
    validate_source_record,
)
from real_estate_pipeline import history, settings
from real_estate_pipeline.db import copy_rows, engine_scope

# JSON types the Data API may send for each logical column type. Codes
# (brokered_by, street, zip_code) arrive as numbers, e.g. 6016.0.
ALLOWED_JSON_TYPES = {
    "code": {"number", "string", "null"},
    "category": {"string", "null"},
    "integer": {"number", "null"},
    "float": {"number", "null"},
    "date": {"string", "null"},
}
BLOCKING_MISSING_COLUMNS = frozenset({TARGET_COLUMN, *REQUIRED_COLUMNS})

# How the model absorbs unseen categories (real_estate_model.models): logged
# with every report so the strategy is explicit next to the detection.
UNKNOWN_CATEGORY_STRATEGY = {
    "status, state": "OneHotEncoder(handle_unknown='ignore') -> all-zero vector "
    "(ridge, random_forest); OrdinalEncoder(unknown_value=-1) -> treated as missing "
    "by HistGradientBoosting",
    "city_state, zip_code, brokered_by": "TargetEncoder -> unseen value gets the global "
    "target mean of the training data",
    "missing values": "constant '__missing__' category / median imputation",
}


def _conformed(record: dict[str, Any]) -> dict[str, Any]:
    """The record restricted to the contract columns; absent ones become None.

    This is how the pipeline adapts to a schema change that is not blocking:
    extra columns are ignored and a missing optional column is treated as
    missing values. RAW keeps the record exactly as received.
    """
    return {column: record.get(column) for column in SOURCE_COLUMNS}


# --------------------------------------------------------------------------- #
# validate_schema
# --------------------------------------------------------------------------- #
def validate_schema(batch: dict[str, Any]) -> dict[str, Any]:
    with engine_scope() as engine, engine.connect() as connection:
        received = connection.execute(
            text("SELECT received_schema FROM raw.batches WHERE batch_id = :id"),
            {"id": batch["batch_id"]},
        ).scalar_one() or {}

    expected = set(SOURCE_COLUMNS)
    missing = sorted(expected - set(received))
    extra = sorted(set(received) - expected)
    type_changes = {
        column: {"expected": sorted(ALLOWED_JSON_TYPES[COLUMN_TYPES[column]]), "received": kinds}
        for column, kinds in received.items()
        if column in expected and not set(kinds) <= ALLOWED_JSON_TYPES[COLUMN_TYPES[column]]
    }
    blocking_missing = sorted(set(missing) & BLOCKING_MISSING_COLUMNS)
    blocking = bool(blocking_missing) or batch["record_count"] == 0
    changed = bool(missing or extra or type_changes)
    report = {
        "expected_columns": list(SOURCE_COLUMNS),
        "received_schema": received,
        "missing_columns": missing,
        "extra_columns": extra,
        "type_changes": type_changes,
        "changed": changed,
        "blocking": blocking,
        "blocking_reason": (
            f"faltan columnas obligatorias {blocking_missing}" if blocking_missing
            else "lote sin registros" if batch["record_count"] == 0 else None
        ),
        "adaptation": (
            "columnas extra ignoradas; columnas opcionales faltantes tratadas como nulos; "
            "valores con tipo inesperado se validan por fila"
            if changed and not blocking else None
        ),
        "missing_api_batches": batch.get("missing_api_batches", []),
    }
    history.update(batch["batch_id"], schema_report=report)
    print(f"Schema: changed={changed} blocking={blocking} missing={missing} extra={extra} types={list(type_changes)}")
    return {"blocking": blocking, "changed": changed, "missing_columns": missing, "extra_columns": extra}


# --------------------------------------------------------------------------- #
# validate_data_quality
# --------------------------------------------------------------------------- #
def validate_data_quality(batch: dict[str, Any]) -> dict[str, Any]:
    """Validate every row of the batch and persist validated/rejected statuses.

    Rejection reasons: contract violations (missing target/status, negative or
    non-numeric values, non-integer bed/bath, bad dates), duplicates inside the
    batch, and exact duplicates of rows already validated in earlier batches.
    Outliers are NOT rejected here: they are valid observations kept out of
    training by preprocess_data (exclusion_reason), and counted in the report.
    """
    batch_id = batch["batch_id"]
    with engine_scope() as engine:
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT raw_record_id, record_key, record_data
                    FROM raw.records WHERE batch_id = :id ORDER BY row_number
                    """
                ),
                {"id": batch_id},
            ).all()
            previous_keys = set(
                connection.execute(
                    text(
                        """
                        SELECT DISTINCT r.record_key FROM raw.records r
                        WHERE r.batch_id = :id AND EXISTS (
                            SELECT 1 FROM raw.records p
                            WHERE p.record_key = r.record_key
                              AND p.batch_id <> :id AND p.status = 'validated'
                        )
                        """
                    ),
                    {"id": batch_id},
                ).scalars()
            )

        reasons: Counter[str] = Counter()
        nulls: Counter[str] = Counter()
        outliers: Counter[str] = Counter()
        seen: set[str] = set()
        statuses = []
        prices = []
        numeric_values: dict[str, list[float]] = {c: [] for c, k in COLUMN_TYPES.items() if k in NUMERIC_TYPES}
        for raw_record_id, key, data in rows:
            record = _conformed(data)
            for column, value in record.items():
                nulls[column] += is_missing(value)
            errors = validate_source_record(record)
            if key in seen:
                errors.append("duplicate_in_batch")
            elif key in previous_keys:
                errors.append("duplicate_of_previous_batch")
            seen.add(key)
            reasons.update(errors)
            if not errors:
                reason = exclusion_reason(record, settings.TRAINING_BOUNDS)
                for item in (reason or "").split(";"):
                    if item:
                        outliers[item] += 1
                prices.append(float(record[TARGET_COLUMN]))
                for column in numeric_values:
                    if not is_missing(record[column]):
                        numeric_values[column].append(float(record[column]))
            statuses.append(
                (raw_record_id, "rejected" if errors else "validated", "; ".join(errors) or None)
            )

        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMP TABLE tmp_status (raw_record_id BIGINT, status TEXT, "
                    "status_reason TEXT) ON COMMIT DROP"
                )
            )
            copy_rows(connection, "tmp_status", ("raw_record_id", "status", "status_reason"), statuses)
            connection.execute(
                text(
                    """
                    UPDATE raw.records r
                    SET status = t.status, status_reason = t.status_reason,
                        validated_at = CURRENT_TIMESTAMP
                    FROM tmp_status t WHERE r.raw_record_id = t.raw_record_id
                    """
                )
            )

    total = len(rows)
    rejected = sum(1 for _, status, _ in statuses if status == "rejected")
    validated = total - rejected
    rejected_share = rejected / total if total else 1.0
    blocking = total == 0 or rejected_share > settings.MAX_REJECTED_FRACTION
    price = pd.Series(prices, dtype="float64")
    report = {
        "rows": total,
        "validated_rows": validated,
        "rejected_rows": rejected,
        "rejected_share": round(rejected_share, 6),
        "max_rejected_fraction": settings.MAX_REJECTED_FRACTION,
        "rejection_reasons": dict(reasons),
        "null_counts": {column: count for column, count in nulls.items() if count},
        "duplicates_in_batch": reasons.get("duplicate_in_batch", 0),
        "duplicates_of_previous_batches": reasons.get("duplicate_of_previous_batch", 0),
        "out_of_range_counts": dict(outliers),
        "training_bounds": settings.TRAINING_BOUNDS,
        "target": (
            {
                "min": float(price.min()), "p01": float(price.quantile(0.01)),
                "median": float(price.median()), "p99": float(price.quantile(0.99)),
                "max": float(price.max()), "mean": float(price.mean()),
                "non_positive": int((price <= 0).sum()),
            }
            if len(price) else None
        ),
        "numeric_ranges": {
            column: {"min": min(values), "max": max(values)}
            for column, values in numeric_values.items() if values
        },
        "blocking": blocking,
        "blocking_reason": (
            f"{rejected_share:.2%} de filas rechazadas > {settings.MAX_REJECTED_FRACTION:.2%}"
            if blocking and total else ("lote sin registros" if blocking else None)
        ),
    }
    history.update(batch_id, quality_report=report)
    print(
        f"Quality: {validated}/{total} validated, {rejected} rejected "
        f"({rejected_share:.2%}) reasons={dict(reasons)} outliers={dict(outliers)} blocking={blocking}"
    )
    return {"blocking": blocking, "validated_rows": validated, "rejected_rows": rejected,
            "rejected_share": rejected_share}


# --------------------------------------------------------------------------- #
# Helpers shared by detect_new_categories and detect_data_drift
# --------------------------------------------------------------------------- #
def _batch_frame(batch_id: str, eligible_only: bool) -> pd.DataFrame:
    """Validated rows of the batch as normalized contract values (+ city_state)."""
    with engine_scope() as engine, engine.connect() as connection:
        rows = connection.execute(
            text("SELECT record_data FROM raw.records WHERE batch_id = :id AND status = 'validated'"),
            {"id": batch_id},
        ).scalars().all()
    records = []
    for data in rows:
        record = _conformed(data)
        if eligible_only and exclusion_reason(record, settings.TRAINING_BOUNDS):
            continue
        records.append({column: normalize_value(column, record[column]) for column in SOURCE_COLUMNS})
    frame = pd.DataFrame.from_records(records, columns=list(SOURCE_COLUMNS))
    return _with_city_state(frame)


def _with_city_state(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["city_state"] = [
        None if city is None or (isinstance(city, float) and np.isnan(city))
        else f"{city}, {state}"
        for city, state in zip(frame.get("city", []), frame.get("state", []), strict=True)
    ]
    return frame


def reference_version(connection: Any, batch_id: str) -> str | None:
    """CLEAN version to compare a batch against (drift, new categories).

    The current CLEAN_VERSION when earlier batches already exist under it;
    right after a change of the training bounds they have not been rebuilt yet
    (preprocess_data does it), so the most recent earlier version is used.
    """
    return connection.execute(
        text(
            """
            SELECT transformation_version FROM clean.records
            WHERE batch_id <> :id
            GROUP BY transformation_version
            ORDER BY (transformation_version = :current) DESC, MAX(processed_at) DESC
            LIMIT 1
            """
        ),
        {"id": batch_id, "current": settings.CLEAN_VERSION},
    ).scalar()


def _history_frame(batch_id: str, columns: list[str]) -> pd.DataFrame:
    """Training-eligible CLEAN rows of every OTHER batch (the historical reference)."""
    select = ", ".join(columns)
    with engine_scope() as engine, engine.connect() as connection:
        version = reference_version(connection, batch_id)
        return pd.read_sql_query(
            text(
                f"""
                SELECT {select} FROM clean.records
                WHERE transformation_version = :version AND batch_id <> :id
                  AND exclusion_reason IS NULL
                """
            ),
            connection,
            params={"version": version, "id": batch_id},
        )


# --------------------------------------------------------------------------- #
# detect_new_categories
# --------------------------------------------------------------------------- #
INFORMATIVE_CATEGORY_COLUMNS = ("zip_code", "brokered_by")


def detect_new_categories(batch: dict[str, Any]) -> dict[str, Any]:
    """Categories of the batch never seen in CLEAN, and the share of rows they cover.

    `city` is compared as "city, state" (the same city name exists in several
    states). Only NEW_CATEGORY_COLUMNS can trigger training; ZIP codes and
    brokers are high-cardinality ids, so their novelty is reported only.
    """
    batch_id = batch["batch_id"]
    current = _batch_frame(batch_id, eligible_only=False)
    with engine_scope() as engine, engine.connect() as connection:
        version = reference_version(connection, batch_id)
    has_history = version is not None

    columns: dict[str, Any] = {}
    rows = len(current)
    for column in (*settings.NEW_CATEGORY_COLUMNS, *INFORMATIVE_CATEGORY_COLUMNS):
        compared = "city_state" if column == "city" else column
        expression = "city || ', ' || state" if compared == "city_state" else compared
        sql = (
            f"SELECT DISTINCT {expression} FROM clean.records "
            f"WHERE batch_id <> :id AND transformation_version = :v AND {expression} IS NOT NULL"
        )
        with engine_scope() as engine, engine.connect() as connection:
            known = set(connection.execute(text(sql), {"id": batch_id, "v": version}).scalars())
        values = current[compared].dropna()
        new_mask = ~values.isin(known)
        counts = values[new_mask].value_counts()
        share = float(new_mask.sum() / rows) if rows else 0.0
        columns[column] = {
            "compared_as": compared,
            "known_categories": len(known),
            "batch_categories": int(values.nunique()),
            "new_categories": int(len(counts)),
            "rows_with_new_category": int(new_mask.sum()),
            "share_of_batch": round(share, 6),
            "top_new": {str(key): int(value) for key, value in counts.head(15).items()},
            "can_trigger_training": column in settings.NEW_CATEGORY_COLUMNS,
        }

    significant = {
        column: info["share_of_batch"]
        for column, info in columns.items()
        if has_history and info["can_trigger_training"]
        and info["new_categories"] > 0
        and info["share_of_batch"] >= settings.NEW_CATEGORY_MIN_SHARE
    }
    report = {
        "baseline": not has_history,
        "columns": columns,
        "min_share": settings.NEW_CATEGORY_MIN_SHARE,
        "significant": significant,
        "unknown_category_strategy": UNKNOWN_CATEGORY_STRATEGY,
    }
    history.update(batch_id, new_categories_report=report)
    print(
        "New categories: "
        + ", ".join(f"{c}={i['new_categories']} ({i['share_of_batch']:.2%})" for c, i in columns.items())
        + f"; significant={significant}"
    )
    return {"baseline": not has_history, "significant": significant}


# --------------------------------------------------------------------------- #
# detect_data_drift
# --------------------------------------------------------------------------- #
PSI_EPSILON = 1e-4


def _psi(expected: np.ndarray, actual: np.ndarray) -> float:
    expected = np.clip(expected, PSI_EPSILON, None)
    actual = np.clip(actual, PSI_EPSILON, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def numeric_psi(reference: pd.Series, current: pd.Series, bins: int) -> dict[str, Any]:
    reference = reference.dropna().astype("float64")
    current = current.dropna().astype("float64")
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        # (Almost) constant reference: three bins, below / equal to / above its value.
        value = float(reference.median())
        edges = np.array([-np.inf, value, np.nextafter(value, np.inf), np.inf])
    else:
        edges[0], edges[-1] = -np.inf, np.inf
    expected = np.histogram(reference, edges)[0] / len(reference)
    actual = np.histogram(current, edges)[0] / len(current)
    return {
        "psi": round(_psi(expected, actual), 6),
        "reference_median": float(reference.median()),
        "batch_median": float(current.median()),
        "reference_mean": float(reference.mean()),
        "batch_mean": float(current.mean()),
    }


def categorical_psi(reference: pd.Series, current: pd.Series, top: int = 20) -> dict[str, Any]:
    reference = reference.dropna().astype(str)
    current = current.dropna().astype(str)
    categories = list(reference.value_counts().head(top).index)
    def distribution(values: pd.Series) -> np.ndarray:
        shares = values.where(values.isin(categories), "__other__").value_counts(normalize=True)
        return np.array([shares.get(category, 0.0) for category in [*categories, "__other__"]])
    batch_top = current.value_counts(normalize=True).head(5)
    return {
        "psi": round(_psi(distribution(reference), distribution(current)), 6),
        "batch_top": {str(key): round(float(value), 4) for key, value in batch_top.items()},
    }


def detect_data_drift(batch: dict[str, Any]) -> dict[str, Any]:
    """PSI of each DRIFT_FEATURE: training-eligible batch rows vs CLEAN history."""
    batch_id = batch["batch_id"]
    features = list(settings.DRIFT_FEATURES)
    current = _batch_frame(batch_id, eligible_only=True)
    reference = _history_frame(batch_id, features)
    if reference.empty or current.empty:
        report = {
            "baseline": reference.empty,
            "reason": "sin historial de referencia" if reference.empty else "lote sin filas elegibles",
            "features": {},
            "drifted_features": [],
        }
    else:
        results = {}
        for feature in features:
            if COLUMN_TYPES.get(feature) in NUMERIC_TYPES:
                results[feature] = {"type": "numeric", **numeric_psi(reference[feature], current[feature], settings.DRIFT_BINS)}
            else:
                results[feature] = {"type": "categorical", **categorical_psi(reference[feature], current[feature])}
        drifted = sorted(
            (feature for feature, info in results.items() if info["psi"] >= settings.DRIFT_PSI_THRESHOLD),
            key=lambda feature: -results[feature]["psi"],
        )
        report = {
            "baseline": False,
            "method": f"PSI con {settings.DRIFT_BINS} bins por cuantiles de la referencia",
            "threshold": settings.DRIFT_PSI_THRESHOLD,
            "reference_rows": int(len(reference)),
            "batch_rows": int(len(current)),
            "features": results,
            "drifted_features": drifted,
        }
    history.update(batch_id, drift_report=report)
    print(
        "Drift: "
        + ", ".join(f"{f}={i['psi']:.3f}" for f, i in report["features"].items())
        + f"; drifted={report['drifted_features']}"
    )
    return {"baseline": report["baseline"], "drifted_features": report["drifted_features"]}
