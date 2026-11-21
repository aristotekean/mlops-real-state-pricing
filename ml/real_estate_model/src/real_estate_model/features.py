"""Feature engineering shared by CLEAN generation and the served model."""

from collections.abc import Iterable, Mapping
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from real_estate_model.contract import INPUT_COLUMNS, NUMERIC_INPUTS, is_missing

TRANSFORMATION_VERSION = "price-features-v1"

DAYS_PER_YEAR = 365.25

ENGINEERED_NUMERIC = (*NUMERIC_INPUTS, "prev_sold_year", "previously_sold")

# Split by cardinality because the encoders differ: a handful of values can be
# one-hot encoded, thousands of cities, ZIP codes or brokers cannot.
LOW_CARDINALITY_CATEGORICAL = ("status", "state")
HIGH_CARDINALITY_CATEGORICAL = ("city_state", "zip_code", "brokered_by")
ENGINEERED_CATEGORICAL = (*LOW_CARDINALITY_CATEGORICAL, *HIGH_CARDINALITY_CATEGORICAL)

FEATURE_COLUMNS = (*ENGINEERED_NUMERIC, *ENGINEERED_CATEGORICAL)


def _prev_sold_year(value: Any) -> float:
    """Previous sale as a fractional year, e.g. 2020-07-01 -> 2020.5.

    A point in time, not "years ago": the feature must not depend on the day
    the pipeline or the API happens to run.
    """
    if is_missing(value):
        return np.nan
    sold = date.fromisoformat(str(value)[:10])
    return sold.year + (sold.timetuple().tm_yday - 1) / DAYS_PER_YEAR


def _city_state(city: Any, state: Any) -> Any:
    """Qualify the city with its state: the same city name repeats across states."""
    if is_missing(city):
        return np.nan
    return str(city) if is_missing(state) else f"{city}, {state}"


def _categorical(values: pd.Series) -> pd.Series:
    """Use np.nan for missing categories: SimpleImputer does not detect None."""
    values = values.astype(object)
    return values.where(~values.map(is_missing), np.nan)


def engineer_features(inputs: pd.DataFrame) -> pd.DataFrame:
    """Derive the model features from contract inputs (see ``prepare_input``)."""
    missing = [column for column in INPUT_COLUMNS if column not in inputs.columns]
    if missing:
        raise ValueError("Inputs are missing columns: " + ", ".join(missing))

    features = pd.DataFrame(index=inputs.index)
    for column in NUMERIC_INPUTS:
        features[column] = pd.to_numeric(inputs[column], errors="coerce").astype(
            "float64"
        )
    features["prev_sold_year"] = (
        inputs["prev_sold_date"].map(_prev_sold_year).astype("float64")
    )
    features["previously_sold"] = features["prev_sold_year"].notna().astype("float64")

    features["city_state"] = _categorical(
        pd.Series(
            [
                _city_state(city, state)
                for city, state in zip(inputs["city"], inputs["state"], strict=True)
            ],
            index=inputs.index,
            dtype=object,
        )
    )
    for column in ("status", "state", "zip_code", "brokered_by"):
        features[column] = _categorical(inputs[column])

    return features.loc[:, list(FEATURE_COLUMNS)]


def feature_frame(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Rebuild a typed feature DataFrame from CLEAN ``features`` JSON objects."""
    frame = pd.DataFrame.from_records(list(rows), columns=list(FEATURE_COLUMNS))
    for column in ENGINEERED_NUMERIC:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
    for column in ENGINEERED_CATEGORICAL:
        frame[column] = _categorical(frame[column])
    return frame


def features_to_json(features: pd.DataFrame) -> list[dict[str, Any]]:
    """Serialize engineered features to JSON-safe dicts (NaN becomes null)."""
    records = []
    for row in features.itertuples(index=False, name=None):
        record = {}
        for name, value in zip(features.columns, row, strict=True):
            if is_missing(value):
                record[name] = None
            elif hasattr(value, "item"):
                record[name] = value.item()
            else:
                record[name] = value
        records.append(record)
    return records


class FeatureEngineer(TransformerMixin, BaseEstimator):
    """Stateless sklearn step so the served model accepts contract inputs."""

    def fit(self, X: pd.DataFrame, y: Any = None) -> "FeatureEngineer":
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X, columns=list(INPUT_COLUMNS))
        return engineer_features(X)

    def get_feature_names_out(self, input_features: Any = None) -> np.ndarray:
        return np.asarray(FEATURE_COLUMNS, dtype=object)
