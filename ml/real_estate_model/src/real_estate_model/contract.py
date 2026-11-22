"""Source schema, inference input contract, and per-record validation rules."""

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

import pandas as pd

DATASET_SCHEMA_VERSION = "real-estate-listings-v1"
TARGET_COLUMN = "price"

MISSING_TOKENS = frozenset({"", "nan", "null", "none"})

# Logical type of every source column, in CSV order.
#
# INFERRED from the data dictionary, not from the data itself: correct a type
# here and every derived tuple below (and the validation rules) follows.
#
#   code      categorical label that looks like a number; kept as text
#   category  categorical text
#   integer   whole, non-negative number
#   float     non-negative real number
#   date      calendar date in ISO 8601 (YYYY-MM-DD)
COLUMN_TYPES: dict[str, str] = {
    "brokered_by": "code",
    "status": "category",
    "price": "float",
    "bed": "integer",
    "bath": "integer",
    "acre_lot": "float",
    "street": "code",
    "city": "category",
    "state": "category",
    "zip_code": "code",
    "house_size": "float",
    "prev_sold_date": "date",
}
NUMERIC_TYPES = frozenset({"integer", "float"})
CATEGORICAL_TYPES = frozenset({"code", "category"})

SOURCE_COLUMNS = tuple(COLUMN_TYPES)

# Columns that may not be missing in a source record (besides the target).
REQUIRED_COLUMNS = ("status",)

# `street` is an encoded address, close to unique per property: it identifies a
# listing instead of describing it, so it stays in RAW and out of the model.
NON_INPUT_COLUMNS = frozenset({TARGET_COLUMN, "street"})

INPUT_COLUMNS = tuple(
    column for column in SOURCE_COLUMNS if column not in NON_INPUT_COLUMNS
)
NUMERIC_INPUTS = tuple(
    column for column in INPUT_COLUMNS if COLUMN_TYPES[column] in NUMERIC_TYPES
)
CATEGORICAL_INPUTS = tuple(
    column for column in INPUT_COLUMNS if COLUMN_TYPES[column] in CATEGORICAL_TYPES
)
DATE_INPUTS = tuple(
    column for column in INPUT_COLUMNS if COLUMN_TYPES[column] == "date"
)

# Closed domains, when a column has one. Empty until the real values are known.
ALLOWED_VALUES: dict[str, frozenset[str]] = {}

# Assumption: US ZIP codes, which lose their leading zeros when read as numbers.
ZIP_CODE_COLUMN = "zip_code"
ZIP_CODE_LENGTH = 5

# The source has no row or property identifier. A property is recognised by its
# address so that two listings of the same property share a data split.
PROPERTY_KEY_COLUMNS = ("street", "city", "state", "zip_code")

_INTEGRAL_FLOAT = re.compile(r"^(\d+)\.0+$")


def is_missing(value: Any) -> bool:
    """Return whether a raw or API value represents a missing observation."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in MISSING_TOKENS
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _number(column: str, value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{column} must be numeric, got {value!r}") from exc
    if not math.isfinite(number):
        raise ValueError(f"{column} must be finite, got {value!r}")
    return number


def _iso_date(column: str, value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value).strip()[:10]).isoformat()
    except ValueError as exc:
        raise ValueError(f"{column} must be a YYYY-MM-DD date, got {value!r}") from exc


def _label(column: str, value: Any) -> str:
    """Text form of a categorical value; codes drop a spurious ``.0`` suffix."""
    if isinstance(value, float) and value.is_integer():
        text = str(int(value))
    else:
        text = str(value).strip()
    if COLUMN_TYPES[column] == "code":
        integral = _INTEGRAL_FLOAT.match(text)
        if integral:
            text = integral.group(1)
        if column == ZIP_CODE_COLUMN and text.isdigit():
            text = text.zfill(ZIP_CODE_LENGTH)
    return text


def normalize_value(column: str, value: Any) -> Any:
    """Convert one value to its contract type; missing becomes ``None``.

    Numbers become ``float``, dates an ISO string, and categorical values
    (including numeric-looking codes) ``str``. Raises ``ValueError`` when the
    value cannot be read as the column's type.
    """
    if is_missing(value):
        return None
    kind = COLUMN_TYPES[column]
    if kind in NUMERIC_TYPES:
        return _number(column, value)
    if kind == "date":
        return _iso_date(column, value)
    return _label(column, value)


def validate_source_record(record: Mapping[str, Any]) -> list[str]:
    """Return the data-quality violations of one RAW source record."""
    missing_columns = [column for column in SOURCE_COLUMNS if column not in record]
    if missing_columns:
        return ["missing_columns:" + ",".join(missing_columns)]

    errors: list[str] = []
    for column, kind in COLUMN_TYPES.items():
        value = record[column]
        if is_missing(value):
            if column == TARGET_COLUMN or column in REQUIRED_COLUMNS:
                errors.append(f"missing_{column}")
            continue
        try:
            normalized = normalize_value(column, value)
        except ValueError:
            errors.append(f"invalid_{column}")
            continue
        if kind in NUMERIC_TYPES:
            if normalized < 0:
                errors.append(f"negative_{column}")
            elif kind == "integer" and not normalized.is_integer():
                errors.append(f"invalid_{column}")
            elif column == TARGET_COLUMN and normalized == 0:
                errors.append(f"non_positive_{column}")
        allowed = ALLOWED_VALUES.get(column)
        if allowed is not None and normalized not in allowed:
            errors.append(f"invalid_{column}")
    return errors


# Plausible range of each numeric field FOR TRAINING, as (low, high) with None
# for an open end. A valid record outside a range stays in RAW and CLEAN but is
# excluded from training: the extremes of the source (515 M USD, 444 bedrooms,
# 100,000 acres, 1.5 M sq ft) are data-entry errors that dominate any squared
# error and make linear models extrapolate wildly. The defaults keep ~99.9% of
# the group data; the pipeline may override them (TRAINING_BOUNDS_JSON).
DEFAULT_TRAINING_BOUNDS: dict[str, tuple[float | None, float | None]] = {
    "price": (10_000.0, 20_000_000.0),
    "bed": (None, 20.0),
    "bath": (None, 20.0),
    "house_size": (100.0, 30_000.0),
    "acre_lot": (None, 1_000.0),
}


def exclusion_reason(
    record: Mapping[str, Any],
    bounds: Mapping[str, tuple[float | None, float | None]] | None = None,
) -> str | None:
    """Return why a VALID record cannot be used for training, if it cannot.

    Expects a record that already passed ``validate_source_record``. Returns
    ``out_of_range_<column>`` reasons joined by ``;`` or ``None``.
    """
    reasons = []
    for column, (low, high) in (DEFAULT_TRAINING_BOUNDS if bounds is None else bounds).items():
        value = record.get(column)
        if is_missing(value):
            continue
        number = _number(column, value)
        if (low is not None and number < low) or (high is not None and number > high):
            reasons.append(f"out_of_range_{column}")
    return ";".join(reasons) or None


def record_key(record: Mapping[str, Any]) -> str:
    """Content hash that identifies a source record (the source has no id).

    Two byte-identical listings share a key, which is how duplicates are found.
    """
    values = [
        "" if is_missing(record.get(column)) else str(record[column]).strip()
        for column in SOURCE_COLUMNS
    ]
    return hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).hexdigest()


def split_key(record: Mapping[str, Any]) -> str:
    """Key that decides the data split: the property, or the record without one.

    Listings of one property (same address) must not straddle train and test,
    so they share a key. Without a street the record stands on its own.
    """
    if is_missing(record.get("street")):
        return "record:" + record_key(record)
    parts = [
        "" if is_missing(record.get(column)) else _label(column, record[column])
        for column in PROPERTY_KEY_COLUMNS
    ]
    return "property:" + json.dumps(parts, ensure_ascii=False)


def prepare_input(record: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one record to the model input contract.

    Extra keys such as the target or ``street`` are ignored.
    """
    return {
        column: normalize_value(column, record.get(column)) for column in INPUT_COLUMNS
    }


def input_frame(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Build a model input DataFrame with the contract's columns and dtypes."""
    frame = pd.DataFrame.from_records(
        [prepare_input(record) for record in records], columns=list(INPUT_COLUMNS)
    )
    for column in NUMERIC_INPUTS:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
    for column in (*CATEGORICAL_INPUTS, *DATE_INPUTS):
        frame[column] = frame[column].astype(object).where(frame[column].notna(), None)
    return frame
