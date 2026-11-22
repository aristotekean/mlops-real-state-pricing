"""Pure helpers of the batch checks: schema observation, drift and CLEAN version."""

import importlib

import numpy as np
import pandas as pd
import pytest

from real_estate_model.contract import SOURCE_COLUMNS
from real_estate_pipeline import settings
from real_estate_pipeline.raw import _jsonb_safe, observed_schema
from real_estate_pipeline.validation import _conformed, categorical_psi, numeric_psi


def test_observed_schema_lists_every_json_type():
    schema = observed_schema([{"price": 1.0, "city": "A"}, {"price": None, "city": "B", "extra": True}])
    assert schema == {"city": ["string"], "extra": ["boolean"], "price": ["null", "number"]}


def test_conformed_ignores_extra_and_fills_missing_columns():
    record = _conformed({"price": 1.0, "unexpected": "x"})
    assert list(record) == list(SOURCE_COLUMNS)
    assert record["price"] == 1.0 and record["city"] is None and "unexpected" not in record


def test_jsonb_safe_replaces_non_finite_numbers_only():
    assert _jsonb_safe({"a": float("nan"), "b": float("inf"), "c": 1.5, "d": "x"}) == {
        "a": None, "b": None, "c": 1.5, "d": "x"
    }


def test_psi_is_zero_for_the_same_distribution_and_large_for_a_shift():
    rng = np.random.default_rng(0)
    reference = pd.Series(rng.lognormal(12, 0.5, 50_000))
    same = numeric_psi(reference, pd.Series(rng.lognormal(12, 0.5, 20_000)), bins=10)
    shifted = numeric_psi(reference, pd.Series(rng.lognormal(13, 0.5, 20_000)), bins=10)
    assert same["psi"] < 0.02
    assert shifted["psi"] > 0.2


def test_numeric_psi_handles_a_constant_reference():
    assert numeric_psi(pd.Series([3.0] * 100), pd.Series([3.0] * 50 + [4.0] * 50), bins=10)["psi"] > 0.2


def test_categorical_psi_detects_a_new_dominant_category():
    reference = pd.Series(["Texas"] * 500 + ["Ohio"] * 500)
    assert categorical_psi(reference, pd.Series(["Texas"] * 50 + ["Ohio"] * 50))["psi"] < 0.01
    assert categorical_psi(reference, pd.Series(["Massachusetts"] * 100))["psi"] > 0.2


def test_clean_version_changes_with_the_training_bounds(monkeypatch):
    default = settings.CLEAN_VERSION
    monkeypatch.setenv("TRAINING_BOUNDS_JSON", '{"price": [1, 2]}')
    changed = importlib.reload(settings).CLEAN_VERSION
    monkeypatch.delenv("TRAINING_BOUNDS_JSON")
    assert importlib.reload(settings).CLEAN_VERSION == default
    assert changed != default and changed.startswith(default.split("+")[0])
