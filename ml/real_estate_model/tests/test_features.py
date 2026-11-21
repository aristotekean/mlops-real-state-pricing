import json

import numpy as np
import pandas as pd
import pytest
from records import source_record, source_records

from real_estate_model.contract import input_frame
from real_estate_model.features import (
    FEATURE_COLUMNS,
    FeatureEngineer,
    engineer_features,
    feature_frame,
    features_to_json,
)


def features_of(**overrides):
    return engineer_features(input_frame([source_record(**overrides)])).iloc[0]


def test_feature_columns_and_order():
    features = engineer_features(input_frame(source_records(8)))
    assert list(features.columns) == list(FEATURE_COLUMNS)
    assert len(features) == 8


def test_numeric_inputs_pass_through():
    row = features_of()
    assert (row["bed"], row["bath"], row["acre_lot"], row["house_size"]) == (
        3.0,
        2.0,
        0.12,
        920.0,
    )


def test_previous_sale_becomes_a_fractional_year():
    row = features_of(prev_sold_date="2020-07-01")
    assert row["prev_sold_year"] == pytest.approx(2020.5, abs=0.01)
    assert row["previously_sold"] == 1.0


def test_never_sold_property():
    row = features_of(prev_sold_date="")
    assert np.isnan(row["prev_sold_year"])
    assert row["previously_sold"] == 0.0


def test_city_is_qualified_by_state():
    assert features_of(city="Springfield", state="Illinois")["city_state"] == (
        "Springfield, Illinois"
    )
    assert features_of(city="Springfield", state="")["city_state"] == "Springfield"
    assert pd.isna(features_of(city="")["city_state"])


def test_categoricals_keep_labels_and_missing_is_nan():
    row = features_of(brokered_by="")
    assert (row["status"], row["state"], row["zip_code"]) == (
        "for_sale",
        "Puerto Rico",
        "00601",
    )
    assert pd.isna(row["brokered_by"])


def test_features_survive_the_json_round_trip():
    features = engineer_features(input_frame(source_records(8)))
    stored = json.loads(json.dumps(features_to_json(features)))
    pd.testing.assert_frame_equal(feature_frame(stored), features)


def test_missing_input_column_is_an_error():
    inputs = input_frame([source_record()]).drop(columns=["house_size"])
    with pytest.raises(ValueError, match="house_size"):
        engineer_features(inputs)


def test_feature_engineer_matches_the_function():
    inputs = input_frame(source_records(8))
    step = FeatureEngineer().fit(inputs)
    pd.testing.assert_frame_equal(step.transform(inputs), engineer_features(inputs))
    assert list(step.get_feature_names_out()) == list(FEATURE_COLUMNS)
