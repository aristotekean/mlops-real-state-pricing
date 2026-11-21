import csv

import numpy as np
import pandas as pd
import pytest
from records import source_record, source_records

from real_estate_model.contract import SOURCE_COLUMNS, input_frame
from real_estate_model.features import engineer_features
from real_estate_model.models import (
    CANDIDATES,
    PRIMARY_METRIC,
    build_candidate,
    build_serving_pipeline,
    evaluate,
    evaluate_predictions,
    is_better,
)
from real_estate_model.training import best_candidate, prepare_dataset, train_candidates

RECORDS = source_records(240)
INPUTS = input_frame(RECORDS)
FEATURES = engineer_features(INPUTS)
PRICES = pd.Series([float(record["price"]) for record in RECORDS])


@pytest.mark.parametrize("name", CANDIDATES)
def test_candidate_learns_prices(name):
    model = build_candidate(name).fit(FEATURES, PRICES)
    predictions = model.predict(FEATURES)

    assert predictions.shape == (len(RECORDS),)
    assert np.isfinite(predictions).all()
    assert (predictions > 0).all()
    metrics = evaluate(model, FEATURES, PRICES, prefix="train")
    assert set(metrics) == {
        "train_rmsle",
        "train_rmse",
        "train_mae",
        "train_median_ae",
        "train_mape",
        "train_r2",
    }
    # Better than always predicting the mean price.
    assert metrics["train_r2"] > 0


def test_unknown_candidate():
    with pytest.raises(ValueError, match="Unknown candidate model"):
        build_candidate("xgboost")


def test_serving_pipeline_accepts_contract_inputs_and_unseen_categories():
    candidate = build_candidate("hist_gradient_boosting").fit(FEATURES, PRICES)
    serving = build_serving_pipeline(candidate)

    np.testing.assert_allclose(serving.predict(INPUTS), candidate.predict(FEATURES))
    unseen = input_frame(
        [source_record(city="Nowhere", state="Atlantis", zip_code="99999", bed="")]
    )
    assert np.isfinite(serving.predict(unseen)).all()


def test_evaluate_predictions_values():
    metrics = evaluate_predictions(
        pd.Series([100.0, 200.0]), [110.0, 180.0], prefix="validation"
    )
    assert metrics["validation_mae"] == pytest.approx(15.0)
    assert metrics["validation_rmse"] == pytest.approx(np.sqrt(250.0))
    assert metrics["validation_mape"] == pytest.approx(0.1)
    assert metrics["validation_r2"] == pytest.approx(0.9)
    assert PRIMARY_METRIC in metrics


def test_negative_predictions_are_scored_as_zero():
    metrics = evaluate_predictions(pd.Series([100.0, 300.0]), [-50.0, 300.0], prefix="test")
    assert metrics["test_mae"] == pytest.approx(50.0)
    assert np.isfinite(metrics["test_rmsle"])


def test_lower_primary_metric_wins():
    assert is_better(0.2, 0.3)
    assert not is_better(0.3, 0.2)
    results = {
        "ridge": (None, {PRIMARY_METRIC: 0.40}),
        "random_forest": (None, {PRIMARY_METRIC: 0.25}),
    }
    assert best_candidate(results) == "random_forest"


def test_offline_training_from_csv(tmp_path):
    rows = [*RECORDS, source_record(price="0"), source_record(status="")]
    path = tmp_path / "listings.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    data = prepare_dataset(path)
    assert (data.rejected_rows, data.excluded_rows) == (2, 0)
    assert len(data.target) == len(RECORDS)
    assert data.target.dtype == "float64"
    assert set(data.split) == {"train", "validation", "test"}

    results = train_candidates(data, ("ridge",))
    assert {"train_rmsle", "validation_rmsle", "test_rmsle"} <= set(results["ridge"][1])
