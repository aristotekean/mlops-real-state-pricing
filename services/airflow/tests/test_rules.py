"""Training decision and promotion rule (real_estate_pipeline.rules)."""

import pytest

from real_estate_pipeline.rules import Thresholds, decide_promotion, decide_training

T = Thresholds(
    min_train_rows=1000,
    min_evaluation_rows=200,
    min_batch_rows=5000,
    min_volume_growth=0.20,
    drift_psi_threshold=0.20,
    new_category_min_share=0.01,
    degradation_threshold=0.10,
    primary_metric="mae",
    guard_metric="rmse",
    promotion_min_improvement=0.03,
    promotion_max_guard_degradation=0.01,
)

OK = {"blocking": False}
NO_DEGRADATION = {"available": True, "degradation": 0.02, "metric": "rmsle",
                  "batch_value": 0.30, "reference_validation": 0.29, "production_version": "3"}


def prep(eligible=20_000, accumulated=120_000, train=84_000, validation=18_000, test=18_000):
    return {
        "eligible_rows": eligible,
        "accumulated_eligible_rows": accumulated,
        "accumulated_split_rows": {"train": train, "validation": validation, "test": test},
    }


def categories(share=0.0, column="state", baseline=False):
    return {
        "baseline": baseline,
        "columns": {column: {"new_categories": 1 if share else 0, "share_of_batch": share,
                             "can_trigger_training": True}},
    }


def drift(**psi):
    return {"features": {feature: {"psi": value} for feature, value in psi.items()}}


def decide(**overrides):
    arguments = {
        "schema": OK, "quality": OK, "categories": categories(), "drift": drift(price=0.05),
        "preprocessing": prep(), "has_production": True, "production_trained_rows": 110_000,
        "degradation": NO_DEGRADATION, "thresholds": T,
    }
    return decide_training(**(arguments | overrides))


def test_no_relevant_change_skips():
    outcome = decide()
    assert (outcome.decision, outcome.rule) == ("skip", "no_relevant_change")
    assert outcome.rules["volume"]["growth"] == pytest.approx(10_000 / 110_000, abs=1e-6)


def test_invalid_batch_vetoes_even_with_triggers():
    outcome = decide(
        quality={"blocking": True, "blocking_reason": "40% de filas rechazadas > 5%"},
        has_production=False,
        drift=drift(price=3.0),
    )
    assert (outcome.decision, outcome.rule) == ("skip", "invalid_batch")
    assert "40% de filas rechazadas" in outcome.reason


def test_insufficient_data_vetoes_baseline():
    outcome = decide(has_production=False, preprocessing=prep(train=500, validation=100, test=100))
    assert (outcome.decision, outcome.rule) == ("skip", "insufficient_data")


def test_first_model_is_a_baseline():
    outcome = decide(has_production=False, production_trained_rows=0,
                     degradation={"available": False})
    assert (outcome.decision, outcome.rule) == ("train", "baseline")


def test_small_batch_skips_even_with_drift_and_new_categories():
    outcome = decide(preprocessing=prep(eligible=4054), drift=drift(price=2.0),
                     categories=categories(share=1.0))
    assert (outcome.decision, outcome.rule) == ("skip", "small_batch")
    # The triggers are still evaluated and recorded for the audit trail.
    assert outcome.rules["drift"]["triggered"] and outcome.rules["new_categories"]["triggered"]


@pytest.mark.parametrize(
    ("overrides", "trigger", "text"),
    [
        ({"preprocessing": prep(accumulated=140_000)}, "volume", "crecimiento de datos"),
        ({"drift": drift(price=0.05, house_size=0.31)}, "drift", "house_size (PSI 0.31)"),
        ({"categories": categories(share=0.05)}, "new_categories", "state (5.0% del lote)"),
        ({"degradation": {**NO_DEGRADATION, "degradation": 0.25}}, "degradation", "+25%"),
    ],
)
def test_each_trigger_alone_trains(overrides, trigger, text):
    outcome = decide(**overrides)
    assert (outcome.decision, outcome.rule) == ("train", "triggers")
    assert outcome.rules[trigger]["triggered"]
    assert text in outcome.reason


def test_thresholds_are_inclusive_and_below_them_nothing_triggers():
    assert decide(drift=drift(price=0.20)).decision == "train"
    assert decide(drift=drift(price=0.1999)).decision == "skip"
    assert decide(categories=categories(share=0.0099)).decision == "skip"
    assert decide(degradation={**NO_DEGRADATION, "degradation": 0.0999}).decision == "skip"


def test_new_categories_never_trigger_on_the_baseline_batch():
    outcome = decide(categories=categories(share=1.0, baseline=True))
    assert not outcome.rules["new_categories"]["triggered"]


def metrics(mae, rmse, **others):
    return {"validation_mae": mae, "validation_rmse": rmse, "validation_r2": others.get("r2", 0.8)}


def promote(candidate, production, **thresholds):
    return decide_promotion(
        candidate=candidate, production=production, candidate_version="6",
        production_version="5", thresholds=Thresholds(**{**T.__dict__, **thresholds}),
    )


def test_promotes_when_mae_improves_enough_and_rmse_holds():
    outcome = promote(metrics(94.0, 100.5), metrics(100.0, 100.0))
    assert outcome.promote
    assert outcome.deltas["mae"] == pytest.approx(0.06)
    assert outcome.deltas["rmse"] == pytest.approx(-0.005)
    assert outcome.reason.startswith("Se promueve")


def test_rejects_when_mae_gain_is_below_the_minimum():
    outcome = promote(metrics(98.0, 95.0), metrics(100.0, 100.0))
    assert not outcome.promote
    assert "< 3% requerido" in outcome.reason


def test_rejects_when_rmse_worsens_beyond_tolerance():
    outcome = promote(metrics(90.0, 101.5), metrics(100.0, 100.0))
    assert not outcome.promote
    assert "fuera de la tolerancia" in outcome.reason


def test_promotion_thresholds_are_configurable():
    assert promote(metrics(98.0, 95.0), metrics(100.0, 100.0), promotion_min_improvement=0.01).promote


def test_promotes_a_baseline_and_a_resumed_champion():
    baseline = decide_promotion(candidate=metrics(1, 1), production=None, candidate_version="1",
                                production_version=None, thresholds=T)
    resumed = decide_promotion(candidate=metrics(1, 1), production=metrics(1, 1),
                               candidate_version="4", production_version="4", thresholds=T)
    assert baseline.promote and "línea base" in baseline.reason
    assert resumed.promote and resumed.deltas == {}
