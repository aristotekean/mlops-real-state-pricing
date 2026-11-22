"""GET /history: the per-batch training/promotion log consumed by Streamlit (RF9)."""

from __future__ import annotations

from datetime import UTC, datetime

from conftest import InMemoryRepository, make_app


def _entry(**overrides):
    base = {
        "api_batch_number": 1,
        "group_number": 1,
        "received_at": datetime(2026, 11, 1, 12, 0, tzinfo=UTC),
        "record_count": 1000,
        "validated_rows": 950,
        "eligible_rows": 900,
        "training_decision": "train",
        "training_reason": "baseline inicial",
        "candidate_model": "hist_gradient_boosting",
        "candidate_version": "1",
        "promotion_decision": "promoted",
        "promotion_reason": "no existía modelo productivo previo",
        "production_version_before": None,
        "production_version_after": "1",
        "metric_deltas": {"validation_rmsle": -0.12},
        "mlflow_experiment_id": "7",
        "mlflow_parent_run_id": "run-parent",
        "mlflow_candidate_run_id": "run-cand",
        "final_status": "completed",
        "error_message": None,
        "finished_at": datetime(2026, 11, 1, 12, 5, tzinfo=UTC),
    }
    base.update(overrides)
    return base


async def test_history_returns_batch_entries(running_client):
    repository = InMemoryRepository(history=[_entry(), _entry(api_batch_number=2, training_decision="skip")])
    app, _, _, _ = make_app(repository=repository)
    client = await running_client(app)

    response = await client.get("/history")

    assert response.status_code == 200, response.text
    entries = response.json()["entries"]
    assert len(entries) == 2
    assert entries[0]["api_batch_number"] == 1
    assert entries[0]["training_decision"] == "train"
    assert entries[0]["metric_deltas"] == {"validation_rmsle": -0.12}
    assert entries[1]["training_decision"] == "skip"


async def test_history_tolerates_extra_columns(running_client):
    """A new audit column must not break the endpoint (extra='allow')."""
    repository = InMemoryRepository(history=[_entry(brand_new_column="surprise")])
    app, _, _, _ = make_app(repository=repository)
    client = await running_client(app)

    body = (await client.get("/history")).json()
    assert body["entries"][0]["brand_new_column"] == "surprise"


async def test_history_returns_503_when_database_unavailable(running_client):
    app, _, _, _ = make_app(repository=InMemoryRepository(fail_history=True))
    client = await running_client(app)

    response = await client.get("/history")

    assert response.status_code == 503
    assert response.json()["detail"]["error_code"] == "database_unavailable"


async def test_history_empty_when_no_batches(running_client):
    app, _, _, _ = make_app()
    client = await running_client(app)

    assert (await client.get("/history")).json() == {"entries": []}
