"""Probes, bounded model loading, backpressure and the inference-log circuit."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from uuid import uuid4

import joblib
import numpy as np
import pytest

from api_inference.metrics import INFERENCE_LOG_FAILURES
from api_inference.model import (
    InferenceError,
    InferenceTimeoutError,
    ModelBusyError,
    ModelLoadError,
    ModelService,
)
from api_inference.repository import InferenceLogRecord
from api_inference.spool import DurableInferenceLog, InferenceLogSpool
from conftest import (
    FakeModel,
    FakeRegistry,
    InMemoryRepository,
    make_app,
    make_settings,
    valid_features,
)


class GatedModel(FakeModel):
    """Regressor whose ``predict`` blocks on ``release`` once ``armed``."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.entered = threading.Event()
        self.release = threading.Event()

    def predict(self, frame):
        if self.armed:
            self.entered.set()
            self.release.wait(timeout=5)
        return super().predict(frame)


class BrokenModel(FakeModel):
    def predict(self, frame):
        raise RuntimeError("boom")


class CountingRepository(InMemoryRepository):
    log_calls: int = 0

    async def log(self, record: InferenceLogRecord) -> None:
        self.log_calls += 1
        await super().log(record)


def service_for(registry: FakeRegistry, **limits: Any) -> ModelService:
    return ModelService(
        name="real_estate",
        alias="production",
        refresh_seconds=0,
        resolve_version=registry.resolve,
        load_model=registry.load,
        **limits,
    )


def failed_record() -> InferenceLogRecord:
    return InferenceLogRecord(
        request_id=uuid4(), input_data={"bed": 3}, status="failed", error_code="x"
    )


async def wait_until(condition, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, "condition never became true"
        await asyncio.sleep(0.01)


async def test_live_ignores_database_and_model(running_client):
    app, _, _, _ = make_app(
        registry=FakeRegistry(version=None), repository=InMemoryRepository(fail_ping=True)
    )
    client = await running_client(app)

    response = await client.get("/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_with_model(running_client):
    app, _, _, _ = make_app()
    client = await running_client(app)

    response = await client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "model_loaded": True}


async def test_ready_without_model_once_startup_settled_keeps_the_pod_in_rotation(
    running_client,
):
    app, _, _, _ = make_app(registry=FakeRegistry(version=None))
    client = await running_client(app)

    response = await client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "model_loaded": False}


async def test_ready_without_model_is_503_when_model_is_required(running_client):
    app, _, _, _ = make_app(registry=FakeRegistry(version=None), readiness_requires_model=True)
    client = await running_client(app)

    response = await client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "model_loaded": False}


async def test_startup_does_not_wait_for_a_slow_registry(running_client):
    gate = threading.Event()
    registry = FakeRegistry()
    resolve = registry.resolve

    def slow_resolve(name: str, alias: str):
        gate.wait(timeout=5)
        return resolve(name, alias)

    registry.resolve = slow_resolve  # type: ignore[method-assign]
    app, _, _, service = make_app(registry=registry, model_startup_wait_seconds=0.05)
    try:
        client = await running_client(app)

        assert service.loaded is None
        assert (await client.get("/live")).status_code == 200
        not_ready = await client.get("/ready")
        assert not_ready.status_code == 503
        assert not_ready.json() == {"status": "not_ready", "model_loaded": False}
    finally:
        gate.set()

    await wait_until(lambda: service.loaded is not None)
    assert (await client.get("/ready")).status_code == 200


async def test_startup_returns_as_soon_as_the_model_loads_with_refresh_enabled(running_client):
    app, _, _, service = make_app(model_refresh_seconds=30, model_startup_wait_seconds=5)
    started = asyncio.get_running_loop().time()

    await running_client(app)

    assert asyncio.get_running_loop().time() - started < 1
    assert service.loaded is not None


async def test_health_bounds_a_hanging_database_ping(running_client):
    class HangingRepository(InMemoryRepository):
        async def ping(self) -> None:
            await asyncio.sleep(30)

    app, _, _, _ = make_app(repository=HangingRepository(), health_db_timeout_seconds=0.05)
    client = await running_client(app)

    response = await asyncio.wait_for(client.get("/health"), timeout=2)

    assert response.status_code == 200
    assert response.json()["database"] == "error"


async def test_load_times_out_and_releases_the_lock():
    gate = threading.Event()
    registry = FakeRegistry()
    load = registry.load

    def slow_load(uri: str):
        gate.wait(timeout=5)
        return load(uri)

    registry.load = slow_load  # type: ignore[method-assign]
    service = service_for(registry, load_timeout_seconds=0.05)
    try:
        with pytest.raises(ModelLoadError, match="timed out"):
            await service.load()
        assert service.loaded is None
        assert "timed out" in (service.last_error or "")
    finally:
        gate.set()

    loaded = await asyncio.wait_for(service.load(), timeout=2)
    assert loaded.version == "3"


async def test_resolve_times_out():
    gate = threading.Event()
    registry = FakeRegistry()

    def slow_resolve(name: str, alias: str):
        gate.wait(timeout=5)
        return registry.resolve(name, alias)

    service = ModelService(
        name="real_estate",
        alias="production",
        refresh_seconds=0,
        resolve_version=slow_resolve,
        load_model=registry.load,
        resolve_timeout_seconds=0.05,
    )
    try:
        with pytest.raises(ModelLoadError, match="timed out"):
            await service.load()
    finally:
        gate.set()


async def test_model_failing_the_smoke_prediction_is_not_swapped_in():
    registry = FakeRegistry(version="3")
    service = service_for(registry)
    await service.load()
    registry.version = "4"
    registry.model = BrokenModel()

    assert await service.refresh_if_changed() is False

    assert service.loaded is not None and service.loaded.version == "3"
    assert "smoke prediction failed" in (service.last_error or "")
    with pytest.raises(ModelLoadError, match="smoke prediction failed"):
        await service.load()


async def test_predict_rejects_when_the_queue_is_full_and_recovers():
    model = GatedModel()
    service = service_for(FakeRegistry(model=model), predict_max_workers=1, predict_max_pending=0)
    await service.load()
    model.armed = True
    first = asyncio.create_task(service.predict(valid_features()))
    try:
        await asyncio.to_thread(model.entered.wait, 2)

        with pytest.raises(ModelBusyError):
            await service.predict(valid_features())
    finally:
        model.release.set()

    assert (await first).prediction == 350_000.0
    assert (await service.predict(valid_features())).prediction == 350_000.0


async def test_predict_times_out_and_keeps_the_slot_until_the_thread_ends():
    model = GatedModel()
    service = service_for(
        FakeRegistry(model=model),
        predict_max_workers=1,
        predict_max_pending=0,
        predict_timeout_seconds=0.05,
    )
    await service.load()
    model.armed = True
    try:
        with pytest.raises(InferenceTimeoutError) as timed_out:
            await service.predict(valid_features())
        assert timed_out.value.model.version == "3"
        with pytest.raises(ModelBusyError):
            await service.predict(valid_features())
    finally:
        model.release.set()

    model.armed = False
    await wait_until(lambda: service.in_flight == 0)
    assert (await service.predict(valid_features())).prediction == 350_000.0


async def test_inference_error_names_the_model_that_failed():
    model = FakeModel()
    service = service_for(FakeRegistry(model=model))
    await service.load()
    model.predict = BrokenModel().predict  # type: ignore[method-assign]

    with pytest.raises(InferenceError) as failed:
        await service.predict(valid_features())

    assert failed.value.model.version == "3"
    assert isinstance(failed.value.__cause__, RuntimeError)


async def test_predict_ignores_a_persisted_n_jobs_and_runs_sequentially():
    class JobsModel(FakeModel):
        def __init__(self) -> None:
            super().__init__()
            self.effective_jobs: list[int] = []

        def predict(self, frame):
            self.effective_jobs.append(joblib.effective_n_jobs(-1))
            return np.array([1.0] * len(frame))

    model = JobsModel()
    service = service_for(FakeRegistry(model=model))
    await service.load()

    await service.predict(valid_features())

    assert model.effective_jobs[-1] == 1


async def test_overloaded_predict_returns_503_and_logs_the_request(running_client):
    model = GatedModel()
    app, _, repository, _ = make_app(
        registry=FakeRegistry(model=model), predict_max_workers=1, predict_max_pending=0
    )
    client = await running_client(app)
    model.armed = True
    first = asyncio.create_task(client.post("/predict", json={"features": valid_features()}))
    try:
        await asyncio.to_thread(model.entered.wait, 2)

        response = await client.post("/predict", json={"features": valid_features()})
    finally:
        model.release.set()

    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert response.json()["detail"]["error_code"] == "overloaded"
    assert (await first).status_code == 200
    overloaded = [row for row in repository.rows if row.error_code == "overloaded"]
    assert len(overloaded) == 1 and overloaded[0].status == "failed"
    assert overloaded[0].model_version == "3"


async def test_slow_predict_returns_504_and_logs_the_request(running_client):
    model = GatedModel()
    app, _, repository, _ = make_app(
        registry=FakeRegistry(model=model), predict_timeout_seconds=0.05
    )
    client = await running_client(app)
    model.armed = True
    try:
        response = await client.post("/predict", json={"features": valid_features()})
    finally:
        model.release.set()

    assert response.status_code == 504
    assert response.json()["detail"]["error_code"] == "inference_timeout"
    assert [row.error_code for row in repository.rows] == ["inference_timeout"]
    assert repository.rows[0].model_version == "3"


async def test_open_circuit_spools_without_calling_the_database_until_a_replay_succeeds(
    tmp_path,
):
    repository = CountingRepository(fail_log=True)
    spool = InferenceLogSpool(tmp_path / "spool.jsonl")
    log = DurableInferenceLog(repository, spool, retry_after_seconds=60)

    assert await log.write(failed_record()) is False
    assert repository.log_calls == 1

    repository.fail_log = False
    assert await log.write(failed_record()) is False
    assert repository.log_calls == 1
    assert spool.pending() == 2

    assert await log.replay() == 2
    assert await log.write(failed_record()) is True
    assert len(repository.rows) == 3


async def test_failed_replay_keeps_the_circuit_open(tmp_path):
    repository = CountingRepository(fail_log=True)
    spool = InferenceLogSpool(tmp_path / "spool.jsonl")
    log = DurableInferenceLog(repository, spool, retry_after_seconds=60)
    await log.write(failed_record())

    assert await log.replay() == 0
    calls_after_replay = repository.log_calls
    await log.write(failed_record())

    assert repository.log_calls == calls_after_replay
    assert spool.pending() == 2


async def test_appends_do_not_reread_the_spool(tmp_path, monkeypatch):
    spool = InferenceLogSpool(tmp_path / "spool.jsonl")
    log = DurableInferenceLog(InMemoryRepository(fail_log=True), spool)
    pending_calls = 0
    pending = spool.pending

    def counting_pending() -> int:
        nonlocal pending_calls
        pending_calls += 1
        return pending()

    monkeypatch.setattr(spool, "pending", counting_pending)

    for _ in range(5):
        await log.write(failed_record())

    assert pending_calls == 1
    assert pending() == 5


async def test_full_spool_drops_the_row_instead_of_growing(tmp_path):
    spool = InferenceLogSpool(tmp_path / "spool.jsonl", max_bytes=1)
    log = DurableInferenceLog(InMemoryRepository(fail_log=True), spool)
    await log.write(failed_record())
    size_when_full = spool.size()
    failures = INFERENCE_LOG_FAILURES._value.get()

    assert await log.write(failed_record()) is False

    assert spool.size() == size_when_full
    assert spool.pending() == 1
    assert INFERENCE_LOG_FAILURES._value.get() == failures + 1


def test_new_settings_have_safe_defaults():
    settings = make_settings()

    assert settings.readiness_requires_model is False
    assert settings.predict_max_workers >= 1
    assert settings.inference_log_spool_max_bytes > 0
