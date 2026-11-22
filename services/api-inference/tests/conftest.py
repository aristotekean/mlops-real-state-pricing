"""Shared fakes: an in-memory model registry, a fake sklearn regressor, and an
in-memory inference log repository. No MLflow server, no Postgres."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import httpx
import numpy as np
import pytest
from real_estate_model.contract import INPUT_COLUMNS, NUMERIC_INPUTS

from api_inference.main import create_app
from api_inference.model import ModelLoadError, ModelService, RegisteredVersion
from api_inference.repository import FeedbackRecord, InferenceLogRecord, LoggedRequest
from api_inference.settings import Settings


class FakeModel:
    """Minimal stand-in for the served sklearn regression pipeline."""

    def __init__(self, price: float = 350_000.0):
        self.price = price
        self.frames: list[Any] = []

    def predict(self, frame):
        self.frames.append(frame)
        return np.array([self.price] * len(frame), dtype="float64")


class FakeRegistry:
    """Fake MLflow registry: resolves an alias to a version and loads a model."""

    def __init__(self, version: str | None = "3", model: Any | None = None):
        self.version = version
        self.model = model or FakeModel()
        self.resolve_calls = 0
        self.load_calls = 0
        self.fail_resolve = version is None

    def resolve(self, name: str, alias: str) -> RegisteredVersion:
        self.resolve_calls += 1
        if self.fail_resolve or self.version is None:
            raise ModelLoadError(f"no version for {name}@{alias}")
        return RegisteredVersion(
            name=name, alias=alias, version=self.version, run_id=f"run-{self.version}"
        )

    def load(self, model_uri: str) -> Any:
        self.load_calls += 1
        return self.model


@dataclass
class InMemoryRepository:
    rows: list[InferenceLogRecord] = field(default_factory=list)
    feedback: dict[UUID, FeedbackRecord] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)
    fail_ping: bool = False
    fail_log: bool = False
    fail_feedback: bool = False
    fail_history: bool = False
    opened: bool = False

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.opened = False

    async def ping(self) -> None:
        if self.fail_ping:
            raise RuntimeError("database down")

    async def log(self, record: InferenceLogRecord) -> None:
        if self.fail_log:
            raise RuntimeError("insert failed")
        self.rows.append(record)

    async def get_log(self, request_id: UUID) -> LoggedRequest | None:
        for row in self.rows:
            if row.request_id == request_id:
                return LoggedRequest(
                    request_id=row.request_id, status=row.status, prediction=row.prediction
                )
        return None

    async def upsert_feedback(self, record: FeedbackRecord) -> bool:
        if self.fail_feedback:
            raise RuntimeError("upsert failed")
        created = record.request_id not in self.feedback
        self.feedback[record.request_id] = record
        return created

    async def list_history(self, limit: int = 100) -> list[dict[str, Any]]:
        if self.fail_history:
            raise RuntimeError("history query failed")
        return self.history[:limit]


def make_settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "mlflow_tracking_uri": "http://mlflow.test",
        "model_name": "real_estate",
        "model_alias": "production",
        "model_refresh_seconds": 0,  # background refresh disabled in tests
        "model_load_retries": 2,
        "model_load_backoff_seconds": 0.0,
        "train_database_url": "postgresql://app:app@localhost:5432/real_estate",
        # Spool disabled by default so a failing fake repository behaves as
        # before (row counted as lost); spool tests pass a tmp path explicitly.
        "inference_log_spool_path": "",
        "inference_log_replay_seconds": 0,
    }
    base.update(overrides)
    return Settings(**base)


def make_service(registry: FakeRegistry, settings: Settings) -> ModelService:
    return ModelService(
        name=settings.model_name,
        alias=settings.model_alias,
        refresh_seconds=settings.model_refresh_seconds,
        resolve_version=registry.resolve,
        load_model=registry.load,
        resolve_timeout_seconds=settings.model_resolve_timeout_seconds,
        load_timeout_seconds=settings.model_load_timeout_seconds,
        predict_max_workers=settings.predict_max_workers,
        predict_max_pending=settings.predict_max_pending,
        predict_timeout_seconds=settings.predict_timeout_seconds,
    )


def make_app(
    registry: FakeRegistry | None = None,
    repository: InMemoryRepository | None = None,
    tracer_provider: Any | None = None,
    **settings_overrides: Any,
):
    settings = make_settings(**settings_overrides)
    registry = registry or FakeRegistry()
    repository = repository or InMemoryRepository()
    service = make_service(registry, settings)
    app = create_app(
        settings=settings,
        model_service=service,
        repository=repository,
        tracer_provider=tracer_provider,
    )
    return app, registry, repository, service


def valid_features() -> dict[str, Any]:
    """A request that satisfies the real estate input contract (strict types)."""
    features: dict[str, Any] = {
        "brokered_by": "101",
        "status": "for_sale",
        "bed": 3,
        "bath": 2,
        "acre_lot": 0.25,
        "city": "Austin",
        "state": "Texas",
        "zip_code": "78701",
        "house_size": 1800.0,
        "prev_sold_date": "2020-05-01",
    }
    # Keep it in sync with the contract: every input column, nothing else.
    assert set(features) == set(INPUT_COLUMNS), set(features) ^ set(INPUT_COLUMNS)
    return features


# A couple of handy references for the tests.
A_NUMERIC_INPUT = NUMERIC_INPUTS[0]  # e.g. "bed"


@pytest.fixture
async def running_client():
    """Factory fixture: builds an app, runs its lifespan, yields an HTTP client."""
    stack: list[Any] = []

    async def _start(app) -> httpx.AsyncClient:
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )
        stack.append((lifespan, client))
        return client

    yield _start

    for lifespan, client in reversed(stack):
        await client.aclose()
        await lifespan.__aexit__(None, None, None)
