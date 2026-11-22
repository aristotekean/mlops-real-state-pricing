"""FastAPI application factory for the real estate inference service."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry.sdk.trace import TracerProvider

from api_inference.metrics import METRICS_PATH, MetricsMiddleware, metrics_response
from api_inference.model import ModelLoadError, ModelService
from api_inference.predict import build_router, build_validation_handler
from api_inference.repository import InferenceLogRepository, PostgresInferenceLogRepository
from api_inference.schemas import (
    BatchHistoryEntry,
    ErrorDetail,
    HealthResponse,
    HistoryResponse,
    LiveResponse,
    ModelInfoResponse,
    ReadyResponse,
    ReloadResponse,
)
from api_inference.settings import Settings, get_settings
from api_inference.spool import DurableInferenceLog, InferenceLogSpool
from api_inference.tracing import build_tracer_provider, get_tracer, instrument_app

logger = logging.getLogger("api_inference")

API_VERSION = "0.1.0"

MLFLOW_HTTP_DEFAULTS = {
    "MLFLOW_HTTP_REQUEST_TIMEOUT": "15",
    "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "2",
}
"""Applied only when the variable is unset. MLflow's own defaults (120 s, 7
retries with backoff) keep a worker thread busy for minutes after the model
service already gave up on the call."""


def _configure_logging(level: str) -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level.upper(),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    root.setLevel(level.upper())


async def _initial_load(service: ModelService, settings: Settings) -> None:
    """Bounded startup retries; on exhaustion the API starts ``unavailable``.

    ``service.startup_settled`` flips when the attempts are over, whatever the
    outcome: ``/ready`` uses it to keep a pod out of rotation while it is
    still trying to get its first model.
    """
    try:
        for attempt in range(1, settings.model_load_retries + 1):
            try:
                await service.load()
                return
            except ModelLoadError as exc:
                logger.warning(
                    "initial model load attempt %d/%d failed: %s",
                    attempt, settings.model_load_retries, exc,
                )
                if attempt < settings.model_load_retries:
                    await asyncio.sleep(settings.model_load_backoff_seconds * attempt)
        logger.error(
            "model %s@%s unavailable after %d attempts; serving 503 until the "
            "background refresh succeeds",
            settings.model_name, settings.model_alias, settings.model_load_retries,
        )
    finally:
        service.startup_settled = True


async def _model_lifecycle(
    startup: asyncio.Task[None], service: ModelService, settings: Settings
) -> None:
    """Alias polling that starts once the ``startup`` load is over. Both run
    in the background so a slow registry never keeps the server from
    listening; the lifespan waits on ``startup`` alone, never on this loop."""
    try:
        await startup
    except Exception:
        logger.exception("initial model load crashed")
    if settings.model_refresh_seconds > 0:
        await _refresh_loop(service, settings.model_refresh_seconds)


async def _refresh_loop(service: ModelService, interval: float) -> None:
    """Poll the registry alias forever; ``refresh_if_changed`` never raises."""
    while True:
        await asyncio.sleep(interval)
        try:
            if service.loaded is None:
                await service.load()
            else:
                await service.refresh_if_changed()
        except ModelLoadError:
            pass  # already recorded in ``service.last_error``
        except Exception:  # keep the loop alive on unexpected errors
            logger.exception("model refresh loop error")


async def _replay_loop(inference_log: DurableInferenceLog, interval: float) -> None:
    """Replay spooled ``inference.logs`` rows at startup and then periodically."""
    while True:
        try:
            await inference_log.replay()
        except Exception:  # keep the loop alive on unexpected errors
            logger.exception("inference log replay loop error")
        await asyncio.sleep(interval)


def create_app(
    settings: Settings | None = None,
    model_service: ModelService | None = None,
    repository: InferenceLogRepository | None = None,
    tracer_provider: TracerProvider | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    _configure_logging(settings.log_level)
    if settings.mlflow_tracking_uri:
        os.environ.setdefault("MLFLOW_TRACKING_URI", settings.mlflow_tracking_uri)
    for variable, value in MLFLOW_HTTP_DEFAULTS.items():
        os.environ.setdefault(variable, value)

    service = model_service or ModelService(
        name=settings.model_name,
        alias=settings.model_alias,
        refresh_seconds=settings.model_refresh_seconds,
        resolve_timeout_seconds=settings.model_resolve_timeout_seconds,
        load_timeout_seconds=settings.model_load_timeout_seconds,
        predict_max_workers=settings.predict_max_workers,
        predict_max_pending=settings.predict_max_pending,
        predict_timeout_seconds=settings.predict_timeout_seconds,
    )
    repo = repository or PostgresInferenceLogRepository(
        lambda: settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        connect_timeout_seconds=settings.db_connect_timeout_seconds,
    )
    spool = (
        InferenceLogSpool(
            Path(settings.inference_log_spool_path),
            max_bytes=settings.inference_log_spool_max_bytes,
        )
        if settings.inference_log_spool_path
        else None
    )
    inference_log = DurableInferenceLog(
        repo, spool, retry_after_seconds=settings.inference_log_circuit_seconds
    )
    owned_provider = (
        None
        if tracer_provider is not None
        else build_tracer_provider(
            settings.otel_exporter_otlp_endpoint, settings.otel_traces_sample_ratio
        )
    )
    provider = tracer_provider or owned_provider
    tracer = get_tracer(provider)

    async def _cancel(task: asyncio.Task[None] | None) -> None:
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await repo.open()
        startup_task: asyncio.Task[None] | None = None
        model_task: asyncio.Task[None] | None = None
        replay_task: asyncio.Task[None] | None = None
        try:
            if spool is not None and settings.inference_log_replay_seconds > 0:
                replay_task = asyncio.create_task(
                    _replay_loop(inference_log, settings.inference_log_replay_seconds),
                    name="inference-log-replay",
                )
            startup_task = asyncio.create_task(
                _initial_load(service, settings), name="model-startup"
            )
            model_task = asyncio.create_task(
                _model_lifecycle(startup_task, service, settings), name="model-lifecycle"
            )
            await asyncio.wait({startup_task}, timeout=settings.model_startup_wait_seconds)
            if not service.startup_settled:
                logger.warning(
                    "model not loaded after %g s; accepting connections while "
                    "the load continues in the background",
                    settings.model_startup_wait_seconds,
                )
            yield
        finally:
            await _cancel(model_task)
            await _cancel(startup_task)
            await _cancel(replay_task)
            service.close()
            await repo.close()
            if owned_provider is not None:
                await asyncio.to_thread(owned_provider.shutdown)

    app = FastAPI(title="Real Estate Inference API", version=API_VERSION, lifespan=lifespan)
    app.state.settings = settings
    app.state.model_service = service
    app.state.repository = repo
    app.state.inference_log = inference_log
    app.include_router(build_router(service, repo, inference_log, tracer))
    app.add_exception_handler(
        RequestValidationError, build_validation_handler(inference_log, tracer)
    )
    app.add_middleware(MetricsMiddleware)
    if provider is not None:
        instrument_app(app, provider)

    @app.get("/live", response_model=LiveResponse, tags=["ops"])
    async def live() -> LiveResponse:
        """Liveness probe: the process answers. It checks no dependency, so a
        database or registry outage never gets the container restarted."""
        return LiveResponse(status="ok")

    @app.get(
        "/ready",
        response_model=ReadyResponse,
        responses={503: {"model": ReadyResponse}},
        tags=["ops"],
    )
    async def ready() -> JSONResponse:
        """Readiness probe: 503 while the startup load is still running, and
        afterwards too when ``READINESS_REQUIRES_MODEL`` is set and no model
        is loaded. The database is not part of it: predictions are served and
        spooled without it."""
        model_loaded = service.loaded is not None
        is_ready = model_loaded or (
            service.startup_settled and not settings.readiness_requires_model
        )
        body = ReadyResponse(
            status="ready" if is_ready else "not_ready", model_loaded=model_loaded
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE,
            content=body.model_dump(),
        )

    @app.get("/health", response_model=HealthResponse, tags=["ops"])
    async def health() -> HealthResponse:
        """Human-facing status: always 200, reports the model and a database
        ping bounded by ``HEALTH_DB_TIMEOUT_SECONDS``. Probes use ``/live``
        and ``/ready`` instead."""
        try:
            await asyncio.wait_for(repo.ping(), settings.health_db_timeout_seconds)
            database = "ok"
        except Exception as exc:
            logger.warning("health: database ping failed: %s", exc)
            database = "error"
        return HealthResponse(
            status="ok", model_loaded=service.loaded is not None, database=database
        )

    @app.get("/model-info", response_model=ModelInfoResponse, tags=["model"])
    async def model_info() -> ModelInfoResponse:
        info = asdict(service.info())
        return ModelInfoResponse(**info, tracking_uri=settings.mlflow_tracking_uri)

    @app.post("/model/reload", response_model=ReloadResponse, tags=["model"])
    async def reload_model() -> ReloadResponse:
        """Force a reload of whatever version the alias points to right now."""
        previous = service.loaded.version if service.loaded else None
        try:
            loaded = await service.load()
        except ModelLoadError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorDetail(error_code="model_load_error", message=str(exc)).model_dump(),
            ) from exc
        return ReloadResponse(
            previous_version=previous,
            current_version=loaded.version,
            changed=previous != loaded.version,
        )

    @app.get("/history", response_model=HistoryResponse, tags=["history"])
    async def history(limit: int = 100) -> HistoryResponse:
        """Training/promotion log per batch (RF9), read from the audit schema.

        Streamlit consumes this instead of querying the database directly, so
        the UI needs no database credentials. A database outage returns 503;
        the model keeps serving predictions regardless.
        """
        bounded = max(1, min(limit, 500))
        try:
            rows = await repo.list_history(bounded)
        except Exception as exc:
            logger.warning("history: query failed: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorDetail(
                    error_code="database_unavailable",
                    message="the training history could not be read; retry later",
                ).model_dump(),
            ) from exc
        return HistoryResponse(entries=[BatchHistoryEntry(**row) for row in rows])

    @app.get(METRICS_PATH, include_in_schema=False)
    async def metrics() -> Response:
        return metrics_response()

    return app


app = create_app()
