"""Model loading strategy: load at startup, poll the registry alias in the
background, swap atomically, and run inference off the event loop."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import joblib
from real_estate_model.contract import INPUT_COLUMNS, input_frame

from api_inference.metrics import MODEL_RELOADS, set_model_info

logger = logging.getLogger(__name__)


class ModelError(Exception):
    """Base class for model-service failures."""


class ModelNotLoadedError(ModelError):
    """No model version is loaded yet (startup failed or registry empty)."""


class ModelLoadError(ModelError):
    """Resolving the alias or loading the artifact failed."""


class ModelBusyError(ModelError):
    """Every prediction slot (running plus queued) is taken; retry shortly."""


class InferenceError(ModelError):
    """The model raised while scoring; ``model`` is the version that failed."""

    def __init__(self, model: LoadedModel, message: str) -> None:
        super().__init__(message)
        self.model = model


class InferenceTimeoutError(InferenceError):
    """The prediction did not finish within the configured time."""


@dataclass(frozen=True)
class RegisteredVersion:
    """The concrete version an alias currently points to."""

    name: str
    alias: str
    version: str
    run_id: str | None


@dataclass(frozen=True)
class LoadedModel:
    model: Any
    name: str
    version: str
    alias: str
    run_id: str | None
    loaded_at: datetime

    @property
    def uri(self) -> str:
        return f"models:/{self.name}/{self.version}"


@dataclass(frozen=True)
class PredictionResult:
    # The estimated price (the regression target). A plain float so the API
    # returns a JSON number and ``inference.logs.prediction`` stores it as one.
    prediction: float
    model: LoadedModel


@dataclass(frozen=True)
class ModelInfo:
    name: str
    alias: str
    status: str  # "ready" | "unavailable"
    version: str | None
    run_id: str | None
    loaded_at: datetime | None
    last_check_at: datetime | None
    last_error: str | None
    refresh_seconds: float


ResolveVersion = Callable[[str, str], RegisteredVersion]
LoadModel = Callable[[str], Any]

_SMOKE_VALUES: dict[str, Any] = {
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


def smoke_features() -> dict[str, Any]:
    """One plausible listing covering every contract input (``None`` for
    columns added to the contract later), used to prove a model can score."""
    return {column: _SMOKE_VALUES.get(column) for column in INPUT_COLUMNS}


def mlflow_resolve_version(name: str, alias: str) -> RegisteredVersion:
    """Blocking: ask the MLflow registry which version the alias points to."""
    from mlflow import MlflowClient

    try:
        details = MlflowClient().get_model_version_by_alias(name, alias)
    except Exception as exc:  # MlflowException and transport errors alike
        raise ModelLoadError(f"cannot resolve models:/{name}@{alias}: {exc}") from exc
    return RegisteredVersion(
        name=name, alias=alias, version=str(details.version), run_id=details.run_id
    )


def mlflow_load_model(model_uri: str) -> Any:
    """Blocking: download and deserialise the sklearn flavor."""
    import mlflow.sklearn

    try:
        return mlflow.sklearn.load_model(model_uri)
    except Exception as exc:
        raise ModelLoadError(f"cannot load {model_uri}: {exc}") from exc


class ModelService:
    """Holds the currently served model and knows how to replace it.

    The MLflow client and scikit-learn are blocking, so every call into them
    runs in a worker thread. ``_lock`` serialises load/refresh so two
    concurrent reloads cannot interleave; readers never take the lock because
    swapping ``self.loaded`` is a single reference assignment.

    Registry calls are bounded by ``resolve_timeout_seconds`` and
    ``load_timeout_seconds`` so a hung MLflow call cannot hold ``_lock``
    forever. A freshly loaded model must score ``smoke_features`` before it
    replaces the current one.

    Predictions run on a dedicated pool of ``predict_max_workers`` threads. At
    most ``predict_max_workers + predict_max_pending`` predictions are admitted
    at once; beyond that ``predict`` raises ``ModelBusyError`` instead of
    queueing without bound. A prediction slower than
    ``predict_timeout_seconds`` raises ``InferenceTimeoutError``; its thread
    cannot be interrupted, so it keeps its slot until it finishes.
    """

    def __init__(
        self,
        *,
        name: str,
        alias: str,
        refresh_seconds: float,
        resolve_version: ResolveVersion | None = None,
        load_model: LoadModel | None = None,
        resolve_timeout_seconds: float = 15.0,
        load_timeout_seconds: float = 120.0,
        predict_max_workers: int = 4,
        predict_max_pending: int = 64,
        predict_timeout_seconds: float = 10.0,
    ) -> None:
        self.name = name
        self.alias = alias
        self.refresh_seconds = refresh_seconds
        self.resolve_timeout_seconds = resolve_timeout_seconds
        self.load_timeout_seconds = load_timeout_seconds
        self.predict_timeout_seconds = predict_timeout_seconds
        self.max_in_flight = predict_max_workers + predict_max_pending
        self.in_flight = 0
        self.startup_settled = False
        self._resolve_version = resolve_version or mlflow_resolve_version
        self._load_model = load_model or mlflow_load_model
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=predict_max_workers, thread_name_prefix="predict"
        )
        self.loaded: LoadedModel | None = None
        self.last_check_at: datetime | None = None
        self.last_error: str | None = None

    def close(self) -> None:
        """Stop accepting predictions; running ones are left to finish."""
        self._executor.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ loading
    async def load(self) -> LoadedModel:
        """Resolve the alias and (re)load that version unconditionally."""
        async with self._lock:
            target = await self._resolve()
            return await self._load_version(target)

    async def refresh_if_changed(self) -> bool:
        """Reload only when the alias moved to another version.

        Returns ``True`` when a new version was swapped in. Failures are logged
        and recorded in ``last_error``; the current model keeps serving.
        """
        async with self._lock:
            try:
                target = await self._resolve()
            except ModelLoadError as exc:
                logger.warning("model refresh: %s", exc)
                return False
            if self.loaded is not None and self.loaded.version == target.version:
                return False
            try:
                await self._load_version(target)
            except ModelLoadError as exc:
                logger.warning("model refresh: %s", exc)
                return False
            return True

    async def _resolve(self) -> RegisteredVersion:
        started = time.perf_counter()
        try:
            target = await asyncio.wait_for(
                asyncio.to_thread(self._resolve_version, self.name, self.alias),
                self.resolve_timeout_seconds,
            )
        except TimeoutError as exc:
            message = (
                f"resolving models:/{self.name}@{self.alias} timed out after "
                f"{self.resolve_timeout_seconds:g} s"
            )
            self.last_error = message
            MODEL_RELOADS.labels(result="failure").inc()
            raise ModelLoadError(message) from exc
        except ModelLoadError as exc:
            self.last_error = str(exc)
            MODEL_RELOADS.labels(result="failure").inc()
            raise
        except Exception as exc:
            self.last_error = str(exc)
            MODEL_RELOADS.labels(result="failure").inc()
            raise ModelLoadError(str(exc)) from exc
        finally:
            self.last_check_at = datetime.now(UTC)
        logger.debug(
            "resolved %s@%s -> v%s in %.0f ms",
            self.name, self.alias, target.version, (time.perf_counter() - started) * 1000,
        )
        return target

    async def _load_version(self, target: RegisteredVersion) -> LoadedModel:
        uri = f"models:/{target.name}/{target.version}"
        started = time.perf_counter()
        try:
            model = await asyncio.wait_for(
                asyncio.to_thread(self._load_and_check, uri), self.load_timeout_seconds
            )
        except TimeoutError as exc:
            message = f"loading {uri} timed out after {self.load_timeout_seconds:g} s"
            self.last_error = message
            MODEL_RELOADS.labels(result="failure").inc()
            raise ModelLoadError(message) from exc
        except ModelLoadError as exc:
            self.last_error = str(exc)
            MODEL_RELOADS.labels(result="failure").inc()
            raise
        except Exception as exc:
            self.last_error = str(exc)
            MODEL_RELOADS.labels(result="failure").inc()
            raise ModelLoadError(f"cannot load {uri}: {exc}") from exc
        loaded = LoadedModel(
            model=model,
            name=target.name,
            version=target.version,
            alias=target.alias,
            run_id=target.run_id,
            loaded_at=datetime.now(UTC),
        )
        previous = self.loaded
        self.loaded = loaded  # atomic reference swap
        self.last_error = None
        set_model_info(loaded.name, loaded.version, loaded.alias)
        MODEL_RELOADS.labels(result="success").inc()
        logger.info(
            "model loaded: %s (previous=%s) in %.0f ms",
            uri, previous.version if previous else None, (time.perf_counter() - started) * 1000,
        )
        return loaded

    def _load_and_check(self, uri: str) -> Any:
        """Blocking: load the artifact and require one finite smoke prediction."""
        model = self._load_model(uri)
        try:
            self._score(model, smoke_features())
        except Exception as exc:
            raise ModelLoadError(
                f"smoke prediction failed for {uri}: {type(exc).__name__}: {exc}"
            ) from exc
        return model

    # ---------------------------------------------------------------- inference
    async def predict(self, features: Mapping[str, Any]) -> PredictionResult:
        """Run one prediction in a worker thread against the current model."""
        loaded = self.loaded
        if loaded is None:
            raise ModelNotLoadedError("no model version is loaded")
        if self.in_flight >= self.max_in_flight:
            raise ModelBusyError(f"{self.in_flight} predictions already in flight")
        future = asyncio.get_running_loop().run_in_executor(
            self._executor, self._predict_sync, loaded, features
        )
        self.in_flight += 1
        future.add_done_callback(self._release_slot)
        try:
            return await asyncio.wait_for(asyncio.shield(future), self.predict_timeout_seconds)
        except TimeoutError as exc:
            raise InferenceTimeoutError(
                loaded, f"prediction exceeded {self.predict_timeout_seconds:g} s"
            ) from exc
        except Exception as exc:
            raise InferenceError(loaded, f"{type(exc).__name__}: {exc}") from exc

    def _release_slot(self, future: asyncio.Future[PredictionResult]) -> None:
        """Free the slot when the worker thread ends, which can be after the
        caller timed out; reading the exception keeps asyncio from logging it
        as never retrieved."""
        self.in_flight -= 1
        if not future.cancelled():
            future.exception()

    @staticmethod
    def _score(model: Any, features: Mapping[str, Any]) -> float:
        """One prediction with estimator-level parallelism turned off: a
        persisted ``n_jobs=-1`` would spend more on dispatching threads for a
        single row than on scoring it."""
        frame = input_frame([features])
        with joblib.parallel_config(backend="sequential"):
            prediction = float(model.predict(frame)[0])
        if not math.isfinite(prediction):
            raise ValueError(f"model returned a non-finite price: {prediction!r}")
        return prediction

    @classmethod
    def _predict_sync(cls, loaded: LoadedModel, features: Mapping[str, Any]) -> PredictionResult:
        return PredictionResult(prediction=cls._score(loaded.model, features), model=loaded)

    # --------------------------------------------------------------- reporting
    def info(self) -> ModelInfo:
        loaded = self.loaded
        return ModelInfo(
            name=self.name,
            alias=self.alias,
            status="ready" if loaded is not None else "unavailable",
            version=loaded.version if loaded else None,
            run_id=loaded.run_id if loaded else None,
            loaded_at=loaded.loaded_at if loaded else None,
            last_check_at=self.last_check_at,
            last_error=self.last_error,
            refresh_seconds=self.refresh_seconds,
        )
