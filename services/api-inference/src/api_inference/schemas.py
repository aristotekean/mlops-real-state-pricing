"""Pydantic models for the HTTP contract (see docs/data-contract.md)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from real_estate_model.contract import INPUT_COLUMNS, NUMERIC_INPUTS
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    create_model,
)


class HealthResponse(BaseModel):
    status: Literal["ok"]
    model_loaded: bool
    database: Literal["ok", "error"]


class ModelInfoResponse(BaseModel):
    name: str
    alias: str
    status: Literal["ready", "unavailable"]
    version: str | None
    run_id: str | None
    loaded_at: datetime | None
    last_check_at: datetime | None
    last_error: str | None
    refresh_seconds: float
    tracking_uri: str | None


class ReloadResponse(BaseModel):
    previous_version: str | None
    current_version: str
    changed: bool


class ErrorDetail(BaseModel):
    error_code: str
    message: str


# --------------------------------------------------------------------------- #
# /predict
# --------------------------------------------------------------------------- #
def _python_name(column: str) -> str:
    """Contract keys are used verbatim as field aliases; the Python attribute
    name only needs to be a valid identifier (no hyphens)."""
    return column.replace("-", "_")


def _feature_fields() -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for column in INPUT_COLUMNS:
        if column in NUMERIC_INPUTS:
            # Strict types keep "6" out of numeric fields and 1 out of codes.
            annotation: Any = StrictFloat | StrictInt | None
            description = "Numeric input; JSON number or null."
        else:
            annotation = StrictStr | None
            description = "Categorical input; JSON string (codes included) or null."
        fields[_python_name(column)] = (
            annotation,
            Field(..., alias=column, description=description),
        )
    return fields


# Every model input of the real estate contract, built from the shared contract
# so the API and the training pipeline cannot drift. The target (``price``),
# ``street`` and any other source column are unknown keys here and therefore
# rejected (``extra="forbid"``).
PredictFeatures = create_model(
    "PredictFeatures",
    __config__=ConfigDict(extra="forbid", populate_by_name=False),
    **_feature_fields(),
)


class PredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: UUID | None = Field(
        default=None, description="Idempotency/audit key; generated when omitted."
    )
    features: PredictFeatures  # type: ignore[valid-type]

    def feature_values(self) -> dict[str, Any]:
        """Validated features keyed by the contract column names."""
        return self.features.model_dump(by_alias=True)


class ModelIdentity(BaseModel):
    name: str
    version: str
    alias: str


class PredictResponse(BaseModel):
    request_id: UUID
    # The estimated listing price (the regression target), a JSON number.
    prediction: float
    model: ModelIdentity
    processing_time_ms: float


# --------------------------------------------------------------------------- #
# /predict/{request_id}/feedback
# --------------------------------------------------------------------------- #
class FeedbackRequest(BaseModel):
    """Observed outcome for a request already stored in ``inference.logs``."""

    model_config = ConfigDict(extra="forbid")

    price: float = Field(
        gt=0, description="Observed sale/listing price; same domain as the training target."
    )
    source: str | None = Field(
        default=None, max_length=200, description="Free-text origin of the label (MLS, audit...)."
    )


class FeedbackResponse(BaseModel):
    request_id: UUID
    price: float
    prediction: float | None = Field(
        description="Price the model returned for this request; null when it failed to score."
    )
    created: bool = Field(description="False when a previous label was replaced.")


# --------------------------------------------------------------------------- #
# /history  (RF9 · training and promotion log shown in Streamlit)
# --------------------------------------------------------------------------- #
class BatchHistoryEntry(BaseModel):
    """One row of ``audit.batch_history_summary``: the outcome of one batch.

    ``extra="allow"`` keeps the API tolerant to columns the pipeline may add to
    the audit view without forcing a redeploy of the inference service.
    """

    model_config = ConfigDict(extra="allow")

    api_batch_number: int | None = None
    group_number: int | None = None
    received_at: datetime | None = None
    record_count: int | None = None
    validated_rows: int | None = None
    eligible_rows: int | None = None
    training_decision: str | None = None
    training_reason: str | None = None
    candidate_model: str | None = None
    candidate_version: str | None = None
    promotion_decision: str | None = None
    promotion_reason: str | None = None
    production_version_before: str | None = None
    production_version_after: str | None = None
    metric_deltas: dict[str, Any] | None = None
    mlflow_experiment_id: str | None = None
    mlflow_parent_run_id: str | None = None
    mlflow_candidate_run_id: str | None = None
    final_status: str | None = None
    error_message: str | None = None
    finished_at: datetime | None = None


class HistoryResponse(BaseModel):
    entries: list[BatchHistoryEntry]
