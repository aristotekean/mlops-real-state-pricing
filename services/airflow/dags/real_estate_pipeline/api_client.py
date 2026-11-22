"""Robust client for the external Data API (cristiandiaz13/mlops-puj:data-api-pf-v2).

Observed contract (GET /data?group_number=<n>[&day=<Tuesday|Wednesday>]):

* 200 ``{"group_number": n, "batch_number": k, "data": [ {12 columns}, ... ]}``
  where every call hands out the NEXT batch of the group (the API keeps the
  counter in memory: the endpoint is NOT idempotent).
* 400 ``{"detail": "Ya se recolectó toda la información minima necesaria"}``
  once every batch of the group was delivered (end of data).
* 422 for invalid parameters.

Transient failures (connection errors, timeouts, 5xx) are retried inside the
call with exponential backoff; everything else is classified so the DAG can
branch on it instead of failing blindly.
"""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import requests

from real_estate_pipeline import settings

log = logging.getLogger(__name__)

END_OF_DATA_MARKER = "recolect"  # "Ya se recolectó toda la información ..."


class DataApiError(RuntimeError):
    """The API answered something the pipeline cannot use (not transient)."""


class DataApiUnavailable(RuntimeError):
    """The API could not be reached after every in-task attempt (transient)."""


@dataclass(frozen=True)
class FetchResult:
    """Outcome of one GET /data call.

    kind: ``batch`` (new data), ``empty`` (200 with no rows) or ``end_of_data``.
    """

    kind: str
    request_url: str
    http_status: int
    body: bytes = b""
    group_number: int | None = None
    api_batch_number: int | None = None
    records: list[dict[str, Any]] = field(default_factory=list)
    detail: str | None = None

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


def _request(url: str, params: dict[str, Any]) -> requests.Response:
    attempts = max(1, settings.DATA_API_MAX_ATTEMPTS)
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=(
                    settings.DATA_API_CONNECT_TIMEOUT_SECONDS,
                    settings.DATA_API_READ_TIMEOUT_SECONDS,
                ),
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            error: str = f"{type(exc).__name__}: {exc}"
        else:
            if response.status_code < 500:
                return response
            error = f"HTTP {response.status_code}: {response.text[:300]}"
        if attempt == attempts:
            raise DataApiUnavailable(f"Data API unavailable after {attempts} attempts: {error}")
        delay = settings.DATA_API_BACKOFF_SECONDS * 2 ** (attempt - 1)
        log.warning("Data API attempt %s/%s failed (%s); retrying in %.0fs", attempt, attempts, error, delay)
        time.sleep(delay)
    raise AssertionError("unreachable")


def fetch_batch() -> FetchResult:
    """Request the next batch of the configured group and classify the answer."""
    url = f"{settings.DATA_API_URL}/data"
    params: dict[str, Any] = {"group_number": settings.DATA_API_GROUP_NUMBER}
    if settings.DATA_API_DAY:
        params["day"] = settings.DATA_API_DAY
    response = _request(url, params)
    request_url = response.url

    if response.status_code == 400:
        detail = _detail(response)
        if END_OF_DATA_MARKER in detail.lower():
            return FetchResult("end_of_data", request_url, 400, detail=detail)
        raise DataApiError(f"Data API rejected the request (400): {detail}")
    if response.status_code != 200:
        raise DataApiError(
            f"Unexpected Data API status {response.status_code}: {_detail(response)}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise DataApiError(f"Data API returned invalid JSON: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise DataApiError("Data API response must be an object with a `data` list.")
    if not isinstance(payload.get("batch_number"), int):
        raise DataApiError("Data API response has no integer `batch_number`.")
    group_number = payload.get("group_number")
    if group_number != settings.DATA_API_GROUP_NUMBER:
        raise DataApiError(
            f"Data API answered group {group_number!r}, "
            f"expected {settings.DATA_API_GROUP_NUMBER}."
        )
    records = payload["data"]
    if not all(isinstance(record, dict) for record in records):
        raise DataApiError("Every element of `data` must be a JSON object.")
    return FetchResult(
        kind="batch" if records else "empty",
        request_url=request_url,
        http_status=200,
        body=response.content,
        group_number=group_number,
        api_batch_number=payload["batch_number"],
        records=records,
    )


def _detail(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:500]
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return str(body)[:500]
