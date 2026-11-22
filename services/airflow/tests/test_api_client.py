"""Data API client: classification of every answer and retry behaviour."""

import json

import pytest
import requests

from real_estate_pipeline import api_client, settings


class FakeResponse:
    def __init__(self, status, body):
        self.status_code = status
        self.content = json.dumps(body).encode() if not isinstance(body, bytes) else body
        self.text = self.content.decode(errors="replace")
        self.url = "http://data-api:80/data?group_number=6"

    def json(self):
        return json.loads(self.content)


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(settings, "DATA_API_GROUP_NUMBER", 6)
    monkeypatch.setattr(settings, "DATA_API_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(api_client.time, "sleep", lambda seconds: None)


def answer(monkeypatch, *responses):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        item = responses[min(len(calls), len(responses)) - 1]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(api_client.requests, "get", fake_get)
    return calls


ROW = {"brokered_by": 1.0, "status": "for_sale", "price": 100000.0}


def test_new_batch(monkeypatch):
    calls = answer(monkeypatch, FakeResponse(200, {"group_number": 6, "batch_number": 2, "data": [ROW]}))
    result = api_client.fetch_batch()
    assert (result.kind, result.api_batch_number, result.records) == ("batch", 2, [ROW])
    assert len(result.sha256) == 64
    assert calls == [{"group_number": 6}]


def test_end_of_data_is_not_an_error(monkeypatch):
    answer(monkeypatch, FakeResponse(400, {"detail": "Ya se recolectó toda la información minima necesaria"}))
    result = api_client.fetch_batch()
    assert result.kind == "end_of_data" and "recolect" in result.detail


def test_empty_batch(monkeypatch):
    answer(monkeypatch, FakeResponse(200, {"group_number": 6, "batch_number": 3, "data": []}))
    assert api_client.fetch_batch().kind == "empty"


def test_transient_errors_are_retried(monkeypatch):
    calls = answer(
        monkeypatch,
        requests.ConnectionError("refused"),
        FakeResponse(503, {"detail": "busy"}),
        FakeResponse(200, {"group_number": 6, "batch_number": 0, "data": [ROW]}),
    )
    assert api_client.fetch_batch().kind == "batch"
    assert len(calls) == 3


def test_gives_up_after_the_configured_attempts(monkeypatch):
    calls = answer(monkeypatch, requests.Timeout("slow"))
    with pytest.raises(api_client.DataApiUnavailable, match="after 3 attempts"):
        api_client.fetch_batch()
    assert len(calls) == 3


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (FakeResponse(400, {"detail": "group_number inválido"}), "rejected the request"),
        (FakeResponse(422, {"detail": "bad"}), "Unexpected Data API status 422"),
        (FakeResponse(200, b"<html>"), "invalid JSON"),
        (FakeResponse(200, {"batch_number": 1}), "`data` list"),
        (FakeResponse(200, {"group_number": 6, "data": []}), "batch_number"),
        (FakeResponse(200, {"group_number": 7, "batch_number": 1, "data": [ROW]}), "expected 6"),
        (FakeResponse(200, {"group_number": 6, "batch_number": 1, "data": [1, 2]}), "JSON object"),
    ],
)
def test_unusable_answers_fail_without_retrying(monkeypatch, response, message):
    calls = answer(monkeypatch, response)
    with pytest.raises(api_client.DataApiError, match=message):
        api_client.fetch_batch()
    assert len(calls) == 1
