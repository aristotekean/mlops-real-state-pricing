"""The UI starts the trace: calls to the API carry the W3C ``traceparent``."""

from __future__ import annotations

import pytest
import requests
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from requests.adapters import HTTPAdapter

from frontend.client import ApiClient
from frontend.tracing import build_tracer_provider, instrument_requests, uninstrument_requests


class RecordingAdapter(HTTPAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.sent: list[requests.PreparedRequest] = []

    def send(self, request, **kwargs):
        self.sent.append(request)
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"prediction": 350000.0, "status": "ok"}'
        response.request = request
        response.url = request.url
        return response


@pytest.fixture
def exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    instrument_requests(provider)
    yield exporter
    uninstrument_requests()


@pytest.fixture
def adapter() -> RecordingAdapter:
    return RecordingAdapter()


@pytest.fixture
def client(adapter: RecordingAdapter) -> ApiClient:
    session = requests.Session()
    session.mount("http://", adapter)
    return ApiClient("http://api.test", session=session)


def test_predict_starts_a_trace_and_propagates_it_to_the_api(exporter, adapter, client):
    client.predict({"bed": 3})

    (span,) = exporter.get_finished_spans()
    assert span.kind is SpanKind.CLIENT
    assert span.parent is None
    assert span.name == "POST"
    version, trace_id, span_id, flags = adapter.sent[0].headers["traceparent"].split("-")
    assert trace_id == format(span.context.trace_id, "032x")
    assert span_id == format(span.context.span_id, "016x")
    assert int(flags, 16) & 1 == 1


def test_health_polling_is_not_traced(exporter, adapter, client):
    client.health()

    assert exporter.get_finished_spans() == ()
    assert "traceparent" not in adapter.sent[0].headers


def test_an_api_host_named_like_the_excluded_path_is_still_traced(exporter, adapter):
    session = requests.Session()
    session.mount("http://", adapter)

    ApiClient("http://api-health.test", session=session).predict({"bed": 3})

    assert len(exporter.get_finished_spans()) == 1
    assert "traceparent" in adapter.sent[0].headers


def test_requests_are_untouched_without_instrumentation(adapter, client):
    client.predict({"bed": 3})

    assert "traceparent" not in adapter.sent[0].headers


def test_tracing_is_disabled_without_an_endpoint():
    assert build_tracer_provider(None) is None
    assert build_tracer_provider("") is None
    assert build_tracer_provider("   ") is None


def test_tracer_provider_identifies_the_frontend_service():
    provider = build_tracer_provider("http://jaeger.test:4318")
    try:
        assert provider.resource.attributes["service.name"] == "frontend"
    finally:
        provider.shutdown()
