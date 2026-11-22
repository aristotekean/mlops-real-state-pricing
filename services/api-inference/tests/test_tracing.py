"""Tracing of inference requests, asserted on an in-memory span exporter."""

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from conftest import FakeModel, FakeRegistry, InMemoryRepository, make_app, valid_features

from api_inference.tracing import build_tracer_provider

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACEPARENT = f"00-{TRACE_ID}-b7ad6b7169203331-01"


class FailingModel(FakeModel):
    def predict(self, frame):
        raise ValueError("boom")


@pytest.fixture
def exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def provider(exporter: InMemorySpanExporter) -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


def spans_by_name(exporter: InMemorySpanExporter) -> dict[str, object]:
    return {span.name: span for span in exporter.get_finished_spans()}


async def test_predict_emits_server_span_with_model_and_log_children(
    running_client, exporter, provider
):
    app, _, _, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    response = await client.post("/predict", json={"features": valid_features()})

    assert response.status_code == 200
    spans = spans_by_name(exporter)
    assert set(spans) == {"POST /predict", "model.predict", "inference_log.write"}
    server = spans["POST /predict"]
    assert server.kind is SpanKind.SERVER
    assert server.attributes["inference.request_id"] == response.json()["request_id"]
    assert server.attributes["inference.outcome"] == "succeeded"
    for name in ("model.predict", "inference_log.write"):
        assert spans[name].parent.span_id == server.context.span_id
        assert spans[name].context.trace_id == server.context.trace_id
    model_span = spans["model.predict"]
    assert model_span.attributes["model.name"] == "real_estate"
    assert model_span.attributes["model.version"] == "3"
    assert model_span.attributes["model.alias"] == "production"
    assert spans["inference_log.write"].attributes["inference_log.stored"] is True


async def test_predict_continues_the_trace_started_by_the_caller(
    running_client, exporter, provider
):
    app, _, _, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    response = await client.post(
        "/predict", json={"features": valid_features()}, headers={"traceparent": TRACEPARENT}
    )

    assert response.status_code == 200
    server = spans_by_name(exporter)["POST /predict"]
    assert format(server.context.trace_id, "032x") == TRACE_ID
    assert format(server.parent.span_id, "016x") == "b7ad6b7169203331"


async def test_health_and_metrics_are_not_traced(running_client, exporter, provider):
    app, _, _, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    assert (await client.get("/health")).status_code == 200
    assert (await client.get("/metrics")).status_code == 200

    assert exporter.get_finished_spans() == ()


async def test_a_host_named_like_an_excluded_path_is_still_traced(
    running_client, exporter, provider
):
    app, _, _, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    response = await client.post(
        "/predict",
        json={"features": valid_features()},
        headers={"host": "metrics-health.internal"},
    )

    assert response.status_code == 200
    assert "POST /predict" in spans_by_name(exporter)


async def test_log_write_span_reports_a_row_that_was_not_stored(
    running_client, exporter, provider
):
    app, _, _, _ = make_app(
        repository=InMemoryRepository(fail_log=True), tracer_provider=provider
    )
    client = await running_client(app)

    response = await client.post("/predict", json={"features": valid_features()})

    assert response.status_code == 200
    assert spans_by_name(exporter)["inference_log.write"].attributes["inference_log.stored"] is False


async def test_inference_error_marks_the_model_span_as_failed(
    running_client, exporter, provider
):
    app, _, _, _ = make_app(
        registry=FakeRegistry(model=FailingModel()), tracer_provider=provider
    )
    client = await running_client(app)

    response = await client.post("/predict", json={"features": valid_features()})

    assert response.status_code == 500
    spans = spans_by_name(exporter)
    assert spans["model.predict"].status.status_code is StatusCode.ERROR
    assert spans["POST /predict"].attributes["inference.outcome"] == "inference_error"


async def test_validation_error_is_traced_with_its_request_id(
    running_client, exporter, provider
):
    app, _, _, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    response = await client.post("/predict", json={"features": {"bed": "three"}})

    assert response.status_code == 422
    spans = spans_by_name(exporter)
    assert set(spans) == {"POST /predict", "inference_log.write"}
    server = spans["POST /predict"]
    assert server.attributes["inference.request_id"] == response.json()["request_id"]
    assert server.attributes["inference.outcome"] == "validation_error"


async def test_logged_row_stores_the_trace_id_of_its_request(
    running_client, exporter, provider
):
    app, _, repository, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    await client.post("/predict", json={"features": valid_features()})
    await client.post("/predict", json={"features": {"bed": "three"}})

    succeeded, failed = repository.rows
    traces = [
        format(span.context.trace_id, "032x")
        for span in exporter.get_finished_spans()
        if span.name == "POST /predict"
    ]
    assert [succeeded.trace_id, failed.trace_id] == traces
    assert succeeded.trace_id != failed.trace_id


async def test_logged_row_stores_the_trace_id_started_by_the_caller(
    running_client, provider
):
    app, _, repository, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    await client.post(
        "/predict", json={"features": valid_features()}, headers={"traceparent": TRACEPARENT}
    )

    assert repository.rows[0].trace_id == TRACE_ID


async def test_logged_row_has_no_trace_id_when_the_trace_is_not_sampled(
    running_client, exporter, provider
):
    app, _, repository, _ = make_app(tracer_provider=provider)
    client = await running_client(app)

    response = await client.post(
        "/predict",
        json={"features": valid_features()},
        headers={"traceparent": f"00-{TRACE_ID}-b7ad6b7169203331-00"},
    )

    assert response.status_code == 200
    assert exporter.get_finished_spans() == ()
    assert repository.rows[0].trace_id is None


async def test_logged_row_has_no_trace_id_when_tracing_is_off(running_client):
    app, _, repository, _ = make_app()
    client = await running_client(app)

    response = await client.post("/predict", json={"features": valid_features()})

    assert response.status_code == 200
    assert repository.rows[0].trace_id is None


def test_tracing_is_disabled_without_an_endpoint():
    assert build_tracer_provider(None, 1.0) is None
    assert build_tracer_provider("", 1.0) is None
    assert build_tracer_provider("   ", 1.0) is None


def test_tracer_provider_samples_root_spans_by_the_configured_ratio():
    never = build_tracer_provider("http://jaeger.test:4318", 0.0)
    always = build_tracer_provider("http://jaeger.test:4318", 1.0)
    try:
        assert never.resource.attributes["service.name"] == "api-inference"
        with never.get_tracer("t").start_as_current_span("root") as span:
            assert span.get_span_context().trace_flags.sampled is False
        with always.get_tracer("t").start_as_current_span("root") as span:
            assert span.get_span_context().trace_flags.sampled is True
    finally:
        never.shutdown()
        always.shutdown()
