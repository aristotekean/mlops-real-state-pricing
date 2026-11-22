"""OpenTelemetry tracing exported over OTLP/HTTP (Jaeger); off without an endpoint.

The provider is passed explicitly to every instrumentation instead of being
registered as the process-wide default, so MLflow's own tracing and the tests
never share state with it.
"""

from __future__ import annotations

from fastapi import FastAPI
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import NoOpTracerProvider, Tracer

SERVICE_NAME = "api-inference"
TRACER_NAME = "api_inference"
TRACES_PATH = "/v1/traces"
EXCLUDED_URLS = r"/health(\?|$),/metrics(\?|$)"


def build_tracer_provider(endpoint: str | None, sample_ratio: float) -> TracerProvider | None:
    """Provider that batches spans to ``endpoint``; ``None`` when tracing is off.

    A caller that already made a sampling decision (the frontend) is followed;
    ``sample_ratio`` only applies to requests that start a trace here.
    """
    endpoint = (endpoint or "").strip()
    if not endpoint:
        return None
    provider = TracerProvider(
        resource=Resource.create({"service.name": SERVICE_NAME}),
        sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint.rstrip("/") + TRACES_PATH))
    )
    return provider


def get_tracer(provider: TracerProvider | None) -> Tracer:
    return (provider or NoOpTracerProvider()).get_tracer(TRACER_NAME)


def instrument_app(app: FastAPI, provider: TracerProvider) -> None:
    """One server span per request; probes and scrapes are left out."""
    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=provider,
        excluded_urls=EXCLUDED_URLS,
        exclude_spans=["send", "receive"],
    )
