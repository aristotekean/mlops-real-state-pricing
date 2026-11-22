"""OpenTelemetry tracing for the calls the UI makes to the Inference API.

The trace of an inference request starts here: ``requests`` is instrumented so
every call opens a client span and sends its ``traceparent`` header, which the
API continues. Off without an OTLP endpoint.
"""

from __future__ import annotations

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

SERVICE_NAME = "frontend"
TRACES_PATH = "/v1/traces"
EXCLUDED_URLS = r"/health(\?|$)"


def build_tracer_provider(endpoint: str | None) -> TracerProvider | None:
    """Provider that batches spans to ``endpoint``; ``None`` when tracing is off."""
    endpoint = (endpoint or "").strip()
    if not endpoint:
        return None
    provider = TracerProvider(resource=Resource.create({"service.name": SERVICE_NAME}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint.rstrip("/") + TRACES_PATH))
    )
    return provider


def instrument_requests(provider: TracerProvider) -> None:
    """Trace every ``requests`` call of the process except the health polling."""
    RequestsInstrumentor().instrument(tracer_provider=provider, excluded_urls=EXCLUDED_URLS)


def uninstrument_requests() -> None:
    RequestsInstrumentor().uninstrument()


def configure_tracing(endpoint: str | None) -> bool:
    """Enable tracing when an endpoint is configured; call once per process."""
    provider = build_tracer_provider(endpoint)
    if provider is None:
        return False
    instrument_requests(provider)
    return True
