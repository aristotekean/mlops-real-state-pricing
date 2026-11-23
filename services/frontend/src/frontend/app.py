"""Streamlit UI: estimate a listing's price and inspect the training history.

The two obligatory sections of RF9:
  1. Inferencia: fill in a property, call the API, show the predicted price and
     the model version that produced it.
  2. Historial de entrenamiento y despliegue: per-batch log of whether a model
     was trained, why, whether it was promoted, the performance change versus
     the production model and the associated MLflow identifiers.

Inference goes exclusively through the Inference API; the UI never talks to
MLflow or the database directly (the API serves the history too).
"""

from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
import streamlit as st

from frontend.client import ApiClient, ApiError
from frontend.examples import EXAMPLES
from frontend.fields import (
    FIELDS,
    FIELDS_BY_NAME,
    SECTIONS,
    default_values,
    from_widget_value,
    to_widget_value,
    widget_options,
)
from frontend.tracing import configure_tracing
from frontend.validation import validate_features

API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Precios inmobiliarios", page_icon="🏠", layout="wide")


@st.cache_resource
def get_client() -> ApiClient:
    configure_tracing(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
    return ApiClient(API_URL)


def _key(name: str) -> str:
    return f"field_{name}"


def _set_form(values: dict[str, Any]) -> None:
    for name, value in values.items():
        st.session_state[_key(name)] = to_widget_value(FIELDS_BY_NAME[name], value)


def _init_state() -> None:
    if "initialised" not in st.session_state:
        _set_form(default_values())
        st.session_state.initialised = True
        st.session_state.result = None
        st.session_state.error = None


def _load_example() -> None:
    _set_form(EXAMPLES[st.session_state.example_choice]["features"])
    st.session_state.result = None
    st.session_state.error = None


def _reset_form() -> None:
    _set_form(default_values())
    st.session_state.result = None
    st.session_state.error = None


def _current_features() -> dict[str, Any]:
    return {
        field.name: from_widget_value(field, st.session_state[_key(field.name)])
        for field in FIELDS
    }


def _send(body: Any) -> None:
    st.session_state.result = None
    st.session_state.error = None
    try:
        st.session_state.result = get_client().predict_raw(body)
    except ApiError as exc:
        st.session_state.error = exc


def _sidebar() -> None:
    client = get_client()
    st.sidebar.header("Servicio")
    st.sidebar.caption(f"API: `{API_URL}`")
    try:
        health = client.health()
        st.sidebar.success("API disponible")
        if health.get("database") != "ok":
            st.sidebar.warning("La API no puede escribir el registro de inferencias.")
    except ApiError as exc:
        st.sidebar.error(f"API no disponible: {exc}")
        return
    try:
        info = client.model_info()
    except ApiError as exc:
        st.sidebar.error(str(exc))
        return
    st.sidebar.subheader("Modelo productivo")
    if info.get("status") == "ready":
        st.sidebar.metric("Versión", info.get("version"))
        st.sidebar.write(f"**{info.get('name')}** @ `{info.get('alias')}`")
        st.sidebar.caption(f"Cargado: {info.get('loaded_at')}")
    else:
        st.sidebar.warning(
            "No hay un modelo productivo cargado todavía. Ejecuta el DAG de "
            "entrenamiento en Airflow."
        )
        if info.get("last_error"):
            st.sidebar.caption(info["last_error"])
    st.sidebar.button("Actualizar estado")


def _render_form() -> None:
    with st.form("listing"):
        for title, fields in SECTIONS:
            st.subheader(title)
            columns = st.columns(3)
            for index, field in enumerate(fields):
                column = columns[index % 3]
                key = _key(field.name)
                if field.kind == "select":
                    column.selectbox(field.label, widget_options(field), key=key, help=field.help)
                elif field.kind == "number":
                    if field.decimals > 0:
                        column.number_input(
                            field.label,
                            min_value=float(field.min_value), max_value=float(field.max_value),
                            step=10.0 ** (-field.decimals), format=f"%.{field.decimals}f",
                            key=key, help=field.help,
                        )
                    else:
                        column.number_input(
                            field.label,
                            min_value=int(field.min_value), max_value=int(field.max_value),
                            step=1, key=key, help=field.help,
                        )
                else:
                    column.text_input(field.label, key=key, help=field.help)
        submitted = st.form_submit_button("Estimar precio", type="primary", width="stretch")
    if submitted:
        features = _current_features()
        errors = validate_features(features)
        if errors:
            st.session_state.result = None
            st.session_state.error = ApiError(
                "Corrige los campos marcados antes de enviar.", field_errors=errors
            )
        else:
            _send({"features": features})


def _render_json() -> None:
    st.caption(
        "Envía el cuerpo tal cual a `POST /predict`. Útil para ver cómo la API valida "
        "el contrato: prueba a quitar un campo, enviar un número como texto o agregar "
        "`price`."
    )
    default = json.dumps({"features": _current_features()}, indent=2, ensure_ascii=False)
    body_text = st.text_area("Cuerpo JSON", value=default, height=420)
    if st.button("Enviar JSON"):
        try:
            body = json.loads(body_text)
        except json.JSONDecodeError as exc:
            st.session_state.result = None
            st.session_state.error = ApiError(f"JSON inválido: {exc}")
        else:
            _send(body)


def _render_result() -> None:
    error: ApiError | None = st.session_state.error
    result: dict[str, Any] | None = st.session_state.result
    if error is not None:
        st.error(error.message)
        if error.field_errors:
            st.dataframe(
                pd.DataFrame(error.field_errors, columns=["Campo", "Error"]),
                hide_index=True, width="stretch",
            )
        if error.request_id:
            st.caption(f"request_id: `{error.request_id}` (registrado como fallido)")
        return
    if result is None:
        return

    price = result["prediction"]
    model = result["model"]
    st.success(f"Precio estimado: **${price:,.0f}**")
    col_price, col_model, col_time = st.columns(3)
    col_price.metric("Precio estimado (USD)", f"${price:,.0f}")
    col_model.metric("Modelo", f"{model['name']} v{model['version']}")
    col_time.metric("Tiempo de procesamiento", f"{result['processing_time_ms']:.1f} ms")
    st.caption(f"Alias productivo: `{model['alias']}`  ·  request_id: `{result['request_id']}`")


# --------------------------------------------------------------------------- #
# RF9 · sección 2: historial de entrenamiento y despliegue
# --------------------------------------------------------------------------- #
_DECISION_LABELS = {"train": "Entrenó", "skip": "No entrenó"}
_PROMOTION_LABELS = {"promoted": "Promovido", "rejected": "Rechazado"}


def _decision_label(entry: dict[str, Any]) -> str:
    return _DECISION_LABELS.get(entry.get("training_decision"), entry.get("training_decision") or "—")


def _promotion_label(entry: dict[str, Any]) -> str:
    return _PROMOTION_LABELS.get(entry.get("promotion_decision"), entry.get("promotion_decision") or "—")


def _history_summary_row(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "Lote": entry.get("api_batch_number"),
        "Recibido": entry.get("received_at"),
        "Registros": entry.get("record_count"),
        "Decisión": _decision_label(entry),
        "Promoción": _promotion_label(entry),
        "Modelo candidato": entry.get("candidate_model"),
        "Versión": entry.get("candidate_version"),
        "Estado": entry.get("final_status"),
    }


def _render_batch_detail(entry: dict[str, Any]) -> None:
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Decisión de entrenamiento", _decision_label(entry))
    col_b.metric("Promoción", _promotion_label(entry))
    col_c.metric("Estado final", entry.get("final_status") or "—")

    if entry.get("training_reason"):
        st.write(f"**Razón de entrenamiento:** {entry['training_reason']}")
    if entry.get("promotion_reason"):
        st.write(f"**Razón de promoción:** {entry['promotion_reason']}")

    before = entry.get("production_version_before")
    after = entry.get("production_version_after")
    if before or after:
        st.write(f"**Modelo productivo:** `{before or '—'}` → `{after or '—'}`")

    deltas = entry.get("metric_deltas")
    if isinstance(deltas, dict) and deltas:
        st.write("**Cambio de desempeño vs. productivo:**")
        st.dataframe(
            pd.DataFrame(
                [(metric, value) for metric, value in deltas.items()],
                columns=["Métrica", "Δ (candidato − productivo)"],
            ),
            hide_index=True, width="stretch",
        )

    mlflow_ids = {
        "experiment_id": entry.get("mlflow_experiment_id"),
        "parent_run_id": entry.get("mlflow_parent_run_id"),
        "candidate_run_id": entry.get("mlflow_candidate_run_id"),
    }
    if any(mlflow_ids.values()):
        st.write("**Identificadores de MLflow:**")
        st.json({k: v for k, v in mlflow_ids.items() if v})

    if entry.get("error_message"):
        st.error(f"Error del lote: {entry['error_message']}")


def _render_history() -> None:
    st.subheader("Historial de entrenamiento y despliegue")
    st.caption(
        "Resultado de cada lote procesado por el DAG de Airflow: si se entrenó o no y por qué, "
        "si el candidato fue promovido o rechazado, el cambio de desempeño frente al modelo "
        "productivo y los identificadores de MLflow."
    )
    st.button("Actualizar historial", key="refresh_history")
    try:
        entries = get_client().history(limit=200)
    except ApiError as exc:
        st.error(f"No se pudo obtener el historial: {exc}")
        return
    if not entries:
        st.info("Todavía no hay lotes procesados. Ejecuta el DAG de Airflow para poblar el historial.")
        return

    st.dataframe(
        pd.DataFrame([_history_summary_row(e) for e in entries]),
        hide_index=True, width="stretch",
    )
    st.divider()
    for entry in entries:
        batch = entry.get("api_batch_number")
        promotion = _PROMOTION_LABELS.get(entry.get("promotion_decision"), "")
        label = f"Lote {batch} · {_decision_label(entry)}" + (f" · {promotion}" if promotion else "")
        with st.expander(label):
            _render_batch_detail(entry)


def main() -> None:
    _init_state()
    _sidebar()

    st.title("🏠 Estimación de precios inmobiliarios")
    st.caption(
        "Estima el precio de una propiedad a partir de sus características usando el modelo "
        "productivo registrado en MLflow, y consulta el historial de entrenamiento del sistema."
    )

    tab_inference, tab_history = st.tabs(["Inferencia", "Historial de entrenamiento"])

    with tab_inference:
        col_choice, col_load, col_reset = st.columns([3, 1, 1], vertical_alignment="bottom")
        col_choice.selectbox("Valores de ejemplo", list(EXAMPLES), key="example_choice")
        col_load.button("Cargar ejemplo", on_click=_load_example, width="stretch")
        col_reset.button("Restablecer", on_click=_reset_form, width="stretch")
        observed = EXAMPLES[st.session_state.example_choice]["observed"]
        st.caption(f"Precio de referencia del ejemplo: `${observed:,.0f}`")

        tab_form, tab_json = st.tabs(["Formulario", "JSON (avanzado)"])
        with tab_form:
            _render_form()
        with tab_json:
            _render_json()

        st.divider()
        _render_result()

    with tab_history:
        _render_history()


main()
