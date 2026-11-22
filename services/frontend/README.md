# frontend

Interfaz Streamlit para estimar precios inmobiliarios y consultar el historial
de entrenamiento. Habla **solo** con la API de inferencia (`API_URL`); nunca con
MLflow ni con la base de datos directamente (la API también sirve el historial).

## Funcionalidad

Dos secciones obligatorias (RF9):

### 1. Inferencia

- Formulario con los campos del contrato de entrada
  ([docs/data-contract.md](../../docs/data-contract.md)), agrupados por sección
  (propiedad, ubicación, metadatos). Los campos que admiten faltantes muestran
  `(desconocido)` o se dejan vacíos y se envían como `null`.
- **Valores de ejemplo**: tres propiedades representativas, con un precio de
  referencia.
- **Resultado**: precio estimado (USD), modelo, versión, alias productivo,
  tiempo de procesamiento y `request_id`.
- **Validación**: el formulario revisa rangos, el formato del ZIP (5 dígitos) y
  de la fecha (`YYYY-MM-DD`) antes de enviar; la pestaña *JSON (avanzado)* envía
  el cuerpo tal cual para mostrar los errores 422 de la API campo por campo (la
  API también registra esas solicitudes como fallidas en `inference.logs`).

### 2. Historial de entrenamiento y despliegue

- Tabla con una fila por lote procesado por el DAG de Airflow (desde
  `GET /history` → `audit.batch_history_summary`).
- Detalle por lote: si se entrenó o no y por qué, si el candidato fue promovido
  o rechazado y por qué, el cambio de desempeño frente al modelo productivo
  (`metric_deltas`), la transición de versión productiva y los identificadores
  de MLflow.

La barra lateral muestra el estado de la API y la versión del modelo cargado
(`/health`, `/model-info`).

## Ejecución

Con el stack completo, desde la raíz del repositorio:

```bash
docker compose up -d --build frontend
# http://localhost:8501  (FRONTEND_PORT y API_URL se definen en el .env de la raíz)
```

Con `OTEL_EXPORTER_OTLP_ENDPOINT` definida, cada llamada a la API (salvo
`/health`) abre una traza de OpenTelemetry que la API continúa y que se
consulta en Jaeger; vacía, el tracing queda apagado. Ver
[`infra/monitoring`](../../infra/monitoring/README.md#trazas-jaeger).

## Pruebas

```bash
cd services/frontend
uv sync
uv run pytest -q
```
