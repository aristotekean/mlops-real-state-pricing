# Pipeline de datos y entrenamiento (API de datos → MLflow)

Este documento describe el DAG `real_estate_incremental_training`: cómo consume la API de datos del curso, cómo valida y transforma cada lote, cómo decide si entrena, cómo registra el modelo en MLflow y cómo decide si lo promueve. Todos los umbrales son parámetros del ConfigMap `pipeline-config` ([infra/k8s/base/airflow/kustomization.yaml](../infra/k8s/base/airflow/kustomization.yaml)); el código que los lee está en [services/airflow/dags/real_estate_pipeline/settings.py](../services/airflow/dags/real_estate_pipeline/settings.py).

## 1. La fuente: API de datos (grupo 6)

Imagen `cristiandiaz13/mlops-puj:data-api-pf-v2`, desplegada en el clúster como `data-api` ([infra/k8s/base/data-api](../infra/k8s/base/data-api)) y consumida desde Airflow en `http://data-api:80`.

| Llamada | Respuesta |
|---|---|
| `GET /data?group_number=6` | `200 {"group_number": 6, "batch_number": k, "data": [...]}`: el **siguiente** lote |
| después del último lote | `400 {"detail": "Ya se recolectó toda la información minima necesaria"}` |
| `GET /restart_data_generation?group_number=6` | reinicia el contador del grupo |

La API **tiene estado**: cada `GET /data` entrega un lote nuevo y el contador vive en la memoria del proceso. Esto condiciona el diseño: una respuesta perdida no se puede volver a pedir, por eso se persiste antes de procesarla (sección 3).

Perfil del grupo 6 (medido al construir el pipeline):

| Lote | Filas | Precio mediano | Observación |
|---:|---:|---:|---|
| 0 | 73.784 | 234.900 | solo precios ≤ 300k y casas ≥ 1.500 sq ft |
| 1 | 94.551 | 179.900 | casas pequeñas; aparece Puerto Rico |
| 2 | 230.366 | 200.000 | todas `sold` (los anteriores eran `for_sale`) |
| 3 | 4.055 | 249.000 | un solo estado: Massachusetts |
| 4 | 320.696 | 395.000 | |
| 5 | 361.457 | 759.900 | propiedades caras, fuerte presencia de California |

Sin nulos ni duplicados exactos; sí hay valores extremos (precio de 1 a 515 M USD, 444 habitaciones, 100.000 acres, 1,5 M sq ft). Los lotes están cortados por rangos, así que el *drift* entre lotes es grande por diseño.

## 2. DAG

```text
start → fetch_batch_from_api → check_new_batch
          ├─ no_new_batch ─────────────────────────────────────────────────────────────┐
          └─ store_raw_batch → validate_schema → validate_data_quality                  │
             → detect_new_categories → detect_data_drift → preprocess_data              │
             → decide_training                                                          │
                 ├─ skip_training ──────────────────────────────────────┐               │
                 └─ train_candidate_model → evaluate_candidate_model    │               │
                    → register_candidate_in_mlflow → compare_with_production            │
                    → decide_promotion ─┬─ promote_model ───────────────┤               │
                                        └─ reject_model ────────────────┤               │
                                                     notify_or_log_result → end ◄───────┘
```

Son las 19 tareas de referencia del enunciado más dos de control: `check_new_batch` y `no_new_batch`. Estas cierran limpiamente las corridas que no reciben datos nuevos (fin de datos, lote vacío o lote ya ingerido) sin tocar RAW. Cada tarea tiene una sola responsabilidad y su lógica vive en el paquete `real_estate_pipeline`. Las tareas se pasan por XCom solo identificadores y resúmenes. Los datos viajan por PostgreSQL y los modelos por MLflow.

| Tarea | Qué hace | Dónde queda el resultado |
|---|---|---|
| `fetch_batch_from_api` | Reanuda un lote sin terminar o pide **uno** nuevo; guarda la respuesta íntegra (gzip) y su SHA-256 y registra el intento, sea cual sea su resultado | `raw.batches`, `raw.api_requests` |
| `check_new_batch` | Ramifica: `new`/`resumed` → procesar; `end_of_data`/`empty`/`duplicate` → `no_new_batch` | — |
| `store_raw_batch` | Una fila por registro, sin modificar (JSONB), con su hash de contenido | `raw.records` |
| `validate_schema` | Columnas faltantes/nuevas y tipos JSON frente al contrato | `audit.batch_history.schema_report` |
| `validate_data_quality` | Reglas por fila, duplicados (en el lote y contra lotes anteriores), nulos, rangos, consistencia del target | `raw.records.status`, `quality_report` |
| `detect_new_categories` | Categorías nunca vistas en CLEAN y % del lote que cubren | `new_categories_report` |
| `detect_data_drift` | PSI de cada variable del lote contra el histórico de CLEAN | `drift_report` |
| `preprocess_data` | Normaliza tipos, marca outliers como no entrenables y asigna partición | `clean.records`, `preprocessing_report` |
| `decide_training` | Aplica las reglas de la sección 5 | `training_decision`, `training_reason`, `training_rules` |
| `skip_training` | Registra que no se entrenó | `audit.batch_history` |
| `train_candidate_model` | Entrena los candidatos (runs anidados) y elige el mejor | MLflow + `mlflow_*` en la auditoría |
| `evaluate_candidate_model` | Métricas train/validation/test y artefactos | MLflow (run del candidato) |
| `register_candidate_in_mlflow` | Nueva versión de `real_estate` con sus tags | MLflow Model Registry |
| `compare_with_production` | Candidato vs. productivo sobre la misma validación | `production_metrics`, `metric_deltas` |
| `decide_promotion` | Ramifica según la regla de la sección 6 | — |
| `promote_model` / `reject_model` | Mueve (o no) el alias `production` y etiqueta la versión | MLflow + `promotion_*` |
| `notify_or_log_result` | Cierra el lote (`completed`) y resume el resultado | `audit.batch_history`, `raw.batches` |

## 3. Resiliencia

- **Lote perdido imposible.** El cuerpo de la respuesta se guarda en `raw.batches.payload_gzip` dentro de la misma tarea que llama a la API. Todo lo demás se reconstruye desde ahí.
- **Reanudación.** Si un lote no llegó a `pipeline_status = 'completed'`, la siguiente corrida lo retoma **sin llamar a la API**, así que nunca se salta ni se adelanta un lote. Todas las tareas son idempotentes: reemplazan sus filas dentro de una transacción.
- **Cada petición es un evento.** `raw.api_requests` guarda una fila por intento de `fetch_batch_from_api`: fecha, `dag_run_id`, URL, código HTTP, duración, número de lote, SHA-256, filas y resultado (`new`, `resumed`, `duplicate`, `empty`, `end_of_data`, `error`). Las corridas sin datos nuevos también quedan auditadas, y las reanudaciones aparecen con `api_called = false`.
- **Sin reprocesos accidentales.** Una respuesta con el mismo grupo, número de lote y SHA-256 (por ejemplo, tras reiniciar la API) se reconoce como `duplicate` y termina en `no_new_batch`. Si el número de lote salta, se registra en `schema_report.missing_api_batches`.
- **Cliente HTTP.** Timeouts de conexión y lectura configurables. Reintenta errores de red y 5xx con backoff exponencial. Distingue el fin de datos (`400` con su mensaje) de un error real, y valida la forma de la respuesta y el grupo devuelto.
- **Logs persistentes.** Los logs de las tareas viven en el PVC `airflow-logs`, así que sobreviven a reinicios y redespliegues del pod de Airflow.
- **Errores visibles.** Reintentos de Airflow con backoff. Si una tarea falla definitivamente, un `on_failure_callback` deja el lote `failed` en `audit.batch_history` con el error, y la corrida termina en `failed`. Un lote con datos malos **no** rompe el DAG: se marca inválido y la decisión es "no entrenar".
- **Infraestructura vs. datos.** Si MLflow no responde, la tarea falla. Solo la respuesta "el modelo registrado no existe" se interpreta como "no hay modelo productivo".

## 4. Datos: RAW, CLEAN y auditoría

Migraciones en [infra/database/migrations](../infra/database/migrations), aplicadas por el Job `train-db-migrate` en la base `postgres-train`, que no es la base de metadatos de MLflow (`mlflow-db`).

- `raw.api_requests`: una fila por petición a la API (o reanudación), con su resultado.
- `raw.batches`: una fila por respuesta de la API, con payload gzip, SHA-256, número de lote, URL, esquema observado y estados (`landed` → `stored`; `in_progress` → `completed`/`failed`).
- `raw.records`: el registro tal cual llegó (JSONB) con `record_key` (hash del contenido) y `status` `validated`/`rejected` más el motivo.
- `clean.records`: columnas tipadas del contrato de entrada, `price`, `raw_record_id` y `batch_id` (trazabilidad), `exclusion_reason` y `data_split`.
- `audit.batch_history` y la vista `audit.batch_history_summary`: una fila por lote con todos los reportes, la decisión de entrenamiento y su motivo, los IDs de MLflow, las métricas del candidato y del productivo, la regla de promoción y su resultado. Streamlit leerá esta tabla.

**Versión de CLEAN.** `transformation_version` es `price-features-v1+bounds-<hash>`: la versión del código de transformación más un hash de los rangos de outliers (`TRAINING_BOUNDS_JSON`), que deciden qué filas se pueden entrenar. Si cambian los rangos, cambia la versión y `preprocess_data` reconstruye todos los lotes anteriores bajo la versión nueva antes de seguir. Un entrenamiento nunca mezcla filas limpiadas con reglas distintas, y las versiones anteriores quedan en CLEAN como historia. Los `clean_record_id` son estables: reprocesar un lote actualiza sus filas (upsert) en lugar de reinsertarlas. Un conjunto de entrenamiento se reconstruye exactamente con (versión de CLEAN, lotes usados), que es lo que registra el snapshot en MLflow (`data/clean_snapshot.json`).

**Outliers.** Los valores fuera de `TRAINING_BOUNDS_JSON` son observaciones válidas. Se quedan en RAW y en CLEAN con `exclusion_reason = out_of_range_<columna>`, pero no entran al entrenamiento. Con los valores por defecto se excluye cerca del 0,1 % de las filas.

**Particiones.** Cada fila elegible se asigna a `train` (70 %), `validation` (15 %) o `test` (15 %) con un hash con sal de la dirección (`street, city, state, zip_code`). Así una propiedad nunca queda partida entre train y test, y la asignación no cambia al llegar lotes nuevos.

**Categorías nuevas.** No rompen el pipeline ni el modelo:
- `status` y `state`: `OneHotEncoder(handle_unknown='ignore')`, o `OrdinalEncoder` con `unknown_value=-1` (tratado como faltante) en HistGradientBoosting.
- `city`, `zip_code` y `brokered_by`: `TargetEncoder`, que asigna la media global a lo no visto.

## 5. Decisión de entrenamiento

Las reglas se evalúan en orden. Las dos primeras vetan el entrenamiento y la tercera fuerza la línea base:

1. **Lote inválido**: esquema bloqueante (falta `price` o `status`) o `% rechazado > MAX_REJECTED_FRACTION` → no entrena.
2. **Datos insuficientes**: acumulado `train < MIN_TRAIN_ROWS` o `validation`/`test < MIN_EVALUATION_ROWS` → no entrena.
3. **Sin modelo productivo** → entrena (línea base).
4. **Lote pequeño**: filas elegibles del lote `< MIN_BATCH_ROWS` → no entrena. Sus filas se usan en el próximo entrenamiento.
5. En otro caso entrena si se cumple **al menos un disparador**:
   - **Volumen**: crecimiento de filas elegibles desde los datos del modelo productivo `≥ MIN_VOLUME_GROWTH`.
   - **Drift**: PSI `≥ DRIFT_PSI_THRESHOLD` en alguna de `DRIFT_FEATURES`.
   - **Categorías nuevas**: en `NEW_CATEGORY_COLUMNS`, cubren `≥ NEW_CATEGORY_MIN_SHARE` del lote.
   - **Degradación**: el modelo productivo, evaluado sobre el lote nuevo (que ya trae su precio), empeora `≥ DEGRADATION_THRESHOLD` en `DEGRADATION_METRIC` respecto de su propia validación.

   Si no se cumple ninguno, no entrena ("el lote no aporta variación relevante").

La periodicidad o el número de lotes nunca es por sí solo un motivo. Las entradas y el resultado de cada regla quedan en `audit.batch_history.training_rules` y en el artefacto `decision/training_decision.json` del run de MLflow.

## 6. Entrenamiento, registro y promoción

- **Candidatos** (`TRAINING_CANDIDATES`): Ridge (línea base lineal) e HistGradientBoosting; RandomForest sigue disponible en el paquete pero no se entrena por defecto. Todos entrenan sobre `log1p(price)` y predicen en USD, con la inversa recortada para que una entrada absurda no dé un precio infinito. Cada uno es un run anidado en MLflow con parámetros, métricas de validación y el modelo en formato MLflow Models (skops, con firma y el paquete `real_estate_model` en `code_paths`). El modelo servido es el pipeline completo (contrato de entrada → features → preprocesamiento → regresor), así que la API y el entrenamiento comparten el mismo código.
- **Selección**: el candidato con mejor `validation_<PRIMARY_METRIC>`.
- **Lo que se registra en MLflow**:
  - lotes usados (`batches_used`) y snapshot de CLEAN;
  - commit (`mlflow.source.git.commit`, desde `GIT_COMMIT` en la imagen);
  - parámetros del modelo y del preprocesamiento, y configuración efectiva;
  - métricas de train, validation y test;
  - gráficos predicho vs. real, residuos, error por decil de precio y por estado, e importancia por permutación;
  - motivo del entrenamiento.
- **Comparación**: el candidato registrado y el productivo se cargan **desde el registro** y se evalúan sobre las mismas filas de validación acumuladas. También se verifica que el candidato reproduzca su métrica de evaluación (control de *training/serving skew*).
- **Regla de promoción**: se promueve si `mejora(PRIMARY_METRIC) ≥ PROMOTION_MIN_IMPROVEMENT` **y** `mejora(GUARD_METRIC) ≥ −PROMOTION_MAX_GUARD_DEGRADATION`. La mejora es relativa y con signo (positiva = mejor). Por defecto: el MAE baja al menos 3 % y el RMSE no empeora más de 1 %. Si no hay modelo productivo, el candidato se promueve como línea base. Un candidato rechazado queda registrado como versión con `promotion_decision=rejected` y el alias no se mueve.
- **Fuente de verdad**: alias `production` del modelo registrado `real_estate`, es decir `models:/real_estate@production`.

## 7. Pruebas

La lógica de decisión está separada de la infraestructura: `real_estate_pipeline/rules.py` decide si entrenar y si promover a partir de los reportes, sin Airflow, MLflow ni base de datos. Las pruebas de [services/airflow/tests](../services/airflow/tests) cubren:

- cada regla de entrenamiento, su orden y sus umbrales (inclusivos);
- la regla de promoción (mejora mínima, tolerancia del guardia, línea base, ejecución reanudada);
- el cliente de la API (lote nuevo, fin de datos, lote vacío, reintentos de errores transitorios, respuestas inválidas que no se reintentan);
- el PSI, el esquema observado y la versión de CLEAN.

```bash
uv run --no-project --with-requirements services/airflow/tests/requirements.txt \
  --with-editable ml/real_estate_model python -m pytest services/airflow/tests
```

## 8. Parámetros para decidir

Valores actuales en `pipeline-config`. Cambiarlos es un commit; Argo CD lo sincroniza y el hash del ConfigMap reinicia Airflow.

| Parámetro | Valor | Qué controla |
|---|---|---|
| `DATA_API_GROUP_NUMBER` | `6` | Grupo asignado |
| `REAL_ESTATE_TRAINING_SCHEDULE` (`.env`) | `*/15 * * * *` | Cada cuánto se pide un lote (`none` = solo manual) |
| `MAX_REJECTED_FRACTION` | `0.05` | % de filas rechazadas que invalida un lote |
| `TRAINING_BOUNDS_JSON` | precio 10k–20M, ≤ 20 hab./baños, 100–30.000 sq ft, ≤ 1.000 acres | Rangos plausibles para entrenar |
| `MIN_BATCH_ROWS` | `5000` | Tamaño mínimo para que un lote dispare entrenamiento |
| `MIN_VOLUME_GROWTH` | `0.20` | Crecimiento de datos que justifica reentrenar |
| `DRIFT_FEATURES` / `DRIFT_PSI_THRESHOLD` | 7 variables / `0.20` | Qué se vigila y desde qué PSI hay drift |
| `NEW_CATEGORY_COLUMNS` / `NEW_CATEGORY_MIN_SHARE` | `status,state,city` / `0.01` | Categorías nuevas relevantes |
| `DEGRADATION_METRIC` / `DEGRADATION_THRESHOLD` | `rmsle` / `0.10` | Degradación del productivo en el lote nuevo |
| `MIN_TRAIN_ROWS` / `MIN_EVALUATION_ROWS` | `1000` / `200` | Suficiencia de datos |
| `TRAINING_CANDIDATES` | `ridge,hist_gradient_boosting` | Modelos a comparar (RandomForest nunca ganó y ocupa ~10× más en RustFS) |
| `TRAINING_MAX_ROWS` | `1000000` | Tope de filas de train (muestra determinista) para acotar memoria y tiempo |
| `PRIMARY_METRIC` / `GUARD_METRIC` | `mae` / `rmse` | Métrica prioritaria y de control |
| `PROMOTION_MIN_IMPROVEMENT` / `PROMOTION_MAX_GUARD_DEGRADATION` | `0.03` / `0.01` | Regla de promoción |

## 9. Operación

```bash
# Activar el DAG y lanzar una corrida (cada corrida = un lote)
kubectl -n real-estate exec deploy/airflow -c scheduler -- airflow dags unpause real_estate_incremental_training
kubectl -n real-estate exec deploy/airflow -c scheduler -- airflow dags trigger real_estate_incremental_training

# Cada petición a la API de datos y su resultado
kubectl -n real-estate exec postgres-train-0 -- psql -U app -d real_estate \
  -c "SELECT requested_at, api_called, http_status, outcome, api_batch_number, record_count FROM raw.api_requests ORDER BY request_id"

# Historial por lote (lo mismo que mostrará Streamlit)
kubectl -n real-estate exec postgres-train-0 -- psql -U app -d real_estate \
  -c "SELECT api_batch_number, training_decision, training_reason, promotion_decision, production_version_after FROM audit.batch_history_summary"

# Empezar de cero (borra datos y modelos): reiniciar el contador de la API y recrear los volúmenes
kubectl -n real-estate exec deploy/airflow -c scheduler -- python -c \
  "import requests; print(requests.get('http://data-api:80/restart_data_generation', params={'group_number': 6}).text)"
kubectl delete namespace real-estate   # luego volver a desplegar
```
