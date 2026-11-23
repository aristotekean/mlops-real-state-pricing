# Evaluación — Proyecto Final MLOps

**Repositorio:** git@github.com:aristotekean/mlops-real-state-pricing.git  ·  **Commit evaluado:** 06344fb (2026-11-22 21:05 -05:00, rama `feat/monitoring-k8s`)  ·  **Evaluado el:** 2026-11-22 21:13 -05:00
**Fecha límite:** 2026-11-26T23:59:59-05:00 (Bogotá)  ·  **Estado:** Provisional (antes de la fecha límite)

## Nota: 3.5 / 5.0

| Criterio | Peso | Nota | Aporte |
|---|---|---|---|
| C1. Documentación: informe y bitácora | 20 % | 3.0 | 0.60 |
| C2. Orquestación con Airflow | 10 % | 4.0 | 0.40 |
| C3. Datos: RAW, CLEAN, trazabilidad e inferencias | 8 % | 3.5 | 0.28 |
| C4. Validación y decisión de entrenamiento | 15 % | 3.0 | 0.45 |
| C5. MLflow, comparación y promoción | 10 % | 3.5 | 0.35 |
| C6. Inferencia con FastAPI | 8 % | 4.0 | 0.32 |
| C7. Streamlit | 5 % | 3.5 | 0.18 |
| C8. CI/CD y GitOps | 7 % | 4.5 | 0.32 |
| C9. Kubernetes | 5 % | 3.5 | 0.18 |
| C10. Observabilidad y Locust | 5 % | 3.0 | 0.15 |
| C11. Código y manejo del repositorio | 4 % | 4.0 | 0.16 |
| C12. Puntualidad | 3 % | 3.0 | 0.09 |
| **Nota base** | | | **3.47** |
| Topes aplicados | | | Ninguno |
| **Nota final** | | | **3.5** |

Aportes sin redondear: 0.60 + 0.40 + 0.28 + 0.45 + 0.35 + 0.32 + 0.175 + 0.315 + 0.175 + 0.15 + 0.16 + 0.09 = 3.465.

## Resumen

El código está muy por encima de la nota: el DAG tiene 21 tareas con tres bifurcaciones, la decisión de entrenamiento combina volumen, drift (PSI), categorías nuevas y degradación, la promoción reevalúa candidato y productivo sobre las mismas filas, y la API recarga el modelo con swap atómico, fallback y predicción de humo. Lo que más resta es la evidencia: el README muestra 14 imágenes coherentes entre sí, pero no hay ninguna de validaciones, drift, decisiones, RAW/CLEAN, auditoría, Model Registry, historial en Streamlit ni de la prueba de carga con Locust, y la regla de la rúbrica deja esos criterios en 3.0–3.5. Además el README es un documento de referencia, no una bitácora: faltan el análisis por lote, el análisis de la prueba de carga y el enlace al video.

## Lo que más subiría la nota

1. **Convertir el README en bitácora y agregar los dos análisis y el video** → C1 de 3.0 a 4.5 (+0.30). Narrar el desarrollo en orden (decisiones, problemas, cómo se resolvieron), una tabla por lote (qué llegó, validaciones, PSI, decisión y razón, métricas frente al productivo), un apartado de la prueba de carga con conclusiones sobre recursos, y el enlace de YouTube.
2. **Capturas de validaciones, drift y decisión de un lote** → C4 de 3.0 a 4.5 (+0.23). Una consulta a `audit.batch_history` con `schema_report`, `quality_report`, `drift_report`, `training_decision`, `training_reason` y el artefacto `decision/training_decision.json` en MLflow. El código ya lo produce; solo falta mostrarlo.
3. **Dejar correr los 6 lotes y mostrar las otras ramas** → C2 de 4.0 a 5.0 (+0.10), C7 de 3.5 a 4.5 (+0.05) y parte de C5. La grilla de Airflow solo muestra `train → promote`. Con el lote 3 (4.055 filas, menor que `MIN_BATCH_ROWS=5000`) debería salir `skip_training`. Capturar la grilla con las ramas distintas y el historial por lote de Streamlit.
4. **Evidencia de MLflow completa y commit real en los runs** → C5 de 3.5 a 4.5 (+0.10). Capturar el detalle de un run (métricas train/validation/test, `batches_used`, `training_reason`) y el Model Registry con el alias `production`. Pasar `--build-arg GIT_COMMIT=${{ github.sha }}` en el workflow: hoy la imagen queda con `GIT_COMMIT=unknown`.
5. **Ejecutar Locust y capturar su efecto** → C10 de 3.0 a 4.5 (+0.08) y C9. Captura de Locust (usuarios, RPS, fallos), Grafana durante la carga (RPS, latencia, errores, CPU/memoria), `kubectl get hpa` y `kubectl top pods`, y *Status → Targets* de Prometheus.
6. **Capturas de RAW, CLEAN y auditoría** → C3 de 3.5 a 4.5 (+0.08). `raw.batches` (número de lote, SHA-256, estado), `clean.records` con `batch_id` y `raw_record_id`, y `audit.batch_history_summary`.
7. **Fusionar `feat/monitoring-k8s` a `main` y actualizar la evidencia del clúster** → C9 de 3.5 a 4.5 (+0.05). Argo CD sincroniza `main`, donde Prometheus, Grafana y Jaeger todavía no existen. Tras el merge, nueva captura de pods y un `kubectl describe pod` con `Requests`/`Limits`. Evaluar desplegar Locust también en el clúster, que es lo que pide el enunciado.

## Evidencia del README (imagen por imagen)

| # | Documento:línea | Sección | Qué dice el texto | Qué muestra realmente la imagen | Veredicto |
|---|---|---|---|---|---|
| 1 | README.md:L7 | Encabezado | Arquitectura de la plataforma | Diagrama propio (Excalidraw): bloque CI/CD-GitOps (GitHub, Actions, Docker Hub, Argo CD), clúster con S1–S9, Jaeger, Data-API y Traefik, y Server B con Locust por Tailscale. Coherente con los manifiestos. Algunos logos se ven recortados al rasterizar el SVG (Grafana, Streamlit, GitHub Actions): conviene revisarlo en GitHub | ✅ |
| 2 | README.md:L318 | 1. CI/CD | Ejecución #9 en verde sobre `main` (`545df1b`): 3 `unit-tests`, 3 `publish-dockerhub` y `update-manifests` | Exactamente eso: run #9, `545df1b`, *Success*, 8m 28s, los siete jobs en verde | ✅ |
| 3 | README.md:L322 | 1. CI/CD | Repositorios `real-estate-*` del namespace `aristotekean` publicados por esa ejecución | Lista de repositorios con *Last pushed: about 1 hour ago*. No se ven los tags, así que no prueba el versionado por SHA. Aparecen además tres repos `diabetes-*` de otro proyecto (22 días) | ⚠️ |
| 4 | README.md:L328 | 2. Argo CD | Application `real-estate` *Healthy / Synced* contra `main` (`ea9922f`), auto-sync, Gateway y cuatro HTTPRoute | Coincide: *Healthy*, *Synced to main (ea9922f)*, *Auto sync is enabled*, *Sync OK* el 22-nov 19:39 con el commit `fix: tracert`; Gateway, 4 HTTPRoute, Services y pods | ✅ |
| 5 | README.md:L334 | 3. Clúster | Pods de `real-estate` en OrbStack (9 componentes) y los de `argocd` | Coincide: 21 pods en ejecución. No hay pods de Prometheus, Grafana ni Jaeger, y no se ven `requests`/`limits` | ⚠️ |
| 6 | README.md:L340 | 4. Airflow | Tres corridas programadas, una por lote, sin fallos; las tres por `train → promote` | Coincide: 3 corridas (19:15, 19:30, 19:45), 0 tareas fallidas, `no_new_batch`, `skip_training` y `reject_model` omitidas. Solo se recorre una rama | ⚠️ |
| 7 | README.md:L346 | 5. MLflow | Experimento `real-estate-pricing` con un run por lote, lanzados desde Airflow | Coincide: `batch-0`, `batch-1`, `batch-2`, origen `airflow`. Solo es la lista: no se ven métricas, parámetros, lote, commit ni razón | ⚠️ |
| 8 | README.md:L350 | 5. MLflow | Artefactos del modelo en el bucket `mlflow` de RustFS | Coincide: `code/`, `MLmodel`, `conda.yaml`, `input_example.json`, `model.skops` (58.9 MiB), `python_env.yaml`, `requirements.txt`, con fecha 2026-11-22 19:46 | ✅ |
| 9 | README.md:L356 | 6. API | `POST /predict` desde `/docs`: `200`, modelo `real_estate` versión 3, alias `production` | Coincide: `200`, `prediction 251387.44`, versión `3`, alias `production`, fecha 23-nov 00:53 GMT | ✅ |
| 10 | README.md:L360 | 6. API | Filas de `inference.logs`: estado, error, versión, tiempo, predicción y `trace_id`; rechazos `model_unavailable` y versiones 1, 2 y 3 | Coincide y cruza con las demás: la fila de la versión 3 tiene la misma predicción y tiempo que la imagen #9, y la fila de la versión 2 (242345.79, `trace_id ef5c88b1…`) coincide con las imágenes #11 y #14 | ✅ |
| 11 | README.md:L366 | 7. Frontend | Estimación con el modelo versión 2 y alias `production` | Coincide: `$242,346`, `real_estate v2`, alias, `request_id` y barra lateral con la API disponible | ✅ |
| 12 | README.md:L372 | 8. Observabilidad | Tasa de peticiones a `/predict` por estado en Prometheus | Gráfica de la consulta indicada, con picos de unos 0.007 req/s. Prueba que Prometheus recolecta, pero no muestra los *targets* ni tráfico de carga | ⚠️ |
| 13 | README.md:L376 | 8. Observabilidad | Dashboard de Grafana, fila **Tráfico HTTP** | Coincide con los paneles descritos, pero con 16 solicitudes en total y 0.18 req/s: no hay prueba de carga. No se ven los paneles de CPU/memoria | ⚠️ |
| 14 | README.md:L380 | 8. Observabilidad | Traza frontend → api-inference con `model.predict` e `inference_log.write`; su id es un `trace_id` de la imagen #10 | Coincide: 4 spans, 2 servicios, id `ef5c88b1…`. La URL es `localhost:16686`, no `jaeger.k8s.orb.local` como dice la tabla de servicios | ✅ |

14 imágenes, todas revisadas. Ninguna rota, repetida ni de otro proyecto; todas las fechas visibles son del 22-nov-2026.

## Lista de evidencia esperada

| # | Evidencia | Estado | Dónde / comentario |
|---|---|---|---|
| 1 | Diagrama de arquitectura propio | ✅ | Imagen #1 y dos diagramas Mermaid (README.md:L67, L106) |
| 2 | Ejecuciones de GitHub Actions en verde | ✅ | Imagen #2 |
| 3 | Imágenes en DockerHub con tags versionados | ⚠️ | Imagen #3: se ven los repositorios, no los tags. Abrir un repo y capturar la pestaña *Tags* con el SHA |
| 4 | Argo CD *Synced/Healthy* y sincronización por un cambio en Git | ✅ | Imagen #4: sincronizado al commit `ea9922f` con auto-sync |
| 5 | Recursos del clúster y evidencia de `requests`/`limits` | ⚠️ | Imagen #5: pods sí; falta `kubectl describe` o `kubectl top` |
| 6 | Airflow: varias ejecuciones con las distintas ramas | ⚠️ | Imagen #6: tres corridas, una sola rama |
| 7 | Validaciones y drift de un lote | ❌ | Sin imagen |
| 8 | Datos en RAW y CLEAN con trazabilidad | ❌ | Sin imagen |
| 9 | Tabla de auditoría o historial con decisiones | ❌ | Sin imagen |
| 10 | MLflow: runs con métricas, parámetros, lote, commit y razón | ⚠️ | Imagen #7: solo la lista de runs |
| 11 | MLflow: Model Registry con el productivo identificado | ❌ | Sin imagen |
| 12 | Un caso promovido y otro rechazado o no entrenado, con métricas | ⚠️ | Solo casos promovidos (imagen #6), sin comparación de métricas |
| 13 | FastAPI: `/docs` o inferencia, y recarga sin redespliegue | ⚠️ | Imagen #9 para la inferencia. La recarga se infiere de la imagen #10 (versiones 1 → 2 → 3), pero el texto no la demuestra ni muestra la edad del pod |
| 14 | Registro de inferencias | ✅ | Imagen #10 |
| 15 | Streamlit: predicción con versión del modelo | ✅ | Imagen #11 |
| 16 | Streamlit: historial por lote | ❌ | Sin imagen (el código existe) |
| 17 | Prometheus: targets activos | ⚠️ | Imagen #12 es una consulta, no *Status → Targets* |
| 18 | Locust: ejecución de la prueba | ❌ | Sin imagen |
| 19 | Grafana: efecto de la prueba de carga | ⚠️ | Imagen #13: dashboard con tráfico manual, sin carga |
| 20 | Enlace al video de YouTube | ❌ | No hay enlace; el commit `62a5d0f` retiró las notas del video |

Balance: 6 ✅ · 8 ⚠️ · 6 ❌.

## Detalle por criterio

### C1. Documentación — 3.0
**Cumple:** README completo y bien escrito, con diagrama propio (imagen #1), dos Mermaid coherentes con los manifiestos, tabla de recursos por componente (README.md:L137-L147) y una sección de evidencias cuyas descripciones coinciden con las imágenes. `docs/pipeline.md` justifica las reglas de entrenamiento y promoción, las particiones y el manejo de outliers.
**Falta o está mal:**
- No es una bitácora: es una guía de referencia (ruta rápida, operación, solución de problemas). No narra el orden del desarrollo, las decisiones ni los problemas encontrados. El historial de git tiene material de sobra (cambio de MinIO a RustFS, de ingress-nginx a Gateway API, digest amd64 de la API de datos, endurecimiento de la API).
- Sin análisis por lote. `docs/pipeline.md:L17-L28` describe el perfil de los 6 lotes "medido al construir el pipeline", pero no hay resultados de las corridas: qué se validó, qué PSI salió, qué se decidió y con qué métricas.
- Sin análisis de la prueba de carga ni conclusiones sobre recursos.
- Sin enlace al video.
- Faltan 6 de los 20 ítems de evidencia y 8 están débiles.
- La elección del modelo y de la métrica prioritaria se justifica en una línea (README.md:L203-L204); la elección de `requests`/`limits` no se justifica.
- La tabla de componentes (README.md:L137-L147) y el Mermaid de Kubernetes no incluyen Prometheus, Grafana ni Jaeger, que el mismo README dice que corren en el clúster.

### C2. Orquestación con Airflow — 4.0
**Cumple:**
- 21 tareas de responsabilidad única (`services/airflow/dags/real_estate_training.py:219-245`) con tres bifurcaciones: sin lote nuevo (`:91-95`), entrenar o no (`:143-147`) y promover o rechazar (`:184-186`).
- Un lote por corrida: `fetch_and_land` llama a la API una vez (`real_estate_pipeline/raw.py:128`) y reanuda un lote sin terminar sin volver a llamarla (`raw.py:88-124`); `max_active_runs=1` (`real_estate_training.py:68`).
- Cliente robusto (`real_estate_pipeline/api_client.py:62-124`): timeouts, reintentos con backoff en errores de red y 5xx, fin de datos (400), respuesta vacía y validación de forma.
- `retries=2` con backoff y `on_failure_callback` (`real_estate_training.py:71-75`). No hay `except` que oculte errores.
- Evidencia: imagen #6.

**Falta o está mal:**
- La evidencia solo muestra la rama `train → promote` (imagen #6). Faltan `skip_training`, `reject_model` y `no_new_batch`.
- Riesgo real: un reintento tras un timeout de lectura puede perder un lote, porque la API tiene estado. Solo queda como aviso en `missing_api_batches` (`raw.py:225-234`). Vale documentarlo como limitación conocida.

### C3. Datos — 3.5
**Cumple:**
- RAW sin modificar: `raw.batches` guarda el cuerpo en gzip con SHA-256, número de lote, estado y conteo (`infra/database/migrations/001_raw_clean_audit.sql:23-51`); `raw.records` en JSONB (`:59-74`); `raw.api_requests` registra cada petición (`004_api_requests.sql:9-27`).
- CLEAN en esquema propio, con FK a `raw.records` y `raw.batches` (`001_raw_clean_audit.sql:86-111`).
- Control de reproceso: UNIQUE `(group_number, api_batch_number, response_sha256)` (`:50`) y duplicados por fila (`real_estate_pipeline/validation.py:132-163`).
- Auditoría: `audit.batch_history` con reportes, decisión, razón, ids de MLflow y promoción (`:122-155`).
- Inferencias: `inference.logs` con todos los campos de RF8 (`002_inference_logs.sql:6-28`). Evidencia: imagen #10.
- MLflow usa `mlflow-db`, una instancia distinta.

**Falta o está mal:**
- No hay imágenes de RAW, CLEAN ni de la auditoría (ítems 8 y 9).
- Los registros válidos de un lote marcado `invalid_batch` igual entran a CLEAN y a los entrenamientos posteriores (`real_estate_pipeline/clean.py:35-44`, `modeling.py:91`), lo que contradice el texto de la regla ("no se usa para entrenar", `rules.py:131-132`). Filtrar por validez del lote en `load_clean` o corregir el mensaje.
- Queda la columna `probabilities` del andamio de clasificación (`002_inference_logs.sql:11-12`).

### C4. Validación y decisión de entrenamiento — 3.0
**Cumple (en código):**
- Esquema: columnas y tipos contra el contrato (`real_estate_pipeline/validation.py:73-95`).
- Calidad: nulos, duplicados, reglas por fila, rangos y consistencia del target (`validation.py:132-224`, `ml/real_estate_model/src/real_estate_model/contract.py:146-209`).
- Drift: PSI contra el histórico de CLEAN, umbral 0.20 sobre 7 variables (`validation.py:387-425`, `settings.py:69-71`).
- Categorías nuevas: `status`, `state`, `city` con participación ≥ 1 % (`validation.py:319-378`); el modelo las tolera con `handle_unknown="ignore"`, `unknown_value=-1` y `TargetEncoder` (`models.py:118-134`).
- Decisión explícita y no trivial (`real_estate_pipeline/rules.py:57-191`): dos vetos, línea base, lote pequeño y cuatro disparadores (crecimiento ≥ 20 %, PSI, categorías nuevas, degradación ≥ 10 % del productivo sobre el lote nuevo). No depende de la periodicidad ni solo del conteo, así que no aplica el tope de 1.5.
- La razón queda en la auditoría y en MLflow (`decision.py:108-114`, `modeling.py:277-293`).

**Falta o está mal:**
- No hay una sola imagen de validaciones, drift o decisiones (ítems 7, 9 y 12). Por la regla de evidencia el criterio no pasa de 3.0, aunque el código merece más.
- El volumen se calcula dentro de la decisión, no como validación propia.
- Con los lotes cortados por rangos, el PSI supera 0.20 casi siempre: conviene analizar en el informe si el drift dispara en todos los lotes y si el umbral discrimina.

### C5. MLflow, comparación y promoción — 3.5
**Cumple:**
- Backend PostgreSQL (`mlflow-db`) y artefactos en RustFS, con el bucket creado por el Job `rustfs-init` (README.md:L92). Evidencia: imágenes #7 y #8.
- Cada run registra lotes usados, parámetros, métricas train/validation/test, gráficos, importancia por permutación, razón y el modelo en formato MLflow Models (`real_estate_pipeline/modeling.py:245-330`, `:371-460`).
- Promoción contra el productivo actual: el campeón se carga por alias y se reevalúa sobre las mismas filas que el candidato (`modeling.py:543-562`). Regla: MAE mejora ≥ 3 % y RMSE no empeora más de 1 % (`rules.py:236-238`).
- Métricas comparables: partición estable por hash con sal de la dirección (`ml/real_estate_model/src/real_estate_model/splits.py:5-27`).
- El rechazado queda registrado con `promotion_decision=rejected` (`modeling.py:632-634`); productivo por alias `production` (`modeling.py:612`).

**Falta o está mal:**
- El commit no llega a MLflow: `services/airflow/Dockerfile:35` declara `ARG GIT_COMMIT=unknown` y el workflow no pasa ese argumento. Los runs quedan con `unknown`. Agregar `build-args: GIT_COMMIT=${{ github.sha }}` al paso de build.
- Evidencia débil: la imagen #7 no muestra métricas, parámetros ni razón, y no hay imagen del Model Registry ni de un candidato rechazado.
- La comparación usa la partición de validación, la misma con la que se elige el candidato; el test solo se reporta. Es defendible, pero hay que justificarlo en el informe.
- `models.py:40-44` declara `PRIMARY_METRIC="validation_rmsle"` mientras el pipeline usa MAE (`settings.py:98`): unificar.

### C6. Inferencia con FastAPI — 4.0
**Cumple:**
- Carga por alias desde MLflow, sin versión ni ruta fija (`services/api-inference/src/api_inference/model.py:125`, `:261`).
- Recarga sin redespliegue: sondeo cada 60 s (`main.py:101-113`) y `POST /model/reload` (`main.py:282-297`); `asyncio.Lock` para las cargas y swap por asignación de referencia (`model.py:189`, `:289`); el modelo anterior sigue sirviendo si la carga falla; predicción de humo antes del swap (`model.py:299-308`).
- `/health`, `/predict`, `/metrics`, más `/live`, `/ready`, `/model-info` y `/history`; Pydantic estricto con `extra="forbid"` (`schemas.py:71-112`).
- Registro de cada inferencia, también las fallidas, con spool si la base cae (`predict.py:133-214`).
- Métricas de peticiones, latencia, errores y `model_info{name,version,alias}` (`metrics.py:33-83`).
- Evidencia: imágenes #9, #10 y #14.

**Falta o está mal:**
- La recarga sin redespliegue no se demuestra de forma explícita. La imagen #10 muestra las versiones 1, 2 y 3, pero falta una captura con la edad del pod (`kubectl get pods`) antes y después del cambio de versión, y el texto que lo afirme.
- `POST /model/reload` no tiene autenticación (la rúbrica habla de "endpoint administrativo protegido"). Como el mecanismo principal es el sondeo, es menor: protegerlo con un token o no exponerlo por el Gateway.

### C7. Streamlit — 3.5
**Cumple:** formulario que consume `/predict` y muestra modelo, versión y alias (`services/frontend/src/frontend/app.py:125-205`; imagen #11). El historial por lote existe y cubre decisión, razones, promoción, deltas de métricas e ids de MLflow (`app.py:211-270`), leyendo `/history`.
**Falta o está mal:** no hay imagen del historial (ítem 16). Sin ella no se puede comprobar que cubra los casos reales de los lotes.

### C8. CI/CD y GitOps — 4.5
**Cumple:**
- Un workflow con matriz que prueba y publica las tres imágenes propias en Docker Hub, multi-arquitectura, con tags `latest` y SHA y credenciales en `secrets` (`.github/workflows/ci-cd-unit-tests-dockerhub.yml:58-96`).
- `update-manifests` escribe el SHA en `infra/k8s/kustomization.yaml` (`:104-141`); hoy los tres `newTag` son `545df1b…` (`infra/k8s/kustomization.yaml:60-68`), el mismo commit de la imagen #2.
- Application de Argo CD con `automated`, `prune` y `selfHeal` sobre `main` (`infra/argocd/application.yaml:19-29`). Evidencia: imágenes #2 y #4.
- Sin `imagePullPolicy: Never` ni imágenes locales en Kubernetes.

**Falta o está mal:**
- La imagen #3 no muestra los tags por SHA.
- Las pruebas de Airflow no corren en el CI, y son las que cubren las reglas de decisión.
- `docker-compose.yml:204-206` y `:254-256` usan `build:`. Compose es un mecanismo auxiliar (README.md:L274), por eso no se penaliza más; conviene decirlo así en el informe.

### C9. Kubernetes — 3.5
**Cumple:** todos los contenedores e initContainers de `infra/k8s/base` declaran `requests` y `limits` de CPU y memoria; probes en todos los Deployments y StatefulSets; PVC por `volumeClaimTemplates`; ConfigMaps; Secrets fuera de git sin contraseñas en claro en los manifiestos; HPA de la API (`infra/k8s/base/api-inference/hpa.yaml:24-32`). La API de datos está en el clúster.
**Falta o está mal:**
- Locust no está en Kubernetes: solo existe `infra/load-testing/docker-compose.yml`. La justificación (README.md:L86, L100) es técnicamente razonable, pero la rúbrica pide todos los componentes en el clúster.
- Prometheus, Grafana y Jaeger en Kubernetes solo existen en la rama `feat/monitoring-k8s`. Argo CD sincroniza `main`, y la imagen #5 no muestra esos pods.
- Sin imagen de `requests`/`limits` aplicados (ítem 5).
- Traefik no declara límite de CPU (`infra/k8s/traefik/deployment.yaml:59-64`).

### C10. Observabilidad y Locust — 3.0
**Cumple:** métricas de la API (`metrics.py:33-83`); Prometheus con descubrimiento de pods (`infra/k8s/base/prometheus/prometheus.yml:19-44`); dashboard versionado y aprovisionado por ConfigMap (`infra/k8s/base/grafana/dashboards/real-estate-api.json`, `base/grafana/kustomization.yaml:17-24`); `locustfile` con carga por escalones (`infra/load-testing/locustfile.py:66-107`); trazas en Jaeger (imagen #14).
**Falta o está mal:**
- No hay evidencia de que Locust se haya ejecutado (ítem 18).
- Grafana muestra 16 solicitudes y 0.18 req/s (imagen #13): no hay efecto de carga que analizar (ítem 19).
- Prometheus no muestra los *targets* (imagen #12).
- El dashboard no tiene panel de la versión del modelo: la métrica `model_info` se exporta pero no se grafica.
- CPU y memoria salen de `process_*` del proceso de la API, no del contenedor.

### C11. Código y manejo del repositorio — 4.0
**Cumple:** organización por componentes; lógica de decisión separada en funciones puras; 169 pruebas en cuatro suites; `.gitignore` adecuado; sin secretos, binarios ni datasets versionados; 4 PRs fusionados, 4 ramas remotas y commits convencionales (4 % de mensajes genéricos).
**Falta o está mal:**
- El aporte está concentrado: `aristotekean` 27 commits y ~21.000 líneas; Juan Camilo Martínez 1 commit (5.137 líneas); Jorge Gil 2 commits (1.582 líneas). `Kevin Bueno` y `aristotekean` usan el mismo correo, así que parecen la misma persona con dos identidades de git: unificarlas con un `.mailmap`.
- Restos del andamio: menciones a MinIO (README.md:L284, `infra/monitoring/prometheus/prometheus.yml:10`) y el comentario desactualizado de `infra/k8s/kustomization.yaml:59`. `CLAUDE.md` todavía dice que el README describe el proyecto de diabetes, y ya no es cierto.

### C12. Puntualidad — 3.0
**Cumple:** todo antes de la fecha límite; sin fechas futuras ni discordantes.
**Falta o está mal:** el 100 % de los commits está en las últimas 72 h (dos días de actividad: 21 y 22 de noviembre). Con más del 80 % concentrado, el nivel es 3.0. Quedan cuatro días: los commits que se hagan repartidos hasta el 26 mejoran esta proporción.

## Kubernetes: recursos y probes

| Workload | Tipo | requests / limits | Probes | Secrets | Credenciales en claro |
|---|---|---|---|---|---|
| airflow (4 contenedores + init) | Deployment | Sí | api-server: startup, readiness, liveness; resto: liveness | Sí | No |
| airflow-init | Job | Sí | No aplica | Sí | No |
| api-inference (+ init) | Deployment + HPA | Sí | startup, readiness, liveness | Sí | No |
| frontend | Deployment | Sí | startup, readiness, liveness | No maneja | No |
| mlflow (+ init) | Deployment | Sí | startup, readiness, liveness | Sí | No |
| data-api | Deployment | Sí | startup, readiness, liveness | No maneja | No |
| mlflow-db, airflow-db, postgres-train | StatefulSet | Sí | readiness, liveness | Sí | No |
| train-db-migrate | Job | Sí | No aplica | Sí | No |
| rustfs / rustfs-init | StatefulSet / Job | Sí | readiness, liveness / no aplica | Sí | No |
| prometheus (solo en esta rama) | StatefulSet | Sí | startup, readiness, liveness | No maneja | No |
| grafana (solo en esta rama) | Deployment | Sí | startup, readiness, liveness | Sí | No |
| jaeger (solo en esta rama) | Deployment | Sí | readiness, liveness | No maneja | No |
| traefik (fuera de la Application) | Deployment | Sin límite de CPU | readiness, liveness | No maneja | No |
| Locust | — | No está en Kubernetes | — | — | — |

Las imágenes propias van sin tag en los manifiestos porque kustomize las fija por SHA (`infra/k8s/kustomization.yaml:60-68`); no es un uso de `latest`.

## Alertas

- **Rama sin fusionar.** `feat/monitoring-k8s` tiene 2 commits que no están en `main` (`f26a73e`, `06344fb`). Están publicados en GitHub, pero el profesor verá `main` por defecto y Argo CD solo sincroniza `main`: Prometheus, Grafana y Jaeger en el clúster no existen hasta el merge.
- **Evidencia de monitoreo anterior al cambio.** Las capturas de Grafana, Prometheus y Jaeger (19:40–20:06) son anteriores al commit que lleva el monitoreo al clúster (21:03), la imagen #5 no muestra esos pods y la #14 usa `localhost:16686`. El README afirma que corren en Kubernetes: hay que volver a tomar esas capturas desde el clúster.
- **Imagen de la API de datos.** La rúbrica indica `cristiandiaz13/mlops-puj:data-api-pf-v1`; el repo usa `data-api-pf-v2` fijada por digest (`infra/k8s/base/data-api/deployment.yaml:37`, README.md:L197). Confirmar con el profesor cuál es la válida.
- **Sin enlace al video de YouTube.**
- **Credenciales por defecto documentadas** en el README (`airflow`/`airflow`, `app`/`app`, `mlflow`/`mlflow`, `admin`/`admin`). No son secretos versionados y el README advierte que son de desarrollo (README.md:L296).
- **`.env.example` no se pudo revisar a mano**; el recolector no reportó posibles secretos en él.
- No se encontraron instrucciones dirigidas al evaluador, commits después del límite ni fechas sospechosas. No hay cambios sin commit ni commits sin push.

## Historial del repositorio

- 36 commits en todas las ramas (27 en la rama evaluada), en 2 días con actividad: 8 el 21-nov y 19 el 22-nov.
- Concentración: 74 % en las últimas 24 h y 100 % en las últimas 72 h.
- Aporte por integrante, sin contar el bot: `aristotekean` 27 commits y 20.982 líneas; `Kevin Bueno` (mismo correo) 4 merges y 224 líneas; Juan Camilo Martínez 1 commit y 5.137 líneas; Jorge Gil 2 commits y 1.582 líneas.
- 4 PRs fusionados, 4 ramas remotas, sin tags.
- Mensajes genéricos: 1 de 27 (4 %).
- 2 commits del bot de despliegue (`chore(infra): deploy real-estate images <sha>`).

---
*Evaluación automática orientativa basada en la rúbrica del curso. No ejecuta el proyecto: revisa el código, el historial y la evidencia. La nota oficial la asigna el profesor.*
