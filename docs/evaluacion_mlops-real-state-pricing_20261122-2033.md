# Evaluación — Proyecto Final MLOps

**Repositorio:** git@github.com:aristotekean/mlops-real-state-pricing.git  ·  **Commit evaluado:** 547c020 (2026-11-22 20:31 -05:00, rama `main`)  ·  **Evaluado el:** 2026-11-22 20:33 -05:00
**Fecha límite:** 2026-11-26T23:59:59-05:00  ·  **Estado:** Provisional (antes de la fecha límite)

## Nota: 3.3 / 5.0

| Criterio | Peso | Nota | Aporte |
|---|---|---|---|
| C1. Documentación: informe y bitácora | 20 % | 2.5 | 0.50 |
| C2. Orquestación con Airflow | 10 % | 3.5 | 0.35 |
| C3. Datos: RAW, CLEAN, trazabilidad e inferencias | 8 % | 3.5 | 0.28 |
| C4. Validación y decisión de entrenamiento | 15 % | 3.0 | 0.45 |
| C5. MLflow, comparación y promoción | 10 % | 3.5 | 0.35 |
| C6. Inferencia con FastAPI | 8 % | 4.0 | 0.32 |
| C7. Streamlit | 5 % | 3.5 | 0.18 |
| C8. CI/CD y GitOps | 7 % | 4.5 | 0.32 |
| C9. Kubernetes | 5 % | 3.0 | 0.15 |
| C10. Observabilidad y Locust | 5 % | 2.5 | 0.13 |
| C11. Código y manejo del repositorio | 4 % | 4.0 | 0.16 |
| C12. Puntualidad | 3 % | 3.0 | 0.09 |
| **Nota base** | | | **3.27** |
| Topes aplicados | | | Ninguno |
| **Nota final** | | | **3.3** |

## Resumen
El código está por encima de la nota: el DAG tiene 21 tareas de responsabilidad única con tres bifurcaciones, la decisión de entrenamiento combina cuatro disparadores técnicos, la promoción compara contra el productivo sobre las mismas filas y la API recarga el alias con swap atómico. Lo que más resta es la evidencia y el informe: el README es una guía de operación con 14 capturas coherentes, pero solo muestra 3 de los 6 lotes, todos por la misma rama (entrenar y promover), y no tiene análisis por lote, prueba de carga, historial en Streamlit, Model Registry ni enlace al video. Además, Prometheus, Grafana y Locust corren en compose y no en Kubernetes.

## Lo que más subiría la nota
1. **Correr los 6 lotes y capturar las ramas distintas.** El lote 3 (4.055 filas, `docs/pipeline.md:24`) es menor que `MIN_BATCH_ROWS=5000`, así que debe recorrer `skip_training`. Capturar la grilla de Airflow con esa rama (y `reject_model` si ocurre), la consulta de `audit.batch_history_summary` y un `drift_report` de un lote → C2 de 3.5 a 4.5 (+0.10), C3 de 3.5 a 4.5 (+0.08), C4 de 3.0 a 4.5 (+0.23).
2. **Reescribir el README como informe y bitácora**: orden del desarrollo, problemas y cómo se resolvieron (ya hay material: migración MinIO → RustFS, ingress-nginx → Gateway API, digest de `data-api` para arm64), justificación de umbrales, una tabla de análisis por lote (qué llegó, drift, decisión, métricas vs. productivo) y el enlace al video de YouTube → C1 de 2.5 a 4.5 (+0.40).
3. **Ejecutar Locust y documentarlo**: captura de Locust (usuarios, RPS, fallos), Grafana durante la carga (RPS, latencia, errores, CPU y memoria del pod, réplicas del HPA), Prometheus en *Status → Targets* y un párrafo de conclusiones sobre recursos → C10 de 2.5 a 4.5 (+0.10) y suma a C1.
4. **Capturas de MLflow que faltan**: detalle de un run (métricas, parámetros, `batches_used`, `git_commit`, `training_reason`), Model Registry con las versiones y el alias `production`, y un caso rechazado o no entrenado con su comparación → C5 de 3.5 a 4.5 (+0.10).
5. **Mover Prometheus, Grafana y Locust a Kubernetes** (manifiestos en `infra/k8s/base`, dashboard como ConfigMap) y agregar evidencia de `requests`/`limits` (`kubectl describe` o `kubectl top pods`) → C9 de 3.0 a 4.5 (+0.08).
6. **Capturar el historial en Streamlit y la recarga sin redespliegue**: pestaña *Historial de entrenamiento* con los 6 lotes; `/model-info` antes y después de una promoción junto a `kubectl get pods` con la misma edad del pod; proteger `POST /model/reload` con un token → C7 de 3.5 a 4.5 (+0.05) y C6 de 4.0 a 5.0 (+0.08).
7. **Mostrar los tags por SHA en Docker Hub** (pestaña *Tags* de una imagen) y repartir commits entre los integrantes en los días que quedan → C8 de 4.5 a 5.0 (+0.04), C11 y C12.

## Evidencia del README (imagen por imagen)

| # | Documento:línea | Sección | Qué dice el texto | Qué muestra realmente la imagen | Veredicto |
|---|---|---|---|---|---|
| 1 | README.md:L7 | Encabezado | Arquitectura de la plataforma | SVG propio: Server A (Kubernetes, S1–S7, Traefik, Jaeger), Server B (Locust), Server C (Prometheus, Grafana), GitHub Actions, Docker Hub y Argo CD. Coherente con los manifiestos | ✅ |
| 2 | README.md:L313 | 1. CI/CD | Ejecución #9 en verde sobre `main` (`545df1b`) | Run #9, *Success*, 3 `unit-tests`, 3 `publish-dockerhub` y `update-manifests`; commit `545df1b` | ✅ |
| 3 | README.md:L317 | 1. CI/CD | Repos `real-estate-*` publicados por esa ejecución | Los tres repos con "about 1 hour ago". No se ven los tags; aparecen además repos `diabetes-*` de otro proyecto | ⚠️ |
| 4 | README.md:L323 | 2. Argo CD | Application `real-estate` *Healthy / Synced* contra `main` (`ea9922f`) con auto-sync | *Healthy*, *Synced to main (ea9922f)*, "Auto sync is enabled", *Sync OK* 19:39 con el comentario del commit; Gateway, 4 HTTPRoutes, Services y pods | ✅ |
| 5 | README.md:L329 | 3. Clúster | Pods de `real-estate` y `argocd` en OrbStack | Lista de pods de ambos namespaces en ejecución. El panel de estadísticas es del pod de Argo CD; no hay `requests`/`limits` | ⚠️ |
| 6 | README.md:L335 | 4. DAG en Airflow | Tres corridas, una por lote, todas por `train_candidate_model` → `promote_model` | Grilla con 3 corridas exitosas (19:15, 19:30, 19:45); `no_new_batch`, `skip_training` y `reject_model` omitidas en las tres. Una sola rama recorrida | ⚠️ |
| 7 | README.md:L341 | 5. MLflow | Un run por lote (`batch-0`, `batch-1`, `batch-2`) | Lista de 3 runs con origen `airflow`; sin columnas de métricas, parámetros ni tags | ⚠️ |
| 8 | README.md:L345 | 5. MLflow | Artefactos del modelo en el bucket `mlflow` de RustFS | `MLmodel`, `model.skops` (58.9 MiB), `conda.yaml`, `requirements.txt`, `code/`; fechas 19:46, coherentes con la tercera corrida | ✅ |
| 9 | README.md:L351 | 6. API | `POST /predict` con `200`, modelo `real_estate` v3, alias `production` | Swagger con respuesta `200`, predicción 251387.44, versión 3, alias `production` | ✅ |
| 10 | README.md:L355 | 6. API | Filas de `inference.logs` con estado, error, versión, tiempo, predicción y `trace_id` | Consulta SQL con 20 filas: `model_unavailable` antes del primer modelo y predicciones de las versiones 1, 2 y 3. La fila 1 coincide con la imagen #9 | ✅ |
| 11 | README.md:L361 | 7. Frontend | Estimación con `real_estate` v2 y alias `production` | Streamlit con $242,346, `real_estate v2`, alias `production` y API disponible. Coincide con la fila 3 de la imagen #10 | ✅ |
| 12 | README.md:L367 | 8. Observabilidad | Tasa de peticiones a `/predict` por estado en Prometheus | Gráfica de la consulta con picos de 0.007 req/s. Prueba que Prometheus recibe métricas, pero no es la vista de *targets* | ⚠️ |
| 13 | README.md:L371 | 8. Observabilidad | Dashboard de Grafana, fila Tráfico HTTP | 16 solicitudes totales, 0.18 req/s, p99 38 ms, 2 errores. Tráfico manual, no una prueba de carga | ⚠️ |
| 14 | README.md:L375 | 8. Observabilidad | Traza en Jaeger frontend → api-inference con `model.predict` e `inference_log.write` | Traza `ef5c88b1…` con 2 servicios y 4 spans; el id coincide con el `trace_id` de la fila 3 de la imagen #10 | ✅ |

Las 14 imágenes cargan, tienen texto alternativo y descripción, y sus horas, versiones e identificadores son coherentes entre sí y con el código. No hay señales de evidencia fabricada.

## Lista de evidencia esperada

| # | Evidencia | Estado | Dónde / comentario |
|---|---|---|---|
| 1 | Diagrama de arquitectura propio | ✅ | Imagen #1 y dos diagramas Mermaid (README.md:L67, L104) |
| 2 | Ejecuciones de GitHub Actions en verde | ✅ | Imagen #2 |
| 3 | Imágenes publicadas en DockerHub con tags versionados | ⚠️ | Imagen #3: se ven los repos, no los tags por SHA |
| 4 | Argo CD *Synced/Healthy* y sincronización por un cambio en Git | ✅ | Imagen #4 |
| 5 | Recursos del clúster y evidencia de `requests`/`limits` | ⚠️ | Imagen #5: pods sí; `requests`/`limits` solo en la tabla de README.md:L135 |
| 6 | Airflow: varias ejecuciones con distintas ramas | ⚠️ | Imagen #6: 3 corridas, una sola rama |
| 7 | Resultados de validaciones y drift de un lote | ❌ | No hay imagen |
| 8 | Datos en RAW y CLEAN con trazabilidad por lote | ❌ | No hay imagen |
| 9 | Tabla de auditoría o historial de lotes con decisiones | ❌ | No hay imagen |
| 10 | MLflow: runs con métricas, parámetros, lote, commit y razón | ⚠️ | Imagen #7: solo la lista de runs |
| 11 | MLflow: Model Registry con versiones y productivo | ❌ | No hay imagen |
| 12 | Entrenado y promovido vs. rechazado o no entrenado | ❌ | Solo casos promovidos (imagen #6) |
| 13 | FastAPI: inferencia y recarga sin redespliegue | ⚠️ | Imagen #9: inferencia sí. La recarga se infiere de las versiones 1 → 2 → 3 de la imagen #10, sin mostrar el mismo pod |
| 14 | Registro de inferencias | ✅ | Imagen #10 |
| 15 | Streamlit: predicción con versión del modelo | ✅ | Imagen #11 |
| 16 | Streamlit: historial por lote | ❌ | No hay imagen |
| 17 | Prometheus: targets activos | ⚠️ | Imagen #12: una consulta, no la vista de targets |
| 18 | Locust: ejecución de la prueba | ❌ | No hay imagen |
| 19 | Grafana: efecto de la prueba de carga | ⚠️ | Imagen #13: dashboard con tráfico manual |
| 20 | Enlace al video de YouTube | ❌ | No hay enlace en el README |

Resumen: 7 ✅ · 6 ⚠️ · 7 ❌.

## Detalle por criterio

### C1. Documentación — 2.5
**Cumple:** diagrama de arquitectura propio y coherente (imagen #1, README.md:L67-L131); sección *Evidencias* con 14 capturas descritas con precisión (README.md:L305-L375); tabla de recursos por componente (README.md:L135-L145); reglas de decisión y de promoción documentadas en `docs/pipeline.md:99-129` con sus parámetros (`docs/pipeline.md:146-165`).
**Falta o está mal:**
- El README es una guía de operación (*Ruta rápida*, *Operación*, *Solución de problemas*), no una bitácora: no narra el orden del desarrollo, los problemas ni las decisiones. Agregar una sección cronológica.
- No hay análisis por lote. `docs/pipeline.md:19-26` describe el perfil de los 6 lotes, pero no qué se validó, qué drift hubo ni qué se decidió en cada uno.
- No hay análisis de la prueba de carga ni conclusión sobre recursos.
- Los umbrales se listan sin justificar por qué PSI 0.20, crecimiento 20 % o mejora de MAE 3 %; tampoco se justifica la elección de recursos.
- No hay enlace al video de YouTube.
- 7 de los 20 ítems de evidencia están ausentes y 6 son débiles.

### C2. Orquestación con Airflow — 3.5
**Cumple:** DAG `real_estate_incremental_training` con las tareas de referencia más `check_new_batch` y `no_new_batch`, y tres bifurcaciones (`services/airflow/dags/real_estate_training.py:91`, `:143`, `:184`); un lote por corrida con `max_active_runs=1` (`:68`); `retries=2` con backoff exponencial y `on_failure_callback` que deja el lote `failed` sin ocultar el error (`:43-54`, `:69-76`); cliente con timeouts, reintentos de errores de red y 5xx, fin de datos, respuesta vacía y validación de forma (`services/airflow/dags/real_estate_pipeline/api_client.py:61-131`).
**Falta o está mal:** la evidencia (imagen #6) muestra tres corridas por la misma rama; faltan `skip_training`, `reject_model` y `no_new_batch`. Solo se recolectaron 3 de los 6 lotes. Correr los restantes y capturar la grilla y el grafo.

### C3. Datos — 3.5
**Cumple:** `raw.batches` guarda la respuesta original en gzip con SHA-256 y número de lote, con restricción de unicidad contra reprocesos (`infra/database/migrations/001_raw_clean_audit.sql:23-50`); `raw.records` en JSONB (`:59`); `clean.records` en otro esquema con `raw_record_id` y `batch_id` (`:86-89`); `audit.batch_history` y su vista (`:122`, `:161`); `inference.logs` con fecha, versión, estado y error (`infra/database/migrations/002_inference_logs.sql:6-26`); `raw.api_requests` audita cada petición (`004_api_requests.sql:9`); `mlflow-db` es una instancia separada.
**Falta o está mal:** solo hay evidencia de `inference.logs` (imagen #10). Agregar consultas de `raw.batches`, `clean.records` agrupado por lote y `audit.batch_history_summary`.

### C4. Validación y decisión de entrenamiento — 3.0
**Cumple:** validación de esquema, calidad (nulos, duplicados dentro del lote y contra lotes anteriores, rangos) y PSI numérico y categórico (`services/airflow/dags/real_estate_pipeline/validation.py:66`, `:111`, `:387-447`); categorías nuevas con su proporción del lote (`:319`); `handle_unknown='ignore'` y `TargetEncoder` (`docs/pipeline.md:95-97`); la decisión combina lote inválido, suficiencia, línea base, lote pequeño y cuatro disparadores (volumen, drift, categorías nuevas, degradación del productivo) en `services/airflow/dags/real_estate_pipeline/rules.py:125-188`, sin depender de la periodicidad ni del conteo bruto; la razón queda en la auditoría y en MLflow (`modeling.py:277`, `:293`).
**Falta o está mal:** no hay ninguna imagen del resultado de las validaciones, del drift ni de una decisión con su razón, así que el criterio queda en 3.0 por la regla de evidencia. Capturar el log de `detect_data_drift` y `decide_training`, o el artefacto `decision/training_decision.json` en MLflow, para un lote que entrena y uno que no.

### C5. MLflow, comparación y promoción — 3.5
**Cumple:** backend en PostgreSQL y artefactos en RustFS (S3), con el bucket creado por el Job `rustfs-init`; cada run registra `batches_used`, `git_commit`, parámetros, métricas, artefactos y razón (`services/airflow/dags/real_estate_pipeline/modeling.py:243-293`, `:496-504`); la comparación carga candidato y productivo desde el registro y los evalúa sobre las mismas filas de validación (`modeling.py:529-559`), con particiones estables por hash de la dirección (`docs/pipeline.md:93`), así que las métricas son comparables; regla explícita MAE ≥ 3 % y RMSE ≤ 1 % (`rules.py:204-248`); alias `production` (`modeling.py:612`); el rechazado queda etiquetado sin mover el alias (`modeling.py:629-636`).
**Falta o está mal:** la imagen #7 no muestra métricas, parámetros ni tags; no hay captura del Model Registry ni de un candidato rechazado. No se documentan las políticas del bucket.

### C6. Inferencia con FastAPI — 4.0
**Cumple:** carga `models:/real_estate@production` por alias (`services/api-inference/src/api_inference/model.py:120-129`); sondeo periódico del alias y `POST /model/reload` (`main.py:282`); `asyncio.Lock` para serializar recargas, swap atómico de la referencia y smoke test antes del cambio, con el modelo anterior como fallback si la carga falla (`model.py:189`, `:208-227`, `:288-306`); `/health`, `/predict` y `/metrics`; Pydantic; métricas de peticiones, latencia, predicciones, versión del modelo y recargas (`metrics.py:33-81`). Evidencia en las imágenes #9 y #10.
**Falta o está mal:** `POST /model/reload` no tiene autenticación (la rúbrica pide un endpoint administrativo protegido). La recarga sin redespliegue no se demuestra de forma explícita: falta `/model-info` antes y después con la edad del pod.

### C7. Streamlit — 3.5
**Cumple:** formulario que consume la API y muestra la versión y el alias (`services/frontend/src/frontend/app.py:203`; imagen #11); pestaña *Historial de entrenamiento* con decisión, razón, promoción, versiones antes y después y deltas de métricas (`app.py:223-301`).
**Falta o está mal:** no hay captura del historial, y con 3 lotes promovidos no cubriría los casos de rechazo o no entrenamiento.

### C8. CI/CD y GitOps — 4.5
**Cumple:** workflow con matriz que construye y publica las tres imágenes propias con tag `latest` y SHA, credenciales en `secrets` (`.github/workflows/ci-cd-unit-tests-dockerhub.yml:62-98`); el job `update-manifests` fija el SHA en `infra/k8s/kustomization.yaml` (`:119-141`), que hoy referencia `545df1b…` en los tres `newTag`; Application de Argo CD con `automated`, `prune` y `selfHeal` apuntando al repo (`infra/argocd/application.yaml`). Evidencia en las imágenes #2, #3 y #4.
**Falta o está mal:** la imagen #3 no muestra los tags versionados. El `build:` de `docker-compose.yml:203` y `:253` es para desarrollo local y no es el despliegue principal, pero conviene aclararlo en el README. Las pruebas de Airflow no corren en CI.

### C9. Kubernetes — 3.0
**Cumple:** los 13 workloads declaran `requests` y `limits`, los de larga vida tienen probes y ninguno lleva credenciales en claro; Secrets fuera de git, PVC por `volumeClaimTemplates`, ConfigMaps y HPA para la API.
**Falta o está mal:** Prometheus, Grafana y Locust no están en Kubernetes: corren con compose (`infra/monitoring/docker-compose.yml`, `infra/load-testing/docker-compose.yml`), y la rúbrica pide todos los componentes en el clúster. No hay evidencia de `requests`/`limits` en una imagen (`kubectl describe` o `kubectl top`).

### C10. Observabilidad y Locust — 2.5
**Cumple:** métricas de la API con `scrape_configs` por `file_sd` (`infra/monitoring/prometheus/prometheus.yml`); dashboard versionado con tráfico, latencia, errores, CPU y memoria del pod y recargas de modelo (`infra/monitoring/grafana/dashboards/real-estate-api.json`); `locustfile` con carga por escalones (`infra/load-testing/locustfile.py:66-102`).
**Falta o está mal:** no hay ninguna evidencia de una ejecución de Locust; Grafana (imagen #13) muestra 16 solicitudes a 0.18 req/s; falta la vista de targets de Prometheus; el dashboard es un archivo montado por compose, no un ConfigMap.

### C11. Código y manejo del repositorio — 4.0
**Cumple:** organización por componentes, lógica de decisión como funciones puras con pruebas, cuatro suites de pruebas, configuración por variables y Secrets, `.gitignore` adecuado, sin `.env` real, modelos binarios ni datasets; 4 PRs fusionados y ramas por funcionalidad; mensajes en formato convencional (4 % genéricos).
**Falta o está mal:** el aporte está concentrado: una persona tiene 29 de 32 commits humanos (las identidades `aristotekean` y `Kevin Bueno` comparten correo), frente a 2 de Jorge Gil y 1 de Juan Camilo Martínez. `CLAUDE.md` todavía dice que el README describe el proyecto de diabetes.

### C12. Puntualidad — 3.0
A tiempo (provisional), sin commits después del límite ni fechas sospechosas, pero el 100 % de los commits está en las últimas 72 h: todo el historial ocupa dos días (2026-11-21 y 2026-11-22). Los commits de los días que quedan hasta el 26 pueden mejorar la distribución.

## Kubernetes: recursos y probes

| Workload | Tipo | requests / limits | Probes | Secrets | Credenciales en claro |
|---|---|---|---|---|---|
| airflow | Deployment | Sí (6 contenedores) | readiness, liveness | Sí | No |
| airflow-init | Job | Sí | No aplica | Sí | No |
| api-inference | Deployment + HPA | Sí | startup, readiness, liveness | Sí | No |
| data-api | Deployment | Sí | readiness, liveness | No | No |
| frontend | Deployment | Sí | readiness, liveness | No | No |
| mlflow | Deployment | Sí | readiness, liveness | Sí | No |
| airflow-db | StatefulSet | Sí | readiness, liveness | Sí | No |
| mlflow-db | StatefulSet | Sí | readiness, liveness | Sí | No |
| postgres-train | StatefulSet | Sí | readiness, liveness | Sí | No |
| train-db-migrate | Job | Sí | No aplica | Sí | No |
| rustfs | StatefulSet | Sí | readiness, liveness | Sí | No |
| rustfs-init | Job | Sí | No aplica | Sí | No |
| traefik | Deployment | Sí | readiness, liveness | No | No |

Las imágenes propias aparecen sin tag en los manifiestos porque `infra/k8s/kustomization.yaml` les fija el SHA. Fuera de Kubernetes: Prometheus, Grafana, Jaeger y Locust.

## Alertas
- **Imagen de la API de datos:** la rúbrica nombra `cristiandiaz13/mlops-puj:data-api-pf-v1` y el proyecto usa `data-api-pf-v2` fijada por digest (`infra/k8s/base/data-api/deployment.yaml`). Confirmar con el profesor que la v2 es válida.
- **Solo 3 de 6 lotes procesados** en la evidencia, todos promovidos.
- **`POST /model/reload` sin autenticación** (`services/api-inference/src/api_inference/main.py:282`).
- **Credenciales de desarrollo en el README** (`airflow`/`airflow`, `admin`/`admin`, `app`/`app`; README.md:L87-L98) y usuario por defecto visible en la imagen #8. Son valores locales, pero conviene cambiarlos o advertirlo junto a la tabla.
- **Imagen #3** muestra repos `diabetes-*` de otro proyecto en la misma cuenta; recortar la captura o filtrar por `real-estate`.
- Sin cambios sin commit ni commits sin push: lo evaluado coincide con `origin/main`.
- Sin instrucciones dirigidas al evaluador, sin secretos versionados y sin fechas sospechosas.

## Historial del repositorio
- 25 commits en `main` (34 en todas las ramas), del 2026-11-21 16:27 al 2026-11-22 20:31; 2 días con actividad (8 y 17 commits).
- Concentración: 72 % en las últimas 24 h y 100 % en las últimas 72 h.
- Aporte por integrante, sin contar el bot: Kevin Bueno (`aristotekean`, dos identidades con el mismo correo) 29 commits y cerca de 20.500 líneas; Juan Camilo Martínez 1 commit y 5.137 líneas; Jorge Gil (dos identidades) 2 commits y 1.582 líneas. `github-actions[bot]`: 2 commits.
- 4 PRs fusionados; ramas remotas `feat/jaeger-tracing`, `fix/api-inference-hardening` y `main`; sin tags.
- Mensajes genéricos: 4 % (1, "Initial commit").

---
*Evaluación automática orientativa basada en la rúbrica del curso. No ejecuta el proyecto: revisa el código, el historial y la evidencia. La nota oficial la asigna el profesor.*
