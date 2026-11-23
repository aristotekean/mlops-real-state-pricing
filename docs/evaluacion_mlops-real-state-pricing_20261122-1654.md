# Evaluación — Proyecto Final MLOps

**Repositorio:** git@github.com:aristotekean/mlops-real-state-pricing.git  ·  **Commit evaluado:** 62a5d0f (2026-11-22 16:48 -05:00, rama `main`, sin cambios locales ni commits sin push)  ·  **Evaluado el:** 2026-11-22 16:54 (Bogotá)
**Fecha límite:** 2026-11-26 23:59:59 -05:00  ·  **Estado:** Provisional (antes de la fecha límite)

## Nota: 2.8 / 5.0

| Criterio | Peso | Nota | Aporte |
|---|---|---|---|
| C1. Documentación: informe y bitácora | 20 % | 2.0 | 0.40 |
| C2. Orquestación con Airflow | 10 % | 3.0 | 0.30 |
| C3. Datos: RAW, CLEAN, trazabilidad e inferencias | 8 % | 3.0 | 0.24 |
| C4. Validación y decisión de entrenamiento | 15 % | 3.0 | 0.45 |
| C5. MLflow, comparación y promoción | 10 % | 3.0 | 0.30 |
| C6. Inferencia con FastAPI | 8 % | 3.0 | 0.24 |
| C7. Streamlit | 5 % | 3.0 | 0.15 |
| C8. CI/CD y GitOps | 7 % | 3.0 | 0.21 |
| C9. Kubernetes | 5 % | 2.5 | 0.13 |
| C10. Observabilidad y Locust | 5 % | 3.0 | 0.15 |
| C11. Código y manejo del repositorio | 4 % | 4.0 | 0.16 |
| C12. Puntualidad | 3 % | 3.0 | 0.09 |
| **Nota base** | | | **2.82** |
| Topes aplicados | | | Ninguno. Se revisaron dos y no se activaron: (a) "README solo guía de comandos sin evidencia" (≤ 2.0): el README tiene arquitectura propia, tablas de diseño y enlaza `docs/pipeline.md` con las decisiones, así que no es solo una guía de comandos, aunque está cerca del límite; (b) "no desplegado en Kubernetes con Argo CD" (≤ 3.0): el mecanismo principal es `infra/k8s` + `infra/argocd/application.yaml` con `automated`. |
| **Nota final** | | | **2.8** |

## Resumen

El código está muy por encima de la nota: DAG de 19 tareas con tres bifurcaciones, RAW/CLEAN/auditoría trazables, decisión de entrenamiento con cuatro disparadores técnicos, promoción contra el productivo sobre las mismas filas, recarga del modelo por sondeo del alias con swap atómico, CI con tags por SHA y Application de Argo CD automatizada. Lo que hunde la nota es la evidencia: de las 10 imágenes del README, 9 apuntan a archivos que no existen, y los pies de varias describen el proyecto anterior (diabetes). Sin evidencia, la rúbrica limita casi todos los criterios a 3.0. Tampoco hay análisis por lote con resultados reales, análisis de la prueba de carga ni enlace al video. Prometheus, Grafana y Locust corren en compose, no en Kubernetes.

## Lo que más subiría la nota

1. **Capturar y publicar la evidencia de cada etapa** (los 20 ítems de la lista de abajo) con un pie que diga qué prueba cada imagen → levanta el tope de 3.0 en C2 a C8 y C10. Con el código actual, C2–C8 pueden pasar de 3.0 a 4.5–5.0: **+1.0 a +1.2** en la nota final.
2. **Reescribir la sección "Evidencias" y la "Ruta rápida" del README para este proyecto**: quitar los enlaces rotos (`README.md:L302-L338`) y el texto de diabetes (`L40`, `L48-L55`, `L93`, `L237`, `L316`, `L328`) → C1 de 2.0 a 3.0 (+0.20) por sí solo, y evita el tope de evidencia fabricada si se restauran capturas viejas.
3. **Agregar el análisis por lote con resultados reales** (tabla por lote 0–5: filas, validaciones, PSI, categorías nuevas, decisión y razón, métricas candidato vs. productivo, promovido o rechazado) y la narración de bitácora (problemas y cómo se resolvieron: arm64/digest de data-api, Gateway API, RustFS) → C1 hacia 4.0–4.5 (+0.40 a +0.50).
4. **Ejecutar Locust y documentar el efecto en Grafana** (RPS, p95, errores, CPU/memoria, réplicas del HPA) con conclusión sobre `requests`/`limits` → C10 de 3.0 a 4.5 (+0.08) y aporta a C1.
5. **Llevar Prometheus, Grafana y Locust a Kubernetes** (manifiestos en `infra/k8s/base/`, dashboard como ConfigMap) y mostrar `kubectl get pods` + `kubectl top pods` → C9 de 2.5 a 4.5 (+0.10) y C10.
6. **Mostrar la sincronización de Argo CD disparada por Git** (captura *Synced/Healthy* en el commit `45053dd` del bot, Actions en verde y tags SHA en Docker Hub) → C8 de 3.0 a 5.0 (+0.14).
7. **Agregar el enlace al video de YouTube** y repartir commits hasta la entrega (hoy el 100 % está en 72 h) → C1 y C12 (+0.03 a +0.06).

## Evidencia del README (imagen por imagen)

| # | Documento:línea | Sección | Qué dice el texto | Qué muestra realmente la imagen | Veredicto |
|---|---|---|---|---|---|
| 1 | README.md:L64 | Arquitectura | "Arquitectura de la plataforma"; el texto describe Server A en Kubernetes, Locust (B) y Prometheus/Grafana (C) | Diagrama propio: GitHub → Actions → DockerHub → ArgoCD → clúster (RustFS, MLflow, Postgres S1 y S5, Airflow, FastAPI, Streamlit), Monitoring en Server C, Locust en Server B, red Tailscale. Coherente con los manifiestos. Detalles: rotula "Kubernetes Ingress controller" (hoy es Gateway API con Traefik) y no dibuja la `data-api` | ✅ |
| 2 | README.md:L302 | 1. Docker Hub | Tres repos `real-estate-*` multi-arquitectura | Archivo `docs/images/2-docker-hub-images.png` no existe | ❌ |
| 3 | README.md:L308 | 2. Clúster | Pods del namespace en OrbStack | Archivo no existe. El texto menciona `minio` e `ingress-nginx-controller`, que ya no están en los manifiestos | ❌ |
| 4 | README.md:L310 | 2. Clúster | Pods en ejecución | Archivo no existe | ❌ |
| 5 | README.md:L312 | 2. Clúster | Services e Ingress | Archivo no existe. El repo ya no tiene Ingress (`infra/k8s/base/gateway`) | ❌ |
| 6 | README.md:L318 | 3. Airflow | "Once tareas, de `validate_source_file` a `promote_model`" | Archivo no existe. El DAG real tiene 19 tareas y no existe `validate_source_file` (`services/airflow/dags/real_estate_training.py`) | ❌ |
| 7 | README.md:L324 | 4. API | `/docs` con `/predict`, `/health`, `/model-info`, `/model/reload` | Archivo no existe | ❌ |
| 8 | README.md:L330 | 5. Frontend | Modelo productivo v2 en la barra lateral | Archivo no existe | ❌ |
| 9 | README.md:L332 | 5. Frontend | Predicción `<30` (readmisión), probabilidades por clase, modelo `diabetes v2` | Archivo no existe. El texto describe otro proyecto: este modelo es una regresión de precio | ❌ |
| 10 | README.md:L338 | 6. MLflow | Experimento `real-estate-pricing`, "tres candidatos anidados" | Archivo no existe. Por defecto se entrenan dos candidatos (`docs/pipeline.md:L162`) | ❌ |

Imágenes revisadas: 10 de 10 (solo 1 disponible). Las 9 restantes se borraron en el commit `b0937a6` ("docs: remove outdated screenshots"), pero los enlaces y pies siguen en el README.

## Lista de evidencia esperada

| # | Evidencia | Estado | Dónde / comentario |
|---|---|---|---|
| 1 | Diagrama de arquitectura propio | ✅ | Imagen #1 y dos diagramas Mermaid (`README.md:L66`, `L101`) |
| 2 | Ejecuciones de GitHub Actions en verde | ❌ | Sin imagen. El commit del bot `45053dd` indica que el workflow corrió |
| 3 | Imágenes en DockerHub con tags versionados | ❌ | Imagen #2 rota |
| 4 | Argo CD *Synced/Healthy* y sincronización por Git | ❌ | Sin imagen |
| 5 | Recursos del clúster y `requests`/`limits` | ❌ | Imágenes #3–#5 rotas; la tabla de `README.md:L131-L140` es texto, no evidencia |
| 6 | Airflow: varias ejecuciones, una por lote, con distintas ramas | ❌ | Imagen #6 rota y con pie de otro DAG |
| 7 | Validaciones y drift de un lote | ❌ | |
| 8 | Datos en RAW y CLEAN con trazabilidad | ❌ | |
| 9 | Tabla de auditoría con decisiones | ❌ | La consulta está en `docs/pipeline.md:L178-L180`, falta su salida |
| 10 | MLflow: runs con métricas, lote, commit y razón | ❌ | Imagen #10 rota |
| 11 | MLflow: Model Registry con alias productivo | ❌ | |
| 12 | Promovido vs. rechazado o no entrenado, con métricas | ❌ | |
| 13 | FastAPI: `/docs` o inferencia, y recarga sin redespliegue | ❌ | Imagen #7 rota; falta la recarga con el mismo pod |
| 14 | Registro de inferencias | ❌ | |
| 15 | Streamlit: predicción con versión | ❌ | Imágenes #8–#9 rotas y con texto de diabetes |
| 16 | Streamlit: historial por lote | ❌ | |
| 17 | Prometheus: targets activos | ❌ | |
| 18 | Locust: usuarios, RPS, fallos | ❌ | |
| 19 | Grafana: efecto de la carga | ❌ | |
| 20 | Enlace al video de YouTube | ❌ | No hay enlace; el commit `62a5d0f` quitó las notas del video |

## Detalle por criterio

### C1. Documentación — 2.0
**Cumple:** diagrama de arquitectura propio y coherente (imagen #1, `README.md:L66-L81`, `L101-L127`); tabla de componentes con probes y recursos (`README.md:L131-L140`); decisiones de pipeline justificadas en `docs/pipeline.md` (métrica prioritaria MAE con RMSE de control, partición por hash de dirección, reglas de entrenamiento y promoción, tabla de parámetros `L150-L165`); perfil de los 6 lotes del grupo 6 (`docs/pipeline.md:L19-L26`).
**Falta o está mal:**
- 9 de 10 imágenes rotas (`README.md:L302-L338`). Solución: capturar de nuevo sobre el sistema actual y guardar en `docs/images/`.
- Texto heredado del proyecto de diabetes que contradice el código: "44 campos" (`L40`, `L93`, `L237`; el contrato tiene 11 columnas de entrada), respuesta de ejemplo con `"prediction": "<30"` y `probabilities` (`L48-L55`; la API devuelve un precio), "once tareas, de `validate_source_file`" (`L316`), "modelo `diabetes v2`" (`L328`), `notebooks/01_eda.ipynb` (`L218`, no existe), `minio-targets.yml` y MinIO en el dashboard (`L270`), `ingress-nginx` (`L306`). Solución: reescribir esas secciones contra `docs/data-contract.md` y el código.
- No es una bitácora: describe el estado final y cómo operarlo, no el orden del desarrollo, los problemas ni lo observado. Solución: sección "Bitácora" con hitos (RustFS, Gateway API, digest amd64 de `data-api`, label de StatefulSet con Argo CD).
- Sin análisis por lote con resultados reales (decisión, razón, métricas vs. productivo) ni análisis de la prueba de carga.
- Sin justificación de la elección de recursos ni de la estrategia de recarga en el README principal (está en `services/api-inference/README.md`, que el README no enlaza).
- Sin enlace al video.

### C2. Orquestación con Airflow — 3.0
**Cumple:** 19 tareas de responsabilidad única con la estructura de referencia más `check_new_batch`/`no_new_batch` (`services/airflow/dags/real_estate_training.py:L85-L245`); tres bifurcaciones `@task.branch` (lote nuevo, entrenar, promover: `L91`, `L143`, `L184`); una petición = un lote = una corrida, con `max_active_runs=1` (`L68`) y reanudación de lotes sin terminar; cliente robusto con timeouts de conexión y lectura, reintentos con backoff en errores de red y 5xx, fin de datos (400), lote vacío y validación de forma y grupo (`real_estate_pipeline/api_client.py:L61-L131`); `retries=2` con backoff exponencial y `on_failure_callback` que marca el lote `failed` sin ocultar el error (`L43-L54`, `L69-L76`).
**Falta o está mal:** ninguna evidencia de ejecuciones (ítem 6). Solución: captura de la grilla con al menos una corrida por rama (`skip_training`, `promote_model`, `reject_model`, `no_new_batch`) y el grafo de una de ellas. Tope de 3.0 por la regla de evidencia.

### C3. Datos — 3.0
**Cumple:** `raw.batches` guarda la respuesta byte a byte (gzip) con SHA-256, número de lote, URL, estado y conteo (`infra/database/migrations/001_raw_clean_audit.sql:L23-L51`); `raw.records` sin modificar en JSONB (`L59-L74`); `clean.records` en esquema separado con `raw_record_id` y `batch_id` (`L86-L111`); deduplicación por `UNIQUE (group_number, api_batch_number, response_sha256)` (`L50`) y `raw.api_requests` por intento (`004_api_requests.sql`); `audit.batch_history` con reportes, decisión, razón e ids de MLflow (`L122-L155`); `inference.logs` con fecha, entrada, predicción, versión, estado y error (`002_inference_logs.sql`); `mlflow-db` es otra instancia.
**Falta o está mal:** sin evidencia (ítems 8, 9 y 14). Solución: salidas de `SELECT` sobre `raw.batches`, `clean.records` agrupado por `batch_id` y `data_split`, `audit.batch_history_summary` e `inference.logs`.

### C4. Validación y decisión de entrenamiento — 3.0
**Cumple:** esquema, calidad (nulos, duplicados en el lote y contra lotes previos, rangos, target), categorías nuevas y drift por PSI numérico y categórico (`real_estate_pipeline/validation.py:L66`, `L111`, `L319`, `L387-L428`), con umbral `DRIFT_PSI_THRESHOLD=0.20`; categorías desconocidas manejadas con `handle_unknown='ignore'`, `unknown_value=-1` y `TargetEncoder` (`docs/pipeline.md:L95-L97`); `decide_training` es una función pura con vetos (lote inválido, datos insuficientes, lote pequeño) y cuatro disparadores: crecimiento de volumen, drift, categorías nuevas y degradación del productivo sobre el lote (`real_estate_pipeline/rules.py:L57-L191`). No depende solo del conteo ni de la periodicidad, así que no aplica el tope de 1.5. La razón queda en `audit.batch_history.training_rules` y en MLflow (`modeling.py:L277`, `L293`). Hay pruebas unitarias de las reglas (`services/airflow/tests/test_rules.py`).
**Falta o está mal:** sin evidencia del resultado de las validaciones ni de las decisiones reales (ítems 7 y 12). Nota de diseño: `docs/pipeline.md:L28` reconoce que el drift entre lotes es grande "por diseño"; con PSI ≥ 0.20 es probable que el disparador de drift se cumpla en casi todos los lotes, así que conviene mostrar un caso real de "no entrenar" (el lote 3, de 4.055 filas, debería caer en `small_batch`). Solución: tabla de auditoría con los 6 lotes y su `training_reason`.

### C5. MLflow, comparación y promoción — 3.0
**Cumple:** backend PostgreSQL y artefactos en `s3://` sobre RustFS con `--serve-artifacts` (`infra/k8s/base/mlflow/deployment.yaml:L137-L139`); bucket creado por el Job `rustfs-init`; cada run registra lotes y snapshot de CLEAN, commit (`mlflow.source.git.commit`), parámetros, métricas train/validation/test, gráficos, importancia por permutación y razón (`real_estate_pipeline/modeling.py:L253-L293`, `L371-L436`, `L458-L463`); candidato y productivo se cargan del registro y se evalúan sobre **las mismas filas de validación** (`modeling.py:L543-L562`), con particiones estables por hash de dirección, así que las métricas son comparables; regla explícita MAE ≥ 3 % y RMSE ≤ 1 % (`rules.py:L204-L248`); rechazado queda como versión etiquetada; alias `production`.
**Falta o está mal:** sin evidencia (ítems 10, 11 y 12). El README no documenta las credenciales y políticas del bucket más allá de mencionar el Job. Solución: capturas del run padre con tags y artefactos, del Model Registry con el alias y de una comparación promovida y otra rechazada.

### C6. Inferencia con FastAPI — 3.0
**Cumple:** carga `models:/real_estate@production` por alias, sin versiones quemadas (`services/api-inference/src/api_inference/model.py:L83-L103`); recarga sin redespliegue por sondeo del alias cada `MODEL_REFRESH_SECONDS` y `POST /model/reload` (`main.py:L66-L78`, `L178-L193`); concurrencia con `asyncio.Lock` y swap atómico de referencia, y fallback al modelo anterior si la descarga falla (`model.py:L141-L160`, `L203-L204`); `/health`, `/predict`, `/metrics`; Pydantic (`schemas.py`); log de inferencias durable con spool (`spool.py`); métricas de peticiones, latencia, predicciones, recargas y versión del modelo (`metrics.py:L33-L80`).
**Falta o está mal:** `POST /model/reload` no está protegido (`main.py:L178`) y queda expuesto por el Gateway; la rúbrica pide endpoint administrativo protegido. Solución: token por cabecera leído de un Secret, o no publicar esa ruta en el `HTTPRoute`. Sin evidencia de `/docs` ni de la recarga con el mismo pod (ítem 13). Solución: `GET /model-info` antes y después de una promoción junto a `kubectl get pods` con la misma edad.

### C7. Streamlit — 3.0
**Cumple:** formulario, consumo de la API, precio estimado y versión del modelo (`services/frontend/src/frontend/app.py:L124-L203`); pestaña de historial con decisión, razón de entrenamiento y de promoción, versión antes y después, deltas de métricas e ids de MLflow (`app.py:L221-L292`), leída de `/history` sin credenciales de base.
**Falta o está mal:** sin evidencia (ítems 15 y 16); los pies actuales describen la interfaz de diabetes (`README.md:L328`). Solución: una captura de la predicción con versión y otra del historial con los lotes reales.

### C8. CI/CD y GitOps — 3.0
**Cumple:** workflow con pruebas, build multi-arquitectura y push de las tres imágenes propias con tags `latest` y SHA, credenciales en `secrets` (`.github/workflows/ci-cd-unit-tests-dockerhub.yml:L55-L99`); job `update-manifests` que fija el SHA en `infra/k8s/kustomization.yaml` (`L104-L141`), y el historial lo confirma con el commit del bot `45053dd`; los manifiestos referencian esas imágenes con `newTag` por SHA (`infra/k8s/kustomization.yaml:L55-L63`); Application con `automated`, `prune` y `selfHeal` apuntando al repo (`infra/argocd/application.yaml`).
**Falta o está mal:** sin evidencia de Actions, DockerHub ni Argo CD (ítems 2, 3 y 4). Las pruebas de Airflow no corren en CI y `services/airflow/requirements.txt` cambia sin que exista un workflow por imagen (es uno solo con matriz, lo cual es aceptable). `docker-compose.yml` conserva `build:` como alternativa (`README.md:L263`); no es el despliegue principal, pero conviene decirlo explícitamente. Los Secrets se aplican con `kubectl apply -k infra/k8s/generated` a mano: está justificado, pero es un paso fuera de GitOps (alternativa: Sealed Secrets o External Secrets).

### C9. Kubernetes — 2.5
**Cumple:** 13 workloads, todos con `requests` y `limits`, probes donde aplica, Secrets por referencia y sin contraseñas en claro; StatefulSets con PVC; HPA para la API (`infra/k8s/base/api-inference/hpa.yaml`); `data-api` desplegada en el clúster.
**Falta o está mal:** **Prometheus, Grafana y Locust no están en Kubernetes**: corren por compose (`infra/monitoring/docker-compose.yml`, `infra/load-testing/docker-compose.yml`, `README.md:L94-L95`). La rúbrica exige todos los componentes en el clúster. Solución: `infra/k8s/base/prometheus`, `grafana` (dashboard y datasource como ConfigMap) y `locust`, con sus recursos y probes. Sin evidencia de pods ni de `kubectl top` (ítem 5).

### C10. Observabilidad y Locust — 3.0
**Cumple:** `scrape_configs` hacia `/metrics` de la API (`infra/monitoring/prometheus/prometheus.yml:L25-L28`); dashboard versionado con RPS, latencia p50/p95/p99, errores, CPU y memoria del proceso, predicciones y recargas (`infra/monitoring/grafana/dashboards/real-estate-api.json`); `locustfile` con carga escalonada (`infra/load-testing/locustfile.py`).
**Falta o está mal:** el dashboard no tiene un panel con la versión del modelo cargado aunque la métrica existe (`metrics.py:L75`). Solución: panel *stat* con `model_info`. El dashboard es un archivo aprovisionado por compose, no un ConfigMap. Sin evidencia de targets, de la ejecución de Locust ni de su efecto en Grafana (ítems 17, 18 y 19).

### C11. Código y manejo del repositorio — 4.0
**Cumple:** organización por componentes (`services/`, `ml/`, `infra/`, `docs/`), paquete compartido entre entrenamiento y servicio, lógica de decisión pura y probada, cuatro suites de pruebas, configuración por variables; sin secretos, `.env`, binarios ni datasets versionados; `.gitignore` adecuado; commits convencionales (6 % genéricos) y 3 PRs fusionados.
**Falta o está mal:** documentación heredada sin limpiar (ver C1); el aporte por integrante es desigual en commits (ver *Historial*); solo queda `origin/main` como rama remota. `README.md:L87-L94` publica credenciales de desarrollo (`airflow`/`airflow`, `admin`/`admin`): son valores por defecto de laboratorio, pero conviene remitir a `.env.example` en vez de escribirlas.

### C12. Puntualidad — 3.0
A tiempo: no hay commits después del límite ni fechas sospechosas. El 100 % de los commits (18 de 18 en `main`) está dentro de las últimas 72 h, en dos días de actividad (21 y 22 de noviembre), lo que cae en el nivel "más del 80 % en las últimas 72 h". Evaluación provisional: quedan cuatro días para repartir el trabajo restante.

## Kubernetes: recursos y probes

| Workload | Tipo | requests/limits | Probes | Secrets | Credenciales en claro |
|---|---|---|---|---|---|
| airflow (4 contenedores + 2 init) | Deployment | ✅ | ✅ | ✅ | No |
| airflow-init | Job | ✅ | n/a | ✅ | No |
| api-inference | Deployment + HPA | ✅ | ✅ | ✅ | No |
| data-api | Deployment | ✅ | ✅ | — | No |
| frontend | Deployment | ✅ | ✅ | — | No |
| mlflow | Deployment | ✅ | ✅ | ✅ | No |
| airflow-db, mlflow-db, postgres-train | StatefulSet | ✅ | ✅ | ✅ | No |
| train-db-migrate | Job | ✅ | n/a | ✅ | No |
| rustfs | StatefulSet | ✅ | ✅ | ✅ | No |
| rustfs-init | Job | ✅ | n/a | ✅ | No |
| traefik | Deployment | ✅ | ✅ | — | No |
| Prometheus, Grafana, Locust | **no están en Kubernetes** (compose) | — | — | — | — |

Las imágenes propias aparecen sin tag en los manifiestos base, pero `infra/k8s/kustomization.yaml` las fija por SHA: no es un problema.

## Alertas

- **9 imágenes rotas** en el README (`L302-L338`): los archivos no existen en el repo.
- **Pies de evidencia de otro proyecto** (`README.md:L316`, `L328`): describen el DAG y la predicción del proyecto de diabetes. Hoy no activa el tope de evidencia fabricada porque no hay imagen, pero **restaurar esas capturas viejas sí lo activaría** (criterio en 0.0 y nota final ≤ 3.0).
- **Texto que contradice el código**: 44 campos, respuesta con `probabilities`, `notebooks/01_eda.ipynb`, `minio-targets.yml`, `ingress-nginx` (ver C1).
- **Imagen de la API de datos**: la rúbrica indica `data-api-pf-v1`; el repo usa `data-api-pf-v2` fijada por digest (`infra/k8s/base/data-api/deployment.yaml`). Confirmar con el profesor que v2 es la vigente y dejarlo escrito en el README.
- **`POST /model/reload` sin protección** y publicado por el Gateway.
- Sin enlace al video de YouTube.
- Sin secretos versionados, sin `.env` real, sin commits posteriores al límite, sin fechas sospechosas y sin instrucciones dirigidas al evaluador.
- Estado local: sin cambios sin commit ni commits sin push; lo evaluado es lo que está en GitHub.

## Historial del repositorio

- 18 commits en `main` (24 contando todas las ramas fusionadas), 2 días con actividad (2026-11-21: 8, 2026-11-22: 10). Concentración: 100 % en las últimas 72 h, 94 % en las últimas 24 h.
- Aporte (todas las ramas, sin el bot): `aristotekean` 17 commits y 17.803 líneas; `Kevin Bueno` 4 commits (merges de PR) y 224 líneas, con el mismo correo que `aristotekean`, es decir, la misma persona; `Juan Camilo Martínez` 1 commit y 5.137 líneas (el pipeline completo); `Jgilq-PUJ` 1 commit y 1.582 líneas (API, Streamlit y observabilidad). Los tres integrantes aportan, pero dos de ellos con un único commit grande: conviene que sigan aportando en commits más pequeños.
- 3 PRs fusionados; solo queda `origin/main` como rama remota; sin tags.
- Mensajes genéricos: 1 de 18 (6 %, "Initial commit"). El resto sigue commits convencionales, salvo `2089920`, que está en español y sin prefijo.

---
*Evaluación automática orientativa basada en la rúbrica del curso. No ejecuta el proyecto: revisa el código, el historial y la evidencia. La nota oficial la asigna el profesor.*
