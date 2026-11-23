# Evaluación — Proyecto Final MLOps

**Repositorio:** git@github.com:aristotekean/mlops-real-state-pricing.git  ·  **Commit evaluado:** 8cec42b (2026-11-22 18:05 -05:00, rama `fix/api-inference-hardening`)  ·  **Evaluado el:** 2026-11-22 18:20 -05:00
**Fecha límite:** 2026-11-26 23:59:59 -05:00 (Bogotá)  ·  **Estado:** Provisional (antes de la fecha límite)

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
| C10. Observabilidad y Locust | 5 % | 2.5 | 0.13 |
| C11. Código y manejo del repositorio | 4 % | 4.0 | 0.16 |
| C12. Puntualidad | 3 % | 3.0 | 0.09 |
| **Nota base** | | | **2.79** |
| Topes aplicados | | | Ninguno. Dos estuvieron cerca: (a) "README que es solo una guía de comandos sin evidencia" (≤ 2.0): no se aplicó porque el README y `docs/pipeline.md` justifican el diseño y hay un diagrama propio, pero la sección *Evidencias* está 100 % *Pendiente* y el profesor podría leerlo distinto; (b) "no desplegado en Kubernetes con Argo CD" (≤ 3.0): no se aplicó porque Server A completo va por kustomize + Argo CD `automated`; Prometheus, Grafana y Locust en compose se penalizan en C9 y C10. |
| **Nota final** | | | **2.8** |

## Resumen
El código es de nivel alto: DAG de 21 tareas con tres bifurcaciones, RAW/CLEAN/auditoría trazables, decisión de entrenamiento multifactor, promoción contra el productivo sobre la misma validación, recarga del modelo sin redespliegue y GitOps con tags por SHA. Lo que tiene la nota en 2.8 no es la ingeniería sino la **evidencia**: el README tiene una sola imagen (el diagrama) y los 19 ítems restantes de la lista están marcados *Pendiente*, así que casi todos los criterios quedan topados en 3.0 por la regla de evidencia. Además el README es un manual de referencia y operación, no una bitácora, y faltan el análisis por lote, el análisis de la prueba de carga y el video. Prometheus, Grafana y Locust no están en Kubernetes.

## Lo que más subiría la nota
1. **Tomar y describir las 19 capturas que el propio README ya lista como pendientes** (`README.md:L309-L355`), cada una con pie que diga qué prueba → destraba el tope de 3.0 en C2 a C8: C2, C3, C5, C7 y C8 podrían llegar a 5.0 y C4 y C6 a 4.5 (≈ **+1.2** en total).
2. **Convertir el README en bitácora**: orden del desarrollo, decisiones y por qué, problemas reales y cómo se resolvieron (ya hay material: la API con estado, el digest amd64 de `data-api`, la migración ingress-nginx → Gateway API, MinIO → RustFS, el label de kustomize que rompía el sync de Argo CD) → C1 de 2.0 a 4.5 junto con los puntos 1, 3 y 4 (**+0.5**).
3. **Escribir el análisis por lote** con los 6 lotes reales del grupo 6: qué llegó, PSI y categorías nuevas detectadas, decisión y razón, métricas del candidato frente al productivo y si se promovió o se rechazó (pegar `audit.batch_history_summary`) → C1, C4 y C5.
4. **Ejecutar Locust y escribir el análisis de carga** con capturas de Grafana durante la prueba (RPS, p95, errores, CPU/memoria, réplicas del HPA) y la conclusión sobre los `requests`/`limits` → C1 y C10.
5. **Llevar Prometheus, Grafana y Locust a Kubernetes** (`infra/k8s/base/{prometheus,grafana,locust}`, dashboard como ConfigMap, con `requests`/`limits` y probes) y añadir un panel con `model_info` → C9 de 2.5 a 5.0 y C10 de 2.5 a 5.0 (**+0.25**, con evidencia).
6. **Fusionar `fix/api-inference-hardening` en `main`**: hoy `main` no tiene `/live`, `/ready`, la predicción de humo ni el tracing, y es lo que ven Argo CD, el CI y el profesor. De paso, proteger `POST /model/reload` (token en un Secret) → C6.
7. **Agregar el enlace al video de YouTube** (ítem 20) y corregir el bug de lotes inválidos que igual entran a CLEAN (ver C4) → C1 y C4.

## Evidencia del README (imagen por imagen)

| # | Documento:línea | Sección | Qué dice el texto | Qué muestra realmente la imagen | Veredicto |
|---|---|---|---|---|---|
| 1 | README.md:L65 | Arquitectura | "Arquitectura de la plataforma"; el texto describe Server A en Kubernetes (S1–S7 + data-api), Locust en Server B, Prometheus y Grafana en Server C, red Tailscale | Diagrama propio (estilo Excalidraw) titulado "Real State Pricing Prediction": bloque CI/CD GitOps (GitHub, GitHub Actions, DockerHub, Argo CD), clúster de Kubernetes "SERVER A" con S1 PostgreSQL (MLflow), S2 MLflow, S3 RustFS, S4 FastAPI, S5 PostgreSQL de entrenamiento, S6 Airflow y S7 Streamlit, bloque Monitoring "SERVER C" (S8 Prometheus, S9 Grafana), Testing "SERVER B" (Locust) y red Tailscale. Coherente con los manifiestos. Dos detalles: dice "Kubernetes Ingress controller" y el repo usa Gateway API con Traefik (`infra/k8s/base/gateway`); no aparece `data-api`, que sí está en el diagrama mermaid | ✅ |

Imágenes revisadas: 1 de 1. No hay imágenes en `docs/pipeline.md` ni en `docs/data-contract.md`.

## Lista de evidencia esperada

| # | Evidencia | Estado | Dónde / comentario |
|---|---|---|---|
| 1 | Diagrama de arquitectura propio | ✅ | Imagen #1 (README.md:L65) más dos diagramas mermaid (L67, L104) |
| 2 | Ejecuciones de GitHub Actions en verde | ❌ | *Pendiente* en README.md:L311 |
| 3 | Imágenes publicadas en DockerHub con tags versionados | ❌ | *Pendiente* en README.md:L312 |
| 4 | Argo CD *Synced/Healthy* y sincronización por un cambio en Git | ❌ | *Pendiente* en README.md:L316 |
| 5 | Pods corriendo y evidencia de `requests`/`limits` | ❌ | *Pendiente* en README.md:L320-L321. La tabla de L135-L145 es texto, no evidencia de ejecución |
| 6 | Airflow: varias ejecuciones, una por lote, con las ramas | ❌ | *Pendiente* en README.md:L325-L326 |
| 7 | Resultados de validaciones y drift de un lote | ❌ | *Pendiente* en README.md:L331 |
| 8 | Datos en RAW y CLEAN con trazabilidad por lote | ❌ | *Pendiente* en README.md:L330 |
| 9 | Tabla de auditoría o historial de lotes | ❌ | *Pendiente* en README.md:L332 |
| 10 | MLflow: runs con métricas, parámetros, lote, commit y razón | ❌ | *Pendiente* en README.md:L336 |
| 11 | MLflow: Model Registry con versiones y alias | ❌ | *Pendiente* en README.md:L337 |
| 12 | Caso promovido y caso rechazado o no entrenado | ❌ | *Pendiente* en README.md:L338 |
| 13 | FastAPI `/docs` o inferencia, y recarga sin redespliegue | ❌ | *Pendiente* en README.md:L342-L343 |
| 14 | Registro de inferencias | ❌ | *Pendiente* en README.md:L344 |
| 15 | Streamlit: predicción con versión del modelo | ❌ | *Pendiente* en README.md:L348 |
| 16 | Streamlit: historial por lote | ❌ | *Pendiente* en README.md:L349 |
| 17 | Prometheus: targets activos | ❌ | *Pendiente* en README.md:L353 |
| 18 | Locust: ejecución de la prueba | ❌ | *Pendiente* en README.md:L354 |
| 19 | Grafana: efecto de la prueba de carga | ❌ | *Pendiente* en README.md:L355 |
| 20 | Enlace al video de YouTube | ❌ | No hay ningún enlace a YouTube en el README ni en `docs/` |

Resultado: 1 de 20.

## Detalle por criterio

### C1. Documentación — 2.0
**Cumple:** diagrama de arquitectura propio y coherente (imagen #1) más dos mermaid; decisiones justificadas en `docs/pipeline.md` (métrica prioritaria MAE con RMSE de control, L164; particiones por hash de la dirección, L93; reglas de entrenamiento, L99-L115; regla de promoción, L129; descarte de RandomForest, L162) y en `docs/data-contract.md` (rangos de outliers, encoders). El perfil de los 6 lotes del grupo 6 está medido (`docs/pipeline.md:L19-L26`). Tabla de recursos y probes por componente (`README.md:L135-L145`).
**Falta o está mal:**
- No es una bitácora: es un manual de referencia y operación (ruta rápida, tablas, operación, solución de problemas). No narra el orden del desarrollo, los problemas encontrados ni qué se observó en cada etapa. Solución: una sección cronológica "Bitácora" con decisión → problema → solución → resultado.
- 19 de 20 evidencias ausentes (`README.md:L309-L355`, todas *Pendiente*).
- No hay análisis por lote con resultados reales: `docs/pipeline.md:L19-L26` describe los datos de entrada, no qué decidió el pipeline ni las métricas frente al productivo.
- No hay análisis de la prueba de carga.
- No hay enlace al video.
- Los `requests`/`limits` se listan pero no se justifican (por qué 250m/512Mi para la API, por qué HPA al 70 %).
- La estrategia de recarga (sondeo cada 60 s más `/model/reload`) se menciona (`README.md:L61`, `L298`) sin explicar por qué se eligió ni cómo maneja concurrencia y fallback, aunque el código sí lo hace.

### C2. Orquestación con Airflow — 3.0
**Cumple:** DAG `real_estate_incremental_training` con 21 tareas de responsabilidad única: las de referencia más `check_new_batch` y `no_new_batch` (`services/airflow/dags/real_estate_training.py:80-245`). Tres bifurcaciones `@task.branch` (`:91`, `:143`, `:184`). Una petición = un lote = una corrida, con `max_active_runs=1` y `catchup=False` (`:65-68`) y reanudación del lote sin terminar sin volver a llamar a la API (`real_estate_pipeline/raw.py:88-124`). Cliente robusto: timeouts 10 s / 300 s, reintentos con backoff para red y 5xx, fin de datos distinguido de un 400 real, respuesta vacía y validación de forma (`api_client.py:61-124`). `retries=2` con backoff exponencial (`real_estate_training.py:71-73`), `on_failure_callback` que deja el lote `failed` sin ocultar el error (`:53-54`).
**Falta o está mal:** ninguna captura de la grilla ni del grafo con las ramas recorridas (ítem 6), por eso no pasa de 3.0. Un salto en el número de lote solo genera una advertencia (`raw.py:227-233`).

### C3. Datos — 3.0
**Cumple:** `raw.batches` guarda la respuesta byte a byte (`payload_gzip`) con SHA-256, número de lote, conteo, esquema recibido y estados (`infra/database/migrations/001_raw_clean_audit.sql:23-51`); `raw.records` guarda cada registro sin modificar en JSONB (`:59-74`). `clean.records` en esquema separado con `raw_record_id` y `batch_id` (`:86-111`). Deduplicación por `UNIQUE(group_number, api_batch_number, response_sha256)` (`:50`) y por `record_key` entre lotes (`validation.py:132-163`). `audit.batch_history` con reportes, decisión, razón, ids de MLflow, métricas y promoción (`:122-155`). `inference.logs` con fecha, entrada, predicción, versión, estado y error (`002_inference_logs.sql:6-28`). MLflow usa otra instancia (`mlflow-db`).
**Falta o está mal:** sin capturas de RAW, CLEAN, auditoría ni `inference.logs` (ítems 8, 9 y 14). `inference.feedback` y la vista `inference.training_examples` no las consume ningún DAG: o se usan o se documentan como trabajo futuro.

### C4. Validación y decisión de entrenamiento — 3.0
**Cumple:** esquema (`validation.py:66-105`), calidad por fila con duplicados, nulos, rangos y estadísticas del target (`:111-241`), drift por PSI con 10 cuantiles y umbral 0.20 contra el histórico de CLEAN (`:384-467`), categorías nuevas con umbral del 1 % del lote (`:319-378`). Categorías desconocidas cubiertas con `handle_unknown="ignore"`, `unknown_value=-1` y `TargetEncoder` (`ml/real_estate_model/src/real_estate_model/models.py:117-136`). La decisión es multifactor y explícita (`rules.py:125-188`): lote inválido, datos insuficientes, línea base, lote pequeño y cuatro disparadores (crecimiento ≥ 20 %, PSI ≥ 0.20, categorías nuevas ≥ 1 %, degradación ≥ 10 % del productivo sobre el lote nuevo). **No aplica el tope de 1.5.** La razón queda en `audit.batch_history` (`decision.py:108-114`) y en MLflow (tag y artefacto, `modeling.py:277-293`).
**Falta o está mal:**
- Sin evidencia de un reporte de validación o drift real (ítem 7) ni de las decisiones (ítem 12).
- **Bug: un lote inválido igual alimenta entrenamientos futuros.** `preprocess_data` corre siempre y carga a CLEAN las filas `validated` sin mirar si el lote quedó bloqueante (`clean.py:40`), aunque la razón registrada dice "no se usa para entrenar" (`rules.py:131-132`) y `docs/pipeline.md:L76` afirma lo mismo. Solución: saltar el lote en `preprocess_data` cuando `schema.blocking` o `quality.blocking`, o filtrar esos lotes al leer CLEAN para entrenar.
- Volumen y suficiencia se evalúan dentro de la decisión, no como validaciones con su propio reporte.

### C5. MLflow, comparación y promoción — 3.0
**Cumple:** backend PostgreSQL y artefactos en `s3://mlflow` sobre RustFS (`infra/k8s/base/mlflow/deployment.yaml:154-156`); bucket creado por el Job `rustfs-init` (`infra/k8s/base/rustfs/job-init.yaml:61`). Cada run registra lote, `dag_run_id`, snapshot de CLEAN, commit (`mlflow.source.git.commit`), parámetros de modelo y preprocesamiento, métricas train/validation/test, gráficos, importancia por permutación y la razón (`modeling.py:245-463`). Comparación contra el alias `production` actual, recargando ambos modelos del registro y evaluándolos **sobre las mismas filas de validación** (`modeling.py:543-562`), con particiones estables por hash: las métricas sí son comparables. Regla explícita: MAE mejora ≥ 3 % y RMSE no empeora > 1 % (`rules.py:229-238`). El rechazado queda registrado con tag y el alias no se mueve (`modeling.py:629-644`).
**Falta o está mal:** sin capturas de runs, registry ni de un caso promovido y otro rechazado (ítems 10, 11 y 12). La validación se usa para elegir el candidato y también para promover; test solo se reporta: conviene justificarlo en el informe. Hiperparámetros fijos, sin búsqueda. Constantes muertas e inconsistentes (`models.py:43-44`, `PRIMARY_METRIC="validation_rmsle"`).

### C6. Inferencia con FastAPI — 3.0
**Cumple:** carga por alias desde MLflow, sin versión ni ruta fija (`services/api-inference/src/api_inference/model.py:125-138`). Recarga sin redespliegue por sondeo cada 60 s (`main.py:101-113`) y bajo demanda, con `asyncio.Lock`, swap por asignación de referencia, predicción de humo antes del swap y fallback al modelo anterior si la carga falla (`model.py:189-308`). `/health`, `/predict`, `/metrics`, `/live`, `/ready`, `/history`. Pydantic estricto con `extra="forbid"` (`schemas.py:63-103`). Cada petición, incluidas las fallidas, va a `inference.logs` con spool en disco si la base cae. Métricas: peticiones, latencia, errores, `model_info{version}` y recargas (`metrics.py:33-83`).
**Falta o está mal:**
- Sin evidencia de `/docs`, de una inferencia ni de la recarga con el mismo pod (ítem 13).
- `POST /model/reload` no tiene autenticación (`main.py:282-283`) y la API se expone por el Gateway. La rúbrica pide un endpoint administrativo **protegido**. Solución: cabecera con token leído de un Secret.
- `/live`, `/ready`, la predicción de humo, el backpressure y el tracing están solo en `fix/api-inference-hardening`, 3 commits por delante de `origin/main`. En `main` las probes apuntan a `/health`, que siempre responde 200.
- La estrategia de recarga no está explicada en el README.

### C7. Streamlit — 3.0
**Cumple:** formulario generado desde el contrato, consumo de la API, predicción con nombre y versión del modelo (`services/frontend/src/frontend/app.py:112`, `:203`). Historial por lote con decisión y razón de entrenamiento, promoción o rechazo, versión productiva antes y después, deltas de métricas e ids de MLflow (`app.py:216-270`).
**Falta o está mal:** sin capturas de la predicción ni del historial con los lotes reales (ítems 15 y 16).

### C8. CI/CD y GitOps — 3.0
**Cumple:** un workflow que prueba tres suites y publica las tres imágenes propias (api-inference, frontend, airflow) para amd64 y arm64, con tags `latest` y SHA y credenciales en `secrets` (`.github/workflows/ci-cd-unit-tests-dockerhub.yml:32-96`). El job `update-manifests` escribe el SHA en `infra/k8s/kustomization.yaml:56-64`, y el historial tiene un commit del bot que lo demuestra. Sin `imagePullPolicy: Never` ni `latest` en los manifiestos. Application de Argo CD con `automated`, `prune` y `selfHeal` sobre `infra/k8s` en `main` (`infra/argocd/application.yaml:19-29`).
**Falta o está mal:**
- Sin capturas de Actions en verde, DockerHub ni Argo CD (ítems 2, 3 y 4).
- Las pruebas de Airflow (`services/airflow/tests`) no corren en CI, y son las que cubren las reglas de decisión.
- El filtro de ramas `feature/**` (`:7`) no coincide con las ramas reales `feat/` y `fix/`.
- Los Secrets y Traefik se aplican a mano fuera de Argo CD: está documentado, pero conviene justificarlo en el informe.
- El `docker-compose.yml` raíz tiene `build:` (`:203`, `:253`); se presenta como entorno local, no como despliegue.

### C9. Kubernetes — 2.5
**Cumple:** Airflow, MLflow, tres PostgreSQL, RustFS, api-inference con HPA, frontend y data-api en Kubernetes. Todos los contenedores, initContainers y Jobs de `infra/k8s/base` declaran `requests` y `limits` de CPU y memoria. Probes de readiness, liveness y startup donde aplica. Secrets por `secretKeyRef`, generados fuera de git; sin contraseñas en claro. PVC por `volumeClaimTemplates`.
**Falta o está mal:**
- **Prometheus, Grafana y Locust no están en Kubernetes**: solo existen como perfiles de compose (`infra/monitoring/docker-compose.yml`, `infra/load-testing/docker-compose.yml`). El enunciado exige todos los componentes en el clúster. Solución: `infra/k8s/base/prometheus`, `grafana` (dashboard y datasource como ConfigMap) y `locust` (master y workers con el `locustfile` en un ConfigMap), sumados a `infra/k8s/kustomization.yaml`.
- Sin evidencia de pods corriendo ni de recursos aplicados (ítem 5).
- Traefik no declara límite de CPU (`infra/k8s/traefik/deployment.yaml:59-64`).

### C10. Observabilidad y pruebas de carga — 2.5
**Cumple:** Prometheus recolecta `/metrics` de la API (`infra/monitoring/prometheus/prometheus.yml:24-32`). Dashboard versionado con RPS, latencias p50/p95/p99, errores 4xx/5xx, CPU y memoria del proceso, predicciones y recargas (`infra/monitoring/grafana/dashboards/real-estate-api.json`). `locustfile` con `/predict` y `/model-info` y forma de carga escalonada opcional (`infra/load-testing/locustfile.py:71-100`).
**Falta o está mal:**
- Ninguna evidencia: ni targets, ni Locust, ni Grafana bajo carga (ítems 17, 18 y 19).
- Todo corre en compose, no en el clúster; el dashboard se monta como archivo, no como ConfigMap.
- Ningún panel usa `model_info`: la versión del modelo cargado no se ve en Grafana.
- Prometheus apunta al Service (`prometheus/targets/api-targets.yml:17`): con varias réplicas del HPA cada scrape cae en un pod distinto y las métricas quedan mezcladas. En Kubernetes, usar `kubernetes_sd_configs` con rol `pod`.

### C11. Código y manejo del repositorio — 4.0
**Cumple:** organización por componentes (`services/`, `ml/`, `infra/`, `docs/`), paquete compartido entre entrenamiento y servicio, configuración por variables y Secrets. Sin `.env` real, secretos, modelos binarios ni datasets versionados; `.gitignore` adecuado. Cuatro suites de pruebas (unas 170 pruebas). Commits convencionales (5 % de mensajes genéricos), 3 PRs fusionados y ramas por funcionalidad.
**Falta o está mal:** el aporte está muy concentrado: un integrante suma 25 de 27 commits humanos (dos identidades con el mismo correo) y los otros dos tienen 1 commit cada uno, aunque sustanciales (5.137 y 1.582 líneas). `CLAUDE.md:9` todavía dice que el README describe el scaffold de diabetes, lo cual ya no es cierto. `.env.example` no se pudo leer por permisos locales; el recolector no detectó secretos.

### C12. Puntualidad — 3.0
A tiempo (0 commits después del límite, sin fechas sospechosas), pero el 100 % de los commits está en las últimas 72 h del historial: todo el repositorio se hizo entre el 2026-11-21 y el 2026-11-22. Es provisional: si el trabajo de evidencia se reparte en los días que quedan, la proporción mejora.

## Kubernetes: recursos y probes

| Workload | Tipo | requests/limits | Probes | Secrets | Credenciales en claro |
|---|---|---|---|---|---|
| airflow (4 contenedores + init) | Deployment | ✅ | api-server: startup, readiness, liveness; resto: liveness | ✅ | No |
| airflow-init | Job | ✅ | n/a | ✅ | No |
| api-inference (+ HPA 1-20) | Deployment | ✅ | startup y liveness `/live`, readiness `/ready` (solo en la rama evaluada) | ✅ | No |
| frontend | Deployment | ✅ | startup, readiness, liveness | n/a | No |
| mlflow | Deployment | ✅ | startup, readiness, liveness | ✅ | No |
| data-api | Deployment | ✅ | startup, readiness, liveness | n/a | No |
| mlflow-db, airflow-db, postgres-train | StatefulSet | ✅ | readiness, liveness | ✅ | No |
| rustfs | StatefulSet | ✅ | readiness, liveness | ✅ | No |
| train-db-migrate, rustfs-init | Job | ✅ | n/a | ✅ | No |
| traefik | Deployment (fuera de Argo CD) | ⚠️ sin límite de CPU | ✅ | n/a | No |
| Prometheus, Grafana, Locust | **No están en Kubernetes** (compose) | ❌ | ❌ | ❌ | — |

## Alertas
- **La rama evaluada no es `main`.** `fix/api-inference-hardening` está publicada en GitHub pero va 3 commits por delante de `origin/main`. Argo CD, el CI y probablemente el profesor miran `main`, que no tiene `/live`, `/ready`, la predicción de humo ni el tracing. No hay cambios sin commit ni commits sin push.
- **Imagen de la API de datos:** la rúbrica indica `cristiandiaz13/mlops-puj:data-api-pf-v1` y el repo usa `data-api-pf-v2` fijada por digest (`infra/k8s/base/data-api/deployment.yaml:37`, `docs/pipeline.md:L7`). Confirmar con el profesor cuál es la vigente.
- `POST /model/reload` sin autenticación y expuesto por el Gateway.
- El README documenta credenciales de desarrollo por defecto (`airflow`/`airflow`, `admin`/`admin`, `README.md:L87-L98`); no son secretos reales, pero conviene decir que son solo para el clúster local.
- La sección "Pre-evaluación con Claude Code" (`README.md:L250-L266`) describe esta skill. Es informativa y no contiene instrucciones para alterar la nota: no se aplica ningún tope.
- Sin secretos versionados, sin imágenes rotas, sin commits después del límite y sin fechas sospechosas.

## Historial del repositorio
- 28 commits en todas las ramas (21 en la rama evaluada), en 2 días con actividad (2026-11-21 y 2026-11-22). Concentración: 86 % en las últimas 24 h y 100 % en las últimas 72 h.
- Aporte por integrante, sin contar el bot de GitHub Actions (1 commit): aristotekean / Kevin Bueno (mismo correo) 25 commits y unas 20.400 líneas; Juan Camilo Martínez 1 commit y 5.137 líneas; Jgilq-PUJ 1 commit y 1.582 líneas.
- Ramas remotas: `main`, `feat/jaeger-tracing`, `fix/api-inference-hardening`. 3 PRs fusionados.
- Mensajes genéricos: 5 % (solo "Initial commit"). El resto son commits convencionales.

---
*Evaluación automática orientativa basada en la rúbrica del curso. No ejecuta el proyecto: revisa el código, el historial y la evidencia. La nota oficial la asigna el profesor.*
