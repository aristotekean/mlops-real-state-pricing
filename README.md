# Real Estate MLOps · Kubernetes, Airflow y MLflow

**Grupo 6:** Kevin Bueno · Juan Camilo Martínez · Jorge Andrés Gil

Plataforma MLOps de extremo a extremo para estimar el precio de propiedades inmobiliarias. Airflow consume la API de datos del curso lote a lote (grupo 6), valida cada lote, lo guarda en RAW y CLEAN, decide con reglas explícitas si reentrena y registra el candidato en MLflow, donde solo recibe el alias `production` si supera al modelo productivo; los artefactos viven en RustFS (almacén S3), una API FastAPI sirve `models:/real_estate@production` en `POST /predict` y un frontend Streamlit lo consulta. Todo Server A corre en Kubernetes (OrbStack) con autoescalado de la API; Locust (Server B) genera carga y Prometheus + Grafana (Server C) la observan.

## Ruta rápida

1. Instalar en el clúster, una sola vez, el Ingress controller y metrics-server (OrbStack no trae ninguno; el HPA necesita el segundo):

   ```bash
   kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.15.1/deploy/static/provider/cloud/deploy.yaml
   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.1/components.yaml
   ```

2. Desplegar Server A completo (usa las imágenes publicadas en Docker Hub, no hace falta construir nada):

   ```bash
   kubectl create namespace real-estate
   bash infra/k8s/scripts/render-env.sh   # .env -> ConfigMaps (infra/k8s/settings) y Secrets (infra/k8s/generated)
   kubectl apply -k infra/k8s/generated   # los Secrets no están en git
   kubectl apply -k infra/k8s             # o registrar la Application de Argo CD, ver más abajo
   kubectl -n real-estate get pods        # esperar Running / Completed; el primer arranque tarda unos minutos
   ```

3. Activar el DAG `real_estate_incremental_training` en <http://airflow.k8s.orb.local> (`airflow` / `airflow`). Se crea pausado; al activarlo el scheduler lanza la primera corrida, que carga el primer lote, entrena y deja el modelo `real_estate` con el alias `production` en MLflow (alrededor de un minuto):

   ```bash
   kubectl -n real-estate exec deploy/airflow -c scheduler -- airflow dags unpause real_estate_incremental_training
   ```

4. Revisar los runs en <http://mlflow.k8s.orb.local> y la versión servida en <http://api.k8s.orb.local/model-info> (`"status": "ready"`; la API recarga el alias sola cada 60 s).
5. Predecir desde el frontend <http://frontend.k8s.orb.local> (botón **Cargar ejemplo** y **Predecir**) o por API. El cuerpo lleva los 44 campos del contrato de [docs/data-contract.md](docs/data-contract.md):

   ```bash
   curl -s -X POST http://api.k8s.orb.local/predict -H 'Content-Type: application/json' -d @payload.json
   ```

   Respuesta esperada:

   ```json
   {
     "request_id": "8a5dbc13-bb70-4301-bb2e-b6ed8021fa0b",
     "prediction": "<30",
     "probabilities": { "NO": 0.35, ">30": 0.23, "<30": 0.41 },
     "model": { "name": "real_estate", "version": "2", "alias": "production" },
     "processing_time_ms": 36.8
   }
   ```

   La documentación interactiva está en <http://api.k8s.orb.local/docs>.

> Mientras no exista un modelo con el alias `production`, `/predict` responde `503` y `/health` responde `200` con `model_loaded=false`. No hace falta reiniciar la API: consulta el registro cada `MODEL_REFRESH_SECONDS` y `POST /model/reload` fuerza la carga.

## Arquitectura

![Arquitectura de la plataforma](docs/images/1-arquitectura.png)

```mermaid
flowchart LR
    subgraph A["Server A · Kubernetes"]
        s5[("S5 postgres-train<br/>raw · clean · inference")] -->|lotes| s6["S6 Airflow<br/>DAG incremental"]
        s6 -->|runs y modelo| s2["S2 MLflow<br/>tracking + registry"]
        s2 <-->|metadatos| s1[("S1 mlflow-db")]
        s2 <-->|artefactos| s3[("S3 RustFS<br/>bucket mlflow")]
        s4["S4 api-inference<br/>FastAPI + HPA"] -->|"models:/real_estate@production"| s2
        s4 -->|inference.logs| s5
        s7["S7 frontend<br/>Streamlit"] -->|/predict| s4
        ing["Ingress nginx"] --> s4 & s7 & s2 & s6
    end
    b["Server B · Locust"] -->|carga| ing
    s8["Server C · S8 Prometheus"] -->|/metrics| s4
    s8 --> s9["S9 Grafana"]
```

Los tres servidores se conectan por una red Tailscale. En OrbStack todo Server A queda en el namespace `real-estate` y el Ingress resuelve los hosts `*.k8s.orb.local` desde el Mac.

| Servicio | URL | Credenciales | Rol |
|----------|-----|--------------|-----|
| `mlflow-db` (S1) | solo red interna | `mlflow` / `mlflow` | Backend store de MLflow |
| `mlflow` (S2) | <http://mlflow.k8s.orb.local> | sin autenticación | Tracking server y Model Registry; sirve los artefactos por proxy hacia RustFS |
| `rustfs` (S3) | consola `http://rustfs.real-estate.svc.cluster.local:9001/rustfs/console/` | `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` de `.env` | Almacén de artefactos, bucket `mlflow` (lo crea el Job `rustfs-init`) |
| `api-inference` (S4) | <http://api.k8s.orb.local> | sin autenticación | Sirve `models:/real_estate@production`; HPA de 1 a 20 pods al 70 % de CPU |
| `postgres-train` (S5) | solo red interna | `app` / `app`, base `real_estate` | Esquemas `raw`, `clean` e `inference` (migraciones por el Job `train-db-migrate`) |
| `airflow` (S6) | <http://airflow.k8s.orb.local> | `airflow` / `airflow` | api-server, scheduler, dag-processor y triggerer en un pod; metadatos en `airflow-db` |
| `frontend` (S7) | <http://frontend.k8s.orb.local> | — | Formulario de 44 campos; solo habla con la API |
| Prometheus (S8) · Grafana (S9) | <http://localhost:9090> · <http://localhost:3000> | Grafana `admin` / `admin` | `infra/monitoring`, perfil `monitoring` de compose (independiente en Server C) |
| Locust (Server B) | <http://localhost:8089> | — | `infra/load-testing`, perfil `loadtest` de compose |

## Infraestructura en Kubernetes

Todo Server A se despliega con `kubectl apply -k infra/k8s` en el namespace `real-estate`. Cada componente tiene su carpeta en `infra/k8s/base/<componente>` con el workload, su Service y, cuando aplica, PVC y Jobs; los ConfigMaps y Secrets se generan desde `.env` con `infra/k8s/scripts/render-env.sh`: los ConfigMaps quedan en `infra/k8s/settings` (versionado) y los Secrets en `infra/k8s/generated` (ignorado por git, se aplica aparte con `kubectl apply -k infra/k8s/generated`). La imagen de los tres componentes propios se fija en `infra/k8s/kustomization.yaml`.

```mermaid
flowchart TB
    env[".env"] -->|render-env.sh| gen["infra/k8s/settings · ConfigMaps (git)<br/>infra/k8s/generated · Secrets (fuera de git)"]

    subgraph ns["namespace real-estate"]
        ing["Ingress nginx<br/>*.k8s.orb.local"]
        ing --> svcfe["svc frontend"] --> fe["Deployment frontend<br/>1 pod · Streamlit"]
        ing --> svcapi["svc api-inference"] --> api["Deployment api-inference<br/>HPA 1-20 pods · FastAPI"]
        ing --> svcml["svc mlflow"] --> ml["Deployment mlflow<br/>1 pod · tracking + registry"]
        ing --> svcaf["svc airflow-apiserver"] --> af["Deployment airflow<br/>1 pod · api-server, scheduler,<br/>dag-processor, triggerer"]

        api --> svcpt["svc postgres-train"] --> pt[("StatefulSet postgres-train<br/>PVC data-postgres-train-0 · 2Gi")]
        af --> svcpt
        af --> svcafdb["svc airflow-db"] --> afdb[("StatefulSet airflow-db<br/>PVC data-airflow-db-0 · 2Gi")]
        ml --> svcmldb["svc mlflow-db"] --> mldb[("StatefulSet mlflow-db<br/>PVC data-mlflow-db-0 · 2Gi")]
        ml --> svcrustfs["svc rustfs"] --> rustfs[("StatefulSet rustfs<br/>PVC data-rustfs-0 · 5Gi")]
        api & af -->|"models:/ · runs"| svcml
        af --- pvcaf[("PVC airflow-logs · 2Gi")]

        jmig["Job train-db-migrate<br/>ConfigMap train-db-migrations"] --> svcpt
        jrustfs["Job rustfs-init<br/>bucket mlflow"] --> svcrustfs
        jaf["Job airflow-init<br/>migraciones + admin"] --> svcafdb
    end

    gen -.->|envFrom| fe & api & ml & af & pt & afdb & mldb & rustfs
    hub[("Docker Hub<br/>aristotekean/real-estate-*")] -.->|pull| fe & api & af
```

Las bases de datos y RustFS son `StatefulSet` con una réplica: el pod tiene identidad estable (`postgres-train-0`), su PVC sale de `volumeClaimTemplates` y sobrevive al pod, y una actualización termina el pod viejo antes de crear el nuevo, así nunca hay dos instancias escribiendo el mismo volumen. Cada uno tiene además un Service headless (`<nombre>-headless`) que exige el StatefulSet; los clientes siguen usando el Service normal. Los componentes sin estado son `Deployment`; la API escala con un HPA y su spool de logs de inferencia vive en un `emptyDir` para poder correr varias réplicas.

| Componente | Workload | ConfigMap | Secret | Persistencia | Probes | requests → limits |
|------------|----------|-----------|--------|--------------|--------|-------------------|
| `mlflow-db` | StatefulSet | `mlflow-db-env` | `mlflow-db-credentials` | `data-mlflow-db-0` (2Gi) | readiness, liveness (`pg_isready`) | 100m/256Mi → 500m/512Mi |
| `airflow-db` | StatefulSet | `airflow-db-env` | `airflow-db-credentials` | `data-airflow-db-0` (2Gi) | readiness, liveness (`pg_isready`) | 100m/256Mi → 500m/512Mi |
| `postgres-train` | StatefulSet + Job `train-db-migrate` | `postgres-train-env`, `train-db-migrations` | `postgres-train-credentials` | `data-postgres-train-0` (2Gi) | readiness, liveness (`pg_isready`) | 100m/256Mi → 500m/512Mi |
| `rustfs` | StatefulSet + Job `rustfs-init` | — | `rustfs-credentials` | `data-rustfs-0` (5Gi) | readiness, liveness (`/health/ready`, `/health/live`) | 100m/256Mi → 500m/1Gi |
| `mlflow` | Deployment | `mlflow-server-env`, `mlflow-client-env` | `mlflow-db-credentials`, `rustfs-credentials` | sin estado (Postgres + RustFS) | startup, readiness, liveness (`/health`) | 250m/512Mi → 1/2Gi |
| `api-inference` | Deployment + HPA | `api-inference-env`, `mlflow-client-env` | `postgres-train-credentials`, `rustfs-credentials` | `emptyDir` spool (256Mi) | startup, readiness, liveness (`/health`) | 250m/512Mi → 1/1Gi |
| `frontend` | Deployment | `frontend-env` | ninguno (no maneja credenciales) | sin estado | startup, readiness, liveness | 100m/256Mi → 500m/512Mi |
| `airflow` | Deployment + Job `airflow-init` | `airflow-env`, `mlflow-client-env` | `airflow-secrets`, `airflow-db-credentials`, `airflow-admin-user`, `rustfs-credentials`, `postgres-train-credentials` | `airflow-logs` (2Gi, logs de tareas) | api-server: startup, readiness, liveness; scheduler, dag-processor y triggerer: liveness | api-server 250m/512Mi → 500m/1Gi · scheduler 500m/1.5Gi → 2/4Gi · dag-processor 150m/256Mi → 250m/768Mi · triggerer 100m/256Mi → 250m/768Mi |

Los init containers de espera (`pg_isready`, `mc`) y los Jobs también declaran `requests` y `limits`; `bash infra/k8s/scripts/check.sh` renderiza `.env` y valida que el kustomize construya antes de commitear.

### CI/CD con GitHub Actions y Argo CD

El workflow `.github/workflows/ci-cd-unit-tests-dockerhub.yml` se dispara con cambios en `services/`, `ml/` y `scripts/` (pushes a `main` y `feature/**`, y pull requests hacia `main`):

1. `unit-tests` corre las tres suites (`api-inference`, `frontend`, `real-estate-model`).
2. `publish-dockerhub`, solo en pushes a `main` y si las pruebas pasan, construye y publica las tres imágenes `real-estate-*` para `linux/amd64` y `linux/arm64` con las etiquetas `latest` y el SHA del commit.
3. `update-manifests` escribe ese SHA en los tres `newTag` de `infra/k8s/kustomization.yaml` y hace commit en `main`.
4. Argo CD detecta el commit y despliega las imágenes nuevas.

Ese commit solo toca `infra/**`, que está fuera de los `paths` del workflow, y se publica con `GITHUB_TOKEN`, por lo que no vuelve a disparar el workflow. Para habilitar la publicación, configura los secretos del repositorio `DOCKERHUB_USERNAME` y `DOCKERHUB_TOKEN`.

`infra/argocd/application.yaml` declara una `Application` que mantiene el clúster sincronizado con `infra/k8s` en `main` (sincronización automática, con `prune` y `selfHeal`). Puesta en marcha, una sola vez:

```bash
# 1. Instalar Argo CD (consulta la guía oficial para tu versión)
kubectl create namespace argocd
kubectl apply -n argocd --server-side \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# 2. Crear los Secrets (Argo CD no los gestiona: no están en git)
kubectl create namespace real-estate
bash infra/k8s/scripts/render-env.sh
kubectl apply -k infra/k8s/generated

# 3. Registrar la aplicación
kubectl apply -f infra/argocd/application.yaml
```

- Los Jobs (`train-db-migrate`, `rustfs-init`, `airflow-init`) son hooks `Sync`: Argo CD los borra y recrea en cada sincronización, así que se vuelven a ejecutar (son idempotentes).
- Los Secrets tienen nombre fijo. Tras cambiar una credencial en `.env`: renderizar, `kubectl apply -k infra/k8s/generated` y `kubectl -n real-estate rollout restart deployment,statefulset`.
- Un cambio de configuración no sensible en `.env` llega al clúster por git: renderizar y commitear `infra/k8s/settings`.
- Si el repositorio es privado, registra sus credenciales en Argo CD; y el job `update-manifests` necesita poder hacer push a `main` (una regla de protección de rama que lo impida hará fallar el paso).

## Datos en PostgreSQL

`infra/database/migrations` crea los esquemas en `postgres-train`, una base distinta de la de metadatos de MLflow (`mlflow-db`). En Kubernetes los aplica el Job `train-db-migrate` (ConfigMap generado con `infra/k8s/scripts/render-migrations.sh`); en compose, el servicio del mismo nombre.

| Esquema | Contenido |
|---------|-----------|
| `raw` | `raw.api_requests`: cada petición a la API de datos y su resultado (lote nuevo, duplicado, fin de datos, error). `raw.batches`: cada respuesta de la API de datos tal cual (gzip + SHA-256), con número de lote, esquema recibido y estado. `raw.records`: cada registro sin modificar (JSONB), con estado `validated` / `rejected` y su motivo |
| `clean` | Registros tipados listos para entrenar, con trazabilidad a RAW (`raw_record_id`, `batch_id`), motivo de exclusión (outliers) y partición `train` / `validation` / `test` por propiedad |
| `audit` | `audit.batch_history`: una fila por lote con validaciones, drift, categorías nuevas, decisión de entrenamiento y su motivo, IDs de MLflow, métricas y promoción (historial para Streamlit) |
| `inference` | `inference.logs`: una fila por petición a `/predict` (fase de inferencia) |

## Entrenamiento y registro del modelo

Los datos llegan de la API del curso (`cristiandiaz13/mlops-puj:data-api-pf-v2`, Deployment `data-api`, **grupo 6**, 6 lotes). Cada corrida del DAG `real_estate_incremental_training` pide **un** lote, lo guarda en RAW, lo valida (esquema, calidad, categorías nuevas, drift), lo procesa a CLEAN y decide con reglas explícitas si entrena. Si entrena, compara el candidato contra el modelo productivo y solo mueve el alias `production` si mejora según la regla de promoción.

El diseño completo, las reglas de decisión y de promoción y la tabla de parámetros ajustables están en **[docs/pipeline.md](docs/pipeline.md)**. Los umbrales viven en el ConfigMap `pipeline-config` de [infra/k8s/base/airflow/kustomization.yaml](infra/k8s/base/airflow/kustomization.yaml).

| Qué se registra | Detalle |
|-----------------|---------|
| Candidatos | Ridge e HistGradientBoosting sobre `log1p(price)`, como runs anidados del experimento `real-estate-pricing` |
| Métrica principal | MAE de validación (USD, interpretable), con RMSE como métrica de control |
| Promoción | Se promueve si el MAE mejora ≥ 3 % y el RMSE no empeora más de 1 % frente al productivo, ambos evaluados sobre la misma validación |
| Trazabilidad | Lotes usados, commit (`GIT_COMMIT`), parámetros, métricas train/validation/test, gráficos, importancia por permutación y motivo del entrenamiento |

## Estructura del repositorio

```text
.
├── .github/workflows/                # CI: pruebas, publicación en Docker Hub y pin de imágenes en infra/k8s
├── docker-compose.yml                # Stack completo en compose (incluye airflow, locust y monitoring)
├── services/
│   ├── api-inference/                # S4 FastAPI: /predict, /health, /model-info, /model/reload, /metrics
│   ├── frontend/                     # S7 Streamlit
│   └── airflow/                      # S6 imagen y DAG real_estate_incremental_training
├── ml/real_estate_model/                # Paquete compartido: contrato, features, splits, modelos
├── infra/
│   ├── argocd/                       # Application de Argo CD que sincroniza infra/k8s
│   ├── database/migrations/          # Esquemas raw, clean e inference
│   ├── k8s/                          # Manifiestos kustomize de Server A (S1–S7, Ingress, HPA)
│   ├── load-testing/                 # Server B: Locust
│   └── monitoring/                   # Server C: Prometheus y Grafana
├── notebooks/01_eda.ipynb            # Análisis exploratorio que justifica el procesamiento
├── scripts/publish_images.sh         # Publica las imágenes multi-arquitectura en Docker Hub
└── docs/                             # data-contract.md y capturas de evidencia
```

## Ejecutar las pruebas

Cada aplicación se prueba por separado con `uv`; ninguna necesita red, MLflow ni PostgreSQL.

```bash
cd services/api-inference && uv sync && uv run pytest -q
cd services/frontend && uv sync && uv run pytest -q
python3 -m venv .venv && .venv/bin/pip install -e ./ml/real_estate_model pytest && .venv/bin/python -m pytest ml/real_estate_model/tests
uv run --no-project --with-requirements services/airflow/tests/requirements.txt --with-editable ml/real_estate_model python -m pytest services/airflow/tests
```

| Suite | Qué verifica |
|-------|--------------|
| `services/api-inference/tests` | `/health` y `/model-info` con y sin modelo, una predicción por petición, `422` ante campos faltantes o tipos incorrectos, `503` sin modelo y las métricas Prometheus |
| `services/frontend/tests` | El cliente HTTP de la API y la construcción de los 44 campos del formulario |
| `ml/real_estate_model/tests` | El contrato de entrada, la ingeniería de características y los candidatos |
| `services/airflow/tests` | Las reglas de entrenamiento y de promoción, el cliente de la API de datos, el PSI y la versión de CLEAN |

## Operación

| Tarea | Cómo |
|-------|------|
| Levantar todo con compose en vez de Kubernetes | Copiar `.env.example` a `.env` y `docker compose up -d` (en Linux ajustar `AIRFLOW_UID` con `id -u`). MLflow en `localhost:5001`, API en `8000`, frontend en `8501`, Airflow en `8080` |
| Publicar una versión nueva | Un push a `main` la publica con el SHA del commit (ver [CI/CD](#cicd-con-github-actions-y-argo-cd)). A mano: `IMAGE_TAG=0.1.2 scripts/publish_images.sh` construye `amd64` y `arm64` y publica. Siempre un tag nuevo: el nodo reutiliza un tag ya descargado aunque el contenido cambie |
| Redesplegar | Con Argo CD, el workflow fija los tres `newTag` en `infra/k8s/kustomization.yaml` y Argo CD sincroniza. A mano: cambiar los tres `newTag`, `kubectl apply -k infra/k8s` y `kubectl -n real-estate rollout status deploy/api-inference deploy/frontend deploy/airflow`. Los PVC conservan modelo, bases y dataset |
| Cambiar una migración SQL | Editar `infra/database/migrations`, correr `bash infra/k8s/scripts/render-migrations.sh` y commitear el ConfigMap. `bash infra/k8s/scripts/check.sh` valida los manifiestos antes de commitear |
| Pausar el reentrenamiento | `kubectl -n real-estate exec deploy/airflow -c scheduler -- airflow dags pause real_estate_incremental_training` (en un portátil conviene tras la primera corrida) |
| Prueba de carga | `LOCUST_HOST=http://api.k8s.orb.local docker compose --profile loadtest up -d` y abrir `localhost:8089`; `LOCUST_STEP_LOAD=1` busca el punto de degradación por escalones |
| Observar el autoescalado | `kubectl -n real-estate get hpa api-inference -w` muestra CPU y réplicas en vivo; añade hasta 2 pods cada 30 s y quita 1 por minuto tras 2 minutos de calma |
| Monitoreo | `docker compose up -d prometheus grafana` levanta solo Prometheus y Grafana (`--profile monitoring up -d` los suma al stack completo); apuntar `prometheus/targets/api-targets.yml` a la API y `prometheus/targets/minio-targets.yml` a MinIO. Grafana abre en el dashboard **Real Estate · Inference API** (tráfico HTTP, latencias, errores, CPU/memoria del pod, MLOps y MinIO) |
| Actualizar un clúster desplegado con los `Deployment` anteriores de las bases | Los `StatefulSet` crean PVC nuevos (`data-<nombre>-0`), así que los datos no migran solos. Tras `kubectl apply -k infra/k8s`, borrar los workloads viejos para que el Service no reparta tráfico entre ambos: `kubectl -n real-estate delete deployment mlflow-db airflow-db postgres-train`; los PVC viejos (`mlflow-db-data`, `airflow-db-data`, `postgres-train-data`) quedan hasta borrarlos a mano. El DAG regenera datos y modelo en la primera corrida |
| Actualizar un clúster desplegado con MinIO | `kubectl apply -k infra/k8s` crea `rustfs` con un PVC vacío (`data-rustfs-0`) pero no borra lo anterior. Borrar los objetos viejos: `kubectl -n real-estate delete statefulset/minio service/minio service/minio-headless job/minio-init --ignore-not-found`; el PVC `data-minio-0` queda hasta borrarlo a mano. Los artefactos no migran solos: el DAG regenera el modelo en la primera corrida |
| Borrar todo | `kubectl delete -k infra/k8s` (los PVC y sus datos se van con el namespace) |

Locust y el monitoreo están detrás de los perfiles `loadtest` y `monitoring` del compose raíz, así que `docker compose up -d` no los arranca. Para el monitoreo:

```bash
docker compose up -d prometheus grafana        # solo Prometheus + Grafana (nombrar el servicio activa su perfil)
docker compose --profile monitoring up -d      # stack completo + monitoreo
```

Prometheus en <http://localhost:9090> y Grafana en <http://localhost:3000> (`admin` / `admin`). Si se levanta desde `infra/monitoring` o `infra/load-testing` por separado (Server C o Server B), el `--profile` también es obligatorio: sin él compose no arranca ningún servicio.

> Los Secrets llevan las credenciales de desarrollo de `.env` (RustFS, `airflow`, `app`, `mlflow`). Son para un clúster local; cámbialas antes de exponerlo a otras máquinas.

## Solución de problemas

- `/predict` responde `503`: todavía no hay un modelo con el alias `production` o MLflow no es alcanzable. Activa el DAG y espera la primera corrida; `curl http://api.k8s.orb.local/model-info` muestra el último error.
- La API sigue sirviendo una versión vieja tras promover otra: el refresco es cada 60 s; `curl -X POST http://api.k8s.orb.local/model/reload` la fuerza.
- El HPA muestra `TARGETS <unknown>`: metrics-server no está instalado o aún no reporta (tarda alrededor de un minuto). Pods `Pending` al escalar: el nodo no tiene memoria libre que reservar (OrbStack: Settings → System).
- El pod corre una imagen distinta a la de Docker Hub: el tag ya existía en la máquina (`imagePullPolicy: IfNotPresent` y OrbStack comparte el almacén con Docker). Publica con un tag nuevo.
- `kubectl apply` dice que `airflow-init` es inmutable: el Job aún no venció su TTL de 10 minutos. `kubectl -n real-estate delete job airflow-init` y vuelve a aplicar.
- MLflow responde `403` o la UI carga sin runs desde otra máquina: añade el host a `MLFLOW_ALLOWED_HOSTS` y el origen a `MLFLOW_CORS_ALLOWED_ORIGINS` en `infra/k8s/base/mlflow/deployment.yaml`.
- Los hosts `*.k8s.orb.local` solo existen en OrbStack. En un servidor Linux sustitúyelos en `infra/k8s/base/ingress/ingress.yaml` por el nombre Tailscale del nodo y expón el controller con el LoadBalancer o NodePort del clúster.

## Evidencias

### 1. Imágenes publicadas en Docker Hub

Los tres repositorios públicos del namespace `aristotekean`: `real-estate-airflow`, `real-estate-frontend` y `real-estate-api-inference`, cada uno con el manifiesto multi-arquitectura (`linux/amd64` y `linux/arm64`) que consumen compose y Kubernetes.

![Imágenes en Docker Hub](docs/images/2-docker-hub-images.png)

### 2. Clúster de Kubernetes

OrbStack con el namespace `real-estate` en ejecución: los pods de `airflow`, `airflow-db`, `api-inference`, `frontend`, `minio` (hoy `rustfs`), `mlflow`, `mlflow-db` y `postgres-train`, más `ingress-nginx-controller`. El detalle del pod de Airflow muestra los `initContainers` `wait-for-airflow-db` y `wait-for-migrations` y los cuatro contenedores (api-server, dag-processor, scheduler y triggerer). La salida de `kubectl get svc -A` y `kubectl get ingress -A` muestra los Services del namespace, el LoadBalancer del Ingress controller y el Ingress `real_estate` con los hosts `frontend`, `api`, `mlflow` y `airflow` en `k8s.orb.local`.

![Clúster de Kubernetes](docs/images/3-kubernetes-cluster.png)

![Pods en ejecución](docs/images/3-1-kubernetes-pods.png)

![Services e Ingress del clúster](docs/images/3-2-kubernetes-ctrl.png)

### 3. DAG de entrenamiento en Airflow

Corrida programada de `real_estate_incremental_training` con las once tareas en orden, de `validate_source_file` a `promote_model`. `train_candidates` tarda 31 s con el lote cargado; las demás, entre uno y tres segundos.

![DAG de entrenamiento en Airflow](docs/images/4-airflow.png)

### 4. API de inferencia

Documentación interactiva de la Real Estate Inference API en `api.k8s.orb.local/docs`, con los endpoints `POST /predict`, `GET /health`, `GET /model-info` y `POST /model/reload`.

![API de inferencia](docs/images/5-api-inference.png)

### 5. Frontend

Streamlit en `frontend.k8s.orb.local`: la barra lateral confirma la API disponible y el modelo productivo `real_estate` versión 2 con el alias `production`; el formulario carga un encuentro de ejemplo del dataset. Tras **Predecir**, la respuesta es `<30` (readmisión en menos de 30 días), con las probabilidades por clase, el modelo `diabetes v2`, 36,8 ms de procesamiento y el `request_id` registrado en `inference.logs`.

![Frontend](docs/images/6-frontend.png)

![Predicción desde el frontend](docs/images/6-1-frontend-prediction.png)

### 6. Experimentos en MLflow

MLflow 3.16.1 en `mlflow.k8s.orb.local`, experimento `real-estate-pricing` con los runs padre de cada corrida del DAG (`training-scheduled__…` y `training-manual__…`), con origen `airflow`; dentro de cada uno están los tres candidatos anidados.

![Experimentos en MLflow](docs/images/7-mlflow-experiments.png)
