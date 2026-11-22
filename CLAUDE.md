# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

End-to-end MLOps platform that estimates real estate prices (regression on `price`). Airflow pulls batches from the course Data API, lands them in PostgreSQL (RAW → CLEAN), decides by explicit rules whether to retrain, registers the candidate in MLflow and moves the `production` alias only if it beats the current model. A FastAPI service serves `models:/real_estate@production` and a Streamlit frontend consumes it. Server A runs on Kubernetes (OrbStack, namespace `real-estate`); Locust (Server B) and Prometheus + Grafana (Server C) run from compose profiles.

The repository was bootstrapped from a diabetes-readmission scaffold. Parts of `README.md` (44-field form, class probabilities, MinIO, ingress-nginx, `notebooks/`) and its evidence captions still describe that project. The code is the source of truth: `ml/real_estate_model/src/real_estate_model/contract.py` for the contract, documented in `docs/data-contract.md`, and `docs/pipeline.md` for the pipeline.

README and `docs/` prose are in Spanish; code, comments and commit messages are in English (conventional commits).

## Commands

There is no repo-level build, lockfile or linter. Each suite has its own environment and none needs network, MLflow or PostgreSQL.

```bash
# api-inference (uv project, Python 3.14)
cd services/api-inference && uv sync --locked --group dev && uv run pytest -q
uv run pytest tests/test_predict.py::test_name -q          # single test

# frontend (uv project)
cd services/frontend && uv sync --locked --group dev && uv run pytest -q

# shared model package (no lockfile: throwaway environment)
cd ml/real_estate_model && uv run --no-project --with . --with pytest pytest -q tests

# Airflow pipeline logic (from the repo root; not run by CI)
uv run --no-project --with-requirements services/airflow/tests/requirements.txt \
  --with-editable ml/real_estate_model python -m pytest services/airflow/tests
```

```bash
# Compose stack (every variable must exist in .env; there are no defaults)
cp .env.example .env && docker compose up -d --build
docker compose --profile loadtest up -d        # Locust
docker compose up -d prometheus grafana        # monitoring only

# Kubernetes
bash infra/k8s/scripts/check.sh                # pre-commit: render .env, kustomize build, migrations drift
bash infra/k8s/scripts/render-env.sh           # .env -> infra/k8s/settings (git) + infra/k8s/generated (Secrets, ignored)
bash infra/k8s/scripts/render-migrations.sh    # infra/database -> migrations ConfigMap (add --check to verify)
kubectl apply -k infra/k8s/generated && kubectl apply -k infra/k8s
```

## Architecture

### One shared package, three consumers

`ml/real_estate_model` owns the input contract, feature engineering, split assignment and candidate models. Airflow trains with it, the registered MLflow model embeds it (`code_paths`), and api-inference builds its request schema from `INPUT_COLUMNS`. The served artifact is the full pipeline (contract → features → preprocessing → regressor), so training and serving run the same code. Consequences:

- A contract or feature change affects all three; run all four test suites.
- Every Dockerfile builds from the **repository root** (`docker build -f services/<name>/Dockerfile .`) because it copies `ml/real_estate_model`. api-inference resolves it as a uv path dependency (`../../ml/real_estate_model`); Airflow pip-installs it.
- `scikit-learn` and `mlflow` are pinned to the same versions in the package, `services/api-inference/pyproject.toml` and `services/airflow/requirements.txt`; change them together or the skops model will not load.

### Airflow pipeline (`services/airflow/dags`)

`real_estate_training.py` only wires tasks; each task lazily imports its logic from the `real_estate_pipeline` package next to it. Tasks pass identifiers and summaries through XCom; data travels through PostgreSQL and models through MLflow.

- The Data API is **stateful**: each `GET /data` hands out the next batch and cannot be replayed. `fetch_batch_from_api` persists the raw response (gzip + SHA-256) in the same task that calls the API, and an unfinished batch is resumed without calling the API again. Hence `max_active_runs=1` and idempotent tasks (each replaces its own rows in one transaction).
- `rules.py` holds the training and promotion decisions as pure functions over reports, with no Airflow, MLflow or database imports. This is what `services/airflow/tests` covers; keep new decision logic there.
- Thresholds are environment variables read in `real_estate_pipeline/settings.py`. In Kubernetes they come from the `pipeline-config` ConfigMap in `infra/k8s/base/airflow/kustomization.yaml`.
- `transformation_version` includes a hash of `TRAINING_BOUNDS_JSON`; changing the bounds makes `preprocess_data` rebuild every earlier batch under the new version.
- Splits are assigned by a salted hash of the property address, so a property never straddles train and test.

### Inference API (`services/api-inference`)

`create_app` in `main.py` takes its dependencies (model registry functions, inference-log repository) as arguments; tests inject the in-memory fakes from `tests/conftest.py`. `ModelService` polls the registry alias every `MODEL_REFRESH_SECONDS` and swaps the model without a restart; with no `production` model, `/predict` returns 503 and `/health` returns 200 with `model_loaded=false`. Every request is written to `inference.logs` on `postgres-train` through `DurableInferenceLog`, which spools rows to a JSONL file while the database is down and replays them later. `/history` exposes `audit.batch_history` to the frontend, which talks only to this API.

Tracing is OpenTelemetry exported over OTLP/HTTP to Jaeger (`jaeger` service in `infra/monitoring`), enabled only when `OTEL_EXPORTER_OTLP_ENDPOINT` is non-empty. The trace starts in the frontend (`requests` instrumentation, `frontend/tracing.py`) and the API continues it (`api_inference/tracing.py`). The provider is passed explicitly to `create_app(tracer_provider=...)` and never registered globally, so tests assert on an in-memory exporter. The `opentelemetry-*` pins in both services move together and must stay compatible with the `opentelemetry-sdk` that `mlflow` resolves.

### Database

`infra/database/migrations` creates the `raw`, `clean`, `audit` and `inference` schemas on `postgres-train`, a different instance from the MLflow and Airflow metadata databases. `migrate.sh` applies files in name order and records them in `public.schema_migrations`. After editing a migration, re-run `render-migrations.sh` and commit the generated ConfigMap.

### Kubernetes and GitOps (`infra/k8s`)

- `base/<component>` holds the manifests; `kustomization.yaml` at the root composes them. `settings/` and `base/postgres-train/migrations-configmap.yaml` are **generated and committed**: edit `.env` or `infra/database` and re-render, never by hand. Kustomize cannot read files outside its root, which is why they are rendered.
- Secrets live in `infra/k8s/generated/` (git-ignored), have fixed names and are applied by hand. Argo CD never sees them; after a credential change, re-apply and `rollout restart`.
- Argo CD (`infra/argocd/application.yaml`) syncs `infra/k8s` on `main` with prune and self-heal, so manual cluster edits are reverted. The init and migration Jobs are Sync hooks and re-run on every sync.
- `infra/k8s/traefik` (Gateway API controller) is installed separately, once, and is not part of the Argo CD application.
- The project images are referenced without a tag in the manifests; the three `newTag` values in `infra/k8s/kustomization.yaml` are rewritten by CI.
- `data-api` is the instructor's image, published for amd64 only; it is pinned by digest so it runs on arm64 nodes.

### CI/CD (`.github/workflows/ci-cd-unit-tests-dockerhub.yml`)

Triggers on changes under `services/`, `ml/` and `scripts/`. It runs three suites (api-inference, frontend, real-estate-model; the Airflow tests are not included), and on pushes to `main` publishes the three `real-estate-*` images (amd64 + arm64, tagged `latest` and the commit SHA), then commits the SHA into `infra/k8s/kustomization.yaml` on `main`. Expect a bot commit `chore(infra): deploy real-estate images <sha>` after each such push; pull before pushing again.
