# Airflow (S6) image: official base plus the Python dependencies the DAGs need
# to read the training database, train the model and register it in MLflow.
#
# Build context is the repository root, so the shared ml/real_estate_model package
# is installed into the image:
#   docker compose build airflow-scheduler
#   docker build -f services/airflow/Dockerfile -t real-estate-airflow .

ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

USER root

COPY services/airflow/requirements.txt /tmp/requirements.txt
COPY ml/real_estate_model /tmp/real-estate-model
COPY scripts /opt/airflow/scripts
# DAGs are baked in so a Kubernetes deployment needs no bind mount; the local
# compose still mounts services/airflow/dags over this copy for fast iteration.
COPY services/airflow/dags /opt/airflow/dags
RUN chown -R airflow:root /tmp/real-estate-model /tmp/requirements.txt \
    /opt/airflow/scripts /opt/airflow/dags

USER airflow

# Airflow only adds dags/, config/ and plugins/ to sys.path; the repo-root
# scripts package copied above lives in /opt/airflow, so expose it explicitly.
ENV PYTHONPATH=/opt/airflow

# Re-stating apache-airflow pins its version so pip cannot upgrade or
# downgrade it while resolving the DAG dependencies.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt \
    /tmp/real-estate-model
