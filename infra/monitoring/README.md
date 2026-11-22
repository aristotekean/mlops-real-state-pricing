# Observabilidad · Server C (Prometheus + Grafana + Jaeger)

Stack de monitoreo de la API de inferencia. Prometheus recolecta las métricas
que la API expone en `/metrics`; Grafana las visualiza en el dashboard
**Real Estate · Inference API**. Jaeger recibe las trazas de cada solicitud de
inferencia, desde el frontend hasta la escritura del log.

```
api-inference:8000/metrics  ──scrape──▶  S8 Prometheus  ──query──▶  S9 Grafana
frontend ──traceparent──▶ api-inference ──OTLP/HTTP──▶  S10 Jaeger
```

> El almacén de artefactos de este proyecto es **RustFS**, que no expone un
> endpoint de métricas Prometheus, por lo que no se monitorea. El enunciado
> (RF10) solo exige observabilidad de la **API de inferencia**.

## Qué se mide

Este módulo **solo consume** las métricas: la API las expone en `/metrics`
(instrumentada con `prometheus-client`, componente de la API, fuera de esta
carpeta). El dashboard usa exactamente estas series:

**HTTP (middleware de la API)** — la ruta se identifica por la etiqueta `path`
(plantilla de ruta; las no reconocidas van como `unmatched`), y `status` es el
**código HTTP** (`200`, `404`, `503`, …):

| Métrica | Tipo | Etiquetas | Uso en el dashboard |
|---------|------|-----------|---------------------|
| `http_requests_total` | Counter | `method`, `path`, `status` | total, req/s, por endpoint, por estado, errores, tasa de error |
| `http_request_duration_seconds` | Histogram | `method`, `path` | latencia promedio y percentiles p50/p95/p99 |
| `http_requests_in_progress` | Gauge | — | solicitudes en curso |

**Dominio (MLOps)**:

| Métrica | Tipo | Etiquetas | Uso en el dashboard |
|---------|------|-----------|---------------------|
| `predictions_total` | Counter | `status` | predicciones por resultado (succeeded / validation_error / model_unavailable / inference_error) |
| `prediction_duration_seconds` | Histogram | — | latencia de inferencia p50/p95/p99 |
| `model_reloads_total` | Counter | `result` | recargas de modelo (success / failure) |
| `inference_log_failures_total` | Counter | — | fallos al escribir `inference.logs` |

**Proceso (colectores por defecto de `prometheus_client`)**:

| Métrica | Tipo | Uso en el dashboard |
|---------|------|---------------------|
| `process_cpu_seconds_total` | Counter | CPU del pod de la API |
| `process_resident_memory_bytes` | Gauge | memoria del pod de la API |

Los errores se cuentan como `status=~"4..\|5.."`.

> El almacén de artefactos (RustFS) no expone métricas Prometheus, así que el
> dashboard no incluye paneles de almacenamiento. Si en el futuro se adopta un
> store con endpoint de métricas, se añade aquí su job de scrape y su sección.

## Paneles del dashboard

Cubre los mínimos exigidos por el enunciado y añade observabilidad de dominio:

**Tráfico HTTP** — solicitudes totales · req/s · latencia promedio · p95 (KPIs);
serie p50/p95/p99; solicitudes por código de estado; por endpoint; nº de errores
y tasa de error (KPI y en el tiempo).
**Recursos del pod** — CPU y memoria de la API.
**MLOps** — predicciones por resultado; latencia de predicción p50/p95/p99;
recargas de modelo; solicitudes en curso; fallos de log.

## Uso local

El compose raíz incluye este stack bajo el perfil `monitoring`, así que un
`docker compose up` normal no lo arranca. Desde la raíz del repo:

```bash
docker compose up -d prometheus grafana jaeger # solo monitoreo (nombrar el servicio activa su perfil)
docker compose --profile monitoring up -d      # stack completo + monitoreo
```

También es un stack autónomo (Server C): se levanta por su cuenta desde esta
carpeta, sin depender del resto. El perfil es obligatorio también aquí, sin él
compose no arranca nada, y el `.env` de la raíz se pasa con `--env-file` porque
el directorio del proyecto pasa a ser esta carpeta:

```bash
cd infra/monitoring
docker compose --env-file ../../.env -f docker-compose.yml --profile monitoring up -d
```

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Prometheus | <http://localhost:9090> | — |
| Grafana | <http://localhost:3000> | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` del `.env` (`admin` / `admin` en `.env.example`) |
| Jaeger | <http://localhost:16686> | — |

Grafana abre directamente en el dashboard **Real Estate · Inference API** (datasource
y dashboard se aprovisionan solos desde `grafana/provisioning`).

Prometheus hace scrape del target definido en
[`prometheus/targets/api-targets.yml`](prometheus/targets/api-targets.yml)
(`api-inference.real-estate.svc.cluster.local:8000`). Por defecto apunta a la
API del clúster Kubernetes: OrbStack resuelve los Services del clúster desde los
contenedores. Con el stack de compose usar `api-inference:8000`. Si la API no es
alcanzable, el target aparece como **DOWN** (Status → Targets) y sus paneles
muestran "No data".

### Conectar a la API (misma máquina)

Desde la raíz del repo no hay nada que conectar: `docker compose up -d prometheus grafana`
ya comparte la red con la API. Si en cambio levantas el monitoreo desde esta
carpeta, la API corre en el compose raíz (`name: real_estate` → red `real_estate_default`)
y, para que Prometheus la alcance **sin tocar el compose raíz**, el overlay
[`docker-compose.integration.yml`](docker-compose.integration.yml) une Prometheus
a esa red:

```bash
# 1) la API arriba (desde la raíz del repo; crea la red real_estate_default)
docker compose up -d

# 2) el monitoreo conectado a la red de la API
cd infra/monitoring
docker compose --env-file ../../.env -f docker-compose.yml -f docker-compose.integration.yml --profile monitoring up -d
```

Ahora el target `real-estate-api` pasa a **UP** y los paneles se llenan.
`file_sd` recarga el target sin reiniciar, así que para apuntar a otra dirección
basta editar `prometheus/targets/api-targets.yml`.

## Trazas (Jaeger)

La API y el frontend se instrumentan con OpenTelemetry y exportan por OTLP/HTTP
a Jaeger v2 (`jaegertracing/jaeger`, modo all-in-one). Cada inferencia hecha
desde la interfaz produce **una traza con dos servicios**:

```
frontend       POST                    llamada HTTP de Streamlit a la API
api-inference  └─ POST /predict        solicitud completa (validación incluida)
api-inference     ├─ model.predict     normalización + inferencia del modelo
api-inference     └─ inference_log.write   fila en inference.logs (o spool)
```

| Span | Atributos propios |
|------|-------------------|
| `POST /predict` | `inference.request_id`, `inference.outcome` (`succeeded`, `validation_error`, `model_unavailable`, `inference_error`) |
| `model.predict` | `model.name`, `model.version`, `model.alias`; queda en error si el modelo falla o no está cargado |
| `inference_log.write` | `inference_log.stored`: `false` si la fila fue al spool o se perdió |

Los dos sentidos del cruce entre el log y las trazas:

- **De una fila a su traza.** `inference.logs.trace_id` guarda el id de la
  traza; se abre en `http://localhost:16686/trace/<trace_id>`. Solo se guarda
  si la traza fue muestreada, así que un valor no nulo siempre apunta a una
  traza exportada.
- **De una traza a su fila.** En Jaeger, servicio `api-inference` y tag
  `inference.request_id=<uuid>`, que es la clave primaria de `inference.logs`.

Las trazas **no** se guardan en ninguna de las bases de datos del proyecto:
PostgreSQL solo conserva ese `trace_id`. Los spans viven en Jaeger.

**Activación.** El tracing se enciende con `OTEL_EXPORTER_OTLP_ENDPOINT`, la URL
base del receptor OTLP/HTTP (sin `/v1/traces`). Vacía, la API y el frontend no
crean spans ni abren conexiones. La dirección depende de dónde corren:

| Dónde corren la API y el frontend | Valor |
|-----------------------------------|-------|
| compose raíz, junto a Jaeger | `http://jaeger:4318` |
| Kubernetes u otra máquina | `http://<host de Server C>:<JAEGER_OTLP_PORT>`, una dirección alcanzable desde los pods |

En Kubernetes el valor llega por los ConfigMaps `api-inference-env` y
`frontend-env`: se cambia en `.env`, se vuelve a ejecutar
`infra/k8s/scripts/render-env.sh` y se hace commit de `infra/k8s/settings`. El
valor versionado está vacío, es decir, tracing apagado en el clúster.

**Muestreo.** Quien inicia la traza decide si se guarda. El frontend guarda
todas las suyas y la API respeta esa decisión. `OTEL_TRACES_SAMPLE_RATIO`
(de `0` a `1`) solo aplica a las solicitudes que llegan a la API sin traza
propia, como las de Locust o `curl`; conviene bajarlo durante las pruebas de
carga para que el tracing no altere la latencia que se mide.

**Qué no se traza.** `/health` y `/metrics` (sondas y scrape), el refresco
periódico del modelo y el reintento del spool.

**Límites.** Las trazas viven en la memoria del contenedor y se pierden al
reiniciarlo. Si Jaeger no está arriba, la exportación falla en segundo plano
(queda un error en el log) y las solicitudes se atienden igual.

## Despliegue en un Server C dedicado

Cuando el monitoreo corre en una máquina aparte del clúster, apunta el scrape a
la dirección de la API en `prometheus/targets/api-targets.yml` (host:puerto
alcanzable desde Server C) y levanta el stack con `docker-compose.yml`.
Prometheus recarga el target sin reiniciar (`file_sd`).

### Conectar Server C a la red Tailscale (opcional, instalado en el host)

En la arquitectura del equipo los tres servidores se conectan por una **tailnet**
de Tailscale. Esta es la forma **instalando Tailscale en el host** de Server C
(no requiere contenedor ni auth key; es un extra del equipo, no del taller):

1. **Instalar Tailscale** en Server C: <https://tailscale.com/download>.
2. **Iniciar sesión** (login en el navegador):
   ```bash
   sudo tailscale up
   ```
   Al autenticarte, Server C queda como un nodo de la tailnet.
3. **Verificar** que está conectado y ver las IPs/nombres de los nodos:
   ```bash
   tailscale status
   ```
4. **Apuntar Prometheus a la API por la tailnet** en
   `prometheus/targets/api-targets.yml`, usando la **IP de tailnet** de Server A
   (las IPs `100.x.y.z` se enrutan desde los contenedores a través del host):
   ```yaml
   - targets: ["100.x.y.z:8000"]   # IP de Server A en la tailnet
   ```
   > Si prefieres el **nombre** (`server-a:8000`) en vez de la IP, corre Prometheus
   > con `network_mode: host` para que use el MagicDNS del host.
5. **Levantar el monitoreo** normal:
   ```bash
   docker compose --env-file ../../.env -f docker-compose.yml --profile monitoring up -d
   ```

Así Server C alcanza la API de Server A **de forma privada por la tailnet**, sin
abrir puertos públicos y sin cambiar el stack de monitoreo.

## Variables de entorno

Se definen en el `.env` de la raíz del repositorio (plantilla en
[`.env.example`](../../.env.example)); compose no trae valores por defecto y se
detiene si falta alguna. En un Server C dedicado basta copiar ese `.env` con
estas siete variables.

| Variable | Valor en `.env.example` | Descripción |
|----------|---------|-------------|
| `PROMETHEUS_PORT` | `9090` | puerto publicado de Prometheus |
| `PROMETHEUS_RETENTION` | `15d` | retención de la TSDB |
| `GRAFANA_PORT` | `3000` | puerto publicado de Grafana |
| `GRAFANA_ADMIN_USER` | `admin` | usuario admin de Grafana |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | contraseña admin de Grafana |
| `JAEGER_UI_PORT` | `16686` | puerto publicado de la UI de Jaeger |
| `JAEGER_OTLP_PORT` | `4318` | puerto publicado del receptor OTLP/HTTP |

La API y el frontend (Server A) leen además estas dos:

| Variable | Valor sugerido | Descripción |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4318` | receptor OTLP/HTTP; debe existir, vacía apaga el tracing |
| `OTEL_TRACES_SAMPLE_RATIO` | `1.0` | fracción de solicitudes sin traza propia que la API guarda |

## Estructura

```
infra/monitoring/
├── docker-compose.yml                    S8 Prometheus + S9 Grafana + S10 Jaeger (independiente)
├── docker-compose.integration.yml        overlay: conecta Prometheus a la red de la API
├── prometheus/
│   ├── prometheus.yml                    configuración de scrape (file_sd)
│   └── targets/
│       └── api-targets.yml               target de la API (editable por entorno)
└── grafana/
    ├── provisioning/
    │   ├── datasources/datasource.yml    Prometheus (uid: prometheus, URL por env)
    │   └── dashboards/dashboards.yml      provider de dashboards
    └── dashboards/
        └── real-estate-api.json              dashboard principal
```
