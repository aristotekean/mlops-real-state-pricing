#!/usr/bin/env python3
"""
Recolecta datos objetivos de un repositorio de proyecto final MLOps para su evaluación.

NO ejecuta código del repositorio: solo lee archivos y consulta el historial de git.

Lee el repositorio local tal como está en disco: los archivos versionados más los
nuevos que git no ignora. Deja fuera lo ignorado por .gitignore, la carpeta .claude/
(donde vive esta skill) y los reportes evaluacion_*.md anteriores.

Uso:
    python3 recolectar.py <ruta_repo> --fecha-limite 2026-11-26T23:59:59-05:00 \
        --salida evidencia.json [--imagenes-dir ./imagenes]

Salida: un JSON con historial git, inventario de componentes, análisis del README
(imágenes, texto alrededor, enlaces) y señales de alerta. Imprime un resumen legible.
Solo usa la biblioteca estándar de Python (3.8+).
"""
import argparse
import collections
import datetime as dt
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request

EXCLUIR_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", "env", ".mypy_cache",
                ".pytest_cache", ".ipynb_checkpoints", ".idea", ".vscode"}
EXT_TEXTO = {".py", ".yml", ".yaml", ".md", ".txt", ".toml", ".cfg", ".ini", ".json", ".sh",
             ".sql", ".env", ".tpl", ".j2", ".conf", ".dockerfile", ".ipynb", ""}
EXT_IMAGEN = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp"}
EXT_MODELO = {".pkl", ".pickle", ".joblib", ".h5", ".pt", ".pth", ".onnx", ".sav", ".keras", ".cbm"}
MAX_LECTURA = 1_500_000  # bytes por archivo de texto
EXCLUIR_PREFIJOS = (".claude/",)
RX_REPORTE_PREVIO = re.compile(r"^evaluacion_[^/]*\.md$")
_LISTADOS = {}

MENSAJES_GENERICOS = re.compile(
    r"^(update|updates|updated|fix|fixes|changes?|cambios?|commit|wip|test|prueba|asdf|\.+|-+|"
    r"add files via upload|update readme\.md|initial commit|first commit|final|ok|listo|save|"
    r"subiendo|actualizacion|actualización|varios|misc)\s*\.?$", re.I)

# (categoría, regex sobre ruta, regex sobre contenido) — una coincidencia en cualquiera basta.
SENALES = [
    ("airflow_dag", None, r"(from airflow|import airflow)[\s\S]{0,4000}?(DAG\(|@dag\b)"),
    ("mlflow_tracking", None, r"mlflow\.(start_run|log_metric|log_param|log_model|autolog|set_experiment)"),
    ("mlflow_registro", None, r"(register_model|registered_model_name|create_registered_model|"
                              r"set_registered_model_alias|transition_model_version_stage)"),
    ("mlflow_produccion", None, r"(models:/[^\"'\s]+(@|/)(production|Production|prod|champion)|"
                                r"set_registered_model_alias\([^)]*(production|prod|champion)|"
                                r"stage\s*=\s*[\"']Production)"),
    ("mlflow_backend_postgres", None, r"(backend[-_]store[-_]uri[^\n]*postgres|postgresql(\+psycopg2)?://)"),
    ("mlflow_s3_minio", None, r"(MLFLOW_S3_ENDPOINT_URL|default[-_]artifact[-_]root[^\n]*s3://|"
                              r"artifacts[-_]destination[^\n]*s3://|minio/minio|bitnami/minio)"),
    ("boto3", None, r"import boto3|from boto3"),
    ("fastapi", None, r"from fastapi import|import fastapi"),
    ("streamlit", None, r"import streamlit|from streamlit"),
    ("shap", None, r"import shap|from shap"),
    ("prometheus_metricas", None, r"(prometheus_client|prometheus_fastapi_instrumentator|"
                                  r"starlette_exporter|/metrics)"),
    ("prometheus_config", r"prometheus.*\.ya?ml$", r"scrape_configs:"),
    ("grafana", None, r"(grafana/grafana|grafana\.ini|\"panels\"\s*:|apiVersion:\s*1[\s\S]{0,200}"
                      r"(datasources|providers):)"),
    ("grafana_configmap", None, r"kind:\s*ConfigMap[\s\S]{0,3000}?(grafana_dashboard|dashboards?\.json|"
                                r"\"panels\")"),
    ("dockerfile", r"(^|/)(Dockerfile|[^/]*\.dockerfile|Dockerfile\.[^/]+)$", None),
    ("docker_compose", r"(^|/)(docker-)?compose[^/]*\.ya?ml$", None),
    ("compose_build_local", r"(^|/)(docker-)?compose[^/]*\.ya?ml$", r"^\s+build\s*:"),
    ("gha_workflow", r"^\.github/workflows/[^/]+\.ya?ml$", None),
    ("gha_docker_push", r"^\.github/workflows/", r"(docker/build-push-action|docker push|"
                                                 r"docker/login-action)"),
    ("k8s_manifiesto", None, r"^kind:\s*(Deployment|StatefulSet|Service|DaemonSet|Job|CronJob|"
                             r"PersistentVolumeClaim|Ingress)\b"),
    ("helm_chart", r"(^|/)Chart\.ya?ml$", None),
    ("argocd_application", None, r"kind:\s*(Application|ApplicationSet)\b[\s\S]{0,2000}?argoproj|"
                                 r"argoproj\.io/v1alpha1[\s\S]{0,300}?kind:\s*Application"),
    ("argocd_autosync", None, r"syncPolicy:[\s\S]{0,200}?automated"),
    ("airflow_gitsync", None, r"(gitSync|git-sync|GIT_SYNC_)"),
    ("bucket_auto", None, r"(mc mb|mc mc|create_bucket|make_bucket|MINIO_DEFAULT_BUCKETS|"
                          r"head_bucket|bucket_exists)"),
    ("datos_api_externa", None, r"(/data\?group_number|[\"']group_number[\"']|group_number=)"),
    ("raw_data", None, r"(raw[_-]?data|rawdata|RAW_DATA)"),
    ("clean_data", None, r"(clean[_-]?data|cleandata|CLEAN_DATA|processed[_-]?data)"),
    ("drift_o_decision", None, r"(drift|ks_2samp|kolmogorov|psi\b|population stability|"
                               r"wasserstein|chi2_contingency|evidently|should_retrain|"
                               r"need(s)?_retrain|reentren|retrain)"),
    ("data_api_imagen", None, r"cristiandiaz13/mlops-puj:data-api"),
    ("mlflow_alias", None, r"(set_registered_model_alias|get_model_version_by_alias|models:/[^\"'\s]+@)"),
    ("mlflow_git_commit", None, r"(mlflow\.source\.git\.commit|GITHUB_SHA|GIT_COMMIT|git rev-parse|commit_sha|code_version)"),
    ("validacion_esquema", None, r"(validate_schema|validar_esquema|pandera|great_expectations|expected_columns|columnas_esperadas)"),
    ("validacion_calidad", None, r"(validate_data_quality|duplicated\(|drop_duplicates|isnull\(\)|isna\(\)|outlier|iqr)"),
    ("drift_estadistico", None, r"(ks_2samp|kolmogorov|\bpsi\b|population.stability|wasserstein|jensenshannon|chi2_contingency|evidently|data_drift)"),
    ("nuevas_categorias", None, r"(handle_unknown|new_categor|nuevas?_categor|unseen|min_frequency|infrequent)"),
    ("tabla_auditoria", None, r"(audit|auditoria|auditoría|batch_history|batch_log|training_history|historial_lotes|decision_log)"),
    ("control_lotes", None, r"(batch_id|lote_id|id_lote|batch_number|response_hash|sha256|md5|ingestion_id)"),
    ("recarga_modelo", None, r"(reload_model|recargar_modelo|/reload|refresh_model|model_watcher|threading\.R?Lock|asyncio\.Lock)"),
    ("pydantic", None, r"from pydantic import"),
    ("registro_inferencias", None, r"(inference_log|inference_events?|predictions_log|prediction_log|inferencias|log_inference|registro_inferencia)"),
    ("locust", r"(^|/)locustfile[^/]*\.py$", r"from locust import"),
    ("k8s_secret", None, r"(kind:\s*Secret\b|secretKeyRef|secretRef:|SealedSecret|ExternalSecret)"),
    ("k8s_probes", None, r"(readinessProbe|livenessProbe|startupProbe):"),
    ("k8s_resources", None, r"^\s+(limits|requests):\s*$"),
    ("tests", r"(^|/)(tests?/|test_[^/]+\.py$|[^/]+_test\.py$)", None),
]

SECRETOS = [
    ("aws_key", r"AKIA[0-9A-Z]{16}"),
    ("github_token", r"gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,}"),
    ("dockerhub_token", r"dckr_pat_[A-Za-z0-9_-]{20,}"),
    ("clave_privada", r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    ("kubeconfig_con_credenciales", r"(client-key-data|client-certificate-data)\s*:\s*[A-Za-z0-9+/=]{40,}"),
    ("kubeconfig_token", r"^\s*token\s*:\s*[A-Za-z0-9._\-]{30,}"),
]


def sh(args, cwd):
    r = subprocess.run(args, cwd=cwd, capture_output=True, text=True, errors="replace")
    return r.stdout


def parse_fecha(s):
    s = s.strip()
    if s.endswith("Z"):  # Python < 3.11 no acepta el sufijo Z
        s = s[:-1] + "+00:00"
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    return d


# ------------------------------------------------------------------ git
def analizar_git(repo, limite):
    sep = "\x1f"
    raw = sh(["git", "log", "--first-parent", "HEAD", f"--format=%H{sep}%an{sep}%ae{sep}%aI{sep}%cI{sep}%P{sep}%s"], repo)
    todos = sh(["git", "log", "--all", "--format=%H"], repo).split()
    commits = []
    for line in raw.splitlines():
        p = line.split(sep)
        if len(p) < 7:
            continue
        commits.append({"hash": p[0][:10], "autor": p[1], "email": p[2], "fecha_autor": p[3],
                        "fecha_commit": p[4], "merge": len(p[5].split()) > 1, "mensaje": p[6]})
    if not commits:
        return {"error": "sin commits"}
    ahora = dt.datetime.now(dt.timezone.utc)
    fechas = [parse_fecha(c["fecha_commit"]) for c in commits]
    despues = [c for c, f in zip(commits, fechas) if limite and f > limite]
    ultimo_antes = next((c for c, f in zip(commits, fechas) if not limite or f <= limite), None)
    futuras = [c for c, f in zip(commits, fechas) if f > ahora + dt.timedelta(hours=1)]
    discrepancias = [c for c in commits
                     if abs(parse_fecha(c["fecha_autor"]) - parse_fecha(c["fecha_commit"])) > dt.timedelta(days=2)]

    # Autores por todo el historial (no solo first-parent)
    autores_raw = sh(["git", "log", "--all", "--format=%an <%ae>"], repo).splitlines()
    autores = collections.Counter(autores_raw)
    numstat = sh(["git", "log", "--all", "--no-merges", "--format=@@%an", "--numstat"], repo)
    lineas = collections.Counter()
    actual = None
    for line in numstat.splitlines():
        if line.startswith("@@"):
            actual = line[2:]
        elif actual and "\t" in line:
            a, d, _ = line.split("\t", 2)
            if a.isdigit() and d.isdigit():
                lineas[actual] += int(a) + int(d)

    por_dia = collections.Counter(f.astimezone(dt.timezone(dt.timedelta(hours=-5))).date().isoformat()
                                  for f in fechas)
    primero, ultimo = min(fechas), max(fechas)
    # Concentración: qué fracción de commits cae en las últimas horas del trabajo
    # (referencia = el menor entre el último commit y la fecha límite).
    ref = min(ultimo, limite) if limite else ultimo
    ventanas = {"referencia": ref.isoformat()}
    for horas in (24, 72, 168):
        n = sum(1 for f in fechas if ref - dt.timedelta(hours=horas) < f <= ref)
        ventanas[f"ultimas_{horas}h"] = {"commits": n, "proporcion": round(n / len(fechas), 2)}
    genericos = [c["mensaje"] for c in commits if MENSAJES_GENERICOS.match(c["mensaje"].strip())]
    ramas = [b.strip() for b in sh(["git", "branch", "-r"], repo).splitlines() if "->" not in b]
    merges_pr = [c for c in commits if c["merge"] and re.search(r"pull request #\d+", c["mensaje"], re.I)]
    tags = sh(["git", "tag", "--list"], repo).split()
    sin_commit = [l for l in sh(["git", "status", "--porcelain"], repo).splitlines()
                  if l.strip() and not excluido(l[3:].strip().strip('"').split(" -> ")[-1])]
    upstream = sh(["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"], repo).strip()
    sin_push = sh(["git", "rev-list", "--count", "@{u}..HEAD"], repo).strip() if upstream else ""

    return {
        "estado_local": {
            "cambios_sin_commit": sin_commit[:60],
            "total_cambios_sin_commit": len(sin_commit),
            "rama_remota_seguida": upstream or None,
            "commits_sin_push": int(sin_push) if sin_push.isdigit() else None,
        },
        "total_commits_rama_principal": len(commits),
        "total_commits_todas_ramas": len(set(todos)),
        "primer_commit": primero.isoformat(), "ultimo_commit": ultimo.isoformat(),
        "dias_con_commits": len(por_dia),
        "dias_entre_primero_y_ultimo": (ultimo - primero).days,
        "commits_por_dia": dict(sorted(por_dia.items())),
        "concentracion": ventanas,
        "fecha_limite": limite.isoformat() if limite else None,
        "commits_despues_de_limite": [{k: c[k] for k in ("hash", "autor", "fecha_commit", "mensaje")}
                                      for c in despues],
        "ultimo_commit_antes_de_limite": ultimo_antes and {k: ultimo_antes[k] for k in
                                                           ("hash", "fecha_commit", "mensaje")},
        "commits_con_fecha_futura": [c["hash"] for c in futuras],
        "commits_fecha_autor_vs_commit_difieren_mas_2_dias": [
            {k: c[k] for k in ("hash", "fecha_autor", "fecha_commit")} for c in discrepancias[:20]],
        "autores_commits": dict(autores.most_common()),
        "autores_lineas_modificadas": dict(lineas.most_common()),
        "mensajes_genericos": {"cantidad": len(genericos), "proporcion": round(len(genericos) / len(commits), 2),
                               "ejemplos": genericos[:10]},
        "ramas_remotas": ramas, "merges_de_pull_requests": len(merges_pr), "tags": tags,
        "ultimos_commits": [{k: c[k] for k in ("hash", "autor", "fecha_commit", "mensaje")} for c in commits[:15]],
    }


# ------------------------------------------------------------------ archivos
def excluido(rel):
    """La skill instalada en .claude/ y los reportes previos no son parte del proyecto."""
    return rel.startswith(EXCLUIR_PREFIJOS) or bool(RX_REPORTE_PREVIO.match(rel))


def listar_archivos(repo):
    """Rutas relativas que git conoce: versionadas más nuevas no ignoradas."""
    if repo not in _LISTADOS:
        r = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                           cwd=repo, capture_output=True)
        if r.returncode == 0:
            rels = sorted(set(p for p in r.stdout.decode("utf-8", errors="replace").split("\0") if p))
        else:
            rels = []
            for raiz, dirs, archivos in os.walk(repo):
                dirs[:] = [d for d in dirs if d not in EXCLUIR_DIRS]
                rels += [os.path.relpath(os.path.join(raiz, a), repo).replace(os.sep, "/") for a in archivos]
        _LISTADOS[repo] = [rel for rel in rels
                           if not excluido(rel) and not EXCLUIR_DIRS.intersection(rel.split("/")[:-1])]
    return _LISTADOS[repo]


def recorrer(repo):
    for rel in listar_archivos(repo):
        ruta = os.path.join(repo, rel)
        if os.path.isfile(ruta):
            yield rel, ruta


def leer(ruta):
    try:
        with open(ruta, "rb") as f:
            data = f.read(MAX_LECTURA)
        if b"\x00" in data[:4096]:
            return None
        return data.decode("utf-8", errors="replace")
    except OSError:
        return None


def es_puntero_lfs(texto):
    return texto is not None and texto.startswith("version https://git-lfs.github.com/spec/v1")


def analizar_archivos(repo):
    senales = collections.defaultdict(list)
    grandes, modelos, datos, secretos, env_files, lfs = [], [], [], [], [], []
    total, por_ext = 0, collections.Counter()
    for rel, ruta in recorrer(repo):
        total += 1
        ext = os.path.splitext(rel)[1].lower()
        por_ext[ext or "(sin ext)"] += 1
        try:
            tam = os.path.getsize(ruta)
        except OSError:
            continue
        if tam > 10_000_000:
            grandes.append({"ruta": rel, "mb": round(tam / 1e6, 1)})
        if ext in EXT_MODELO:
            modelos.append(rel)
        if ext in {".csv", ".parquet", ".feather"} and tam > 1_000_000:
            datos.append({"ruta": rel, "mb": round(tam / 1e6, 1)})
        base = os.path.basename(rel)
        if base == ".env" or (base.startswith(".env") and not base.endswith((".example", ".sample", ".template"))):
            env_files.append(rel)
        texto = None
        if ext in EXT_TEXTO or base.startswith("Dockerfile") or ext in {".yml", ".yaml"}:
            texto = leer(ruta)
            if es_puntero_lfs(texto):
                lfs.append(rel)
        for cat, rx_ruta, rx_cont in SENALES:
            ok_ruta = rx_ruta and re.search(rx_ruta, rel, re.I)
            if rx_ruta and rx_cont:
                ok = ok_ruta and texto and re.search(rx_cont, texto, re.M)
            elif rx_ruta:
                ok = ok_ruta
            else:
                ok = texto and re.search(rx_cont, texto, re.M)
            if ok:
                senales[cat].append(rel)
        if texto:
            for nombre, rx in SECRETOS:
                if re.search(rx, texto):
                    secretos.append({"tipo": nombre, "ruta": rel})
    # Detalle de workflows
    workflows = []
    for rel in senales.get("gha_workflow", []):
        t = leer(os.path.join(repo, rel)) or ""
        workflows.append({
            "ruta": rel,
            "triggers": re.findall(r"^\s{0,2}(push|pull_request|workflow_dispatch|schedule|release)\s*:", t, re.M),
            "filtra_paths": bool(re.search(r"^\s+paths\s*:", t, re.M)),
            "build_push_action": "docker/build-push-action" in t,
            "login_dockerhub": "docker/login-action" in t or "docker login" in t,
            "usa_secrets": sorted(set(re.findall(r"secrets\.([A-Z0-9_]+)", t))),
            "tags": re.findall(r"tags\s*:\s*[|>]?\s*\n?\s*([^\n]+)", t)[:6],
            "credenciales_en_claro": bool(re.search(r"(password|token)\s*:\s*['\"]?[A-Za-z0-9_\-]{8,}", t, re.I)
                                          and "secrets." not in t),
        })
    # Imágenes declaradas en manifiestos / compose
    imagenes = collections.Counter()
    for cat in ("k8s_manifiesto", "docker_compose", "helm_chart"):
        for rel in senales.get(cat, []):
            base_dir = os.path.dirname(os.path.join(repo, rel)) if cat == "helm_chart" else None
            archivos = [os.path.join(repo, rel)]
            if base_dir:
                archivos = [os.path.join(r, f) for r, _, fs in os.walk(base_dir) for f in fs
                            if f.endswith((".yaml", ".yml"))]
            for a in archivos:
                t = leer(a) or ""
                for m in re.findall(r"^\s*(?:-\s*)?image\s*:\s*[\"']?([^\s\"'#]+)", t, re.M):
                    imagenes[m] += 1
                for m in re.findall(r"^\s*repository\s*:\s*[\"']?([^\s\"'#]+)", t, re.M):
                    imagenes[m] += 1
    return {
        "total_archivos": total,
        "archivos_por_extension": dict(por_ext.most_common(15)),
        "senales": {k: sorted(set(v))[:40] for k, v in sorted(senales.items())},
        "senales_ausentes": [c for c, _, _ in SENALES if c not in senales],
        "workflows": workflows,
        "imagenes_contenedor_referenciadas": dict(imagenes.most_common(60)),
        "archivos_grandes_mas_10mb": grandes, "modelos_binarios_en_repo": modelos[:30],
        "datasets_en_repo_mas_1mb": datos[:30], "archivos_env_versionados": env_files,
        "posibles_secretos": secretos, "punteros_git_lfs": lfs[:30],
    }


RX_WORKLOAD = re.compile(r"^kind:\s*(Deployment|StatefulSet|DaemonSet|Job|CronJob)\b", re.M)


def analizar_workloads(repo):
    """Revisión por texto (sin PyYAML) de recursos, probes y secretos en workloads de Kubernetes."""
    workloads = []
    for rel, ruta in recorrer(repo):
        if not rel.lower().endswith((".yaml", ".yml")) or rel.startswith(".github/"):
            continue
        texto = leer(ruta) or ""
        for doc in re.split(r"^---\s*$", texto, flags=re.M):
            m = RX_WORKLOAD.search(doc)
            if not m:
                continue
            if re.search(r"app\.kubernetes\.io/part-of:\s*argocd|quay\.io/argoproj/|ghcr\.io/dexidp/", doc):
                continue  # instalación de Argo CD (terceros): no es un componente del equipo
            nombre = re.search(r"^metadata:\s*\n(?:[ \t]+.*\n)*?[ \t]+name:\s*[\"']?([^\s\"']+)", doc, re.M)
            contenedores = len(re.findall(r"^\s*(?:-\s*)?image\s*:", doc, re.M))
            workloads.append({
                "archivo": rel, "kind": m.group(1), "nombre": nombre.group(1) if nombre else "?",
                "contenedores": contenedores,
                "bloques_requests": len(re.findall(r"^\s+requests:\s*$", doc, re.M)),
                "bloques_limits": len(re.findall(r"^\s+limits:\s*$", doc, re.M)),
                "readinessProbe": "readinessProbe:" in doc, "livenessProbe": "livenessProbe:" in doc,
                "usa_secrets": bool(re.search(r"secretKeyRef|secretRef:", doc)),
                "password_en_claro": bool(re.search(
                    r"name:\s*[\"']?[A-Z_]*(PASSWORD|SECRET|TOKEN|ACCESS_KEY)[A-Z_]*[\"']?\s*\n\s*value:\s*[\"']?[^\s\"'{]{3,}",
                    doc) or re.search(r"://[^:/\s]+:[^@/\s{]{3,}@", doc)),
                "imagen_latest_o_sin_tag": bool(re.search(
                    r"^\s*(?:-\s*)?image\s*:\s*[\"']?[^\s:\"'@{]+(:latest)?[\"']?\s*$", doc, re.M)),
            })
    return {
        "total": len(workloads),
        "sin_requests_o_limits": [f"{w['archivo']}:{w['nombre']}" for w in workloads
                                  if w["bloques_requests"] < w["contenedores"] or w["bloques_limits"] < w["contenedores"]],
        "sin_probes": [f"{w['archivo']}:{w['nombre']}" for w in workloads
                       if w["kind"] in ("Deployment", "StatefulSet") and not (w["readinessProbe"] or w["livenessProbe"])],
        "con_password_en_claro": [f"{w['archivo']}:{w['nombre']}" for w in workloads if w["password_en_claro"]],
        "con_imagen_latest_o_sin_tag": [f"{w['archivo']}:{w['nombre']}" for w in workloads if w["imagen_latest_o_sin_tag"]],
        "detalle": workloads[:80],
        "nota": "Análisis por texto: las plantillas Helm con {{ }} y los values.yaml pueden requerir revisión manual.",
    }


def arbol(repo, profundidad=3, max_por_dir=25):
    raiz = {}
    for rel, _ in recorrer(repo):
        partes = rel.split("/")
        nodo = raiz
        for parte in partes[:-1]:
            nodo = nodo.setdefault(parte, {})
        nodo.setdefault(partes[-1], None)
    lineas = []

    def rec(nodo, nivel, prefijo):
        items = sorted(nodo)
        for nombre in items[:max_por_dir]:
            hijo = nodo[nombre]
            es_dir = isinstance(hijo, dict)
            lineas.append(f"{prefijo}{nombre}{'/' if es_dir else ''}")
            if es_dir and nivel < profundidad:
                rec(hijo, nivel + 1, prefijo + "  ")
        if len(items) > max_por_dir:
            lineas.append(f"{prefijo}... (+{len(items) - max_por_dir} más)")
    rec(raiz, 1, "")
    return lineas


# ------------------------------------------------------------------ README
RX_IMG_MD = re.compile(r"!\[([^\]]*)\]\(\s*<?([^)\s>]+)>?(?:\s+[\"']([^\"']*)[\"'])?\s*\)")
RX_IMG_HTML = re.compile(r"<img\b[^>]*?\bsrc\s*=\s*[\"']([^\"']+)[\"'][^>]*>", re.I)
RX_ALT_HTML = re.compile(r"\balt\s*=\s*[\"']([^\"']*)[\"']", re.I)
RX_REF_DEF = re.compile(r"^\s*\[([^\]]+)\]:\s*(\S+)", re.M)
RX_IMG_REF = re.compile(r"!\[([^\]]*)\]\[([^\]]*)\]")


def buscar_readme(repo):
    for nombre in os.listdir(repo):
        if nombre.lower() in ("readme.md", "readme.markdown", "readme"):
            return os.path.join(repo, nombre)
    return None


def descargar(url, destino_dir, idx):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "evaluador-proyecto-mlops/1.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            tipo = r.headers.get("Content-Type", "")
            data = r.read(15_000_001)
        if len(data) > 15_000_000:
            return None, "mayor a 15 MB"
        ext = {"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp",
               "image/svg+xml": ".svg"}.get(tipo.split(";")[0].strip())
        if not ext:
            ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()
            if ext not in EXT_IMAGEN:
                return None, f"no es imagen ({tipo or 'tipo desconocido'})"
        ruta = os.path.join(destino_dir, f"remota_{idx:03d}{ext}")
        with open(ruta, "wb") as f:
            f.write(data)
        return ruta, None
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def contexto_encabezados(lineas):
    vigente, actual, en_codigo = [""] * len(lineas), "", False
    for i, l in enumerate(lineas):
        if l.strip().startswith("```"):
            en_codigo = not en_codigo
        elif not en_codigo:
            m = re.match(r"^#{1,6}\s+(.*)", l)
            if m:
                actual = m.group(1).strip()
        vigente[i] = actual
    return vigente


def extraer_imagenes(ruta_md, repo, imagenes_dir, offset, vigente=None):
    """Imágenes de un .md con su contexto; descarga las remotas y resuelve las locales."""
    texto = leer(ruta_md) or ""
    lineas = texto.splitlines()
    vigente = vigente or contexto_encabezados(lineas)
    refs = {k.lower(): v for k, v in RX_REF_DEF.findall(texto)}
    doc = os.path.relpath(ruta_md, repo)
    imagenes = []
    for i, l in enumerate(lineas):
        hallados = [(m.group(1), m.group(2), m.group(3) or "") for m in RX_IMG_MD.finditer(l)]
        hallados += [(RX_ALT_HTML.search(m.group(0)).group(1) if RX_ALT_HTML.search(m.group(0)) else "",
                      m.group(1), "") for m in RX_IMG_HTML.finditer(l)]
        hallados += [(m.group(1), refs.get((m.group(2) or m.group(1)).lower(), ""), "")
                     for m in RX_IMG_REF.finditer(l)]
        for alt, src, titulo in hallados:
            antes = [x for x in lineas[max(0, i - 4):i] if x.strip()][-3:]
            despues = [x for x in lineas[i + 1:i + 6] if x.strip()][:3]
            imagenes.append({"documento": doc, "linea": i + 1, "alt": alt, "src": src, "titulo": titulo,
                             "seccion": vigente[i], "texto_antes": antes, "texto_despues": despues})
    base_dir = os.path.dirname(ruta_md)
    for n, img in enumerate(imagenes, start=offset):
        src = img["src"]
        if src.startswith(("http://", "https://")):
            url = src
            m = re.match(r"https://github\.com/([^/]+/[^/]+)/blob/(.+)", src)
            if m:
                url = f"https://raw.githubusercontent.com/{m.group(1)}/{m.group(2)}"
            local, err = descargar(url, imagenes_dir, n)
            img.update(tipo="remota", disponible=bool(local), archivo_local=local, error=err)
        elif src.startswith("data:"):
            img.update(tipo="data-uri", disponible=True, archivo_local=None, error=None)
        else:
            p = urllib.parse.unquote(src.split("#")[0].split("?")[0])
            p = os.path.normpath(os.path.join(repo if p.startswith("/") else base_dir, p.lstrip("/")))
            existe = os.path.isfile(p)
            lfs = existe and es_puntero_lfs(leer(p))
            img.update(tipo="local", disponible=existe and not lfs, archivo_local=p if existe else None,
                       error=None if existe else "archivo no existe en el repo", puntero_lfs=lfs)
            if existe:
                img["mb"] = round(os.path.getsize(p) / 1e6, 2)
    return imagenes


def analizar_readme(repo, imagenes_dir):
    ruta = buscar_readme(repo)
    if not ruta:
        return {"existe": False}
    texto = leer(ruta) or ""
    lineas = texto.splitlines()
    # contexto: encabezado vigente por línea
    encabezados, vigente = [], [""] * len(lineas)
    actual, en_codigo, lineas_codigo = "", False, 0
    for i, l in enumerate(lineas):
        if l.strip().startswith("```"):
            en_codigo = not en_codigo
            lineas_codigo += 1
            vigente[i] = actual
            continue
        if en_codigo:
            lineas_codigo += 1
        else:
            m = re.match(r"^(#{1,6})\s+(.*)", l)
            if m:
                actual = m.group(2).strip()
                encabezados.append({"nivel": len(m.group(1)), "titulo": actual, "linea": i + 1})
        vigente[i] = actual

    os.makedirs(imagenes_dir, exist_ok=True)
    imagenes = extraer_imagenes(ruta, repo, imagenes_dir, 0, vigente)
    md_enlazados = sorted(set(re.findall(r"\]\((?!https?://)([^)\s#]+\.md)", texto)))
    for md in md_enlazados:
        p = os.path.normpath(os.path.join(os.path.dirname(ruta), urllib.parse.unquote(md)))
        if os.path.isfile(p) and p.startswith(repo):
            imagenes += extraer_imagenes(p, repo, imagenes_dir, len(imagenes))
    vistos = collections.Counter(i["src"] for i in imagenes)
    repetidas = [s for s, c in vistos.items() if c > 1]

    # Imágenes del repo no referenciadas en el README
    referenciadas = {os.path.normpath(i["archivo_local"]) for i in imagenes if i.get("archivo_local")}
    huerfanas = [rel for rel, r in recorrer(repo)
                 if os.path.splitext(rel)[1].lower() in EXT_IMAGEN and os.path.normpath(r) not in referenciadas]

    palabras_prosa = len(re.findall(r"\w+", re.sub(r"```[\s\S]*?```", " ", texto)))
    comandos = len(re.findall(r"^\s*(\$ |docker |kubectl |helm |git |pip |python |cd |sudo |curl )", texto, re.M))
    enlaces = re.findall(r"\]\((https?://[^)\s]+)\)|<(https?://[^>\s]+)>|(?<![\(<\"'])(https?://[^\s)\"'<>\]]+)", texto)
    enlaces = [a or b or c for a, b, c in enlaces]
    youtube = [u for u in enlaces if re.search(r"(youtube\.com|youtu\.be)", u)]
    fechas_mencionadas = re.findall(r"\b(20\d{2}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]20\d{2}|"
                                    r"\d{1,2} de [a-záéíóú]+( de 20\d{2})?)\b", texto, re.I)
    return {
        "existe": True, "ruta": os.path.relpath(ruta, repo), "lineas": len(lineas),
        "palabras_fuera_de_codigo": palabras_prosa, "lineas_en_bloques_de_codigo": lineas_codigo,
        "proporcion_codigo": round(lineas_codigo / max(1, len(lineas)), 2),
        "lineas_que_parecen_comandos": comandos,
        "encabezados": encabezados, "imagenes": imagenes,
        "total_imagenes": len(imagenes),
        "imagenes_no_disponibles": [i["src"] for i in imagenes if not i.get("disponible")],
        "imagenes_readme_principal": sum(1 for i in imagenes if i["documento"] == os.path.relpath(ruta, repo)),
        "imagenes_sin_texto_alternativo": sum(1 for i in imagenes
                                              if i["alt"].strip().lower() in ("", "image", "imagen", "img")),
        "imagenes_repetidas": repetidas,
        "imagenes_en_repo_no_usadas_en_readme": huerfanas[:40],
        "enlaces_youtube": youtube, "otros_md_enlazados": md_enlazados,
        "enlaces_externos": sorted(set(enlaces))[:60],
        "fechas_mencionadas": [f[0] for f in fechas_mencionadas][:30],
        "posible_instruccion_al_evaluador": bool(re.search(
            r"(ignora|ignore|olvida|disregard)[^\n]{0,60}(instruc|criteri|rubric|prompt)|"
            r"(asigna|asignar|pon|give|otorga)[^\n]{0,40}(nota|calificaci[oó]n|score|grade)[^\n]{0,20}(5|cinco|m[aá]xim)",
            texto, re.I)),
    }


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("repo")
    ap.add_argument("--fecha-limite", default=None, help="ISO 8601, ej. 2026-11-26T23:59:59-05:00")
    ap.add_argument("--salida", default="evidencia.json")
    ap.add_argument("--imagenes-dir", default="imagenes_remotas")
    a = ap.parse_args()
    repo = os.path.abspath(a.repo)
    if not os.path.exists(os.path.join(repo, ".git")):
        sys.exit(f"{repo} no es un repositorio git")
    limite = parse_fecha(a.fecha_limite) if a.fecha_limite else None

    resultado = {
        "repo": repo,
        "generado": dt.datetime.now(dt.timezone.utc).isoformat(),
        "remoto": sh(["git", "remote", "get-url", "origin"], repo).strip(),
        "rama": sh(["git", "rev-parse", "--abbrev-ref", "HEAD"], repo).strip(),
        "commit_evaluado": sh(["git", "rev-parse", "--short", "HEAD"], repo).strip(),
        "git": analizar_git(repo, limite),
        "estructura": arbol(repo),
        "archivos": analizar_archivos(repo),
        "kubernetes": analizar_workloads(repo),
        "readme": analizar_readme(repo, os.path.abspath(a.imagenes_dir)),
    }
    with open(a.salida, "w", encoding="utf-8") as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False)

    g, ar, rd = resultado["git"], resultado["archivos"], resultado["readme"]
    print(f"Repo: {resultado['remoto']}  rama={resultado['rama']}  commit={resultado['commit_evaluado']}")
    if "error" not in g:
        print(f"Commits (rama principal): {g['total_commits_rama_principal']}  | días con commits: "
              f"{g['dias_con_commits']}  | {g['primer_commit'][:10]} → {g['ultimo_commit'][:10]}")
        print(f"Después de la fecha límite: {len(g['commits_despues_de_limite'])}  | "
              f"concentración: {g['concentracion']}")
        el = g["estado_local"]
        print(f"Estado local: {el['total_cambios_sin_commit']} cambios sin commit  | commits sin push: "
              f"{el['commits_sin_push']}  | sigue a: {el['rama_remota_seguida']}")
        print(f"Autores: {g['autores_commits']}")
        print(f"Mensajes genéricos: {g['mensajes_genericos']['proporcion']:.0%}  | PRs fusionados: "
              f"{g['merges_de_pull_requests']}  | ramas remotas: {len(g['ramas_remotas'])}")
    print(f"Señales presentes: {', '.join(ar['senales'])}")
    print(f"Señales ausentes:  {', '.join(ar['senales_ausentes'])}")
    k8 = resultado["kubernetes"]
    print(f"Workloads K8s: {k8['total']}  | sin requests/limits: {len(k8['sin_requests_o_limits'])}  | "
          f"sin probes: {len(k8['sin_probes'])}  | credenciales en claro: {len(k8['con_password_en_claro'])}")
    if ar["posibles_secretos"] or ar["archivos_env_versionados"]:
        print(f"ALERTA secretos/.env: {ar['posibles_secretos']} {ar['archivos_env_versionados']}")
    if rd.get("existe"):
        print(f"README: {rd['lineas']} líneas, {rd['palabras_fuera_de_codigo']} palabras de prosa, "
              f"{rd['total_imagenes']} imágenes ({len(rd['imagenes_no_disponibles'])} no disponibles), "
              f"proporción código {rd['proporcion_codigo']:.0%}, YouTube: {len(rd['enlaces_youtube'])}")
        if rd["posible_instruccion_al_evaluador"]:
            print("ALERTA: el README parece contener instrucciones dirigidas al evaluador")
    else:
        print("README: NO EXISTE")
    print(f"JSON completo: {os.path.abspath(a.salida)}")


if __name__ == "__main__":
    main()
