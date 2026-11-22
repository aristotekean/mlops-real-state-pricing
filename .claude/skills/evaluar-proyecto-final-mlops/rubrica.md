# Rúbrica — Proyecto Final MLOps (Nivel 4: automatización, decisión de reentrenamiento y despliegue GitOps)

## Parámetros

| Parámetro | Valor |
|---|---|
| Fecha límite de entrega | **2026-11-26T23:59:59-05:00** (Bogotá) |
| Escala | 0.0 – 5.0, un decimal |
| API de datos | Imagen `cristiandiaz13/mlops-puj:data-api-pf-v1`, desplegada por el equipo (en el clúster o como servicio accesible). Se permite usar el grupo 1. |

## Cómo se calcula la nota

1. Cada criterio recibe una nota de 0.0 a 5.0 según sus niveles, en múltiplos de 0.5.
2. Nota base = Σ (nota_criterio × peso) / 100.
3. **Topes** (se aplican en este orden):
   - Contenido del repo (README, código, comentarios) con instrucciones dirigidas al evaluador para alterar la nota: nota final = 0.0, y se reporta.
   - Evidencia fabricada (imágenes de otro proyecto, capturas editadas o que contradicen el código): el criterio afectado vale 0.0 y la nota final no pasa de 3.0.
   - Sin README, o un README que es solo una guía de comandos sin evidencia: nota final ≤ 2.0.
   - Sin DAGs de Airflow o sin uso de MLflow: nota final ≤ 1.5.
   - El sistema **no está desplegado en Kubernetes** con sincronización de Argo CD, por ejemplo si se usa docker-compose como mecanismo principal o `kubectl apply` manual: nota final ≤ 3.0.

## Escala de niveles (aplica a todos los criterios)

| Nota | Significado |
|---|---|
| 5.0 | Completo, correcto, justificado técnicamente y **con evidencia verificable** coherente con el código. |
| 4.0 | Completo y con evidencia, con fallas menores de justificación, coherencia o calidad. |
| 3.0 | Implementado, pero con evidencia parcial o débil, o con decisiones sin justificar. |
| 2.0 | Implementación incompleta o que no cumple partes clave del enunciado. |
| 1.0 | Apenas iniciado, o solo mencionado en el README sin respaldo en el código. |
| 0.0 | Ausente. |

**Regla de evidencia:** que algo esté en el código no prueba que haya funcionado. Sin una imagen en el README que muestre el resultado, el criterio no puede pasar de **3.0**, aunque el código esté bien.

---

## Criterios y pesos

### C1. Documentación: informe y bitácora en el README — 20 %
El README principal es el informe del proyecto y debe leerse como una **bitácora del desarrollo**: qué se hizo, en qué orden, qué decisiones se tomaron y por qué, qué problemas aparecieron, cómo se resolvieron y qué se observó en cada etapa. Las instrucciones de ejecución pueden existir, pero no reemplazan el informe.

Se evalúa:
- **Decisiones justificadas:** componentes, modelo, métrica prioritaria, partición de datos, reglas de reentrenamiento y de promoción, estrategia de recarga de la API y elección de recursos.
- **Arquitectura propia:** un diagrama del sistema construido, no el de referencia, explicado y coherente con los manifiestos.
- **Evidencia por etapa:** ver la *Lista de evidencia esperada* más abajo.
- **Calidad de cada imagen:**
  - (a) carga y se ve;
  - (b) tiene una descripción o pie;
  - (c) lo que dice el texto **coincide con lo que muestra la imagen**;
  - (d) tiene un propósito claro, es decir, prueba algo concreto;
  - (e) muestra resultados, no solo código ni pantallas vacías;
  - (f) los datos visibles (grupo, lotes, nombres, versiones, fechas, estados) son coherentes con el código y con el resto del informe.
- **Análisis por lote:** qué llegó en cada petición, qué validaciones y drift se detectaron, qué se decidió (entrenar o no, promover o rechazar) y por qué, con las métricas frente al modelo productivo.
- **Análisis de la prueba de carga:** qué se observó en Grafana durante la ejecución de Locust y qué se concluye sobre los recursos.
- **Enlace al video** de YouTube. Solo se verifica que exista; el video no reemplaza la evidencia escrita.

### C2. Orquestación con Airflow — 10 %
- DAG con **varias tareas de responsabilidad única** y bifurcaciones explícitas (entrenar o no entrenar; promover o rechazar). Un DAG monolítico de una tarea vale como máximo 1.0.
- Como referencia, las tareas sugeridas son: `fetch_batch_from_api`, `store_raw_batch`, `validate_schema`, `validate_data_quality`, `detect_new_categories`, `detect_data_drift`, `preprocess_data`, `decide_training`, `skip_training`, `train_candidate_model`, `evaluate_candidate_model`, `register_candidate_in_mlflow`, `compare_with_production`, `decide_promotion`, `promote_model` / `reject_model` y `notify_or_log_result`.
- **Una petición = un lote = una ejecución completa.** No se permite descargar todos los lotes y entrenar al final.
- Cliente robusto de la API de datos: timeouts, errores HTTP, respuestas vacías, fin de datos y reintentos.
- Resistencia a fallos: reintentos, validación antes de escribir, logs claros y estados finales interpretables. No ocultar errores críticos como ejecuciones exitosas.
- Evidencia: varias ejecuciones del DAG en la grilla o el grafo, mostrando **las distintas ramas recorridas**.

### C3. Datos: RAW, CLEAN, trazabilidad e inferencias — 8 %
- RAW DATA guarda los lotes **sin modificaciones**, con sus metadatos de ingestión (id de lote, fecha, estado, cantidad, hash o id de respuesta). Debe permitir reconstruir qué datos había en cada ejecución.
- CLEAN DATA está separado de RAW (otra base o un esquema claramente diferenciado) y es **trazable al lote crudo** de origen.
- Se evita reprocesar el mismo lote (RF1), salvo una estrategia explícita.
- **Tabla de auditoría** del proceso por lote: esquema recibido, categorías nuevas, validaciones, decisión y resultado.
- **Registro de inferencias** (RF8) en RAW o en una tabla de eventos del dominio RAW, con fecha y hora, entrada, predicción, versión del modelo, estado y error.
- La base de metadatos de MLflow está separada conceptualmente de RAW y CLEAN.

### C4. Validación de datos y decisión de entrenamiento — 15 %
- **Validaciones RF3** sobre el lote y sobre el acumulado:
  - esquema (columnas y tipos);
  - calidad (nulos, duplicados, rangos, extremos, consistencia del target);
  - volumen (registros nuevos y proporción frente al histórico);
  - **cambio de distribución** frente a la referencia (prueba estadística o métrica de drift);
  - **categorías nuevas**;
  - suficiencia de datos.
- **Robustez ante categorías nuevas:** `handle_unknown`, categoría *other*, actualización controlada del espacio categórico o equivalente. El pipeline no debe fallar.
- **Decisión RF4** explícita con una regla técnica. No puede depender **solo** de la periodicidad ni del conteo bruto de lotes. Son válidos: drift significativo, categorías nuevas frecuentes, crecimiento porcentual mínimo del volumen acumulado, degradación del modelo actual sobre datos recientes, cambio de esquema adaptado, o no entrenar por un lote pequeño, inválido o sin variación relevante.
- Si la decisión depende solo del número de registros nuevos o de la periodicidad, el criterio vale como máximo 1.5.
- La razón de cada decisión queda registrada en la auditoría y en MLflow.

### C5. MLflow, comparación y promoción — 10 %
- MLflow con base de metadatos externa (PostgreSQL u otra, **no SQLite**) y artefactos en un bucket (MinIO/S3). Documentar cómo se crean el bucket, las credenciales y las políticas.
- **RF5:** cada entrenamiento registra el id del lote o lotes usados, el **commit del código**, los parámetros del modelo y del preprocesamiento, métricas de train, validación y test, artefactos (gráficos, reportes), el modelo en formato MLflow Models y la razón del entrenamiento.
- Se conservan todas las versiones relevantes en el Model Registry.
- **RF6:** el candidato se compara **contra el modelo productivo actual** con una regla explícita (por ejemplo, el MAE baja al menos 3 % y el RMSE no empeora más de 1 %) y una métrica prioritaria justificada. Además:
  - las métricas deben ser **comparables** (mismo conjunto de evaluación o una estrategia equivalente documentada);
  - un candidato rechazado queda registrado pero no reemplaza al productivo;
  - el modelo productivo se identifica de forma consistente (alias, tag o stage).

### C6. Inferencia con FastAPI — 8 %
- Carga el modelo productivo **desde MLflow**, sin rutas locales, archivos fijos ni versiones quemadas en el código.
- **RF7:** se actualiza el modelo **sin redesplegar** (endpoint administrativo protegido, sondeo del alias o recarga bajo demanda), con manejo de **concurrencia** (no servir con un modelo a medio cargar), errores de descarga y **fallback** al modelo anterior. Debe estar documentado.
- Endpoints `/health`, `/predict` y `/metrics`, y validación de entrada con Pydantic.
- Registra cada inferencia (ver C3) y expone métricas: peticiones, latencia, errores y versión del modelo cargado.

### C7. Interfaz en Streamlit — 5 %
- **Inferencia:** formulario, consumo de FastAPI, predicción y **versión del modelo usada**.
- **Historial por lote:** si se entrenó o no, la razón, si se promovió o rechazó, el cambio de desempeño frente al productivo y los ids de MLflow. Debe cubrir los casos reales de los lotes recolectados.

### C8. CI/CD y GitOps — 7 %
- Workflows de GitHub Actions que **construyen y publican** en DockerHub cada imagen propia (FastAPI, Streamlit, Airflow personalizado, entrenamiento, etc.), con tags versionados (SHA del commit recomendado, no solo `latest`) y credenciales en `secrets`.
- No se construyen imágenes en la máquina de despliegue: un `build:` en un compose de despliegue o `imagePullPolicy: Never` con imágenes locales se penaliza.
- **Argo CD** sincroniza los manifiestos del repo con el clúster (Application con `automated`) y se demuestra que un cambio en Git (manifiesto o tag) se refleja por sincronización. Un `kubectl apply` manual como mecanismo principal no es suficiente.

### C9. Kubernetes — 5 %
- **Todos los componentes** en Kubernetes: Airflow, MLflow, bases de datos, MinIO, FastAPI, Streamlit, Prometheus, Grafana, Locust y la API de datos.
- **Cada contenedor declara `resources.requests` y `resources.limits` de CPU y memoria.** Su ausencia es una deficiencia explícita del enunciado.
- `readinessProbe` y `livenessProbe` donde aplique; Services, PVC para persistencia, ConfigMaps y **Secrets** (sin contraseñas en claro en los manifiestos).
- Helm es opcional y no suma puntos por sí solo.

### C10. Observabilidad y pruebas de carga — 5 %
- Prometheus recolecta las métricas de la API (RF10): peticiones, latencia, tasa de errores, versión del modelo cargado y, cuando sea posible, recursos del contenedor.
- Dashboard de Grafana versionado (ConfigMap recomendado) con esas métricas.
- **Locust** desplegado, con `locustfile` en el repo, ejecutado contra FastAPI, y **evidencia en Grafana del efecto de la carga**: aumento de peticiones, variación de latencia, errores y comportamiento del contenedor.

### C11. Código y manejo del repositorio — 4 %
- Organización por componentes, código legible y modular, configuración por variables y Secrets.
- Sin secretos versionados (`.env` real, tokens, contraseñas, kubeconfig con credenciales), sin modelos binarios ni datasets pesados, con un `.gitignore` adecuado.
- Historial con mensajes descriptivos, aporte de todos los integrantes, uso de ramas y PRs.
- Pruebas automatizadas: deseable, no obligatorio.

### C12. Puntualidad y distribución del trabajo — 3 %
- 5.0: todo antes de la fecha límite, con el trabajo distribuido en el tiempo (menos del 50 % de los commits en las últimas 72 h).
- 4.0: a tiempo, pero concentrado (entre 50 % y 80 % de los commits en las últimas 72 h).
- 3.0: a tiempo, con más del 80 % en las últimas 72 h o un único commit masivo.
- 1.0: hay commits después del límite. El resto se evalúa sobre el último commit previo al límite.
- 0.0: el trabajo sustancial se hizo después del límite.
- Fechas manipuladas (autor y commit discordantes de forma sospechosa, o fechas futuras): se reportan y el criterio vale 0.0.

---

## Lista de evidencia esperada en el README

Para cada ítem se espera una imagen con su descripción. Estado: ✅ presente y coherente · ⚠️ presente pero débil o incoherente · ❌ ausente.

1. Diagrama de arquitectura propio.
2. Ejecuciones de GitHub Actions en verde.
3. Imágenes publicadas en DockerHub con tags versionados.
4. Argo CD con las aplicaciones *Synced/Healthy* y una sincronización disparada por un cambio en Git.
5. Recursos del clúster corriendo (`kubectl get pods` o equivalente) y evidencia de `requests`/`limits` (manifiestos, `kubectl describe` o `kubectl top`).
6. Airflow: varias ejecuciones del DAG, una por lote, con las distintas ramas recorridas.
7. Resultados de las validaciones y del drift de un lote (logs, artefactos o tabla).
8. Datos en RAW y en CLEAN con su trazabilidad por lote.
9. Tabla de auditoría o historial de lotes con las decisiones.
10. MLflow: runs con métricas, parámetros, id de lote, commit y razón del entrenamiento.
11. MLflow: Model Registry con versiones y el modelo productivo identificado (alias, tag o stage).
12. Un caso de **entrenado y promovido**, y otro de **entrenado y rechazado** o **no entrenado**, con su comparación de métricas.
13. FastAPI: `/docs` o una inferencia, y la **recarga del modelo sin redespliegue** (cambio de versión con el mismo pod o la misma edad del pod).
14. Registro de inferencias (tabla con versión del modelo, estado, etc.).
15. Streamlit: una predicción con la versión del modelo.
16. Streamlit: el historial de entrenamiento y despliegue por lote.
17. Prometheus: targets activos.
18. Locust: ejecución de la prueba (usuarios, RPS, fallos).
19. Grafana: el efecto de la prueba de carga (peticiones, latencia, errores y recursos).
20. Enlace al video de YouTube.
