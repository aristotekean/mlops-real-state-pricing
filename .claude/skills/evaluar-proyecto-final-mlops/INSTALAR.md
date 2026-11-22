# Skill: evaluar-proyecto-final-mlops

Pre-evaluación del Proyecto Final de Operaciones de Machine Learning (Nivel 4). Lee **este repositorio en disco**, revisa el código, el historial y la evidencia del README, y da una nota orientativa de 0 a 5 con lo que deberías mejorar.

Esta copia está adaptada para evaluar el proyecto donde está instalada: no clona nada desde GitHub.

## Requisitos
- [Claude Code](https://claude.com/claude-code)
- `git` y `python3` (3.8 o superior)

## Instalación
La skill vive dentro del proyecto que evalúa:

```bash
mkdir -p .claude/skills
cp -r evaluar-proyecto-final-mlops .claude/skills/
```

## Uso
Dentro de Claude Code, abierto en la raíz del proyecto:

```
/evaluar-proyecto-final-mlops
```

o en lenguaje natural: *"evalúa mi proyecto final de MLOps"*.

El reporte se guarda como `evaluacion_<repo>_<fecha>.md` en la raíz del repo.

## Importante
- La skill **no ejecuta** tu proyecto: evalúa lo que está en el repositorio. Si algo funciona pero no lo muestras en el README, no cuenta.
- Evalúa lo que hay **en disco**, con los cambios sin commit incluidos. El profesor verá lo que esté en GitHub: el reporte avisa qué falta publicar.
- No se evalúan los archivos ignorados por git, la carpeta `.claude/` ni los reportes anteriores.
- Antes de la fecha límite la nota es **provisional**.
- La nota es orientativa. La calificación oficial la asigna el profesor.
