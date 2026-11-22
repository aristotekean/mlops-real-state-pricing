"""Pure decision logic: training decision and promotion rule.

No Airflow, MLflow or database here: the tasks gather the inputs and these
functions decide, so the rules can be unit-tested in isolation
(services/airflow/tests/test_rules.py). Thresholds come in through a
``Thresholds`` object so a test can vary them without touching the
environment.
"""

from dataclasses import dataclass
from typing import Any

from real_estate_model.models import relative_improvement


@dataclass(frozen=True)
class Thresholds:
    min_train_rows: int
    min_evaluation_rows: int
    min_batch_rows: int
    min_volume_growth: float
    drift_psi_threshold: float
    new_category_min_share: float
    degradation_threshold: float
    primary_metric: str
    guard_metric: str
    promotion_min_improvement: float
    promotion_max_guard_degradation: float

    @classmethod
    def from_settings(cls) -> "Thresholds":
        from real_estate_pipeline import settings

        return cls(
            min_train_rows=settings.MIN_TRAIN_ROWS,
            min_evaluation_rows=settings.MIN_EVALUATION_ROWS,
            min_batch_rows=settings.MIN_BATCH_ROWS,
            min_volume_growth=settings.MIN_VOLUME_GROWTH,
            drift_psi_threshold=settings.DRIFT_PSI_THRESHOLD,
            new_category_min_share=settings.NEW_CATEGORY_MIN_SHARE,
            degradation_threshold=settings.DEGRADATION_THRESHOLD,
            primary_metric=settings.PRIMARY_METRIC,
            guard_metric=settings.GUARD_METRIC,
            promotion_min_improvement=settings.PROMOTION_MIN_IMPROVEMENT,
            promotion_max_guard_degradation=settings.PROMOTION_MAX_GUARD_DEGRADATION,
        )


@dataclass(frozen=True)
class TrainingDecision:
    decision: str  # "train" | "skip"
    rule: str
    reason: str
    rules: dict[str, Any]


def decide_training(
    *,
    schema: dict[str, Any],
    quality: dict[str, Any],
    categories: dict[str, Any],
    drift: dict[str, Any],
    preprocessing: dict[str, Any],
    has_production: bool,
    production_trained_rows: int,
    degradation: dict[str, Any],
    thresholds: Thresholds,
) -> TrainingDecision:
    """Apply the training rules in order (see decision.py for the list).

    ``degradation`` is the production model scored on the new batch: a dict
    with ``available`` and, when available, ``degradation`` (relative
    worsening), ``metric``, ``batch_value``, ``reference_validation`` and
    ``production_version``.
    """
    t = thresholds
    eligible = preprocessing["eligible_rows"]
    split_rows = preprocessing["accumulated_split_rows"]
    accumulated = preprocessing["accumulated_eligible_rows"]
    rules: dict[str, Any] = {}

    growth = (
        (accumulated - production_trained_rows) / production_trained_rows
        if production_trained_rows else None
    )
    rules["volume"] = {
        "batch_eligible_rows": eligible,
        "accumulated_eligible_rows": accumulated,
        "production_trained_on_rows": production_trained_rows or None,
        "growth": None if growth is None else round(growth, 6),
        "min_growth": t.min_volume_growth,
        "triggered": growth is not None and growth >= t.min_volume_growth,
    }
    psi = {feature: info["psi"] for feature, info in drift.get("features", {}).items()}
    drifted = sorted(
        (feature for feature, value in psi.items() if value >= t.drift_psi_threshold),
        key=lambda feature: -psi[feature],
    )
    rules["drift"] = {
        "drifted_features": drifted,
        "psi": psi,
        "threshold": t.drift_psi_threshold,
        "triggered": bool(drifted),
    }
    significant = {
        column: info["share_of_batch"]
        for column, info in categories.get("columns", {}).items()
        if not categories.get("baseline") and info.get("can_trigger_training")
        and info["new_categories"] > 0 and info["share_of_batch"] >= t.new_category_min_share
    }
    rules["new_categories"] = {
        "significant": significant,
        "min_share": t.new_category_min_share,
        "triggered": bool(significant),
    }
    degradation = {
        **degradation,
        "threshold": t.degradation_threshold,
        "triggered": bool(
            degradation.get("available") and degradation["degradation"] >= t.degradation_threshold
        ),
    }
    rules["degradation"] = degradation

    if schema["blocking"] or quality["blocking"]:
        details = "; ".join(
            item for item in (schema.get("blocking_reason"), quality.get("blocking_reason")) if item
        )
        decision, rule = "skip", "invalid_batch"
        reason = (
            f"No entrena: lote inválido ({details}). Queda en RAW para auditoría y no se usa "
            "para entrenar."
        )
    elif (split_rows["train"] < t.min_train_rows
          or min(split_rows["validation"], split_rows["test"]) < t.min_evaluation_rows):
        decision, rule = "skip", "insufficient_data"
        reason = (
            f"No entrena: datos insuficientes (train={split_rows['train']}, "
            f"validation={split_rows['validation']}, test={split_rows['test']}; mínimos "
            f"{t.min_train_rows}/{t.min_evaluation_rows})."
        )
    elif not has_production:
        decision, rule = "train", "baseline"
        reason = (
            f"Entrena: línea base inicial, no existe modelo productivo "
            f"({accumulated:,} filas elegibles acumuladas)."
        )
    elif eligible < t.min_batch_rows:
        decision, rule = "skip", "small_batch"
        reason = (
            f"No entrena: lote pequeño ({eligible:,} filas elegibles < {t.min_batch_rows:,}). "
            "Sus filas quedan en CLEAN y se usarán en el próximo entrenamiento."
        )
    else:
        triggers = []
        if rules["volume"]["triggered"]:
            triggers.append(
                f"crecimiento de datos {growth:.0%} desde el modelo productivo "
                f"(≥ {t.min_volume_growth:.0%})"
            )
        if drifted:
            triggers.append(
                "cambio de distribución en "
                + ", ".join(f"{feature} (PSI {psi[feature]:.2f})" for feature in drifted)
            )
        if significant:
            triggers.append(
                "nuevas categorías en "
                + ", ".join(f"{column} ({share:.1%} del lote)" for column, share in significant.items())
            )
        if degradation["triggered"]:
            triggers.append(
                f"degradación del modelo productivo v{degradation['production_version']}: "
                f"{degradation['metric'].upper()} {degradation['batch_value']:.4f} en el lote vs "
                f"{degradation['reference_validation']:.4f} en validación "
                f"(+{degradation['degradation']:.0%})"
            )
        if triggers:
            decision, rule = "train", "triggers"
            reason = "Entrena: " + "; ".join(triggers) + "."
        else:
            decision, rule = "skip", "no_relevant_change"
            reason = (
                "No entrena: el lote no aporta variación relevante (crecimiento "
                f"{'n/a' if growth is None else f'{growth:.0%}'} < {t.min_volume_growth:.0%}, "
                f"sin drift ≥ {t.drift_psi_threshold}, sin categorías nuevas relevantes, "
                f"sin degradación ≥ {t.degradation_threshold:.0%})."
            )

    rules["decision"] = {"decision": decision, "rule": rule}
    return TrainingDecision(decision, rule, reason, rules)


METRIC_NAMES = ("mae", "rmse", "rmsle", "median_ae", "mape", "r2")


@dataclass(frozen=True)
class PromotionDecision:
    promote: bool
    reason: str
    deltas: dict[str, float]


def decide_promotion(
    *,
    candidate: dict[str, float],
    production: dict[str, float] | None,
    candidate_version: str,
    production_version: str | None,
    thresholds: Thresholds,
    prefix: str = "validation",
) -> PromotionDecision:
    """Promotion rule on metrics computed over the SAME rows.

    Promote when there is no production model, or when
        gain(primary) >= promotion_min_improvement and
        gain(guard)   >= -promotion_max_guard_degradation,
    with gain the signed relative improvement (positive = better).
    """
    t = thresholds
    if production is None or production_version is None:
        return PromotionDecision(
            True, "Se promueve: no existía modelo productivo previo; el candidato es la línea base.", {}
        )
    if production_version == candidate_version:
        return PromotionDecision(
            True, f"La versión {candidate_version} ya es la productiva (ejecución reanudada).", {}
        )
    key = lambda metric: f"{prefix}_{metric}"  # noqa: E731
    deltas = {
        metric: round(relative_improvement(candidate[key(metric)], production[key(metric)], metric), 6)
        for metric in METRIC_NAMES
        if key(metric) in candidate and key(metric) in production
    }
    primary_gain, guard_gain = deltas[t.primary_metric], deltas[t.guard_metric]
    primary_ok = primary_gain >= t.promotion_min_improvement
    guard_ok = guard_gain >= -t.promotion_max_guard_degradation
    promote = primary_ok and guard_ok
    p, g = key(t.primary_metric), key(t.guard_metric)
    detail = (
        f"{t.primary_metric.upper()} {candidate[p]:,.2f} vs {production[p]:,.2f} "
        f"(v{production_version}): mejora {primary_gain:+.2%} "
        f"({'≥' if primary_ok else '<'} {t.promotion_min_improvement:.0%} requerido); "
        f"{t.guard_metric.upper()} {candidate[g]:,.2f} vs {production[g]:,.2f}: "
        f"{guard_gain:+.2%} ({'dentro' if guard_ok else 'fuera'} de la tolerancia "
        f"-{t.promotion_max_guard_degradation:.0%})"
    )
    return PromotionDecision(promote, ("Se promueve: " if promote else "No se promueve: ") + detail, deltas)
