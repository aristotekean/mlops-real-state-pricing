"""Candidate estimators, evaluation metrics, and the served pipeline."""

from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    mean_squared_log_error,
    median_absolute_error,
    r2_score,
)
from sklearn.model_selection import KFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    OneHotEncoder,
    OrdinalEncoder,
    StandardScaler,
    TargetEncoder,
)

from real_estate_model.features import (
    ENGINEERED_NUMERIC,
    HIGH_CARDINALITY_CATEGORICAL,
    LOW_CARDINALITY_CATEGORICAL,
    FeatureEngineer,
)

RANDOM_STATE = 42
CANDIDATES = ("ridge", "hist_gradient_boosting", "random_forest")
MISSING_CATEGORY = "__missing__"

# Root mean squared logarithmic error: prices span orders of magnitude, so the
# selection metric scores relative error instead of letting the few most
# expensive properties decide. It is an error: LOWER IS BETTER.
PRIMARY_METRIC = "validation_rmsle"
PRIMARY_METRIC_GREATER_IS_BETTER = False


# Base metric name -> whether a higher value is better. Every metric reported by
# ``evaluate_predictions`` is listed, so the pipeline can pick any of them as
# its selection or promotion metric.
METRIC_GREATER_IS_BETTER = {
    "rmsle": False,
    "rmse": False,
    "mae": False,
    "median_ae": False,
    "mape": False,
    "r2": True,
}

# Upper bound of a prediction in log1p(price) space (exp(21) ~ 1.3 billion USD).
# Clipping before expm1 keeps an absurd input from producing an infinite price.
MAX_LOG_PRICE = 21.0


def is_better(candidate: float, reference: float) -> bool:
    """Return whether ``candidate`` beats ``reference`` on the primary metric."""
    if PRIMARY_METRIC_GREATER_IS_BETTER:
        return candidate > reference
    return candidate < reference


def metric_greater_is_better(metric: str) -> bool:
    """Direction of a metric given as ``mae`` or with a split prefix (``validation_mae``)."""
    for name, greater in METRIC_GREATER_IS_BETTER.items():
        if metric == name or metric.endswith(f"_{name}"):
            return greater
    raise ValueError(f"Unknown metric: {metric}")


def relative_improvement(candidate: float, reference: float, metric: str) -> float:
    """Signed relative gain of ``candidate`` over ``reference``; positive is better.

    For an error metric 0.06 means the error is 6% lower; for R2 it is the
    relative increase. A zero reference gives 0 (no meaningful ratio).
    """
    if reference == 0:
        return 0.0
    gain = (candidate - reference) if metric_greater_is_better(metric) else (reference - candidate)
    return float(gain / abs(reference))


def price_from_log(values: Any) -> Any:
    """Inverse of log1p with the prediction clipped to a plausible price range."""
    return np.expm1(np.clip(values, 0.0, MAX_LOG_PRICE))


def _impute_category() -> SimpleImputer:
    return SimpleImputer(
        strategy="constant", fill_value=MISSING_CATEGORY, keep_empty_features=True
    )


def _preprocessor(
    *, impute_numeric: bool, scale: bool, one_hot: bool
) -> ColumnTransformer:
    """Numeric, low-cardinality and high-cardinality blocks, in that order."""
    numeric_steps: list[tuple[str, Any]] = []
    if impute_numeric:
        numeric_steps.append(
            ("imputer", SimpleImputer(strategy="median", keep_empty_features=True))
        )
    if scale:
        # Linear models only: sizes and lot areas are heavy-tailed, so they
        # enter in log scale (all numeric features are non-negative).
        numeric_steps.append(("log1p", FunctionTransformer(np.log1p, feature_names_out="one-to-one")))
        numeric_steps.append(("scaler", StandardScaler()))

    if one_hot:
        low_encoder: Any = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    else:
        # Unknown categories become -1, which gradient boosting treats as missing.
        low_encoder = OrdinalEncoder(
            handle_unknown="use_encoded_value", unknown_value=-1, dtype=np.float64
        )

    # Thousands of cities, ZIP codes and brokers: each category is replaced by
    # the (cross-fitted, smoothed) mean target of its training rows.
    high_steps: list[tuple[str, Any]] = [
        ("imputer", _impute_category()),
        (
            "target",
            TargetEncoder(
                target_type="continuous",
                cv=KFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE),
            ),
        ),
    ]
    if scale:
        high_steps.append(("scaler", StandardScaler()))

    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(numeric_steps) if numeric_steps else "passthrough",
                list(ENGINEERED_NUMERIC),
            ),
            (
                "low_cardinality",
                Pipeline([("imputer", _impute_category()), ("encoder", low_encoder)]),
                list(LOW_CARDINALITY_CATEGORICAL),
            ),
            (
                "high_cardinality",
                Pipeline(high_steps),
                list(HIGH_CARDINALITY_CATEGORICAL),
            ),
        ],
        remainder="drop",
        sparse_threshold=0.0,
        verbose_feature_names_out=False,
    )


def build_candidate(name: str) -> Pipeline:
    """Build an unfitted preprocessing-plus-regressor pipeline.

    Every candidate is trained on ``log1p(price)`` and predicts in price units:
    the target is right-skewed, and the preprocessing sits inside the target
    transformation so target encoding also sees the log price.
    """
    if name == "ridge":
        preprocessor = _preprocessor(impute_numeric=True, scale=True, one_hot=True)
        regressor: Any = Ridge(alpha=1.0, random_state=RANDOM_STATE)
    elif name == "hist_gradient_boosting":
        # Gradient boosting handles missing numeric values natively.
        preprocessor = _preprocessor(impute_numeric=False, scale=False, one_hot=False)
        categorical_mask = (
            [False] * len(ENGINEERED_NUMERIC)
            + [True] * len(LOW_CARDINALITY_CATEGORICAL)
            + [False] * len(HIGH_CARDINALITY_CATEGORICAL)
        )
        regressor = HistGradientBoostingRegressor(
            categorical_features=categorical_mask,
            learning_rate=0.08,
            max_iter=300,
            max_leaf_nodes=31,
            l2_regularization=1.0,
            early_stopping=True,
            validation_fraction=0.1,
            n_iter_no_change=20,
            random_state=RANDOM_STATE,
        )
    elif name == "random_forest":
        preprocessor = _preprocessor(impute_numeric=True, scale=False, one_hot=True)
        regressor = RandomForestRegressor(
            n_estimators=100,
            max_depth=16,
            min_samples_leaf=20,
            max_features="sqrt",
            max_samples=0.25,
            n_jobs=-1,
            random_state=RANDOM_STATE,
        )
    else:
        raise ValueError(f"Unknown candidate model: {name}")
    model = TransformedTargetRegressor(
        regressor=Pipeline([("preprocessor", preprocessor), ("regressor", regressor)]),
        func=np.log1p,
        inverse_func=price_from_log,
        check_inverse=False,
    )
    return Pipeline([("model", model)])


def evaluate(
    model: Any, features: pd.DataFrame, target: pd.Series, *, prefix: str
) -> dict[str, float]:
    """Regression metrics; ``{prefix}_rmsle`` is the selection metric."""
    return evaluate_predictions(target, model.predict(features), prefix=prefix)


def evaluate_predictions(
    target: pd.Series, predictions: Any, *, prefix: str
) -> dict[str, float]:
    """Metrics from predicted prices, usable for any model that returns them."""
    actual = np.asarray(target, dtype="float64")
    # A price cannot be negative, and the logarithmic error is undefined there.
    predicted = np.clip(np.asarray(predictions, dtype="float64"), 0.0, None)
    return {
        f"{prefix}_rmsle": float(np.sqrt(mean_squared_log_error(actual, predicted))),
        f"{prefix}_rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        f"{prefix}_mae": float(mean_absolute_error(actual, predicted)),
        f"{prefix}_median_ae": float(median_absolute_error(actual, predicted)),
        f"{prefix}_mape": float(mean_absolute_percentage_error(actual, predicted)),
        f"{prefix}_r2": float(r2_score(actual, predicted)),
    }


def build_serving_pipeline(fitted_candidate: Pipeline) -> Pipeline:
    """Prefix a fitted candidate with the stateless feature step.

    The result accepts the input contract, so the API and the model comparison
    feed it the same records without re-implementing features.
    """
    return Pipeline([("features", FeatureEngineer()), *fitted_candidate.steps])
