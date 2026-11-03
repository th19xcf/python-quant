#!/usr/bin/env python3

"""L2 因子权重学习器。

所有学习器只依赖训练窗数据，``fit`` 之后才能 ``predict``；
测试窗只传入特征列，绝不读取标签，保证样本外评估不泄露未来信息。

已实现：
- 等权（基线）
- IC 加权（可选半衰期指数衰减）
- ICIR 加权
- 滚动 Ridge（NumPy 闭式解，无需 scikit-learn）
- LightGBM 横截面回归排序（可选依赖组 ``learning``，缺失时显式报错）
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Dict, List, Optional

import numpy as np
import polars as pl
from loguru import logger

from src.quant.learning.labels import LABEL_COL


class OptionalDependencyError(ImportError):
    """可选依赖缺失时抛出，调用方据此标记能力不可用而不是崩溃。"""


class FittedWeightModel:
    """已拟合的权重模型：保存系数与可选的预测函数。"""

    def __init__(self, scheme: str, coefficients: Dict[str, float],
                 predict_fn: Any | None = None,
                 feature_cols: Sequence[str] | None = None) -> None:
        self.scheme = scheme
        self.coefficients = coefficients
        self.feature_cols = list(feature_cols) if feature_cols else []
        self._predict_fn = predict_fn

    def predict(self, frame: pl.DataFrame) -> np.ndarray:
        """对测试样本打分；返回与行序一致的浮点数组。"""
        if self._predict_fn is not None:
            return np.asarray(self._predict_fn(frame), dtype=float)
        if not self.feature_cols:
            return np.zeros(frame.height, dtype=float)
        matrix = frame.select(self.feature_cols).to_numpy().astype(float)
        matrix = np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
        weights = np.array([self.coefficients.get(col, 0.0) for col in self.feature_cols])
        return matrix @ weights


def _date_ic(frame: pl.DataFrame, feature: str,
             label: str = LABEL_COL) -> pl.DataFrame:
    """逐期截面 Rank IC（Spearman）。"""
    return (
        frame.group_by('date')
        .agg(pl.corr(feature, label, method='spearman').alias('ic'))
        .filter(pl.col('ic').is_finite())
        .sort('date')
    )


def _decay(ic_frame: pl.DataFrame, half_life: int) -> np.ndarray:
    """按「距训练末端越远权重越低」生成半衰期衰减系数。"""
    n = ic_frame.height
    if n == 0:
        return np.empty(0, dtype=float)
    ages = np.arange(n - 1, -1, -1, dtype=float)
    if half_life <= 0:
        return np.ones(n, dtype=float)
    return np.power(0.5, ages / float(half_life))


def _weighted_mean_std(values: np.ndarray, weights: np.ndarray) -> tuple[float, float]:
    """加权均值与加权标准差（总体口径）。"""
    if len(values) == 0:
        return 0.0, 0.0
    total = float(weights.sum())
    if total <= 0:
        return 0.0, 0.0
    mean = float(np.sum(values * weights) / total)
    variance = float(np.sum((values - mean) ** 2 * weights) / total)
    return mean, float(np.sqrt(max(variance, 0.0)))


def _normalize(raw: np.ndarray) -> np.ndarray:
    """把原始权重归一化为绝对值和为 1；全零时退化为等权。"""
    denominator = float(np.abs(raw).sum())
    if denominator <= 0:
        return np.ones(len(raw), dtype=float) / max(len(raw), 1)
    return raw / denominator


class BaseWeightLearner:
    """权重学习器基类。"""

    scheme = 'base'

    def fit(self, train: pl.DataFrame, feature_cols: Sequence[str],
            label_col: str = LABEL_COL) -> FittedWeightModel:
        raise NotImplementedError


class EqualWeightLearner(BaseWeightLearner):
    """等权基线：所有因子同权。"""

    scheme = 'equal'

    def fit(self, train: pl.DataFrame, feature_cols: Sequence[str],
            label_col: str = LABEL_COL) -> FittedWeightModel:
        count = max(len(feature_cols), 1)
        return FittedWeightModel(
            self.scheme,
            dict.fromkeys(feature_cols, 1.0 / count),
            feature_cols=feature_cols,
        )


class ICWeightLearner(BaseWeightLearner):
    """IC 加权：权重 ∝ 训练窗平均 IC（可按半衰期衰减旧样本）。"""

    scheme = 'ic'

    def __init__(self, half_life: int = 126) -> None:
        self.half_life = half_life

    def fit(self, train: pl.DataFrame, feature_cols: Sequence[str],
            label_col: str = LABEL_COL) -> FittedWeightModel:
        means: List[float] = []
        for col in feature_cols:
            ic_frame = _date_ic(train, col, label_col)
            if ic_frame.is_empty():
                means.append(0.0)
                continue
            values = ic_frame['ic'].to_numpy().astype(float)
            weights = _decay(ic_frame, self.half_life)
            mean, _ = _weighted_mean_std(values, weights)
            means.append(mean)
        normalized = _normalize(np.array(means, dtype=float))
        return FittedWeightModel(
            self.scheme,
            {col: float(weight) for col, weight in zip(feature_cols, normalized, strict=True)},
            feature_cols=feature_cols,
        )


class ICIRWeightLearner(ICWeightLearner):
    """ICIR 加权：权重 ∝ 训练窗平均 IC / IC 标准差。"""

    scheme = 'icir'

    def fit(self, train: pl.DataFrame, feature_cols: Sequence[str],
            label_col: str = LABEL_COL) -> FittedWeightModel:
        ratios: List[float] = []
        for col in feature_cols:
            ic_frame = _date_ic(train, col, label_col)
            if ic_frame.height < 3:
                ratios.append(0.0)
                continue
            values = ic_frame['ic'].to_numpy().astype(float)
            weights = _decay(ic_frame, self.half_life)
            mean, std = _weighted_mean_std(values, weights)
            ratios.append(mean / std if std > 0 else 0.0)
        normalized = _normalize(np.array(ratios, dtype=float))
        return FittedWeightModel(
            self.scheme,
            {col: float(weight) for col, weight in zip(feature_cols, normalized, strict=True)},
            feature_cols=feature_cols,
        )


def _stack_training_matrix(train: pl.DataFrame, feature_cols: Sequence[str],
                           label_col: str) -> tuple[np.ndarray, np.ndarray]:
    """把训练窗按交易日展开为 (X, y)，并对每个截面做标签去均值。"""
    matrices: List[np.ndarray] = []
    targets: List[np.ndarray] = []
    for group in train.partition_by('date', maintain_order=True):
        matrix = group.select(list(feature_cols)).to_numpy().astype(float)
        label = group[label_col].to_numpy().astype(float)
        if matrix.shape[0] < 2:
            continue
        matrix = np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
        label = np.nan_to_num(label, nan=0.0, posinf=0.0, neginf=0.0)
        label = label - float(label.mean())
        matrices.append(matrix)
        targets.append(label)
    if not matrices:
        return np.empty((0, len(feature_cols))), np.empty(0)
    return np.vstack(matrices), np.concatenate(targets)


class RidgeWeightLearner(BaseWeightLearner):
    """滚动 Ridge：横截面标准化特征 → 去均值前瞻收益。"""

    scheme = 'ridge'

    def __init__(self, ridge_lambda: float = 10.0) -> None:
        self.ridge_lambda = ridge_lambda

    def fit(self, train: pl.DataFrame, feature_cols: Sequence[str],
            label_col: str = LABEL_COL) -> FittedWeightModel:
        matrix, target = _stack_training_matrix(train, feature_cols, label_col)
        n_features = len(feature_cols)
        if matrix.shape[0] == 0:
            coefficients = np.zeros(n_features, dtype=float)
        else:
            gram = matrix.T @ matrix
            rhs = matrix.T @ target
            ridge = self.ridge_lambda * np.eye(n_features, dtype=float)
            try:
                coefficients = np.linalg.solve(gram + ridge, rhs)
            except np.linalg.LinAlgError:
                coefficients = np.linalg.lstsq(gram + ridge, rhs, rcond=None)[0]
        return FittedWeightModel(
            self.scheme,
            {col: float(value) for col, value in zip(feature_cols, coefficients, strict=True)},
            feature_cols=feature_cols,
        )


class LightGBMWeightLearner(BaseWeightLearner):
    """LightGBM 横截面回归排序（可选）。

    只作为对照：walk-forward 切分、purge/embargo 与其它学习器完全一致。
    缺少 ``lightgbm`` 时抛出 OptionalDependencyError，管道会跳过该方案。
    """

    scheme = 'lightgbm'

    def __init__(self, n_estimators: int = 200, learning_rate: float = 0.05,
                 num_leaves: int = 31, min_child_samples: int = 30,
                 random_state: int = 42) -> None:
        self.params: Dict[str, Any] = {
            'n_estimators': n_estimators,
            'learning_rate': learning_rate,
            'num_leaves': num_leaves,
            'min_child_samples': min_child_samples,
            'random_state': random_state,
        }

    def fit(self, train: pl.DataFrame, feature_cols: Sequence[str],
            label_col: str = LABEL_COL) -> FittedWeightModel:
        try:
            import lightgbm as lgb
        except ImportError as exc:  # pragma: no cover - 可选依赖
            raise OptionalDependencyError(
                '未安装 lightgbm；请使用 `poetry install --with learning` 后重试'
            ) from exc

        matrix, target = _stack_training_matrix(train, feature_cols, label_col)
        if matrix.shape[0] < 2:
            return FittedWeightModel(self.scheme, {}, feature_cols=feature_cols)
        model = lgb.LGBMRegressor(**self.params, verbose=-1)
        model.fit(matrix, target)

        def predict(frame: pl.DataFrame) -> np.ndarray:
            values = frame.select(list(feature_cols)).to_numpy().astype(float)
            values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
            return np.asarray(model.predict(values), dtype=float)

        return FittedWeightModel(
            self.scheme,
            {col: float(value) for col, value in zip(feature_cols, model.feature_importances_, strict=True)},
            predict_fn=predict,
            feature_cols=feature_cols,
        )


_LEARNERS = {
    EqualWeightLearner.scheme: EqualWeightLearner,
    ICWeightLearner.scheme: ICWeightLearner,
    ICIRWeightLearner.scheme: ICIRWeightLearner,
    RidgeWeightLearner.scheme: RidgeWeightLearner,
    LightGBMWeightLearner.scheme: LightGBMWeightLearner,
}


def available_schemes() -> list[str]:
    """返回已实现的方案名。"""
    return sorted(_LEARNERS)


def build_learner(name: str, config: Any) -> BaseWeightLearner:
    """按方案名构造学习器；可选依赖缺失由具体学习器在 fit 时抛出。"""
    if name not in _LEARNERS:
        raise KeyError(f'未知权重方案 {name}，可选: {available_schemes()}')
    if name == ICWeightLearner.scheme:
        return ICWeightLearner(half_life=int(getattr(config, 'half_life', 126)))
    if name == ICIRWeightLearner.scheme:
        return ICIRWeightLearner(half_life=int(getattr(config, 'half_life', 126)))
    if name == RidgeWeightLearner.scheme:
        return RidgeWeightLearner(ridge_lambda=float(getattr(config, 'ridge_lambda', 10.0)))
    if name == LightGBMWeightLearner.scheme:
        return LightGBMWeightLearner()
    return EqualWeightLearner()


def weight_history_entry(scheme: str, window_index: int,
                         model: FittedWeightModel) -> Dict[str, object]:
    """把一次拟合的系数整理成可序列化的历史记录。"""
    coefficients = {key: round(value, 6) for key, value in model.coefficients.items()}
    logger.debug(f'{scheme} 窗口 {window_index} 权重: {coefficients}')
    return {'scheme': scheme, 'window': window_index, 'coefficients': coefficients}
