#!/usr/bin/env python3

"""L1/L2 的 walk-forward 编排。

L2（因子权重学习）：每个窗口用训练窗拟合权重，在测试窗打分；
基线包含等权、静态 IC 加权（仅在首个训练窗拟合一次，之后不再更新）。
L1（参数自适应）：按 regime 分档，用训练窗内各状态的超额收益选择
动量回看参数，再应用到测试窗对应状态的交易日；对照为固定参数与
「全样本最优参数」（含未来信息，仅用于展示过拟合风险）。

所有方案共用同一份测试窗样本和同一个组合模拟器，保证可比性。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl
from loguru import logger

from src.quant.learning.config import LearningReport, SchemeMetrics, WalkForwardConfig
from src.quant.learning.evaluator import evaluate_scheme, ic_stats, quantile_analysis
from src.quant.learning.labels import (
    LABEL_COL,
    add_adjusted_prices,
    add_date_index,
    attach_forward_labels,
    feature_columns,
    normalize_features,
    valid_label_filter,
)
from src.quant.learning.regimes import classify_regimes
from src.quant.learning.splits import make_walk_forward_windows
from src.quant.learning.weights import (
    OptionalDependencyError,
    build_learner,
    weight_history_entry,
)

DEFAULT_SCHEMES: Tuple[str, ...] = ('equal', 'ic', 'icir', 'ridge')
DATE_IDX_COL = '__date_idx'
MOMENTUM_FACTORS: Tuple[str, ...] = ('momentum_1m', 'momentum_3m',
                                     'momentum_6m', 'momentum_12m')


def prepare_panel(panel: pl.DataFrame, factor_cols: Sequence[str],
                  config: WalkForwardConfig) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """补齐复权价、标签、特征标准化与 regime，返回 (面板, regime 表)。"""
    prepared = panel.sort(['ts_code', 'date'])
    prepared = add_adjusted_prices(prepared)
    prepared = add_date_index(prepared)
    prepared = attach_forward_labels(prepared, config.horizon)
    prepared = normalize_features(prepared, factor_cols)
    regimes = classify_regimes(prepared)
    if not regimes.is_empty():
        prepared = prepared.join(regimes, on='date', how='left')
    else:
        prepared = prepared.with_columns([
            pl.lit('unknown').alias('regime'),
            pl.lit('unknown').alias('regime_vol'),
            pl.lit('unknown').alias('regime_trend'),
        ])
    return prepared, regimes


def _window_slice(frame: pl.DataFrame, start: int, end: int,
                  require_label: bool = False) -> pl.DataFrame:
    sliced = frame.filter(
        (pl.col(DATE_IDX_COL) >= start) & (pl.col(DATE_IDX_COL) < end))
    if require_label:
        sliced = sliced.filter(pl.col(LABEL_COL).is_finite())
    return sliced


def run_weight_learning(prepared: pl.DataFrame, factor_cols: Sequence[str],
                        config: WalkForwardConfig,
                        schemes: Sequence[str] = DEFAULT_SCHEMES,
                        include_lightgbm: bool = False) -> Tuple[
                            List[SchemeMetrics], List[Dict[str, Any]], List[str]]:
    """L2 权重学习：逐窗口拟合并生成样本外打分。"""
    features = feature_columns(factor_cols)
    windows = make_walk_forward_windows(prepared['date'].n_unique(), config)
    if not windows:
        return [], [], ['walk-forward 窗口不足，无法做样本外评估']

    notes: List[str] = []
    active_schemes = list(schemes)
    if include_lightgbm and 'lightgbm' not in active_schemes:
        active_schemes.append('lightgbm')
    if 'lightgbm' not in active_schemes:
        notes.append('LightGBM 方案未启用（可选依赖组 learning 未安装）')

    oos_frames: Dict[str, List[pl.DataFrame]] = {name: [] for name in active_schemes}
    history: List[Dict[str, Any]] = []
    static_model = None

    for window in windows:
        train = _window_slice(prepared, window.train_start,
                              window.purged_train_end, require_label=True)
        test = _window_slice(prepared, window.test_start, window.test_end)
        if train.is_empty() or test.is_empty():
            continue
        for name in active_schemes:
            try:
                learner = build_learner(name, config)
                model = learner.fit(train, features)
            except OptionalDependencyError as exc:
                notes.append(f'跳过 {name}: {exc}')
                active_schemes = [item for item in active_schemes if item != name]
                oos_frames.pop(name, None)
                continue
            scores = model.predict(test)
            oos_frames[name].append(test.select(
                ['date', 'ts_code', LABEL_COL]).with_columns(
                pl.Series('score', scores, dtype=pl.Float64)))
            history.append(weight_history_entry(name, window.index, model))
            if name == 'ic' and static_model is None:
                static_model = model

        if static_model is not None:
            scores = static_model.predict(test)
            oos_frames.setdefault('static_ic', []).append(
                test.select(['date', 'ts_code', LABEL_COL]).with_columns(
                    pl.Series('score', scores, dtype=pl.Float64)))

    metrics: List[SchemeMetrics] = []
    for name, frames in oos_frames.items():
        if not frames:
            continue
        oos = pl.concat(frames).sort(['date', 'ts_code'])
        display = '静态IC加权' if name == 'static_ic' else name
        metrics.append(evaluate_scheme(display, oos, prepared, config))
    order = {'equal': 0, '静态IC加权': 1, 'ic': 2, 'icir': 3,
             'ridge': 4, 'lightgbm': 5}
    metrics.sort(key=lambda item: order.get(item.name, len(order)))
    return metrics, history, notes


def _combo_excess_by_date(prepared: pl.DataFrame, factor_z: str,
                          sign: int) -> Dict[Any, float]:
    """逐日计算「头部 20% 组合的标签均值 - 全截面标签均值」。"""
    valid = prepared.filter(pl.col(LABEL_COL).is_finite())
    if valid.is_empty():
        return {}
    parts = valid.partition_by('date', as_dict=True)
    result: Dict[Any, float] = {}
    for key, cross in parts.items():
        if cross.height < 5:
            continue
        size = max(1, int(round(cross.height * 0.2)))
        ranked = cross.sort(factor_z, descending=sign > 0).head(size)
        top_mean = float(np.nanmean(ranked[LABEL_COL].to_numpy().astype(float)))
        all_mean = float(np.nanmean(cross[LABEL_COL].to_numpy().astype(float)))
        result[key[0]] = top_mean - all_mean
    return result


def _build_scores(prepared: pl.DataFrame, windows: Sequence[Any],
                  combo_by_date: Dict[Any, Tuple[str, int]]) -> pl.DataFrame:
    """按「日期 → (动量因子, 方向)」映射拼接测试窗打分。"""
    frames: List[pl.DataFrame] = []
    for window in windows:
        test = _window_slice(prepared, window.test_start, window.test_end)
        if test.is_empty():
            continue
        parts = test.partition_by('date', as_dict=True)
        score_rows: List[pl.DataFrame] = []
        for key, cross in parts.items():
            combo = combo_by_date.get(key[0])
            if combo is None:
                continue
            factor_z, sign = combo
            score_rows.append(cross.select(
                ['date', 'ts_code', LABEL_COL, factor_z]).with_columns(
                (pl.col(factor_z) * float(sign)).cast(pl.Float64).alias('score')
            ).select(['date', 'ts_code', LABEL_COL, 'score']))
        if score_rows:
            frames.append(pl.concat(score_rows))
    if not frames:
        return pl.DataFrame({'date': [], 'ts_code': [], 'score': []})
    return pl.concat(frames).sort(['date', 'ts_code'])


def _fixed_combo_scores(prepared: pl.DataFrame, windows: Sequence[Any],
                        combo: Tuple[str, int]) -> pl.DataFrame:
    """固定参数基线：所有测试日使用同一个 (因子, 方向)。"""
    return _build_scores(
        prepared, windows,
        dict.fromkeys(prepared['date'].unique().to_list(), combo))


def run_param_adaptation(prepared: pl.DataFrame, config: WalkForwardConfig,
                         combos: Sequence[str] = MOMENTUM_FACTORS) -> Tuple[
                             List[SchemeMetrics], List[Dict[str, Any]], List[str]]:
    """L1 参数自适应：按 regime 在训练窗内选择动量回看参数与方向。"""
    windows = make_walk_forward_windows(prepared['date'].n_unique(), config)
    if not windows:
        return [], [], ['walk-forward 窗口不足，无法做参数自适应']

    factor_z = {name: f'{name}__z' for name in combos}
    available = [name for name, col in factor_z.items() if col in prepared.columns]
    if not available:
        return [], [], ['缺少动量因子列，跳过 L1 参数自适应']

    # 参数空间 = 回看窗口 × 方向（动量 / 反转）
    candidates: List[Tuple[str, int]] = [
        (name, sign) for name in available for sign in (1, -1)]
    excess = {(name, sign): _combo_excess_by_date(prepared, factor_z[name], sign)
              for name, sign in candidates}
    dates = prepared['date'].unique().sort().to_list()
    regime_rows = (prepared.select(['date', 'regime']).unique()
                   .sort('date').to_dicts())
    regime_map = {row['date']: row['regime'] for row in regime_rows}

    combo_by_date: Dict[Any, Tuple[str, int]] = {}
    history: List[Dict[str, Any]] = []
    for window in windows:
        train_dates = dates[window.train_start:window.purged_train_end]
        global_scores: Dict[Tuple[str, int], float] = {}
        regime_scores: Dict[str, Dict[Tuple[str, int], float]] = {}
        for combo in candidates:
            values = [excess[combo][day] for day in train_dates
                      if day in excess[combo]]
            if not values:
                continue
            global_scores[combo] = float(np.mean(values))
            for day in train_dates:
                if day not in excess[combo]:
                    continue
                regime = regime_map.get(day, 'unknown')
                regime_scores.setdefault(regime, {}).setdefault(combo, 0.0)
                regime_scores[regime][combo] += excess[combo][day]
        if not global_scores:
            continue
        best_global = max(global_scores, key=lambda key: global_scores[key])
        regime_best: Dict[str, Tuple[str, int]] = {}
        for regime, scores in regime_scores.items():
            regime_best[regime] = max(scores, key=lambda key: scores[key])
        history.append({
            'window': window.index,
            'regime_params': {
                key: f'{"+" if sign > 0 else "-"}{factor_z[name]}'
                for key, (name, sign) in regime_best.items()
            },
            'fallback': (f'{"+" if best_global[1] > 0 else "-"}'
                         f'{factor_z[best_global[0]]}'),
        })
        for window_date in dates[window.test_start:window.test_end]:
            combo_by_date[window_date] = regime_best.get(
                regime_map.get(window_date, 'unknown'), best_global)

    adaptive_scores = _build_scores(prepared, windows, combo_by_date)
    metrics: List[SchemeMetrics] = [
        evaluate_scheme('L1-状态自适应', adaptive_scores, prepared, config,
                        notes='按波动率分位×趋势状态滚动选择回看窗口与方向')]

    default_combo: Tuple[str, int] = (
        ('momentum_3m', 1) if 'momentum_3m' in available else candidates[0])
    default_scores = _fixed_combo_scores(prepared, windows, default_combo)
    metrics.append(evaluate_scheme(
        '固定参数-默认', default_scores, prepared, config,
        notes=f'固定 +{default_combo[0]}'))

    # 全样本最优参数：含未来信息，仅作过拟合风险对照
    full_scores = {
        combo: float(np.mean(list(excess[combo].values())))
        for combo in candidates if excess[combo]
    }
    if full_scores:
        best_full = max(full_scores, key=lambda key: full_scores[key])
        full_frame = _fixed_combo_scores(prepared, windows, best_full)
        direction = '+' if best_full[1] > 0 else '-'
        metrics.append(evaluate_scheme(
            '固定参数-全样本最优', full_frame, prepared, config,
            notes=f'全样本最优 {direction}{best_full[0]}（含未来信息，仅作对照）'))

    return metrics, history, []


def evaluate_factor_table(oos_frame: pl.DataFrame, factor_cols: Sequence[str],
                          config: WalkForwardConfig) -> List[SchemeMetrics]:
    """单因子样本外 IC/分层表（不做组合模拟，避免重复开销）。"""
    rows: List[SchemeMetrics] = []
    for col in factor_cols:
        feature = f'{col}__z'
        if feature not in oos_frame.columns:
            continue
        stats = ic_stats(oos_frame, feature)
        quantiles, monotonicity, long_short = quantile_analysis(
            oos_frame, feature, config.n_quantiles)
        rows.append(SchemeMetrics(
            name=col,
            ic_mean=stats['ic_mean'],
            ic_std=stats['ic_std'],
            icir=stats['icir'],
            ic_positive_ratio=stats['ic_positive_ratio'],
            ic_t_stat=stats['ic_t_stat'],
            quantile_returns=quantiles,
            long_short_return=long_short,
            monotonicity=monotonicity,
            n_periods=int(stats['n_periods']),
        ))
    return rows


def collect_oos_frame(prepared: pl.DataFrame,
                      config: WalkForwardConfig) -> pl.DataFrame:
    """拼接所有测试窗的可评估样本，供单因子对照使用。"""
    windows = make_walk_forward_windows(prepared['date'].n_unique(), config)
    frames = [_window_slice(prepared, window.test_start, window.test_end)
              for window in windows]
    if not frames:
        return prepared.head(0)
    return valid_label_filter(pl.concat(frames)).sort(['date', 'ts_code'])


def build_learning_report(panel: pl.DataFrame, factor_cols: Sequence[str],
                          config: WalkForwardConfig,
                          title: str = '自学习第一阶段评估',
                          include_lightgbm: bool = False,
                          data_scope: Dict[str, Any] | None = None
                          ) -> LearningReport:
    """一站式执行 L1 + L2 并返回报告对象。"""
    prepared, _ = prepare_panel(panel, factor_cols, config)
    windows = make_walk_forward_windows(prepared['date'].n_unique(), config)
    weight_schemes, weight_history, weight_notes = run_weight_learning(
        prepared, factor_cols, config, include_lightgbm=include_lightgbm)
    param_schemes, param_history, param_notes = run_param_adaptation(
        prepared, config)
    oos = collect_oos_frame(prepared, config)
    factor_metrics = evaluate_factor_table(oos, factor_cols, config)
    notes = weight_notes + param_notes
    if include_lightgbm:
        notes = [note for note in notes if '未启用' not in note]
    logger.info(
        f'学习实验完成：窗口 {len(windows)}，因子 {len(factor_cols)}，'
        f'权重方案 {len(weight_schemes)}，参数方案 {len(param_schemes)}')
    return LearningReport(
        title=title,
        config=config,
        data_scope=data_scope or {},
        weight_schemes=weight_schemes,
        factor_metrics=factor_metrics,
        param_schemes=param_schemes,
        windows=len(windows),
        weight_history=weight_history + param_history,
        notes=notes,
    )
