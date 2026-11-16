"""风格轮动状态判定：三维滚动分位打分与三档标签（纯计算，不读写文件）。

口径见 ``docs/风格轮动判定方法_2026-11-16.md``。本模块只接收「风格日频面板」
（polars DataFrame），输出规模 / 价值成长 / 高波低波三个维度的得分与档位、
当期占优方向、分化强度与代表风格组合；本地装配与 CLI 在
``style_rotation_local.py``。

统一约定：

- 所有指标在交易日 ``t`` 收盘后计算，可交易口径需整体 ``shift(1)``；
- 分位一律用 250 个有效交易日的滚动分位，不用绝对点位；
- 分组只用当日可得数据（市值当日、估值 PIT、波动率截至 t−1），
  禁止全样本分位与未来分组；
- 指标缺失跳过并把维度内剩余权重重新归一，维度全缺则该维度不参与，
  不插值、不零填、不外推。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Mapping

import numpy as np
import polars as pl

#: 滚动分位窗口（有效交易日）
ROLLING_WINDOW = 250
#: 窗口内至少需要的有效值个数
MIN_SAMPLES = 250
#: 维度档位的中性带（|score| 小于该值视为均衡）
NEUTRAL_BAND = 0.20


@dataclass(frozen=True)
class DimensionSpec:
    """一个风格维度的定义。"""

    key: str
    name: str
    weight: float
    description: str
    positive_label: str
    negative_label: str


@dataclass(frozen=True)
class IndicatorSpec:
    """一个打分指标的定义。"""

    key: str
    name: str
    dimension: str
    column: str
    weight: float
    raw_kind: str
    formula: str
    judgment: str


@dataclass(frozen=True)
class IndicatorResult:
    """某维度某指标的最新读数。"""

    key: str
    name: str
    dimension: str
    raw: float | None
    raw_kind: str
    percentile: float | None
    score: float | None
    asof: date | None
    formula: str
    judgment: str
    weight: float


@dataclass(frozen=True)
class DimensionResult:
    """某一风格维度的汇总（权重为重新归一后的实际权重）。"""

    key: str
    name: str
    weight: float
    score: float
    label: str
    contribution: float
    indicators: tuple[IndicatorResult, ...]
    description: str
    drivers: tuple[str, ...] = field(default_factory=tuple)
    drags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class StylePortfolio:
    """一条代表风格组合的近期强弱。"""

    key: str
    name: str
    ret20: float | None
    ret60: float | None
    rel20: float | None
    rel60: float | None


@dataclass(frozen=True)
class StyleRotationState:
    """一次判定下的完整风格状态。"""

    asof: date
    dimensions: tuple[DimensionResult, ...]
    total_score: float
    direction: str
    portfolios: tuple[StylePortfolio, ...]
    notes: tuple[str, ...] = field(default_factory=tuple)


DIMENSIONS: tuple[DimensionSpec, ...] = (
    DimensionSpec('size', '规模', 0.35, '小盘与大盘的相对强弱与资金关注度', '小盘占优', '大盘占优'),
    DimensionSpec('value', '价值成长', 0.35, '低 PB 价值组与高 PB 成长组的相对强弱', '价值占优', '成长占优'),
    DimensionSpec('vol', '高波低波', 0.30, '高波动组与低波动组的相对强弱', '高波占优', '低波占优'),
)

INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec(
        'size_rs20', '小盘−大盘 20 日收益差', 'size', 'size_rs20', 0.35, 'ret',
        '小盘组 20 日等权收益 − 大盘组 20 日等权收益，取 250 日滚动分位',
        '分位越高说明小盘相对占优',
    ),
    IndicatorSpec(
        'size_rs60', '小盘−大盘 60 日收益差', 'size', 'size_rs60', 0.35, 'ret',
        '小盘组 60 日等权收益 − 大盘组 60 日等权收益，取 250 日滚动分位',
        '中期口径，过滤月度噪声',
    ),
    IndicatorSpec(
        'size_amount', '小盘成交额占比', 'size', 'size_amount', 0.20, 'pct',
        '小盘组成交额 / 全市场成交额，20 日均，取 250 日滚动分位',
        '占比抬升说明资金转向小盘',
    ),
    IndicatorSpec(
        'size_turn', '小盘/大盘换手率比', 'size', 'size_turn', 0.10, 'ratio',
        '小盘组换手率均值 / 大盘组换手率均值，20 日均，取 250 日滚动分位',
        '小盘相对换手升温',
    ),
    IndicatorSpec(
        'vg_rs20', '价值−成长 20 日收益差', 'value', 'vg_rs20', 0.35, 'ret',
        '价值组 20 日等权收益 − 成长组 20 日等权收益，取 250 日滚动分位',
        '分位越高说明价值相对占优',
    ),
    IndicatorSpec(
        'vg_rs60', '价值−成长 60 日收益差', 'value', 'vg_rs60', 0.35, 'ret',
        '价值组 60 日等权收益 − 成长组 60 日等权收益，取 250 日滚动分位',
        '中期口径',
    ),
    IndicatorSpec(
        'vg_pb_gap', '成长/价值 PB 中位数比', 'value', 'vg_pb_gap', 0.20, 'ratio',
        '成长组 PB 中位数 / 价值组 PB 中位数，取 250 日滚动分位',
        '比值抬升说明价值相对更便宜',
    ),
    IndicatorSpec(
        'vg_earn_gap', '价值−成长净利同比差', 'value', 'vg_earn_gap', 0.10, 'pct',
        '价值组净利同比中位数 − 成长组净利同比中位数，取 250 日滚动分位',
        '价值组盈利相对改善',
    ),
    IndicatorSpec(
        'vol_rs20', '高波−低波 20 日收益差', 'vol', 'vol_rs20', 0.50, 'ret',
        '高波组 20 日等权收益 − 低波组 20 日等权收益，取 250 日滚动分位',
        '分位越高说明高波相对占优',
    ),
    IndicatorSpec(
        'vol_rs60', '高波−低波 60 日收益差', 'vol', 'vol_rs60', 0.35, 'ret',
        '高波组 60 日等权收益 − 低波组 60 日等权收益，取 250 日滚动分位',
        '中期口径',
    ),
    IndicatorSpec(
        'vol_disp', '波动率离散度', 'vol', 'vol_disp', 0.15, 'ratio',
        '高波组 60 日已实现波动率中位数 / 低波组中位数，取 250 日滚动分位',
        '离散度抬升通常与高波占优同步，属共动校验',
    ),
)

_SPECS_BY_DIMENSION: dict[str, list[IndicatorSpec]] = {dim.key: [] for dim in DIMENSIONS}
for _spec in INDICATORS:
    _SPECS_BY_DIMENSION[_spec.dimension].append(_spec)

#: 代表风格组合：(key, 展示名, 日收益列)
PORTFOLIOS: tuple[tuple[str, str, str], ...] = (
    ('large_value', '大盘价值', 'ret_large_value'),
    ('large_growth', '大盘成长', 'ret_large_growth'),
    ('small_value', '小盘价值', 'ret_small_value'),
    ('small_growth', '小盘成长', 'ret_small_growth'),
    ('lowvol', '低波', 'ret_lowvol'),
)


def _rolling_percentile(
    values: np.ndarray, window: int = ROLLING_WINDOW, min_samples: int = MIN_SAMPLES
) -> np.ndarray:
    """逐点计算分位（0~100）；并列值取中位秩，不足 min_samples 给 NaN。"""
    out = np.full(values.shape, np.nan, dtype=float)
    for index in range(values.size):
        current = values[index]
        if math.isnan(current):
            continue
        start = max(0, index - window + 1)
        sample = values[start: index + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size >= min_samples:
            less = float(np.count_nonzero(sample < current))
            equal = float(np.count_nonzero(sample == current))
            out[index] = (less + 0.5 * equal) / sample.size * 100.0
    return out


def _series_percentile(frame: pl.DataFrame, column: str) -> np.ndarray:
    """对单列时间序列计算滚动分位（要求 frame 已按 date 升序）。"""
    values = np.asarray(
        [math.nan if value is None else float(value)
         for value in frame[column].cast(pl.Float64).to_list()],
        dtype=float,
    )
    return _rolling_percentile(values)


def _label_for(score: float, dimension: DimensionSpec) -> str:
    if score >= NEUTRAL_BAND:
        return dimension.positive_label
    if score <= -NEUTRAL_BAND:
        return dimension.negative_label
    return '均衡'


def _impacts(indicators: tuple[IndicatorResult, ...]) -> list[tuple[float, str]]:
    """按指标对维度得分的贡献排序（正为拉动、负为拖累）。"""
    weight_sum = sum(item.weight for item in indicators) or 1.0
    scored = [
        (item.weight / weight_sum * (item.score or 0.0), item.name)
        for item in indicators
        if item.score is not None
    ]
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return scored


def _prepare_frame(panel: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    frame = panel.get('style_daily')
    if frame is None or frame.is_empty():
        raise ValueError('缺少风格日频面板 style_daily')
    if 'date' not in frame.columns:
        raise ValueError('style_daily 缺列: date')
    return frame.with_columns(pl.col('date').cast(pl.Date)).sort('date')


def _window_returns(frame: pl.DataFrame, column: str) -> tuple[np.ndarray, np.ndarray]:
    """由日收益列计算 20/60 日净值收益（缺失日按 0 收益衔接）。"""
    nav = (1 + pl.col(column).fill_null(0.0)).cum_prod()
    enriched = frame.select(
        (nav / nav.shift(20) - 1).alias('_ret20'),
        (nav / nav.shift(60) - 1).alias('_ret60'),
    )
    return (
        enriched['_ret20'].cast(pl.Float64).to_numpy(),
        enriched['_ret60'].cast(pl.Float64).to_numpy(),
    )


def _value_at(values: np.ndarray, index: int) -> float | None:
    value = float(values[index])
    return None if math.isnan(value) else value


def _indicator_results(
    frame: pl.DataFrame,
    percentiles: dict[str, np.ndarray],
    target_index: int,
    target: date,
) -> dict[str, list[IndicatorResult]]:
    """取目标日的指标读数；分位为空（未满窗或整列缺失）的指标直接跳过。"""
    results: dict[str, list[IndicatorResult]] = {dim.key: [] for dim in DIMENSIONS}
    for spec in INDICATORS:
        array = percentiles.get(spec.column)
        if array is None:
            continue
        percentile = float(array[target_index])
        if math.isnan(percentile):
            continue
        raw = frame[spec.column][target_index]
        raw_value = None if raw is None else float(raw)
        results[spec.dimension].append(IndicatorResult(
            key=spec.key,
            name=spec.name,
            dimension=spec.dimension,
            raw=None if raw_value is None or math.isnan(raw_value) else raw_value,
            raw_kind=spec.raw_kind,
            percentile=percentile,
            score=(percentile - 50.0) / 50.0,
            asof=target,
            formula=spec.formula,
            judgment=spec.judgment,
            weight=spec.weight,
        ))
    return results


def _dimension_results(
    results_by_dimension: dict[str, list[IndicatorResult]],
) -> tuple[list[DimensionResult], float, list[str]]:
    """维度内加权平均，缺维度时按剩余维度重新归一；返回维度、分化强度与缺失维度名。"""
    available = [
        (dimension, results_by_dimension[dimension.key])
        for dimension in DIMENSIONS
        if results_by_dimension[dimension.key]
    ]
    missing = [dimension.name for dimension in DIMENSIONS
               if not results_by_dimension[dimension.key]]
    if not available:
        raise ValueError('没有任何风格维度具备满窗分位，无法给出风格结论')

    available_weight = sum(dimension.weight for dimension, _ in available)
    dimensions: list[DimensionResult] = []
    total = 0.0
    for dimension, indicators in available:
        weight_sum = sum(item.weight for item in indicators)
        score = sum(item.weight * (item.score or 0.0) for item in indicators) / weight_sum
        scaled = dimension.weight / available_weight
        contribution = scaled * abs(score)
        total += contribution
        impacts = _impacts(tuple(indicators))
        dimensions.append(DimensionResult(
            key=dimension.key,
            name=dimension.name,
            weight=scaled,
            score=score,
            label=_label_for(score, dimension),
            contribution=contribution,
            indicators=tuple(indicators),
            description=dimension.description,
            drivers=tuple(name for impact, name in impacts if impact > 0)[:2],
            drags=tuple(name for impact, name in reversed(impacts) if impact < 0)[:2],
        ))
    return dimensions, total, missing


def _portfolio_results(frame: pl.DataFrame, target_index: int) -> list[StylePortfolio]:
    """代表组合的 20/60 日收益与相对全市场超额。"""
    if 'ret_market' in frame.columns:
        market20, market60 = _window_returns(frame, 'ret_market')
        market20_value = _value_at(market20, target_index)
        market60_value = _value_at(market60, target_index)
    else:
        market20_value = market60_value = None
    portfolios: list[StylePortfolio] = []
    for key, name, column in PORTFOLIOS:
        if column not in frame.columns:
            continue
        ret20, ret60 = _window_returns(frame, column)
        value20 = _value_at(ret20, target_index)
        value60 = _value_at(ret60, target_index)
        portfolios.append(StylePortfolio(
            key=key,
            name=name,
            ret20=value20,
            ret60=value60,
            rel20=None if value20 is None or market20_value is None else value20 - market20_value,
            rel60=None if value60 is None or market60_value is None else value60 - market60_value,
        ))
    return portfolios


def _direction(dimensions: list[DimensionResult]) -> str:
    significant = [item for item in dimensions if item.label != '均衡']
    if not significant:
        return '风格均衡'
    if len(significant) == 1:
        return significant[0].label
    if len(significant) == 2:
        return f'{significant[0].label[:-2]}{significant[1].label}'
    return '＋'.join(item.label[:-2] for item in significant) + '占优'


def evaluate_style_rotation(
    panel: Mapping[str, pl.DataFrame],
    asof: date | None = None,
) -> StyleRotationState:
    """由风格日频面板计算维度得分、占优方向与代表组合。

    Args:
        panel: 键 ``style_daily`` 的日频表，需含 ``date`` 及 ``INDICATORS`` 引用的列。
        asof: 判定截止日（含），默认取面板最新交易日。
    """
    frame = _prepare_frame(panel)
    dates = frame['date'].to_list()
    target = asof or dates[-1]
    eligible = [index for index, day in enumerate(dates) if day <= target]
    if not eligible:
        raise ValueError(f'asof={target} 早于面板首个交易日 {dates[0]}')
    target_index = eligible[-1]
    target = dates[target_index]
    frame = frame.head(target_index + 1)

    percentiles: dict[str, np.ndarray] = {}
    for spec in INDICATORS:
        if spec.column in frame.columns and spec.column not in percentiles:
            percentiles[spec.column] = _series_percentile(frame, spec.column)

    results = _indicator_results(frame, percentiles, target_index, target)
    dimensions, total, missing = _dimension_results(results)
    portfolios = _portfolio_results(frame, target_index)

    notes = [
        f'风格基于 {target} 收盘数据；可交易口径需 shift(1)，最早下一交易日生效。',
        '维度档位基于 250 日滚动分位，与代表组合的绝对收益不必然同向。',
    ]
    if missing:
        notes.append(f'当日无满窗分位、未参与分化强度：{"、".join(missing)}（按剩余维度重新归一）。')
    if not any(dimension.key == 'value' for dimension in dimensions):
        notes.append('价值维度依赖 PB，2015–2017 整列为空，该区间价值维度不可用。')

    return StyleRotationState(
        asof=target,
        dimensions=tuple(dimensions),
        total_score=total,
        direction=_direction(dimensions),
        portfolios=tuple(portfolios),
        notes=tuple(notes),
    )


def format_raw_value(value: float | None, kind: str) -> str:
    if value is None:
        return '—'
    if kind == 'ret':
        return f'{value * 100:+.2f}%'
    if kind == 'pct':
        return f'{value:.2f}%'
    if kind == 'ratio':
        return f'{value:.2f}x'
    return f'{value:.4f}'


def format_dimension_line(dimension: DimensionResult) -> str:
    """一行维度结论：名称、得分、档位与主要依据。"""
    parts = []
    if dimension.drivers:
        parts.append('拉动：' + '、'.join(dimension.drivers))
    if dimension.drags:
        parts.append('拖累：' + '、'.join(dimension.drags))
    basis = '｜'.join(parts) if parts else '—'
    return (
        f'{dimension.name} {dimension.score:+.3f} {dimension.label}'
        f' | 主要依据：{basis}'
    )


def format_portfolio_line(portfolio: StylePortfolio) -> str:
    def pct(value: float | None) -> str:
        return '—' if value is None else f'{value * 100:+.2f}%'

    return (
        f'{portfolio.name} 20 日 {pct(portfolio.ret20)} / 60 日 {pct(portfolio.ret60)}'
        f' | 超额(20 日) {pct(portfolio.rel20)}'
    )


def format_style_detail(state: StyleRotationState) -> str:
    """全部维度与组合明细，供 CLI 与界面复用。"""
    lines = [
        f'风格轮动状态（{state.asof}）：{state.direction}，分化强度 {state.total_score:.2f}',
    ]
    for dimension in state.dimensions:
        lines.append(format_dimension_line(dimension))
    for portfolio in state.portfolios:
        lines.append(format_portfolio_line(portfolio))
    for note in state.notes:
        lines.append(f'说明：{note}')
    return '\n'.join(lines)
