"""市场趋势状态判定：五层分位打分与状态标签（纯计算，不读写文件）。

口径与指标清单见 ``docs/市场趋势状态判定方法_2026-11-15.md``。本模块只接收
日频面板（polars DataFrame 字典），输出各层得分、总分、状态标签与逐指标明细；
本地数据装配与 CLI 入口在 ``market_trend_local.py``。

统一约定：

- 所有指标在交易日 ``t`` 收盘后计算，可交易口径需整体 ``shift(1)``；
- 强弱判据一律用滚动分位（250 个有效交易日；估值用 state_vector 的 1250 日分位），
  不使用绝对点位与绝对成交额；
- 指标缺失时跳过该指标并重新归一化层内权重，不插值、不回填。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Mapping, Sequence

import numpy as np
import polars as pl

#: 滚动分位窗口（有效交易日）
ROLLING_WINDOW = 250
#: 窗口内至少需要的有效值个数
MIN_SAMPLES = 250

#: 五档状态标签的上下界（由来源文件十分制 8-10/6-7/4-5/0-3 线性映射到 [-1, 1]）
LABEL_STRONG_UP = 0.60
LABEL_WEAK_UP = 0.20
LABEL_WEAK_DOWN = -0.20
LABEL_STRONG_DOWN = -0.60

#: 主基准指数：中证全指
PRIMARY_INDEX = '000985.SH'

#: 状态标签
STATE_LABELS = ('强趋势上行', '弱上行', '震荡', '弱势下行', '强下行')


@dataclass(frozen=True)
class LayerSpec:
    """一层指标的定义。"""

    key: str
    name: str
    weight: float
    description: str


@dataclass(frozen=True)
class IndicatorSpec:
    """一个打分指标的定义。"""

    key: str
    name: str
    layer: str
    column: str
    direction: int
    mode: str
    weight: float
    raw_kind: str
    formula: str
    judgment: str


@dataclass(frozen=True)
class IndicatorResult:
    """单个指标的最新读数。"""

    key: str
    name: str
    layer: str
    raw: float | None
    raw_kind: str
    mode: str
    percentile: float | None
    score: float | None
    asof: date | None
    formula: str
    judgment: str
    weight: float


@dataclass(frozen=True)
class LayerResult:
    """一层指标的汇总。"""

    key: str
    name: str
    weight: float
    score: float | None
    contribution: float | None
    indicators: tuple[IndicatorResult, ...]
    description: str


@dataclass(frozen=True)
class TrendState:
    """市场趋势状态判定结果。"""

    asof: date
    label: str
    total_score: float
    layers: tuple[LayerResult, ...]
    notes: tuple[str, ...] = field(default_factory=tuple)


LAYERS: tuple[LayerSpec, ...] = (
    LayerSpec('trend', '趋势', 0.30, '方向与持续性：指数相对长期均线的位置、斜率与中期收益'),
    LayerSpec('breadth', '宽度', 0.25, '上涨是否覆盖多数股票：均线占比、新高新低、涨跌家数'),
    LayerSpec('money', '量价资金', 0.20, '成交与杠杆是否支持方向：量价确认、融资余额与融资买入占比'),
    LayerSpec('vol', '波动情绪', 0.10, '趋势是否稳定：实现波动、下行波动、回撤与涨跌停家数'),
    LayerSpec('value', '估值宏观', 0.15, '赔率与背景：ERP 与全 A 盈利收益率分位'),
)

INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec(
        'trend_dev200', '指数相对 MA200 偏离', 'trend', 'dev200', 1, 'rolling', 0.40,
        'ret',
        'dev200_t = close_t / MA200_t - 1，取 250 日滚动分位',
        '站上长期均线且偏离扩大，中期趋势偏强'),
    IndicatorSpec(
        'trend_slope60', 'MA60 的 10 日斜率', 'trend', 'slope60', 1, 'rolling', 0.30,
        'ret',
        'slope60_t = MA60_t / MA60_(t-10) - 1，取 250 日滚动分位',
        '均线向上说明中期方向向上，反之向下'),
    IndicatorSpec(
        'trend_ret60', '60 日收益', 'trend', 'ret60', 1, 'rolling', 0.30,
        'ret',
        'ret60_t = close_t / close_(t-60) - 1，取 250 日滚动分位',
        '中期收益为正且分位抬升，趋势延续性较强'),
    IndicatorSpec(
        'breadth_ma60', '站上 MA60 占比', 'breadth', 'pct_above_ma60', 1, 'rolling', 0.40,
        'pct',
        'pct_above_ma60_t = 站上 MA60 只数 / sample_size_ma60 × 100，取 250 日滚动分位',
        '占比高说明上涨覆盖面广；分母为全市场含 ST'),
    IndicatorSpec(
        'breadth_ma120', '站上 MA120 占比', 'breadth', 'pct_above_ma120', 1, 'rolling', 0.25,
        'pct',
        'pct_above_ma120_t = 站上 MA120 只数 / sample_size_ma120 × 100，取 250 日滚动分位',
        '更长窗口的覆盖面，过滤短期反弹噪音'),
    IndicatorSpec(
        'breadth_highlow', '250 日新高 − 新低', 'breadth', 'highlow_net', 1, 'rolling', 0.20,
        'pct',
        '(new_high_250_t - new_low_250_t) / total_count_t × 100，取 250 日滚动分位',
        '扩散指标：新高多于新低说明赚钱效应扩散'),
    IndicatorSpec(
        'breadth_updown', '上涨家数占比', 'breadth', 'up_ratio', 1, 'rolling', 0.15,
        'pct',
        'up_count_t / (up_count_t + down_count_t) × 100，取 250 日滚动分位',
        '指数上涨但上涨家数少，属于窄基虚强'),
    IndicatorSpec(
        'money_amount', '量价确认', 'money', 'amount_confirm', 1, 'rolling', 0.40,
        'ratio',
        'confirm_t = (amount_t / mean(amount, 20)) × sign(close_t / close_(t-5) - 1)，取 250 日滚动分位',
        '放量且价格上涨为正；放量下跌为负'),
    IndicatorSpec(
        'money_margin', '融资余额 20 日变化', 'money', 'margin_chg20', 1, 'rolling', 0.30,
        'ret',
        'fin_balance_t / fin_balance_(t-20) - 1（沪深两市合计），取 250 日滚动分位',
        '杠杆资金扩张通常顺周期，退潮时压制趋势'),
    IndicatorSpec(
        'money_margin_buy', '融资买入占比分位', 'money', 'lev_margin_buy_ratio_q', 1,
        'quantile', 0.30, 'q',
        '融资买入额 / 两市成交额 × 100 的 1250 日滚动分位（state_vector_daily）',
        '融资交易活跃度，过高需警惕拥挤'),
    IndicatorSpec(
        'vol_rv20', '20 日已实现波动率', 'vol', 'rv20', -1, 'rolling', 0.30,
        'vol',
        'rv20_t = std(close_t / close_(t-1) - 1, 20)，取 250 日滚动分位',
        '波动越高趋势越不稳定，得分扣减'),
    IndicatorSpec(
        'vol_downside20', '20 日下行波动率', 'vol', 'downside20', -1, 'rolling', 0.25,
        'vol',
        'sqrt(mean(min(ret_i, 0)^2, 20))，取 250 日滚动分位',
        '下行波动抬升说明卖压与风险扩大'),
    IndicatorSpec(
        'vol_mdd60', '60 日最大回撤幅度', 'vol', 'mdd60', -1, 'rolling', 0.25,
        'vol',
        'mdd60_t = -min(close_i / rolling_max(close, 60)_i - 1, 近 60 日)，取 250 日滚动分位',
        '回撤越深，趋势稳定性越差'),
    IndicatorSpec(
        'vol_limit_down', '跌停家数分位', 'vol', 'limit_down_count', -1, 'rolling', 0.10,
        'count',
        'limit_down_count_t，取 250 日滚动分位',
        '跌停突然放大往往是流动性事件先行指标'),
    IndicatorSpec(
        'vol_limit_up', '涨停家数分位', 'vol', 'limit_up_count', 1, 'rolling', 0.10,
        'count',
        'limit_up_count_t，取 250 日滚动分位',
        '涨停家数回升说明短线风险偏好修复'),
    IndicatorSpec(
        'value_erp', '股权风险溢价分位', 'value', 'valuation_erp_q', 1, 'quantile', 0.60,
        'q',
        'ERP = 全 A 盈利收益率 - 10Y 国债收益率 的 1250 日滚动分位（state_vector_daily）',
        '分位越高赔率越好，但只决定长期空间'),
    IndicatorSpec(
        'value_earn_yield', '全 A 盈利收益率分位', 'value', 'val_earn_yield_q', 1,
        'quantile', 0.40, 'q',
        'Σ净利润TTM / Σ总市值 的倒数（缩尾口径）1250 日滚动分位（state_vector_daily）',
        '盈利收益率分位抬升，估值相对便宜'),
)

_LAYER_BY_KEY = {layer.key: layer for layer in LAYERS}
_SPECS_BY_LAYER: dict[str, list[IndicatorSpec]] = {layer.key: [] for layer in LAYERS}
for _spec in INDICATORS:
    _SPECS_BY_LAYER[_spec.layer].append(_spec)


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
        sample = values[start : index + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size >= min_samples:
            less = float(np.count_nonzero(sample < current))
            equal = float(np.count_nonzero(sample == current))
            out[index] = (less + 0.5 * equal) / sample.size * 100.0
    return out


def _to_float_array(frame: pl.DataFrame, column: str) -> np.ndarray:
    values = frame[column].cast(pl.Float64).to_list()
    return np.asarray(
        [math.nan if value is None else float(value) for value in values], dtype=float
    )


def _prepare_frame(panel: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    """把各输入表按主基准交易日对齐成一张日频面板。"""
    index_frame = panel.get('index')
    if index_frame is None or index_frame.is_empty():
        raise ValueError('缺少日频面板 index（主基准指数行情）')
    missing = {'date', 'close', 'amount'} - set(index_frame.columns)
    if missing:
        raise ValueError(f'index 面板缺列: {sorted(missing)}')

    frame = (
        index_frame.select(['date', 'close', 'amount'])
        .with_columns(pl.col('date').cast(pl.Date))
        .sort('date')
        .with_columns(
            pl.col('close').rolling_mean(200, min_samples=200).alias('ma200'),
            pl.col('close').rolling_mean(60, min_samples=60).alias('ma60'),
            pl.col('amount').rolling_mean(20, min_samples=20).alias('_amt_ma20'),
        )
        .with_columns(
            (pl.col('close') / pl.col('ma200') - 1).alias('dev200'),
            (pl.col('ma60') / pl.col('ma60').shift(10) - 1).alias('slope60'),
            (pl.col('close') / pl.col('close').shift(60) - 1).alias('ret60'),
            (pl.col('close') / pl.col('close').shift(5) - 1).alias('ret5'),
            (
                pl.when(pl.col('_amt_ma20') > 0)
                .then(pl.col('amount') / pl.col('_amt_ma20'))
                .otherwise(None)
            ).alias('_amt_ratio'),
        )
        .with_columns(
            pl.col('close').pct_change().alias('_ret1'),
        )
        .with_columns(
            pl.col('_ret1').rolling_std(20, min_samples=20).alias('rv20'),
            pl.col('_ret1')
            .clip(lower_bound=-1e9, upper_bound=0.0)
            .pow(2)
            .rolling_mean(20, min_samples=20)
            .sqrt()
            .alias('downside20'),
            (pl.col('close') / pl.col('close').rolling_max(60, min_samples=60) - 1)
            .rolling_min(60, min_samples=60)
            .mul(-1)
            .alias('mdd60'),
            (pl.col('_amt_ratio') * pl.col('ret5').sign()).alias('amount_confirm'),
        )
        .drop(['ma200', 'ma60', '_amt_ma20', '_ret1', '_amt_ratio'])
    )

    breadth_ma = panel.get('breadth_ma')
    if breadth_ma is not None and not breadth_ma.is_empty():
        frame = frame.join(
            breadth_ma.select(['date', 'pct_above_ma60', 'pct_above_ma120'])
            .with_columns(pl.col('date').cast(pl.Date)),
            on='date', how='left',
        )

    breadth = panel.get('breadth')
    if breadth is not None and not breadth.is_empty():
        daily = (
            breadth.select([
                'trade_date', 'up_count', 'down_count', 'total_count',
                'limit_up_count', 'limit_down_count', 'new_high_250', 'new_low_250',
            ])
            .rename({'trade_date': 'date'})
            .with_columns(pl.col('date').cast(pl.Date))
            .with_columns(
                (
                    pl.when(pl.col('total_count') > 0)
                    .then(
                        (pl.col('new_high_250') - pl.col('new_low_250'))
                        / pl.col('total_count') * 100
                    )
                    .otherwise(None)
                ).alias('highlow_net'),
                (
                    pl.when((pl.col('up_count') + pl.col('down_count')) > 0)
                    .then(
                        pl.col('up_count')
                        / (pl.col('up_count') + pl.col('down_count')) * 100
                    )
                    .otherwise(None)
                ).alias('up_ratio'),
            )
        )
        frame = frame.join(daily, on='date', how='left')

    state = panel.get('state_vector')
    if state is not None and not state.is_empty():
        frame = frame.join(
            state.select([
                'date', 'valuation_erp_q', 'val_earn_yield_q',
                'lev_margin_buy_ratio_q',
            ]).with_columns(pl.col('date').cast(pl.Date)),
            on='date', how='left',
        )

    margin = panel.get('margin')
    if margin is not None and not margin.is_empty():
        margin = (
            margin.filter(pl.col('market').is_in(['sh', 'sz']))
            .with_columns(pl.col('date').cast(pl.Date))
            .group_by('date')
            .agg(pl.col('fin_balance').sum())
            .sort('date')
            .with_columns(
                (pl.col('fin_balance') / pl.col('fin_balance').shift(20) - 1)
                .alias('margin_chg20')
            )
        )
        frame = frame.join(margin.select(['date', 'margin_chg20']), on='date', how='left')

    return frame


def _latest_index(dates: Sequence[date], percentiles: np.ndarray, asof: date) -> int | None:
    for index in range(len(dates) - 1, -1, -1):
        if dates[index] <= asof and not math.isnan(percentiles[index]):
            return index
    return None


def _label_for(total: float) -> str:
    if total >= LABEL_STRONG_UP:
        return '强趋势上行'
    if total >= LABEL_WEAK_UP:
        return '弱上行'
    if total > LABEL_WEAK_DOWN:
        return '震荡'
    if total > LABEL_STRONG_DOWN:
        return '弱势下行'
    return '强下行'


def evaluate_market_trend(
    panel: Mapping[str, pl.DataFrame], asof: date | None = None
) -> TrendState:
    """由日频面板计算五层得分、总分与状态标签。

    Args:
        panel: 键为 ``index`` / ``breadth_ma`` / ``breadth`` / ``state_vector`` / ``margin``
            的日频表；``index`` 必需且需含 ``date``、``close``、``amount``。
        asof: 判定截止日（含），默认取主基准最新交易日。
    """
    frame = _prepare_frame(panel)
    dates = list(frame['date'].to_list())
    if not dates:
        raise ValueError('日频面板为空，无法判定市场趋势')

    target = asof or dates[-1]
    effective = [day for day in dates if day <= target]
    if not effective:
        raise ValueError(f'asof={target} 早于面板首个交易日 {dates[0]}')
    target = effective[-1]

    results_by_layer: dict[str, list[IndicatorResult]] = {key: [] for key in _SPECS_BY_LAYER}
    for spec in INDICATORS:
        if spec.column not in frame.columns:
            continue
        raw_values = _to_float_array(frame, spec.column)
        if spec.mode == 'quantile':
            percentiles = raw_values.copy()
        else:
            percentiles = _rolling_percentile(raw_values)
        index = _latest_index(dates, percentiles, target)
        if index is None:
            continue
        percentile = float(percentiles[index])
        raw = raw_values[index]
        score = spec.direction * (percentile - 50.0) / 50.0
        results_by_layer[spec.layer].append(IndicatorResult(
            key=spec.key,
            name=spec.name,
            layer=spec.layer,
            raw=None if math.isnan(raw) else float(raw),
            raw_kind=spec.raw_kind,
            mode=spec.mode,
            percentile=percentile,
            score=score,
            asof=dates[index],
            formula=spec.formula,
            judgment=spec.judgment,
            weight=spec.weight,
        ))

    layer_results: list[LayerResult] = []
    available_weight = 0.0
    for layer in LAYERS:
        indicators = results_by_layer[layer.key]
        scored = [item for item in indicators if item.score is not None]
        if scored:
            weight_sum = sum(item.weight for item in scored)
            layer_score = sum(item.weight * item.score for item in scored) / weight_sum
            available_weight += layer.weight
        else:
            layer_score = None
        layer_results.append(LayerResult(
            key=layer.key,
            name=layer.name,
            weight=layer.weight,
            score=layer_score,
            contribution=None,
            indicators=tuple(indicators),
            description=layer.description,
        ))

    if available_weight <= 0:
        raise ValueError('没有任何可用指标，无法给出总分')

    total = 0.0
    final_layers: list[LayerResult] = []
    for layer in layer_results:
        if layer.score is None:
            final_layers.append(layer)
            continue
        scaled_weight = layer.weight / available_weight
        contribution = scaled_weight * layer.score
        total += contribution
        final_layers.append(LayerResult(
            key=layer.key,
            name=layer.name,
            weight=scaled_weight,
            score=layer.score,
            contribution=contribution,
            indicators=layer.indicators,
            description=layer.description,
        ))

    notes = _build_notes(final_layers, target)
    return TrendState(
        asof=target,
        label=_label_for(total),
        total_score=total,
        layers=tuple(final_layers),
        notes=notes,
    )


def _build_notes(layers: Sequence[LayerResult], asof: date) -> tuple[str, ...]:
    notes: list[str] = [
        f'状态基于 {asof} 收盘数据；可交易口径需 shift(1)，最早下一交易日生效。'
    ]
    for layer in layers:
        if not layer.indicators:
            notes.append(f'{layer.name}层全部指标缺失，未参与总分。')
            continue
        lagged = [item.name for item in layer.indicators if item.asof and item.asof < asof]
        if lagged:
            notes.append(f'{layer.name}层读数滞后：{"、".join(lagged)}。')
    return tuple(notes)


def format_raw_value(value: float | None, kind: str) -> str:
    if value is None:
        return '—'
    if kind == 'ret':
        return f'{value * 100:+.2f}%'
    if kind == 'pct':
        return f'{value:.2f}%'
    if kind == 'vol':
        return f'{value * 100:.2f}%'
    if kind == 'ratio':
        return f'{value:.2f}x'
    if kind == 'count':
        return f'{value:.0f}'
    if kind == 'q':
        return f'分位 {value:.1f}'
    return f'{value:.4f}'


def format_state_line(state: TrendState) -> str:
    """一行结论：日期、标签、总分与各层得分。"""
    parts = []
    for layer in state.layers:
        if layer.score is None:
            parts.append(f'{layer.name} —')
        else:
            parts.append(f'{layer.name} {layer.score:+.2f}')
    return (
        f'市场趋势状态（{state.asof}）：{state.label} | 总分 {state.total_score:+.3f}'
        f' | {" / ".join(parts)}'
    )


def format_state_detail(state: TrendState) -> str:
    """分层与逐指标明细，供 CLI 与界面复用。"""
    lines = [format_state_line(state)]
    for layer in state.layers:
        if layer.score is None:
            lines.append(f'[{layer.name}] 无可用指标')
            continue
        lines.append(
            f'[{layer.name}] 得分 {layer.score:+.3f} | 贡献 {layer.contribution:+.3f}'
            f' | 权重 {layer.weight:.2f}'
        )
        for item in layer.indicators:
            percentile = '—' if item.percentile is None else f'{item.percentile:.1f}'
            score = '—' if item.score is None else f'{item.score:+.2f}'
            window_text = '1250 日分位' if item.mode == 'quantile' else '250 日分位'
            lines.append(
                f'  {item.name}：读数 {format_raw_value(item.raw, item.raw_kind)}'
                f' | {window_text} {percentile} | 得分 {score} | 数据截至 {item.asof}'
            )
    for note in state.notes:
        lines.append(f'说明：{note}')
    return '\n'.join(lines)
