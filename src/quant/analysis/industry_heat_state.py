"""行业热度状态判定：六层滚动分位打分与五档标签（纯计算，不读写文件）。

口径与指标清单见 ``docs/行业热度判定方法_2026-11-15.md``。本模块只接收
「行业日频指标面板」（polars DataFrame），输出每个申万一级行业的热度分、档位、
逐指标明细与主要依据；本地装配与 CLI 入口在 ``industry_heat_local.py``。

统一约定：

- 所有指标在交易日 ``t`` 收盘后计算，可交易口径需整体 ``shift(1)``；
- 强弱判据一律用 250 个有效交易日的滚动分位，禁止绝对点位与绝对金额；
- 指标缺失跳过并把层内剩余权重重新归一化，层全缺则该层不参与总分；
  不插值、不零填、不外推；
- 行业归属只认申万 PIT 区间表（见文档 §3.3）。
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

#: 五档标签的上下界（与市场趋势页同阈值，便于对照）
LABEL_HOT = 0.60
LABEL_WARM = 0.20
LABEL_COOL = -0.20
LABEL_COLD = -0.60

#: 行业内有效成分少于该值时不产出结论
MIN_INDUSTRY_STOCKS = 5

#: 拥挤度预警门槛（分位）
OVERHEAT_CONC = 95.0
OVERHEAT_TURNOVER = 90.0
OVERHEAT_PREMIUM = 95.0

#: 五档标签
HEAT_LABELS = ('过热', '偏热', '中性', '偏冷', '冷清')


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
    weight: float
    raw_kind: str
    formula: str
    judgment: str


@dataclass(frozen=True)
class IndicatorResult:
    """某行业某指标的最新读数。"""

    key: str
    name: str
    layer: str
    raw: float | None
    raw_kind: str
    percentile: float | None
    score: float | None
    asof: date | None
    formula: str
    judgment: str
    weight: float


@dataclass(frozen=True)
class LayerResult:
    """某行业某一层的汇总。"""

    key: str
    name: str
    weight: float
    score: float | None
    contribution: float | None
    indicators: tuple[IndicatorResult, ...]
    description: str


@dataclass(frozen=True)
class IndustryHeat:
    """单个行业的热度判定结果。"""

    code: str
    name: str
    asof: date
    total_score: float
    label: str
    overheat: bool
    overheat_reasons: tuple[str, ...]
    layers: tuple[LayerResult, ...]
    drivers: tuple[str, ...]
    drags: tuple[str, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IndustryHeatState:
    """一次判定下全部行业的结果。"""

    asof: date
    industries: tuple[IndustryHeat, ...]
    notes: tuple[str, ...] = field(default_factory=tuple)


LAYERS: tuple[LayerSpec, ...] = (
    LayerSpec('rs', '相对强弱', 0.30, '行业相对全 A 等权的动量与相对强弱'),
    LayerSpec('energy', '量能', 0.20, '成交额占比、换手率与量价配合'),
    LayerSpec('crowd', '拥挤度', 0.20, '成交集中、换手极值与估值溢价'),
    LayerSpec('breadth', '广度', 0.15, '行业内上涨与均线覆盖面'),
    LayerSpec('valuation', '估值', 0.10, 'PE/PB 处于自身历史的位置'),
    LayerSpec('fundamental', '景气度', 0.05, '营收与净利同比的行业聚合'),
)

INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec(
        'rs20', '20 日相对强弱', 'rs', 'rs20', 0.35, 'ret',
        'rs20_t = 行业等权 20 日收益 - 全 A 等权 20 日收益，取 250 日滚动分位',
        '短期跑赢全市场说明资金正在流入该行业'),
    IndicatorSpec(
        'rs60', '60 日相对强弱', 'rs', 'rs60', 0.35, 'ret',
        'rs60_t = 行业等权 60 日收益 - 全 A 等权 60 日收益，取 250 日滚动分位',
        '中期相对强弱是行业动量的主读数'),
    IndicatorSpec(
        'mom60', '波动率调整动量', 'rs', 'mom60', 0.20, 'ratio',
        'mom60_t = 60 日收益 / 60 日收益标准差，取 250 日滚动分位',
        '同样涨幅下波动越低，趋势质量越高'),
    IndicatorSpec(
        'dev60', '净值相对 MA60 偏离', 'rs', 'dev60', 0.10, 'ret',
        'dev60_t = 行业等权净值 / MA60(净值) - 1，取 250 日滚动分位',
        '站在中期均线上方说明趋势未破'),
    IndicatorSpec(
        'share20', '成交额占比（20 日均）', 'energy', 'share20', 0.40, 'pct',
        'share20_t = 行业 20 日均成交额 / 全市场 20 日均成交额，取 250 日滚动分位',
        '成交占比抬升代表资金关注度提高'),
    IndicatorSpec(
        'turnover20', '换手率（20 日均）', 'energy', 'turnover20', 0.35, 'pct',
        'turnover20_t = 20 日均（成交股数 / 流通股本），取 250 日滚动分位',
        '换手升温是量能温度的直接读数'),
    IndicatorSpec(
        'amount_confirm', '量价配合', 'energy', 'amount_confirm', 0.25, 'ratio',
        'confirm_t = (成交额 / MA20(成交额)) × sign(5 日收益)，取 250 日滚动分位',
        '放量上涨为正，放量下跌为负'),
    IndicatorSpec(
        'conc', '成交集中度偏离', 'crowd', 'conc', 0.40, 'pct',
        'conc_t = 成交额占比 / 其 250 日均值 - 1，取 250 日滚动分位',
        '占比显著高于自身一年中枢是拥挤的第一信号'),
    IndicatorSpec(
        'turnover_heat', '换手率分位', 'crowd', 'turnover20', 0.35, 'q',
        '换手率 250 日滚动分位（与量能层同源，分位本身即拥挤读数）',
        '换手率高于 90% 分位属于危险区'),
    IndicatorSpec(
        'premium', '估值溢价', 'crowd', 'premium', 0.25, 'ratio',
        'premium_t = 行业 PE 中位数 / 全 A PE 中位数 - 1，取 250 日滚动分位',
        '相对全市场估值溢价抬升，说明定价已充分反映热度'),
    IndicatorSpec(
        'above60', '行业内站上 MA60 占比', 'breadth', 'above60', 0.45, 'pct',
        'above60_t = 站上 MA60 只数 / 有效成分数 × 100，取 250 日滚动分位',
        '占比高说明行业内部普涨而不是少数票拉指数'),
    IndicatorSpec(
        'up_ratio', '行业内上涨家数占比', 'breadth', 'up_ratio', 0.35, 'pct',
        'up_t = 上涨只数 / (上涨 + 下跌只数) × 100，取 250 日滚动分位',
        '行业上涨的覆盖面，过滤单票异动'),
    IndicatorSpec(
        'highlow', '行业内新高−新低净占比', 'breadth', 'highlow_net', 0.20, 'pct',
        'hl_t = (250 日新高只数 - 新低只数) / 有效成分数 × 100，取 250 日滚动分位',
        '新高多于新低说明行业内部扩散'),
    IndicatorSpec(
        'pe_q', '行业 PE 中位数分位', 'valuation', 'pe_med', 0.55, 'x',
        'median(PE_TTM) 的 250 日滚动分位（剔除 PE<=0）',
        '估值为自身历史高位时，热度中的定价成分更高'),
    IndicatorSpec(
        'pb_q', '行业 PB 中位数分位', 'valuation', 'pb_med', 0.45, 'x',
        'median(PB) 的 250 日滚动分位（2018 年前整列缺失）',
        'PB 分位补充 PE 在亏损行业上的盲区'),
    IndicatorSpec(
        'rev_yoy', '行业营收同比中位数', 'fundamental', 'rev_yoy_med', 0.60, 'pct',
        '每只股票取 ann_date<=t 的最新 revenue_yoy，行业内取中位数，再取 250 日滚动分位',
        '营收同比抬升说明景气度仍在改善'),
    IndicatorSpec(
        'np_yoy', '行业净利同比中位数', 'fundamental', 'np_yoy_med', 0.40, 'pct',
        '同 rev_yoy，用 net_profit_yoy',
        '利润增速比营收更贴近盈利兑现'),
)

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
        sample = values[start: index + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size >= min_samples:
            less = float(np.count_nonzero(sample < current))
            equal = float(np.count_nonzero(sample == current))
            out[index] = (less + 0.5 * equal) / sample.size * 100.0
    return out


def _percentiles_by_industry(frame: pl.DataFrame, column: str) -> np.ndarray:
    """按 ``industry_code`` 分组计算滚动分位；要求 frame 已按 (code, date) 排序。"""
    values = np.asarray(
        [math.nan if value is None else float(value)
         for value in frame[column].cast(pl.Float64).to_list()],
        dtype=float,
    )
    codes = frame['industry_code'].to_list()
    out = np.full(values.shape, np.nan, dtype=float)
    start = 0
    total = len(codes)
    while start < total:
        end = start + 1
        while end < total and codes[end] == codes[start]:
            end += 1
        out[start:end] = _rolling_percentile(values[start:end])
        start = end
    return out


def _label_for(total: float) -> str:
    if total >= LABEL_HOT:
        return '过热'
    if total >= LABEL_WARM:
        return '偏热'
    if total > LABEL_COOL:
        return '中性'
    if total > LABEL_COLD:
        return '偏冷'
    return '冷清'


def _overheat_check(indicators: list[IndicatorResult]) -> tuple[bool, tuple[str, ...]]:
    """按拥挤度三指标的分位判断是否过热；缺指标的项不参与。"""
    thresholds = {
        'conc': (OVERHEAT_CONC, '成交集中度偏离'),
        'turnover_heat': (OVERHEAT_TURNOVER, '换手率分位'),
        'premium': (OVERHEAT_PREMIUM, '估值溢价'),
    }
    reasons: list[str] = []
    for item in indicators:
        rule = thresholds.get(item.key)
        if rule is None or item.percentile is None:
            continue
        limit, label = rule
        if item.percentile >= limit:
            reasons.append(f'{label} {item.percentile:.0f} 分位≥{limit:.0f}')
    return bool(reasons), tuple(reasons)


def _prepare_frame(panel: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    frame = panel.get('industry_daily')
    if frame is None or frame.is_empty():
        raise ValueError('缺少行业日频面板 industry_daily')
    required = {'date', 'industry_code', 'industry_name', 'n_stocks'}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f'industry_daily 缺列: {sorted(missing)}')
    return (
        frame.with_columns(pl.col('date').cast(pl.Date))
        .sort(['industry_code', 'date'])
    )


def evaluate_industry_heat(
    panel: Mapping[str, pl.DataFrame],
    asof: date | None = None,
    min_stocks: int = MIN_INDUSTRY_STOCKS,
) -> IndustryHeatState:
    """由行业日频面板计算逐行业热度分、档位与主要依据。

    Args:
        panel: 键 ``industry_daily`` 的行业日频表，需含 ``date``、``industry_code``、
            ``industry_name``、``n_stocks`` 及 ``INDICATORS`` 引用的原始列。
        asof: 判定截止日（含），默认取面板最新交易日。
        min_stocks: 行业有效成分下限，低于该值当日不产出结论。
    """
    frame = _prepare_frame(panel)
    dates = sorted(set(frame['date'].to_list()))
    if not dates:
        raise ValueError('行业日频面板为空，无法判定行业热度')
    target = asof or dates[-1]
    effective = [day for day in dates if day <= target]
    if not effective:
        raise ValueError(f'asof={target} 早于面板首个交易日 {dates[0]}')
    target = effective[-1]

    frame = frame.filter(pl.col('date') <= target)
    percentile_cache: dict[str, np.ndarray] = {}
    for spec in INDICATORS:
        if spec.column in frame.columns and spec.column not in percentile_cache:
            percentile_cache[spec.column] = _percentiles_by_industry(frame, spec.column)

    codes = frame['industry_code'].to_list()
    names = frame['industry_name'].to_list()
    day_values = frame['date'].to_list()
    stock_counts = frame['n_stocks'].to_list()
    total_rows = frame.height

    heats: list[IndustryHeat] = []
    skipped: list[str] = []
    stale: list[str] = []
    insufficient: list[str] = []
    start = 0
    while start < total_rows:
        end = start + 1
        while end < total_rows and codes[end] == codes[start]:
            end += 1
        block = range(start, end)
        code, name = codes[start], names[start]
        latest = max(block, key=lambda index: day_values[index])
        if day_values[latest] != target:
            stale.append(f'{name}（{code}，最后 {day_values[latest]}）')
            start = end
            continue
        if (stock_counts[latest] or 0) < min_stocks:
            skipped.append(f'{name}（{code}）')
            start = end
            continue
        heat = _evaluate_one(
            frame=frame,
            block=block,
            code=code,
            name=name,
            asof=day_values[latest],
            percentiles=percentile_cache,
        )
        if heat is None:
            insufficient.append(f'{name}（{code}）')
        else:
            heats.append(heat)
        start = end
    if not heats:
        raise ValueError('没有任何行业具备满窗指标，无法给出热度结论')

    heats.sort(key=lambda item: item.total_score, reverse=True)
    notes = [f'热度基于 {target} 收盘数据；可交易口径需 shift(1)，最早下一交易日生效。']
    if skipped:
        notes.append(f'有效成分不足 {min_stocks} 只、当日未判定：{"、".join(skipped)}。')
    if stale:
        notes.append(f'当日已无成分、不参与判定：{"、".join(stale)}。')
    if insufficient:
        notes.append(f'历史不足 250 日、无满窗分位，未判定：{"、".join(insufficient)}。')
    lagged = sorted({
        indicator.name
        for heat in heats
        for layer in heat.layers
        for indicator in layer.indicators
        if indicator.asof is not None and indicator.asof < target
    })
    if lagged:
        notes.append(f'部分指标读数滞后于判定日（已在各行标注）：{"、".join(lagged)}。')
    return IndustryHeatState(asof=target, industries=tuple(heats), notes=tuple(notes))


def _evaluate_one(
    *,
    frame: pl.DataFrame,
    block: range,
    code: str,
    name: str,
    asof: date,
    percentiles: Mapping[str, np.ndarray],
) -> IndustryHeat | None:
    results_by_layer: dict[str, list[IndicatorResult]] = {key: [] for key in _SPECS_BY_LAYER}
    for spec in INDICATORS:
        percentile_array = percentiles.get(spec.column)
        if percentile_array is None:
            continue
        index = None
        for candidate in reversed(block):
            value = percentile_array[candidate]
            if not math.isnan(value):
                index = candidate
                break
        if index is None:
            continue
        raw = frame[spec.column][index]
        raw_value = None if raw is None else float(raw)
        percentile = float(percentile_array[index])
        results_by_layer[spec.layer].append(IndicatorResult(
            key=spec.key,
            name=spec.name,
            layer=spec.layer,
            raw=None if raw_value is None or math.isnan(raw_value) else raw_value,
            raw_kind=spec.raw_kind,
            percentile=percentile,
            score=(percentile - 50.0) / 50.0,
            asof=frame['date'][index],
            formula=spec.formula,
            judgment=spec.judgment,
            weight=spec.weight,
        ))

    layers: list[LayerResult] = []
    available_weight = 0.0
    for layer_spec in LAYERS:
        indicators = results_by_layer[layer_spec.key]
        if indicators:
            weight_sum = sum(item.weight for item in indicators)
            layer_score = sum(
                item.weight * (item.score or 0.0) for item in indicators
            ) / weight_sum
            available_weight += layer_spec.weight
        else:
            layer_score = None
        layers.append(LayerResult(
            key=layer_spec.key,
            name=layer_spec.name,
            weight=layer_spec.weight,
            score=layer_score,
            contribution=None,
            indicators=tuple(indicators),
            description=layer_spec.description,
        ))

    if available_weight <= 0:
        return None

    total = 0.0
    final_layers: list[LayerResult] = []
    for summary in layers:
        if summary.score is None:
            final_layers.append(summary)
            continue
        scaled = summary.weight / available_weight
        contribution = scaled * summary.score
        total += contribution
        final_layers.append(LayerResult(
            key=summary.key,
            name=summary.name,
            weight=scaled,
            score=summary.score,
            contribution=contribution,
            indicators=summary.indicators,
            description=summary.description,
        ))

    overheat, reasons = _overheat_check(results_by_layer['crowd'] + results_by_layer['energy'])
    impacts = _impacts(tuple(final_layers))
    drivers = tuple(name for impact, name in impacts if impact > 0)[:3]
    drags = tuple(name for impact, name in reversed(impacts) if impact < 0)[:3]
    notes: list[str] = []
    lagging = [item.name for layer in final_layers for item in layer.indicators
               if item.asof and item.asof < asof]
    if lagging:
        notes.append('滞后指标：' + '、'.join(sorted(set(lagging))))
    return IndustryHeat(
        code=code,
        name=name,
        asof=asof,
        total_score=total,
        label=_label_for(total),
        overheat=overheat,
        overheat_reasons=reasons,
        layers=tuple(final_layers),
        drivers=drivers,
        drags=drags,
        notes=tuple(notes),
    )


def _impacts(layers: tuple[LayerResult, ...]) -> list[tuple[float, str]]:
    """按指标对总分的贡献排序（正为拉动、负为拖累）。"""
    scored: list[tuple[float, str]] = []
    for layer in layers:
        if layer.score is None or layer.contribution is None:
            continue
        weight_sum = sum(item.weight for item in layer.indicators) or 1.0
        for item in layer.indicators:
            if item.score is None:
                continue
            impact = layer.weight * item.score * item.weight / weight_sum
            scored.append((impact, item.name))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return scored


def format_raw_value(value: float | None, kind: str) -> str:
    if value is None:
        return '—'
    if kind == 'ret':
        return f'{value * 100:+.2f}%'
    if kind == 'pct':
        return f'{value:.2f}%'
    if kind == 'ratio':
        return f'{value:.2f}x'
    if kind == 'x':
        return f'{value:.1f}'
    return f'{value:.4f}'


def format_industry_line(heat: IndustryHeat) -> str:
    """一行结论：行业、热度分、档位、主要依据与过热标记。"""
    flag = '｜过热预警：' + '；'.join(heat.overheat_reasons) if heat.overheat else ''
    parts = []
    if heat.drivers:
        parts.append('拉动：' + '、'.join(heat.drivers))
    if heat.drags:
        parts.append('拖累：' + '、'.join(heat.drags))
    basis = '｜'.join(parts) if parts else '—'
    return (
        f'{heat.name}（{heat.code}） {heat.total_score:+.3f} {heat.label}'
        f' | 主要依据：{basis}{flag}'
    )


def format_heat_detail(state: IndustryHeatState) -> str:
    """全部行业明细，供 CLI 与界面复用。"""
    lines = [
        f'行业热度状态（{state.asof}）：{len(state.industries)} 个申万一级行业'
        f' | 最热 {state.industries[0].name} {state.industries[0].total_score:+.2f}'
        f' | 最冷 {state.industries[-1].name} {state.industries[-1].total_score:+.2f}'
    ]
    for heat in state.industries:
        lines.append(format_industry_line(heat))
        layer_text = ' / '.join(
            f'{layer.name} ' + ('—' if layer.score is None else f'{layer.score:+.2f}')
            for layer in heat.layers
        )
        lines.append(f'  {layer_text}')
    for note in state.notes:
        lines.append(f'说明：{note}')
    return '\n'.join(lines)
