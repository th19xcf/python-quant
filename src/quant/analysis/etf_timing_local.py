"""ETF 仓位择时 CLI：读本地数据算当前目标仓位、四只 ETF 权重与金额。

用法：
    python -m src.quant.analysis.etf_timing_local [--asof 2026-11-13] [--capital 1000000]

口径见 ``src/quant/analysis/etf_timing_state.py`` 与
``docs/ETF仓位择时方法_2026-11-17.md``；本模块只读本地数据，不写 ``data/``。
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np

from src.data.trading_calendar import next_trading_day
from src.quant.analysis import analysis_cache
from src.quant.analysis.etf_timing_state import (
    ETF_NAMES,
    allocation_rows,
    level_label,
    level_position,
    next_thresholds,
    run_levels,
    sleeve_allocation,
    sleeve_slots,
)
from src.quant.analysis.market_trend_state import (
    INDICATORS,
    LAYERS,
    _prepare_frame,
    _rolling_percentile,
    _to_float_array,
)

DEFAULT_CAPITAL = 1_000_000.0


@dataclass(frozen=True)
class EtfTimingHolding:
    code: str
    name: str
    role: str
    sleeve_weight: float
    position_weight: float
    amount: float


@dataclass(frozen=True)
class EtfTimingState:
    asof: date
    score: float
    level: int
    label: str
    target_position: float
    execution_date: date | None
    holdings: tuple[EtfTimingHolding, ...]
    cash_weight: float
    up_threshold: float | None
    down_threshold: float | None
    style_text: str
    notes: tuple[str, ...]


def _ffill(values: np.ndarray) -> np.ndarray:
    """与 evaluate_market_trend 的 asof 取「最近非空」等价的前向填充。"""
    out = values.copy()
    last = np.nan
    for index in range(out.size):
        if not np.isnan(out[index]):
            last = out[index]
        elif not np.isnan(last):
            out[index] = last
    return out


def daily_total_scores(
    root: Path | str | None = None,
) -> tuple[list[date], np.ndarray]:
    """复用市场趋势指标定义，逐日复算五层得分与总分。"""
    from src.quant.analysis.market_trend_local import load_local_panel

    panel = load_local_panel(root) if root is not None else load_local_panel()
    frame = _prepare_frame(panel)
    dates = list(frame['date'].to_list())

    layer_scores: dict[str, np.ndarray] = {}
    for layer in LAYERS:
        specs = [s for s in INDICATORS if s.layer == layer.key and s.column in frame.columns]
        numerator = np.zeros(len(dates))
        denominator = np.zeros(len(dates))
        for spec in specs:
            raw = _to_float_array(frame, spec.column)
            percentile = raw.copy() if spec.mode == 'quantile' else _rolling_percentile(raw)
            percentile = _ffill(percentile)
            score = spec.direction * (percentile - 50.0) / 50.0
            valid = ~np.isnan(score)
            numerator[valid] += spec.weight * score[valid]
            denominator[valid] += spec.weight
        with np.errstate(invalid='ignore', divide='ignore'):
            layer_scores[layer.key] = np.where(denominator > 0, numerator / denominator, np.nan)

    total = np.zeros(len(dates))
    available = np.zeros(len(dates))
    for layer in LAYERS:
        values = layer_scores[layer.key]
        valid = ~np.isnan(values)
        total[valid] += layer.weight * values[valid]
        available[valid] += layer.weight
    with np.errstate(invalid='ignore', divide='ignore'):
        total = np.where(available > 0, total / available, np.nan)
    return dates, total


def _load_style(asof: date, refresh: bool):
    from src.quant.analysis.style_rotation_local import (
        load_local_panel as load_style_panel,
    )
    from src.quant.analysis.style_rotation_state import evaluate_style_rotation

    result = analysis_cache.cached(
        'style_rotation', 'state', asof=asof.isoformat(),
        fingerprint={'panel_end': analysis_cache.panel_end()},
        compute=lambda: evaluate_style_rotation(load_style_panel(), asof=asof),
        refresh=refresh,
    )
    return result.value


def _build_state(
    dates: list[date],
    scores: np.ndarray,
    asof: date | None,
    capital: float,
    refresh: bool,
) -> EtfTimingState:
    valid_indexes = [i for i, value in enumerate(scores) if not np.isnan(value)]
    if not valid_indexes:
        raise ValueError('趋势总分序列为空，无法判定')
    if asof is None:
        target_index = valid_indexes[-1]
    else:
        eligible = [i for i in valid_indexes if dates[i] <= asof]
        if not eligible:
            raise ValueError(f'asof={asof} 早于首个有效总分 {dates[valid_indexes[0]]}')
        target_index = eligible[-1]

    signal_date = dates[target_index]
    level = run_levels([float(v) for v in scores[: target_index + 1]])[-1]
    score = float(scores[target_index])
    position = level_position(level)

    style = _load_style(signal_date, refresh)
    scores_by_key = {dimension.key: dimension.score for dimension in style.dimensions}
    slots = sleeve_slots(
        scores_by_key.get('size'), scores_by_key.get('value'), scores_by_key.get('vol'))
    allocation = sleeve_allocation(slots)

    holdings: list[EtfTimingHolding] = []
    for code, role, tag in allocation_rows(slots):
        sleeve_weight = allocation.get(code, 0.0)
        position_weight = position * sleeve_weight
        holdings.append(EtfTimingHolding(
            code=code,
            name=ETF_NAMES.get(code, code),
            role=f'{role}·{tag}',
            sleeve_weight=sleeve_weight,
            position_weight=position_weight,
            amount=round(capital * position_weight, 2),
        ))

    upcoming = next_trading_day(signal_date)
    up, down = next_thresholds(level)
    style_text = '；'.join(
        f'{dimension.name} {dimension.score:+.3f}（{dimension.label}）'
        for dimension in style.dimensions
    )
    notes = [
        '信号为 T 日收盘口径，交易按 T+1 开盘执行。',
        '风格槽只调整权益内部结构，不改变总仓位。',
        '本工具只做仓位与 ETF 选择，不含个股、账户与下单。',
    ]
    return EtfTimingState(
        asof=signal_date,
        score=score,
        level=level,
        label=level_label(level),
        target_position=position,
        execution_date=upcoming,
        holdings=tuple(holdings),
        cash_weight=round(1.0 - position, 10),
        up_threshold=up,
        down_threshold=down,
        style_text=style_text,
        notes=tuple(notes),
    )


def compute_state(
    asof: date | None = None,
    capital: float = DEFAULT_CAPITAL,
    refresh: bool = False,
) -> EtfTimingState:
    """当前（或指定日）目标仓位与 ETF 分配；带落盘缓存。"""
    result = analysis_cache.cached(
        'etf_timing', 'state',
        asof=asof.isoformat() if asof else None,
        fingerprint={'panel_end': analysis_cache.panel_end(), 'capital': capital,
                     'schema': 2},
        compute=lambda: _build_state(*daily_total_scores(), asof=asof,
                                     capital=capital, refresh=refresh),
        refresh=refresh,
    )
    return result.value


def _format_threshold(value: float | None) -> str:
    return '—' if value is None else f'{value:+.2f}'


def run_cli(asof: str | None, capital: float) -> int:
    from loguru import logger

    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = compute_state(asof=target, capital=capital)
    execution = state.execution_date.isoformat() if state.execution_date else '下一交易日待定'

    lines = [
        f'ETF 仓位择时（信号日 {state.asof} 收盘，执行日 {execution} 开盘）',
        f'趋势总分 {state.score:+.3f} -> {state.label} -> 目标仓位 {state.target_position:.0%}',
        f'风格：{state.style_text}',
        f'{"代码":<10}{"名称":<12}{"角色":<12}{"权益内":>8}{"占本金":>8}{"金额":>12}',
    ]
    for holding in state.holdings:
        lines.append(
            f'{holding.code:<10}{holding.name:<12}{holding.role:<12}'
            f'{holding.sleeve_weight:>8.0%}{holding.position_weight:>8.2%}'
            f'{holding.amount:>12,.0f}')
    lines.append(
        f'{"现金":<10}{"":<12}{"":<12}{"":>8}{state.cash_weight:>8.0%}'
        f'{capital * state.cash_weight:>12,.0f}')
    lines.append(
        f'下一档：加仓需总分 >= {_format_threshold(state.up_threshold)}；'
        f'减仓需总分 <= {_format_threshold(state.down_threshold)}')
    lines.extend(f'注：{note}' for note in state.notes)
    for line in lines:
        logger.info(line)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='ETF 仓位择时（只读本地数据）')
    parser.add_argument('--asof', help='信号截止日 YYYY-MM-DD，默认最新')
    parser.add_argument('--capital', type=float, default=DEFAULT_CAPITAL, help='本金，默认 100 万')
    args = parser.parse_args(argv)
    return run_cli(args.asof, args.capital)


if __name__ == '__main__':
    raise SystemExit(main())
