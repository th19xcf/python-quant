#!/usr/bin/env python3

"""市场状态判别与仓位择时（研究模块，不接生产流程）。

纪律约定：
- 所有状态函数只使用 ``t`` 日及之前的行情/宽度数据，禁止任何全样本归一化；
- 仓位在 ``t`` 日收盘确定，``t+1`` 日生效（``executed_position`` 完成 shift）；
- 参数全部事前给定，不在任何测试窗上搜索；
- oracle（完美择时）只用于估计理论上界，不可用于实盘或参数选择。

宽度指标复用 ``src.tech_analysis.indicators.market_breadth`` 的 ABI/ADR 口径；
行情与广度数据全部只读。
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from src.utils.dates import as_date

#: 年化交易日
ANNUAL_DAYS = 252.0

#: 成本费率折算的参考名义本金（使单笔最低佣金不干扰费率）
REF_NOTIONAL = 1_000_000.0


@dataclass(frozen=True)
class RegimeParams:
    """三条极简规则的事前参数。"""

    #: R1：指数收盘同时站上短/长均线 → 满仓，否则降仓
    ma_short: int = 20
    ma_long: int = 60
    #: R1 降仓档位：0.5（半仓）或 0.0（空仓）
    low_exposure: float = 0.5
    #: R2：宽度指标滚动分位窗口与两个阈值
    breadth_metric: str = 'adr'
    breadth_period: int = 10
    breadth_window: int = 120
    breadth_low: float = 0.30
    breadth_high: float = 0.70
    #: R3：目标波动与已实现波动窗口
    target_vol: float = 0.15
    vol_window: int = 20


# ----------------------------------------------------------------------
# 数据加载（只读）
# ----------------------------------------------------------------------
def load_index_daily(
    index_code: str = '000300.SH',
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    store_root: Path | str | None = None,
) -> pl.DataFrame:
    """从本地列存读取指数日线（只读）；返回 date/close 两列。"""
    from src.data.local_store.store import LocalQuoteStore, get_local_quote_store
    from src.utils.config import get_config

    if store_root is not None:
        store = LocalQuoteStore(Path(store_root))
    else:
        store = get_local_quote_store(get_config())
    frame = store.read_file('index', index_code)
    if frame is None or frame.is_empty():
        raise FileNotFoundError(f'本地列存缺少指数 {index_code}')

    out = frame.select(
        pl.col('date').cast(pl.Date),
        pl.col('close').cast(pl.Float64),
    ).sort('date')
    if start is not None:
        out = out.filter(pl.col('date') >= _as_date(start))
    if end is not None:
        out = out.filter(pl.col('date') <= _as_date(end))
    return out


def load_breadth_snapshot(
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    store_root: Path | str | None = None,
) -> pl.DataFrame:
    """读取收盘口径广度快照（只读，不触发增量刷新）。

    与 ``breadth_service.get_breadth`` 的区别：这里直接读 parquet，
    避免研究过程写回 ``data/local_store``。
    """
    from src.data.services.breadth_service import resolve_root, snapshot_path

    path = snapshot_path(resolve_root(store_root))
    if not path.is_file():
        raise FileNotFoundError(f'广度快照不存在：{path}')
    frame = pl.read_parquet(path).sort('trade_date')
    if start is not None:
        frame = frame.filter(pl.col('trade_date') >= _as_date(start))
    if end is not None:
        frame = frame.filter(pl.col('trade_date') <= _as_date(end))
    return frame


def _as_date(value: str | date) -> date:
    """保持原有严格语义（无法解析时抛错），但 datetime 必须归一为 date。"""
    parsed = as_date(value, strict=True)
    if parsed is None:
        raise ValueError(f"无法解析为日期: {value!r}")
    return parsed


# ----------------------------------------------------------------------
# 宽度指标（复用生产口径）
# ----------------------------------------------------------------------
def breadth_value(
    breadth: pl.DataFrame,
    metric: str = 'adr',
    period: int = 10,
) -> pl.DataFrame:
    """返回 date/value 两列；ADR 为上涨均值/下跌均值×100，ABI 为涨跌家数差的占比。"""
    from src.tech_analysis.indicators.market_breadth import calculate_abi, calculate_adr

    frame = breadth.with_columns(pl.col('trade_date').alias('date')).sort('date')
    lazy = frame.lazy()
    if metric == 'adr':
        calculated = calculate_adr(lazy, period=period).collect()
        column = f'adr{period}'
    elif metric == 'abi':
        calculated = calculate_abi(lazy, period=period).collect()
        column = f'abi{period}'
    else:
        raise ValueError(f'不支持的宽度指标：{metric}')
    return calculated.select(
        pl.col('date'), pl.col(column).cast(pl.Float64).alias('value')
    ).sort('date')


def rolling_percentile(values: pl.Series, window: int) -> pl.Series:
    """因果滚动分位：``t`` 值在过去 ``window`` 个值（含 ``t``）中的百分位。

    只回看不前瞻；窗口内有缺失值时输出空值，避免用不完整样本定状态。
    """
    if window <= 0:
        raise ValueError('window 必须为正整数')
    array = values.cast(pl.Float64).to_numpy()
    out = np.full(array.shape, np.nan, dtype=float)
    for index in range(window - 1, array.size):
        sample = array[index - window + 1:index + 1]
        if np.isfinite(sample).all():
            out[index] = float(np.count_nonzero(sample <= array[index])) / float(window)
    return pl.Series(values.name, out)


# ----------------------------------------------------------------------
# 三条规则：t 日状态 → t+1 生效
# ----------------------------------------------------------------------
def ma_position(
    index_frame: pl.DataFrame,
    params: RegimeParams,
) -> pl.DataFrame:
    """R1 均线状态：收盘同时高于短/长均线 → 满仓，否则 ``low_exposure``。"""
    close = index_frame['close'].cast(pl.Float64)
    short_ma = close.rolling_mean(
        window_size=params.ma_short, min_samples=params.ma_short)
    long_ma = close.rolling_mean(
        window_size=params.ma_long, min_samples=params.ma_long)
    above = ((close > short_ma) & (close > long_ma)).fill_null(False)
    position = pl.when(above).then(pl.lit(1.0)).otherwise(
        pl.lit(float(params.low_exposure)))
    return index_frame.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position'))


def breadth_position(
    breadth: pl.DataFrame,
    params: RegimeParams,
) -> pl.DataFrame:
    """R2 宽度分位：ADR 滚动分位低于下限按比例降仓、高于上限满仓。

    中间区间线性过渡，不引入第三个参数；低分位最低仓位为 ``low_exposure``。
    """
    values = breadth_value(
        breadth, metric=params.breadth_metric, period=params.breadth_period)
    percentile = rolling_percentile(values['value'], params.breadth_window)
    span = max(params.breadth_high - params.breadth_low, 1e-9)
    scaled = ((percentile - params.breadth_low) / span).clip(0.0, 1.0)
    position = (
        float(params.low_exposure)
        + (1.0 - float(params.low_exposure)) * scaled
    ).fill_nan(1.0).fill_null(1.0)
    return values.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position'))


def vol_target_position(
    index_frame: pl.DataFrame,
    params: RegimeParams,
) -> pl.DataFrame:
    """R3 波动率目标：仓位 = min(1, 目标波动 / 20 日已实现年化波动)，下限 0。"""
    close = index_frame['close'].cast(pl.Float64)
    returns = close.pct_change()
    realized = returns.rolling_std(
        window_size=params.vol_window, min_samples=params.vol_window
    ) * np.sqrt(ANNUAL_DAYS)
    position = (float(params.target_vol) / realized).clip(0.0, 1.0) \
        .fill_nan(1.0).fill_null(1.0)
    return index_frame.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position'))


def build_rule_positions(
    index_frame: pl.DataFrame,
    breadth: pl.DataFrame,
    params: RegimeParams,
) -> dict[str, pl.DataFrame]:
    """构造全部候选规则的目标仓位（含 R1 半仓/空仓两档与基准满仓）。"""
    base = params
    return {
        'R1_half': ma_position(index_frame, base),
        'R1_empty': ma_position(index_frame, replace(base, low_exposure=0.0)),
        'R2_breadth': breadth_position(breadth, base),
        'R3_vol': vol_target_position(index_frame, base),
        'none': index_frame.select(pl.col('date')).with_columns(
            pl.lit(1.0).alias('position')),
    }


# ----------------------------------------------------------------------
# T+1 执行与成本叠加
# ----------------------------------------------------------------------
def executed_position(position: pl.DataFrame) -> pl.DataFrame:
    """把 ``t`` 日收盘决定的仓位映射为 ``t`` 日收益上的执行仓位（即 p[t-1]）。

    返回 date/exec_position/delta；``delta`` 是当天开盘调仓的仓位变化量。
    """
    frame = position.sort('date')
    frame = frame.with_columns(pl.col('position').shift(1).alias('exec_position'))
    frame = frame.with_columns(
        (pl.col('exec_position') - pl.col('exec_position').shift(1)).alias('delta'))
    return frame.select('date', 'exec_position', 'delta')


def cost_rate_frame(
    dates: list[date],
    cost_model: Any,
) -> pl.DataFrame:
    """按交易日给出买/卖单边成本费率（复用 ``CostModel`` 的历史时间表）。"""
    rows: list[tuple[date, float, float]] = []
    for day in dates:
        buy = cost_model.calculate_buy_cost(
            REF_NOTIONAL, 1.0, trade_date=day, ts_code='000300.SH')
        sell = cost_model.calculate_sell_cost(
            REF_NOTIONAL, 1.0, trade_date=day, ts_code='000300.SH')
        rows.append((day, float(buy) / REF_NOTIONAL, float(sell) / REF_NOTIONAL))
    return pl.DataFrame(
        rows, schema={'date': pl.Date, 'buy_rate': pl.Float64,
                      'sell_rate': pl.Float64}, orient='row')


def overlay_curve(
    curve: pl.DataFrame,
    position: pl.DataFrame,
    cost_model: Any,
    *,
    initial: float = 1_000_000.0,
) -> pl.DataFrame:
    """把目标仓位叠加到满仓策略净值上，并扣除调仓成本。

    ``curve`` 为满仓策略的 date/equity；``position`` 为全历史目标仓位。
    收益口径：``net_t = p[t-1] * r_t - |Δp_t| * 单边费率_t``。
    """
    executed = executed_position(position)
    joined = (
        curve.select('date', 'equity')
        .join(executed, on='date', how='left')
        .sort('date')
        .with_columns([
            pl.col('exec_position').fill_null(1.0),
            pl.col('delta').fill_null(0.0),
        ])
    )
    rates = cost_rate_frame(joined['date'].to_list(), cost_model)
    joined = joined.join(rates, on='date', how='left').with_columns([
        pl.col('buy_rate').fill_null(0.0),
        pl.col('sell_rate').fill_null(0.0),
    ])

    equity = joined['equity'].to_numpy()
    exec_position = joined['exec_position'].to_numpy()
    delta = joined['delta'].to_numpy()
    buy_rate = joined['buy_rate'].to_numpy()
    sell_rate = joined['sell_rate'].to_numpy()
    gross = np.zeros_like(equity)
    # 首行是“窗口起始现金 → 首个记录日收盘”的收益，不能丢掉，
    # 否则叠加净值与基准的逐窗口径不一致。
    if equity.size:
        with np.errstate(divide='ignore', invalid='ignore'):
            gross[0] = equity[0] / initial - 1.0
    with np.errstate(divide='ignore', invalid='ignore'):
        gross[1:] = equity[1:] / equity[:-1] - 1.0
    cost = np.where(
        delta > 0, delta * buy_rate,
        np.where(delta < 0, -delta * sell_rate, 0.0))
    net = exec_position * gross - cost
    overlay = initial * np.cumprod(1.0 + net)
    return joined.with_columns([
        pl.Series('gross_return', gross, dtype=pl.Float64),
        pl.Series('overlay_cost', cost, dtype=pl.Float64),
        pl.Series('net_return', net, dtype=pl.Float64),
        pl.Series('equity_overlay', overlay, dtype=pl.Float64),
    ]).select(
        'date', 'equity', 'exec_position', 'delta', 'overlay_cost',
        'gross_return', 'net_return', 'equity_overlay')


# ----------------------------------------------------------------------
# Oracle 理论上界
# ----------------------------------------------------------------------
def oracle_position(returns: pl.Series, horizon: int) -> pl.Series:
    """完美择时仓位：``t`` 日已知未来 ``horizon`` 天收益，正收益满仓、否则空仓。

    尾部不足 ``horizon`` 天的样本置为空值（无未来信息可依），评估时剔除。
    """
    if horizon <= 0:
        raise ValueError('horizon 必须为正整数')
    array = returns.cast(pl.Float64).to_numpy()
    out = np.full(array.shape, np.nan, dtype=float)
    for index in range(array.size - horizon):
        forward = array[index:index + horizon]
        if np.isfinite(forward).all():
            out[index] = 1.0 if float(np.nansum(forward)) > 0 else 0.0
    return pl.Series(returns.name, out)


def oracle_equity(
    curve: pl.DataFrame,
    horizon: int,
    *,
    initial: float = 1_000_000.0,
) -> pl.DataFrame:
    """用完美择时仓位重构净值（不计调仓成本，作为理论上界）。"""
    equity = curve['equity'].to_numpy()
    if equity.size < 2:
        return pl.DataFrame(schema={'date': pl.Date, 'equity': pl.Float64})
    returns = pl.Series('ret', np.diff(equity) / equity[:-1])
    position = oracle_position(returns, horizon).to_numpy()
    dates = curve['date'].to_list()
    rows: list[dict[str, Any]] = [{'date': dates[0], 'equity': initial}]
    level = initial
    for index in range(returns.len()):
        weight = position[index]
        if not np.isfinite(weight):
            break
        level *= 1.0 + float(weight) * float(returns[index])
        rows.append({'date': dates[index + 1], 'equity': level})
    return pl.DataFrame(rows, schema={'date': pl.Date, 'equity': pl.Float64})


# ----------------------------------------------------------------------
# 状态段统计（量化“状态样本极少”的过拟合风险）
# ----------------------------------------------------------------------
def position_segments(position: pl.DataFrame) -> list[dict[str, Any]]:
    """把目标仓位切成连续同状态段，返回每段起止与天数（判定用 t 日仓位）。"""
    frame = position.sort('date')
    values = frame['position'].to_numpy()
    dates = frame['date'].to_list()
    segments: list[dict[str, Any]] = []
    start = 0
    for index in range(1, len(values) + 1):
        if index == len(values) or not np.isclose(values[index], values[start]):
            segments.append({
                'start': dates[start],
                'end': dates[index - 1],
                'position': float(values[start]),
                'days': index - start,
            })
            start = index
    return segments


__all__ = [
    'ANNUAL_DAYS', 'REF_NOTIONAL', 'RegimeParams',
    'breadth_position', 'breadth_value', 'build_rule_positions',
    'cost_rate_frame', 'executed_position', 'load_breadth_snapshot',
    'load_index_daily', 'ma_position', 'oracle_equity', 'oracle_position',
    'overlay_curve', 'position_segments', 'rolling_percentile',
    'vol_target_position',
]
