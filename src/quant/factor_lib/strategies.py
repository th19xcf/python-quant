#!/usr/bin/env python3

"""
公开规则化策略 —— 只用于「与因子对照」，不追求复杂。

策略来源（全部为公开论文 / 教科书规则）：
    1. 动量轮动：Jegadeesh & Titman (1993) 12-1 动量的短周期版本（60 日），
       每月按过去 60 日收益排序，取前 20% 等权持有；
    2. 双均线：Granville 均线法则 / 经典趋势跟随，MA5 上穿 MA20 持有；
    3. 布林突破：Bollinger (2001) *Bollinger on Bollinger Bands*，
       收盘价上穿上轨做多、下穿中轨平仓；
    4. 低波动选股：Ang, Hodrick, Xing & Zhang (2006) 低波动异象，
       每月按 60 日波动率升序取前 20% 等权。

执行口径（与因子分层回测完全一致）：
    - t 日收盘生成目标持仓，t+1 开盘成交；
    - 停牌（成交量 0）与开盘涨停（不可买）的标的直接剔除，不参与当期持仓；
    - 涨跌停/停牌判定复用 ``ExecutionRules.is_tradable``（传入原始价），
      不在此处重写交易所规则；
    - 成本用 ``CostModel``：卖出侧成本 × 卖出比例 + 买入侧成本 × 买入比例。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.execution_core import ExecutionRules, to_trade_date
from src.quant.factor_lib import ops as o
from src.quant.factor_lib.evaluate import cost_rates, forward_returns, rebalance_dates

C = pl.col

#: 选股比例（前/后 20%）
SELECT_FRACTION = 0.2

STRATEGY_SOURCES = {
    'momentum_rotation': 'Jegadeesh & Titman (1993) 动量效应，60 日版本',
    'dual_ma': '经典双均线趋势跟随（MA5 上穿 MA20 持有）',
    'boll_breakout': 'Bollinger (2001) 布林带突破规则',
    'low_vol_selection': 'Ang et al. (2006) 低波动异象，60 日波动率升序',
}


@dataclass(frozen=True, slots=True)
class StrategyResult:
    """策略回测结果。"""

    name: str
    total_return: float
    annual_return: float
    sharpe: float
    max_drawdown: float
    turnover: float
    periods: int
    benchmark_return: float
    source: str


def _strategy_signals(panel: pl.DataFrame) -> dict[str, pl.Expr]:
    """各策略在 t 日的「是否持有」条件（横截面口径）。"""
    momentum = o.safe_div(C('close'), o.delay(C('close'), 60)) - 1.0
    ma5 = o.ts_mean(C('close'), 5)
    ma20 = o.ts_mean(C('close'), 20)
    std20 = o.ts_std(C('close'), 20)
    upper = ma20 + 2.0 * std20
    breakout_state = (
        pl.when(C('close') < ma20).then(0.0)
        .when(C('close') > upper).then(1.0)
        .otherwise(None)
        .forward_fill()
        .over(o.GROUP)
        .fill_null(0.0)
    )
    return {
        'momentum_rotation': momentum,
        'dual_ma': pl.when(ma5 > ma20).then(1.0).otherwise(0.0),
        'boll_breakout': breakout_state,
        'low_vol_selection': -o.ts_std(C('ret'), 60),
    }


#: 按横截面排名选股的策略（前 20%）；其余为「信号 > 0 即持有」
_RANKED = {'momentum_rotation', 'low_vol_selection'}


def _targets(panel: pl.DataFrame, name: str, dates: list) -> dict:
    """计算每个调仓日的目标持仓集合。"""
    signal = _strategy_signals(panel)[name]
    # 先把信号落成真实列：polars 不允许把横截面 rank 套在带 over 的时序
    # 表达式外层（会整列静默变 null），动量/低波动这类时序信号必须分两步。
    frame = panel.with_columns(signal.alias('_signal'))
    if name in _RANKED:
        frame = frame.with_columns(
            (
                C('_signal').rank().over('date')
                / C('_signal').count().over('date')
            ).alias('_ranked')
        )
        selected = C('_ranked') >= (1.0 - SELECT_FRACTION)
    else:
        selected = C('_signal').fill_null(0.0) > 0
    frame = frame.with_columns(
        selected.fill_null(False).alias('selected')
    ).select(['date', 'ts_code', 'selected']).filter(
        pl.col('date').is_in(dates) & pl.col('selected')
    )
    result: dict = {}
    for (trade_date,), group in frame.group_by(['date'], maintain_order=True):
        result[trade_date] = set(group['ts_code'].to_list())
    return result


def _execution_context(panel: pl.DataFrame) -> dict:
    """t 日行对应的「t+1 日」成交信息（原始价 + 成交量 + 交易日）。"""
    next_row = panel.with_columns([
        C('date').shift(-1).over(o.GROUP).alias('nxt_date'),
        C('raw_open').shift(-1).over(o.GROUP).alias('nxt_open'),
        C('raw_high').shift(-1).over(o.GROUP).alias('nxt_high'),
        C('raw_low').shift(-1).over(o.GROUP).alias('nxt_low'),
        C('raw_close').shift(-1).over(o.GROUP).alias('nxt_close'),
        C('volume').shift(-1).over(o.GROUP).alias('nxt_volume'),
    ])
    context: dict = {}
    for row in next_row.iter_rows(named=True):
        context[(row['date'], row['ts_code'])] = (
            row['nxt_date'], row['raw_close'], row['nxt_open'],
            row['nxt_high'], row['nxt_low'], row['nxt_close'], row['nxt_volume'],
        )
    return context


def _tradable_names(codes: set[str], trade_date, context: dict,
                    rules: ExecutionRules) -> list[str]:
    """t+1 开盘可买入的标的（剔除停牌与开盘涨停）。"""
    tradable: list[str] = []
    for code in sorted(codes):
        info = context.get((trade_date, code))
        if info is None or info[0] is None:
            continue
        nxt_date, prev_close, open_price, high, low, close, volume = info
        if rules.is_tradable(
            'buy', prev_close=prev_close, open_price=open_price, high=high,
            low=low, close=close, volume=volume, ts_code=code,
            is_st=None, trade_date=to_trade_date(nxt_date),
        ):
            tradable.append(code)
    return tradable


def _metrics(name: str, period_returns: list[float], turnovers: list[float],
             benchmark: list[float], horizon: int = 20) -> StrategyResult:
    periods = len(period_returns)
    if periods == 0:
        return StrategyResult(name, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0,
                              STRATEGY_SOURCES[name])
    equity = np.cumprod(1.0 + np.asarray(period_returns, dtype=float))
    total = float(equity[-1] - 1.0)
    annual = float(equity[-1] ** (252.0 / (periods * horizon)) - 1.0)
    std = float(np.std(period_returns, ddof=1)) if periods > 1 else 0.0
    sharpe = (float(np.mean(period_returns) / std * np.sqrt(252.0 / horizon))
              if std > 0 else 0.0)
    peak = np.maximum.accumulate(equity)
    drawdown = float(np.min(equity / peak - 1.0))
    return StrategyResult(
        name=name,
        total_return=total,
        annual_return=annual,
        sharpe=sharpe,
        max_drawdown=drawdown,
        turnover=float(np.mean(turnovers)) if turnovers else 0.0,
        periods=periods,
        benchmark_return=float(np.prod(1.0 + np.asarray(benchmark, dtype=float)) - 1.0),
        source=STRATEGY_SOURCES[name],
    )


def run_strategy(panel: pl.DataFrame, name: str, horizon: int = 20) -> StrategyResult:
    """单个策略的组合回测（等权、T+1 开盘、成本后）。"""
    dates = rebalance_dates(panel, horizon)
    if not dates:
        raise RuntimeError('可用调仓日不足，无法回测策略')
    targets = _targets(panel, name, dates)
    # 直接从 polars 构造字典：pandas 会把 Date 列转成 Timestamp，
    # 与 trade_date（datetime.date）的键类型不一致，查表会全部落空。
    fwd = {
        (row['date'], row['ts_code']): row['fwd']
        for row in forward_returns(panel, horizon).iter_rows(named=True)
    }
    context = _execution_context(panel)
    rules = ExecutionRules()
    buy_rate, sell_rate = cost_rates()

    period_returns: list[float] = []
    turnovers: list[float] = []
    benchmark: list[float] = []
    previous: set[str] = set()

    for trade_date in dates:
        wanted = targets.get(trade_date, set())
        held = set(_tradable_names(wanted, trade_date, context, rules))
        gross_values = [fwd[(trade_date, code)] for code in held
                        if fwd.get((trade_date, code)) is not None]
        gross_values = [v for v in gross_values if np.isfinite(v)]
        gross = float(np.mean(gross_values)) if gross_values else 0.0

        sold = len(previous - held) / max(len(previous), 1) if previous else 0.0
        bought = len(held - previous) / max(len(held), 1) if held else 0.0
        cost = sell_rate * sold + buy_rate * bought
        period_returns.append(gross - cost)
        turnovers.append(max(sold, bought))
        previous = held

        bench_values = [v for (d, _c), v in fwd.items()
                        if d == trade_date and np.isfinite(v)]
        benchmark.append(float(np.mean(bench_values)) if bench_values else 0.0)

    result = _metrics(name, period_returns, turnovers, benchmark, horizon)
    logger.info(f'策略 {name} 完成：{result.periods} 期，累计 {result.total_return:.2%}')
    return result


def run_all(panel: pl.DataFrame, horizon: int = 20) -> pl.DataFrame:
    """跑全部 4 个公开策略并汇总。"""
    from dataclasses import asdict

    rows = [asdict(run_strategy(panel, name, horizon)) for name in STRATEGY_SOURCES]
    return pl.DataFrame(rows, infer_schema_length=None).sort('total_return',
                                                             descending=True)


__all__ = [
    'SELECT_FRACTION',
    'STRATEGY_SOURCES',
    'StrategyResult',
    'run_all',
    'run_strategy',
]
