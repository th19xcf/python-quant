#!/usr/bin/env python3

"""样本外评估：IC/ICIR、分层单调性、换手、成本后组合绩效。

组合模拟口径：
- 信号在 T 日收盘产生，T+1 开盘建仓（避免用当日收盘价成交）；
- 等权做多截面得分最高的 ``top_fraction``；
- 停牌（成交量缺失/为 0）与开盘涨跌停不可成交，退出受阻时最多顺延
  ``max_exit_delay`` 个交易日，之后按最新收盘价强制退出；
- 交易成本统一走项目 ``CostModel``，并按 ``adv20`` / ``total_mv_yi`` 分档计滑点
  （缺列时退回兜底档），绩效统一走 ``PerformanceAnalyzer``。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any, Dict, List, Set

import numpy as np
import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.quant.learning.config import (
    ADV_COL,
    TOTAL_MV_YI_COL,
    SchemeMetrics,
    WalkForwardConfig,
)
from src.quant.learning.labels import LABEL_COL
from src.risk.execution_rules import ExecutionRules


def _spearman(values_a: Sequence[float], values_b: Sequence[float]) -> float:
    """两序列 Spearman 相关系数（秩相关）。"""
    if len(values_a) < 2 or len(values_b) < 2:
        return 0.0
    arr_a = np.asarray(values_a, dtype=float)
    arr_b = np.asarray(values_b, dtype=float)
    rank_a = np.argsort(np.argsort(arr_a)).astype(float)
    rank_b = np.argsort(np.argsort(arr_b)).astype(float)
    if np.std(rank_a) == 0 or np.std(rank_b) == 0:
        return 0.0
    return float(np.corrcoef(rank_a, rank_b)[0, 1])


def per_date_ic(frame: pl.DataFrame, score_col: str,
                label_col: str = LABEL_COL) -> pl.DataFrame:
    """逐期横截面 Rank IC。"""
    if frame.is_empty():
        return pl.DataFrame({'date': [], 'ic': []})
    return (
        frame.filter(pl.col(score_col).is_finite() & pl.col(label_col).is_finite())
        .group_by('date')
        .agg(pl.corr(score_col, label_col, method='spearman').alias('ic'))
        .filter(pl.col('ic').is_finite())
        .sort('date')
    )


def ic_stats(frame: pl.DataFrame, score_col: str,
             label_col: str = LABEL_COL) -> Dict[str, float]:
    """IC 均值、标准差、ICIR、正比率与 t 统计量。"""
    ic_frame = per_date_ic(frame, score_col, label_col)
    if ic_frame.is_empty():
        return {'ic_mean': 0.0, 'ic_std': 0.0, 'icir': 0.0,
                'ic_positive_ratio': 0.0, 'ic_t_stat': 0.0, 'n_periods': 0.0}
    values = ic_frame['ic'].to_numpy().astype(float)
    mean = float(values.mean())
    std = float(values.std(ddof=1)) if len(values) > 1 else 0.0
    icir = mean / std if std > 0 else 0.0
    t_stat = mean / (std / np.sqrt(len(values))) if std > 0 else 0.0
    return {
        'ic_mean': mean,
        'ic_std': std,
        'icir': float(icir),
        'ic_positive_ratio': float((values > 0).mean()),
        'ic_t_stat': float(t_stat),
        'n_periods': float(len(values)),
    }


def quantile_analysis(frame: pl.DataFrame, score_col: str,
                      n_quantiles: int = 5,
                      label_col: str = LABEL_COL) -> tuple[List[float], float, float]:
    """分层平均收益、单调性（分层序号与收益的秩相关）与多空收益。"""
    valid = frame.filter(
        pl.col(score_col).is_finite() & pl.col(label_col).is_finite()).drop_nulls(
            [score_col, label_col])
    if valid.is_empty():
        return [], 0.0, 0.0
    ranked = valid.with_columns([
        pl.col(score_col).rank('average').over('date').alias('__rank'),
        pl.col(score_col).count().over('date').alias('__count'),
    ]).with_columns([
        (((pl.col('__rank') - 1) * n_quantiles / pl.col('__count'))
         .floor().cast(pl.Int64).clip(0, n_quantiles - 1)).alias('__q'),
    ])
    per_date = ranked.group_by(['date', '__q']).agg(
        pl.col(label_col).mean().alias('ret'))
    grouped = (per_date.group_by('__q').agg(pl.col('ret').mean())
               .sort('__q'))
    if grouped.height == 0:
        return [], 0.0, 0.0
    means = grouped['ret'].to_numpy().astype(float).tolist()
    quantiles = grouped['__q'].to_numpy().astype(int).tolist()
    monotonicity = _spearman(quantiles, means)
    long_short = float(means[-1] - means[0]) if len(means) >= 2 else 0.0
    return [float(value) for value in means], float(monotonicity), long_short


def _cross_sections(frame: pl.DataFrame) -> Dict[date, pl.DataFrame]:
    """一次性按交易日分区，避免在调仓循环里反复全表过滤。"""
    parts = frame.partition_by('date', as_dict=True)
    return {key[0]: value for key, value in parts.items()}


def selection_sets(frame: pl.DataFrame, score_col: str, top_fraction: float,
                   rebalance: int) -> List[Set[str]]:
    """按 rebalance 间隔取调仓日的头部标的集合（用于换手计算）。"""
    if frame.is_empty():
        return []
    valid = frame.filter(pl.col(score_col).is_finite())
    sections = _cross_sections(valid)
    signal_dates = sorted(sections)[::max(rebalance, 1)]
    sets: List[Set[str]] = []
    for signal_date in signal_dates:
        cross = sections[signal_date]
        count = cross.height
        if count == 0:
            continue
        size = max(1, int(round(count * top_fraction)))
        ranked = cross.sort(score_col, descending=True).head(size)
        sets.append(set(ranked['ts_code'].to_list()))
    return sets


def turnover_from_sets(sets: Sequence[Set[str]]) -> float:
    """平均单边换手率：``1 - 与上期重合数 / 本期持仓数``。"""
    if len(sets) <= 1:
        return 0.0
    values: List[float] = []
    for previous, current in zip(sets[:-1], sets[1:], strict=True):
        if not current:
            continue
        overlap = len(previous & current)
        values.append(1.0 - overlap / float(len(current)))
    return float(np.mean(values)) if values else 0.0


def _build_bars(panel: pl.DataFrame, codes: Set[str],
                start: date, end: date) -> Dict[str, Dict[date, Dict[str, Any]]]:
    """构造 code -> date -> bar 的查询表，仅覆盖评估区间。"""
    columns = ['ts_code', 'date', 'open', 'high', 'low', 'close', 'pre_close',
               'volume', 'open_hfq', 'close_hfq', ADV_COL, TOTAL_MV_YI_COL]
    # 分档滑点输入是可选列：早期面板（或只用行情构造的测试面板）没有时
    # 不建占位列，成本退回 CostModel 的兜底档。
    columns = [col for col in columns if col in panel.columns]
    sub = panel.filter(
        pl.col('ts_code').is_in(list(codes))
        & (pl.col('date') >= start)
        & (pl.col('date') <= end)
    ).select(columns)
    bars: Dict[str, Dict[date, Dict[str, Any]]] = {}
    for row in sub.iter_rows(named=True):
        bars.setdefault(row['ts_code'], {})[row['date']] = row
    return bars


def _empty_result() -> Dict[str, Any]:
    """无有效样本时的空结果，字段与正常返回保持一致。"""
    return {'equity_curve': [], 'turnover': 0.0, 'performance': {},
            'trades': 0, 'total_cost': 0.0, 'delayed_exits': 0,
            'forced_exits': 0, 'null_cap_trades': 0, 'slippage_bp_counts': {}}


def simulate_long_only(  # noqa: C901 - 事件循环共享可变状态，见下方说明
    panel: pl.DataFrame,
    score_frame: pl.DataFrame,
    config: WalkForwardConfig,
    score_col: str = 'score',
    initial_capital: float = 1_000_000.0,
    max_exit_delay: int = 5,
    cost_model: CostModel | None = None,
) -> Dict[str, Any]:
    """样本外等权多头组合模拟，返回权益曲线、换手与绩效。

    成本走 ``CostModel`` 的分档滑点：若面板带 ``adv20`` / ``total_mv_yi``
    （见 ``data.attach_cost_inputs``），按标的实际档位计费；缺列或该日为空
    时退回兜底档（个股小盘 30bp）。

    事件循环同时维护「信号 → T+1 建仓 → 持有期退出 → 盯市」四段状态，
    拆分后仍需共享可变状态；mccabe 会把闭包复杂度计入父函数，故此处豁免。
    """
    if score_frame.is_empty():
        return _empty_result()

    valid = score_frame.filter(pl.col(score_col).is_finite())
    sections = _cross_sections(valid)
    signal_dates = sorted(sections)
    if not signal_dates:
        return _empty_result()

    calendar = panel['date'].unique().sort().to_list()
    calendar_index = {day: i for i, day in enumerate(calendar)}
    rebalance = max(int(config.horizon), 1)
    signal_dates = signal_dates[::rebalance]
    selection_map: Dict[date, List[str]] = {}
    for signal_date in signal_dates:
        cross = sections[signal_date]
        if cross.is_empty():
            continue
        size = max(1, int(round(cross.height * config.top_fraction)))
        ranked = cross.sort(score_col, descending=True).head(size)
        selection_map[signal_date] = ranked['ts_code'].to_list()

    signal_set = set(signal_dates)
    selected_codes = {code for codes in selection_map.values() for code in codes}
    last_signal = signal_dates[-1]
    end_index = min(calendar_index[last_signal] + config.horizon + max_exit_delay + 1,
                    len(calendar) - 1)
    bars = _build_bars(panel, selected_codes, signal_dates[0], calendar[end_index])

    rules = ExecutionRules()
    costs = cost_model if cost_model is not None else CostModel()
    cash = float(initial_capital)
    holdings: Dict[str, Dict[str, Any]] = {}
    last_close: Dict[str, float] = {}
    equity_curve: List[Dict[str, Any]] = []
    pending_entry: List[str] = []
    trade_count = 0
    total_cost = 0.0
    delayed_exits = 0
    forced_exits = 0
    null_cap_trades = 0
    slippage_bp_counts: Dict[int, int] = {}

    def tradable(code: str, day: date, side: str) -> bool:
        bar = bars.get(code, {}).get(day)
        if bar is None:
            return False
        if bar.get('open') is None or bar.get('open_hfq') is None:
            return False
        return rules.is_tradable(
            side,
            prev_close=bar.get('pre_close'),
            open_price=bar.get('open'),
            high=bar.get('high'),
            low=bar.get('low'),
            close=bar.get('close'),
            volume=bar.get('volume'),
            ts_code=code,
            trade_date=day,
        )

    def price_of(code: str, day: date, column: str) -> float | None:
        bar = bars.get(code, {}).get(day)
        if bar is None:
            return None
        value = bar.get(column)
        return float(value) if value is not None else None

    def cost_of(code: str, day: date, shares: float, price: float,
                side: str) -> float:
        """按标的当日可得的 t-1 分档输入计费（纯函数，不记账）。"""
        bar = bars.get(code, {}).get(day) or {}
        calculator = (costs.calculate_buy_cost if side == 'buy'
                      else costs.calculate_sell_cost)
        return calculator(shares, price, day, code,
                          adv20=bar.get(ADV_COL),
                          total_mv_yi=bar.get(TOTAL_MV_YI_COL))

    def record_trade(code: str, day: date, fee: float) -> None:
        """只在成交确认后记账：累计成本、滑点档分布与市值缺失笔数（审计用）。"""
        nonlocal total_cost, null_cap_trades
        total_cost += fee
        bar = bars.get(code, {}).get(day) or {}
        if bar.get(TOTAL_MV_YI_COL) is None:
            null_cap_trades += 1
        bp = int(round(costs.slippage_rate(
            code, bar.get(ADV_COL), bar.get(TOTAL_MV_YI_COL)) * 1e4))
        slippage_bp_counts[bp] = slippage_bp_counts.get(bp, 0) + 1

    def settle_exits(day_index: int, day: date) -> None:
        """开盘处理到期退出；受阻则顺延，超期按收盘价强制退出。"""
        nonlocal cash, trade_count, delayed_exits, forced_exits
        for code in list(holdings.keys()):
            item = holdings[code]
            if day_index < item['exit_index']:
                continue
            executed = False
            if tradable(code, day, 'sell'):
                price = price_of(code, day, 'open_hfq')
                if price is not None and price > 0:
                    shares = item['shares']
                    fee = cost_of(code, day, shares, price, 'sell')
                    cash += shares * price - fee
                    record_trade(code, day, fee)
                    trade_count += 1
                    executed = True
            if executed:
                last_close.pop(code, None)
                holdings.pop(code, None)
            elif day_index >= item['exit_index'] + max_exit_delay:
                price = price_of(code, day, 'close_hfq') or last_close.get(code)
                if price is not None and price > 0:
                    shares = item['shares']
                    fee = cost_of(code, day, shares, price, 'sell')
                    cash += shares * price - fee
                    record_trade(code, day, fee)
                    trade_count += 1
                    forced_exits += 1
                    last_close.pop(code, None)
                    holdings.pop(code, None)
            else:
                delayed_exits += 1

    def open_entries(day_index: int, day: date) -> None:
        """以前一日收盘信号在本日开盘建仓（T+1），现金等权分配。"""
        nonlocal cash, trade_count
        entry_codes = [code for code in pending_entry if tradable(code, day, 'buy')]
        if not entry_codes:
            return
        allocation = cash / float(len(entry_codes))
        for code in entry_codes:
            price = price_of(code, day, 'open_hfq')
            if price is None or price <= 0:
                continue
            shares = allocation / price
            total = shares * price
            fee = cost_of(code, day, shares, price, 'buy')
            if total + fee > cash:
                continue
            cash -= total + fee
            record_trade(code, day, fee)
            holdings[code] = {
                'shares': shares,
                'entry_index': day_index,
                'exit_index': day_index + rebalance,
            }
            trade_count += 1
            last_close[code] = price

    start_index = calendar_index[signal_dates[0]]
    for day_index in range(start_index, end_index + 1):
        day = calendar[day_index]

        # 1) 开盘先处理到期退出
        settle_exits(day_index, day)

        # 2) 再处理前一日信号对应的 T+1 开盘建仓
        if pending_entry:
            open_entries(day_index, day)
            pending_entry = []

        # 3) 收盘盯市
        for code in holdings:
            price = price_of(code, day, 'close_hfq')
            if price and price > 0:
                last_close[code] = price
        market_value = sum(item['shares'] * last_close.get(code, 0.0)
                           for code, item in holdings.items())
        equity_curve.append({'date': day, 'equity': float(cash + market_value)})

        # 4) 收盘产生信号，下一交易日开盘执行
        if day in signal_set:
            pending_entry = list(selection_map.get(day, []))

    sets = [set(codes) for codes in selection_map.values()]
    performance = PerformanceAnalyzer().analyze(equity_curve, [], frequency='daily')
    return {
        'equity_curve': equity_curve,
        'turnover': turnover_from_sets(sets),
        'performance': performance,
        'trades': trade_count,
        'total_cost': total_cost,
        'delayed_exits': delayed_exits,
        'forced_exits': forced_exits,
        'null_cap_trades': null_cap_trades,
        'slippage_bp_counts': slippage_bp_counts,
    }


def evaluate_scheme(name: str, frame: pl.DataFrame, panel: pl.DataFrame,
                    config: WalkForwardConfig, score_col: str = 'score',
                    notes: str = '') -> SchemeMetrics:
    """对一个样本外打分公司计算全套指标。"""
    if frame.is_empty():
        return SchemeMetrics(name=name, notes=notes or '无有效样本')
    stats = ic_stats(frame, score_col)
    quantiles, monotonicity, long_short = quantile_analysis(
        frame, score_col, config.n_quantiles)
    simulation = simulate_long_only(panel, frame, config, score_col)
    performance = simulation['performance']
    result = SchemeMetrics(
        name=name,
        ic_mean=stats['ic_mean'],
        ic_std=stats['ic_std'],
        icir=stats['icir'],
        ic_positive_ratio=stats['ic_positive_ratio'],
        ic_t_stat=stats['ic_t_stat'],
        quantile_returns=quantiles,
        long_short_return=long_short,
        monotonicity=monotonicity,
        turnover=float(simulation['turnover']),
        annual_return=float(performance.get('annual_return', 0.0)),
        max_drawdown=float(performance.get('max_drawdown', 0.0)),
        sharpe_ratio=float(performance.get('sharpe_ratio', 0.0)),
        total_return=float(performance.get('total_return', 0.0)),
        n_periods=int(stats['n_periods']),
        notes=notes,
    )
    logger.debug(
        f'方案 {name}: IC={result.ic_mean:.4f}, ICIR={result.icir:.3f}, '
        f'年化={result.annual_return:.2f}%, 回撤={result.max_drawdown:.2f}%')
    return result
