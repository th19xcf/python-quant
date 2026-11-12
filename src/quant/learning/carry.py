#!/usr/bin/env python3

"""持仓承接（carry-over）组合模拟：只交易目标名单与现有持仓的差额。

本模块只在 ``simulate_long_only(..., carry=CarryConfig(...))`` 时启用；
``carry=None`` 仍走 ``evaluator.simulate_long_only`` 原有的整仓清仓路径，
已发布数字不受影响。承接模式的口径：

- 信号 T 日收盘产生，T+1 开盘调仓；
- 目标名单 = 当期得分前 N（``N = round(截面只数 × top_fraction)``），可选缓冲带；
- 上期持仓中仍入选者按 ``convergence``（λ）向目标等权收敛，``weight_mode='keep'``
  时原样保留、只交易剔除与新增；
- 不入选者 T+1 开盘一次卖完，跌停/停牌/无价时沿用 ``max_exit_delay`` 顺延，
  超期按最新收盘价强制退出；
- 新进标的按目标权重全额买入；买入额超出现金时按排名顺序尽力成交，余额留现金
  （``invest_residual=True`` 时在调仓末尾按等权把余额继续买入，作为强制满仓对照）；
- 停牌（无 bar / 成交量 0）与开盘涨跌停沿用 ``ExecutionRules``：买入受阻本期放弃、
  下期再收敛；减仓受阻本期跳过；退出受阻进入顺延队列。

成本一律按实际成交名义额走 ``CostModel``，与整仓清仓模式同源。

与 ``src/backtest/engine/execution_core.py`` 的关系（务必按字面理解）：
本模块的 ``_CarryEngine``（``simulate_carry``）是**独立实现的逐日循环**，
**不复用** execution_core 的 T+1 建仓时点、分档滑点、整手取整与
``max_exit_delay`` 退出顺延口径；整手由本模块 ``_lot_floor`` 自行计算。
两侧是两套独立循环，互不跟随，修改任何一侧的成本、时点或整手语义时
必须同步核对另一侧，不能假设等价。2026-11-11 ``a30ce18`` 起，
``evaluator.simulate_long_only`` 在默认 ``carry=DEFAULT_CARRY`` 时
直接委托本模块，即学习侧默认路径就是 ``_CarryEngine``。
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Sequence, Set

import numpy as np
import polars as pl

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.quant.learning.config import (
    ADV_COL,
    TOTAL_MV_YI_COL,
    CarryConfig,
    WalkForwardConfig,
)
from src.risk.execution_rules import ExecutionRules


def _select_targets(ranking: Sequence[str], size: int,
                    holdings: Dict[str, Any],
                    buffer_fraction: float) -> List[str]:
    """按缓冲带规则选出本期目标名单，只数恒为 ``size``。

    无缓冲带时就是得分前 ``size``。有缓冲带时，持仓排名在前
    ``size×(1+buffer)`` 内可继续保留（按排名截断到 ``size`` 只），
    不足部分从非持仓的最高排名补足。
    """
    if size <= 0:
        return []
    if buffer_fraction <= 0:
        return list(ranking[:size])
    limit = min(len(ranking), max(size, int(round(size * (1.0 + buffer_fraction)))))
    retained = [code for code in ranking[:limit] if code in holdings][:size]
    if len(retained) >= size:
        return retained
    retained_set = set(retained)
    picks = retained + [code for code in ranking if code not in retained_set][
        :size - len(retained)]
    return picks


def _lot_floor(shares: float, lot_size: int | None) -> float:
    """按最小交易单位向下取整；``None`` 表示小数股，原样返回。"""
    if lot_size is None or shares <= 0:
        return shares
    return float(int(shares / lot_size) * lot_size)


class _CarryEngine:
    """承接模式的事件循环与账本；状态全部集中在这里，便于逐项自证。"""

    #: 正常持仓不设到期日：承接模式下只有「被剔除且受阻」才会排队退出。
    HOLD_FOREVER = 10 ** 9

    def __init__(self, bars: Dict[str, Dict[date, Dict[str, Any]]],
                 carry: CarryConfig, initial_capital: float,
                 max_exit_delay: int, cost_model: CostModel | None) -> None:
        self.bars = bars
        self.carry = carry
        self.max_exit_delay = max_exit_delay
        self.rules = ExecutionRules()
        self.costs = cost_model if cost_model is not None else CostModel()
        #: 仅用于成交前的仓位试算：同参数克隆，不写入逐笔记账。
        self.estimator = CostModel(self.costs.params)
        self.cash = float(initial_capital)
        self.holdings: Dict[str, Dict[str, Any]] = {}
        self.last_close: Dict[str, float] = {}
        self.equity_curve: List[Dict[str, Any]] = []
        self.periods: List[Dict[str, Any]] = []
        self.sets: List[Set[str]] = []
        self.trade_count = 0
        self.total_cost = 0.0
        self.delayed_exits = 0
        self.forced_exits = 0
        self.null_cap_trades = 0
        self.slippage_bp_counts: Dict[int, int] = {}

    # ------------------------------------------------------------------
    # 行情与计费
    # ------------------------------------------------------------------

    def tradable(self, code: str, day: date, side: str) -> bool:
        bar = self.bars.get(code, {}).get(day)
        if bar is None:
            return False
        if bar.get('open') is None or bar.get('open_hfq') is None:
            return False
        return self.rules.is_tradable(
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

    def price_of(self, code: str, day: date, column: str) -> float | None:
        bar = self.bars.get(code, {}).get(day)
        if bar is None:
            return None
        value = bar.get(column)
        return float(value) if value is not None else None

    def cost_of(self, code: str, day: date, shares: float, price: float,
                side: str) -> float:
        bar = self.bars.get(code, {}).get(day) or {}
        calculator = (self.costs.calculate_buy_cost if side == 'buy'
                      else self.costs.calculate_sell_cost)
        return calculator(shares, price, day, code,
                          adv20=bar.get(ADV_COL),
                          total_mv_yi=bar.get(TOTAL_MV_YI_COL))

    def record_trade(self, code: str, day: date, fee: float) -> None:
        self.total_cost += fee
        bar = self.bars.get(code, {}).get(day) or {}
        if bar.get(TOTAL_MV_YI_COL) is None:
            self.null_cap_trades += 1
        bp = int(round(self.costs.slippage_rate(
            code, bar.get(ADV_COL), bar.get(TOTAL_MV_YI_COL)) * 1e4))
        self.slippage_bp_counts[bp] = self.slippage_bp_counts.get(bp, 0) + 1

    def valuation_price(self, code: str, day: date) -> float | None:
        """执行日开盘估值价；当日无行情时退回最近收盘价。"""
        price = self.price_of(code, day, 'open_hfq')
        if price is None or price <= 0:
            price = self.last_close.get(code)
        return price

    # ------------------------------------------------------------------
    # 买卖执行
    # ------------------------------------------------------------------

    def _buy(self, code: str, day: date, day_index: int,
             want_value: float) -> float:
        """按目标金额买入；返回成交名义额（未成交返回 0）。

        仓位试算用同参数克隆的 ``estimator``，只有真正成交才调用 ``costs``
        记账，因此外部 RecordingCostModel 能逐笔对账。
        """
        if want_value <= 0:
            return 0.0
        price = self.price_of(code, day, 'open_hfq')
        if price is None or price <= 0 or not self.tradable(code, day, 'buy'):
            return 0.0
        bar = self.bars.get(code, {}).get(day) or {}
        budget = min(want_value, self.cash)
        shares = _lot_floor(budget / price, self.carry.lot_size)
        for _ in range(4):
            if shares <= 0:
                return 0.0
            fee = self.estimator.calculate_buy_cost(
                shares, price, day, code,
                adv20=bar.get(ADV_COL), total_mv_yi=bar.get(TOTAL_MV_YI_COL))
            total = shares * price + fee
            if total <= self.cash:
                break
            shares = _lot_floor(shares * (self.cash / total) * 0.999,
                                self.carry.lot_size)
        else:
            return 0.0

        fee = self.cost_of(code, day, shares, price, 'buy')
        total = shares * price + fee
        if total > self.cash:
            return 0.0
        self.cash -= total
        self.record_trade(code, day, fee)
        self.trade_count += 1
        item = self.holdings.get(code)
        if item is None:
            self.holdings[code] = {
                'shares': shares,
                'entry_index': day_index,
                'exit_index': self.HOLD_FOREVER,
            }
        else:
            item['shares'] += shares
            item['exit_index'] = self.HOLD_FOREVER
        self.last_close[code] = price
        return shares * price

    def _sell(self, code: str, day: date, day_index: int, shares: float,
              queue_on_block: bool) -> float:
        """卖出指定股数；返回成交名义额。

        ``queue_on_block=True`` 用于整笔剔除：卖不掉时把 ``exit_index`` 设为当日，
        由 ``settle_exits`` 逐日重试并强退；部分减仓受阻只跳过本期，不排队。
        """
        item = self.holdings.get(code)
        if item is None:
            return 0.0
        shares = min(float(shares), float(item['shares']))
        price = self.price_of(code, day, 'open_hfq')
        if (shares <= 0 or price is None or price <= 0
                or not self.tradable(code, day, 'sell')):
            if queue_on_block:
                item['exit_index'] = day_index
            return 0.0
        fee = self.cost_of(code, day, shares, price, 'sell')
        self.cash += shares * price - fee
        self.record_trade(code, day, fee)
        self.trade_count += 1
        remaining = item['shares'] - shares
        if remaining <= 1e-12:
            self.last_close.pop(code, None)
            self.holdings.pop(code, None)
        else:
            item['shares'] = remaining
        return shares * price

    def settle_exits(self, day_index: int, day: date) -> None:
        """处理排队中的退出：开盘卖不掉则顺延，超期按收盘价强退。"""
        for code in list(self.holdings):
            item = self.holdings[code]
            if day_index < item['exit_index']:
                continue
            price = self.price_of(code, day, 'open_hfq')
            if (price is not None and price > 0
                    and self.tradable(code, day, 'sell')):
                shares = item['shares']
                fee = self.cost_of(code, day, shares, price, 'sell')
                self.cash += shares * price - fee
                self.record_trade(code, day, fee)
                self.trade_count += 1
                self.last_close.pop(code, None)
                self.holdings.pop(code, None)
                continue
            if day_index >= item['exit_index'] + self.max_exit_delay:
                price = (self.price_of(code, day, 'close_hfq')
                         or self.last_close.get(code))
                if price is not None and price > 0:
                    shares = item['shares']
                    fee = self.cost_of(code, day, shares, price, 'sell')
                    self.cash += shares * price - fee
                    self.record_trade(code, day, fee)
                    self.trade_count += 1
                    self.forced_exits += 1
                    self.last_close.pop(code, None)
                    self.holdings.pop(code, None)
            else:
                self.delayed_exits += 1

    # ------------------------------------------------------------------
    # 调仓
    # ------------------------------------------------------------------

    def _exit_dropped(self, target_set: Set[str], day: date,
                      day_index: int) -> float:
        """剔除者开盘一次卖完；仍入选者清除退出队列。返回卖出名义额。"""
        sell_notional = 0.0
        for code in list(self.holdings):
            if code in target_set:
                self.holdings[code]['exit_index'] = self.HOLD_FOREVER
                continue
            sell_notional += self._sell(
                code, day, day_index, self.holdings[code]['shares'],
                queue_on_block=True)
        return sell_notional

    def _trim_overweight(self, target_codes: Sequence[str],
                         target_value: float, desired: Dict[str, float],
                         day: date, day_index: int) -> float:
        """目标模式下先卖出超配，为后续买入腾出现金。"""
        sell_notional = 0.0
        for code in target_codes:
            item = self.holdings.get(code)
            if item is None or code not in desired:
                continue
            price = self.valuation_price(code, day)
            if price is None or price <= 0:
                continue
            reduce_value = item['shares'] * price - desired[code]
            if reduce_value < self.carry.tolerance * target_value:
                continue
            shares = _lot_floor(reduce_value / price, self.carry.lot_size)
            if shares <= 0:
                continue
            sell_notional += self._sell(
                code, day, day_index, shares, queue_on_block=False)
        return sell_notional

    def _buy_underweight(self, codes: Sequence[str], target_set: Set[str],
                         target_value: float, desired: Dict[str, float],
                         day: date, day_index: int) -> float:
        """按排名买入新进与低配；``desired`` 为空即 keep 模式，只买新进。"""
        buy_notional = 0.0
        for code in codes:
            if code not in target_set:
                continue
            item = self.holdings.get(code)
            price = self.valuation_price(code, day)
            if price is None or price <= 0:
                continue
            if item is None:
                want = target_value
            elif code in desired:
                want = desired[code] - item['shares'] * price
            else:
                continue
            if want <= 0 or want < self.carry.tolerance * target_value:
                continue
            buy_notional += self._buy(code, day, day_index, want)
        return buy_notional

    def _converge(self, target_codes: Sequence[str], target_value: float,
                  day: date, day_index: int) -> tuple[float, float]:
        """把保留标的的市值向 1/N 目标按 λ 收敛；返回（买入，卖出）名义额。"""
        desired: Dict[str, float] = {}
        for code in target_codes:
            price = self.valuation_price(code, day)
            if price is None or price <= 0:
                continue
            item = self.holdings.get(code)
            current = item['shares'] * price if item else 0.0
            desired[code] = current + self.carry.convergence * (
                target_value - current)
        sell = self._trim_overweight(target_codes, target_value, desired,
                                     day, day_index)
        buy = self._buy_underweight(target_codes, set(target_codes),
                                    target_value, desired, day, day_index)
        return buy, sell

    def _invest_residual(self, target_codes: Sequence[str], day: date,
                         day_index: int) -> float:
        """强制满仓对照：余额按等权继续买入目标标的。"""
        if self.cash <= 0:
            return 0.0
        candidates = [code for code in target_codes
                      if code in self.holdings] or list(target_codes)
        allocation = self.cash / len(candidates)
        return sum(self._buy(code, day, day_index, allocation)
                   for code in candidates)

    def _rebalance(self, signal_date: date, day_index: int, day: date,
                   ranking: Dict[date, List[str]], sizes: Dict[date, int]) -> None:
        codes = ranking.get(signal_date) or []
        size = sizes.get(signal_date, 0)
        if not codes or size <= 0:
            return
        target_codes = _select_targets(
            codes, size, self.holdings, self.carry.buffer_fraction)
        target_set = set(target_codes)
        old_holdings = set(self.holdings)
        self.sets.append(target_set)

        equity_open = self.cash + sum(
            item['shares'] * (self.valuation_price(code, day) or 0.0)
            for code, item in self.holdings.items())
        target_value = equity_open / len(target_codes)

        sell_notional = self._exit_dropped(target_set, day, day_index)
        if self.carry.weight_mode == 'target':
            buy_notional, more_sell = self._converge(
                target_codes, target_value, day, day_index)
            sell_notional += more_sell
        else:
            buy_notional = self._buy_underweight(
                codes, target_set, target_value, {}, day, day_index)
        if self.carry.invest_residual:
            buy_notional += self._invest_residual(target_codes, day, day_index)

        self.periods.append({
            'signal_date': signal_date,
            'exec_date': day,
            'targets': len(target_codes),
            'targets_codes': list(target_codes),
            'holdings': len(self.holdings),
            'retained': len(target_set & old_holdings),
            'entered': len(target_set - old_holdings),
            'exited': len(old_holdings - target_set),
            'buy_notional': buy_notional,
            'sell_notional': sell_notional,
            'equity_open': equity_open,
            'cash_after': self.cash,
            'holdings_value': {
                code: item['shares'] * (self.valuation_price(code, day) or 0.0)
                for code, item in self.holdings.items()
            },
        })

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    def run(self, calendar: Sequence[date], calendar_index: Dict[date, int],
            signal_dates: Sequence[date], signal_set: Set[date],
            ranking: Dict[date, List[str]], sizes: Dict[date, int],
            end_index: int) -> None:
        start_index = calendar_index[signal_dates[0]]
        pending_signal: date | None = None
        for day_index in range(start_index, end_index + 1):
            day = calendar[day_index]
            self.settle_exits(day_index, day)
            if pending_signal is not None:
                self._rebalance(pending_signal, day_index, day, ranking, sizes)
                pending_signal = None
            for code in self.holdings:
                price = self.price_of(code, day, 'close_hfq')
                if price and price > 0:
                    self.last_close[code] = price
            market_value = sum(item['shares'] * self.last_close.get(code, 0.0)
                               for code, item in self.holdings.items())
            self.equity_curve.append(
                {'date': day, 'equity': float(self.cash + market_value)})
            if day in signal_set:
                pending_signal = day


def _b2_turnover(periods: Sequence[Dict[str, Any]],
                 equity_curve: Sequence[Dict[str, Any]]) -> float:
    """逐调仓周期的成交换手：(买+卖)/2 ÷ 区间平均权益。"""
    if not periods:
        return 0.0
    equity_by_day = {row['date']: row['equity'] for row in equity_curve}
    days = [row['date'] for row in equity_curve]
    values: List[float] = []
    for index, record in enumerate(periods):
        start = record['exec_date']
        stop = periods[index + 1]['exec_date'] if index + 1 < len(periods) else None
        window = [day for day in days
                  if day >= start and (stop is None or day < stop)]
        if not window:
            continue
        mean_equity = float(np.mean([equity_by_day[day] for day in window]))
        if mean_equity <= 0:
            continue
        values.append((record['buy_notional'] + record['sell_notional'])
                      / 2.0 / mean_equity)
    return float(np.mean(values)) if values else 0.0


def _empty_result() -> Dict[str, Any]:
    """无有效样本时与正常返回保持同构的空结果。"""
    return {
        'equity_curve': [], 'turnover': 0.0, 'b2_turnover': 0.0,
        'performance': {}, 'trades': 0, 'total_cost': 0.0,
        'delayed_exits': 0, 'forced_exits': 0, 'null_cap_trades': 0,
        'slippage_bp_counts': {}, 'carry_periods': [],
        'holdings_mean': 0.0, 'retained_mean': 0.0,
    }


def simulate_carry(panel: pl.DataFrame, score_frame: pl.DataFrame,
                   config: WalkForwardConfig, carry: CarryConfig,
                   score_col: str = 'score',
                   initial_capital: float = 1_000_000.0,
                   max_exit_delay: int = 5,
                   cost_model: CostModel | None = None) -> Dict[str, Any]:
    """承接模式的组合模拟；返回字段与整仓清仓模式一致并追加承接明细。"""
    from src.quant.learning.evaluator import (
        _build_bars,
        _cross_sections,
        turnover_from_sets,
    )

    if score_frame.is_empty():
        return _empty_result()
    valid = score_frame.filter(pl.col(score_col).is_finite())
    sections = _cross_sections(valid)
    signal_dates = sorted(sections)
    if not signal_dates:
        return _empty_result()

    calendar = panel['date'].unique().sort().to_list()
    calendar_index = {day: i for i, day in enumerate(calendar)}
    signal_dates = signal_dates[::max(int(config.horizon), 1)]
    signal_set = set(signal_dates)

    ranking: Dict[date, List[str]] = {}
    sizes: Dict[date, int] = {}
    candidates: Set[str] = set()
    for signal_date in signal_dates:
        cross = sections[signal_date]
        if cross.is_empty():
            ranking[signal_date] = []
            sizes[signal_date] = 0
            continue
        size = max(1, int(round(cross.height * config.top_fraction)))
        codes = cross.sort(score_col, descending=True)['ts_code'].to_list()
        ranking[signal_date] = codes
        sizes[signal_date] = size
        limit = min(len(codes), max(
            size, int(round(size * (1.0 + carry.buffer_fraction)))))
        candidates.update(codes[:limit])

    last_signal = signal_dates[-1]
    end_index = min(
        calendar_index[last_signal] + config.horizon + max_exit_delay + 1,
        len(calendar) - 1)
    bars = _build_bars(panel, candidates, signal_dates[0], calendar[end_index])
    engine = _CarryEngine(bars, carry, initial_capital, max_exit_delay, cost_model)
    engine.run(calendar, calendar_index, signal_dates, signal_set, ranking,
               sizes, end_index)

    periods = engine.periods
    return {
        'equity_curve': engine.equity_curve,
        'turnover': turnover_from_sets(engine.sets),
        'b2_turnover': _b2_turnover(periods, engine.equity_curve),
        'performance': PerformanceAnalyzer().analyze(
            engine.equity_curve, [], frequency='daily'),
        'trades': engine.trade_count,
        'total_cost': engine.total_cost,
        'delayed_exits': engine.delayed_exits,
        'forced_exits': engine.forced_exits,
        'null_cap_trades': engine.null_cap_trades,
        'slippage_bp_counts': engine.slippage_bp_counts,
        'carry_periods': periods,
        'holdings_mean': (float(np.mean([row['holdings'] for row in periods]))
                          if periods else 0.0),
        'retained_mean': (float(np.mean([row['retained'] for row in periods]))
                          if periods else 0.0),
    }
