#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测执行内核 —— 撮合、T+1、整手、风控、成本、权益估值的唯一实现。

单票回测（BacktestEngine）与组合回测（PortfolioBacktestEngine）都只是
同一内核的参数化封装：

    单票 = 一个标的 + 全部初始资金
    组合 = 多标的 + 等权分配

A 股交易语义只允许在本模块修改，禁止在具体引擎里再写一份：

    1. t 日收盘后生成的信号，延迟到 t+1 日以开盘价成交（避免未来函数）
    2. 买入股数按 lot_size（默认 100 股）整数倍向下取整
    3. 买入成本在下单当时扣除，卖出所得叠加回剩余现金（不能覆盖）
    4. 开盘价触及涨停不可买、触及跌停不可卖、停牌（成交量为 0）不可交易
    5. 涨跌停幅度按证券代码自适应（主板 10%、创业板/科创板 20%、北交所 30%、ST 5%）
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.risk import Order as RiskOrder, Portfolio as RiskPortfolio, RiskEngine


class ExecutionRules:
    """A 股撮合规则：涨跌停幅度推导 + 当日可交易性判定。"""

    #: 开盘价与涨跌停阈值的最小容差（避免浮点误差把贴板成交误判为可成交）
    LIMIT_TOLERANCE = 0.005

    def __init__(self, limit_pct: float = 0.10, lot_size: int = 100):
        self.limit_pct = limit_pct
        self.lot_size = lot_size

    @staticmethod
    def resolve_limit_pct(ts_code: str = '', is_st: bool = False, default: float = 0.10) -> float:
        """
        按证券代码与 ST 状态推导涨跌停幅度。

        ST/*ST → 5%；创业板 300/301 → 20%；科创板 68/688 → 20%；
        北交所 4/8/9 开头的 6 位代码 → 30%；其余主板默认 10%。
        """
        if is_st:
            return 0.05

        code = (ts_code or '').split('.')[0]
        if not code:
            return default

        if code.startswith(('300', '301')):
            return 0.20
        if code.startswith(('68', '688')):
            return 0.20
        if code.startswith(('4', '8', '9')) and len(code) == 6:
            return 0.30

        return 0.10

    def effective_limit(self, ts_code: str = '', is_st: bool = False) -> float:
        """有代码时按代码自适应，无代码时回退调用方传入的 limit_pct。"""
        return self.resolve_limit_pct(ts_code, is_st, self.limit_pct) if ts_code else self.limit_pct

    def is_tradable(self, side: str, *, prev_close: Optional[float], open_price: Optional[float],
                    high: Optional[float], low: Optional[float], close: Optional[float],
                    volume: Optional[float], ts_code: str = '', is_st: bool = False) -> bool:
        """
        指定交易方向在某一天是否可成交。

        - 停牌（成交量为 0）一律不可交易
        - 有开盘价时：开盘即涨停不可买、开盘即跌停不可卖
          （覆盖一字板 + 开盘封板后盘中打开两种情形）
        - 无开盘价时退化为 high == low 的一字板判断
        - 首根 K 线没有前收盘价，只做停牌判断
        """
        if volume is not None and volume <= 0:
            return False

        if prev_close is None or prev_close <= 0:
            return True

        limit = self.effective_limit(ts_code, is_st)

        if open_price is not None:
            open_pct = open_price / prev_close - 1
            if side == 'buy' and open_pct >= limit - self.LIMIT_TOLERANCE:
                return False
            if side == 'sell' and open_pct <= -(limit - self.LIMIT_TOLERANCE):
                return False
            return True

        if high is not None and low is not None and high == low and close is not None:
            pct = close / prev_close - 1
            if side == 'buy' and pct >= limit - self.LIMIT_TOLERANCE:
                return False
            if side == 'sell' and pct <= -(limit - self.LIMIT_TOLERANCE):
                return False

        return True


class AccountBook:
    """
    账户账本：按标的拆分现金桶（组合等权分配），持仓、最新价与权益估值统一在此维护。

    单票回测只有一个现金桶，等价于原来的全局 current_capital。
    """

    def __init__(self, initial_capital: float, codes: List[str],
                 industry_map: Optional[Dict[str, str]] = None):
        self.codes = list(codes)
        self.industry_map = dict(industry_map or {})
        per_code = initial_capital / len(self.codes) if self.codes else 0.0
        self.cash: Dict[str, float] = {code: per_code for code in self.codes}
        self.positions: Dict[str, int] = {code: 0 for code in self.codes}
        self.last_prices: Dict[str, float] = {}

    @property
    def total_cash(self) -> float:
        return sum(self.cash.values())

    def mark(self, code: str, price: Optional[float]) -> None:
        """更新盯市价（收盘后调用）。"""
        if price is not None and price > 0:
            self.last_prices[code] = float(price)

    def buy(self, code: str, price: float, shares: int, cost: float) -> None:
        """买入：现金扣除成交额与买方成本，持仓增加。"""
        self.cash[code] -= shares * price + cost
        self.positions[code] = self.positions.get(code, 0) + shares

    def sell(self, code: str, price: float, shares: int, cost: float) -> None:
        """卖出：所得净额叠加回该标的现金桶（不能覆盖买入后剩余现金）。"""
        self.cash[code] += shares * price - cost
        self.positions[code] = 0

    def total_equity(self) -> float:
        """权益 = 各桶现金之和 + 各持仓按最新价盯市。"""
        equity = self.total_cash
        for code, qty in self.positions.items():
            if qty > 0:
                equity += qty * self.last_prices.get(code, 0.0)
        return equity

    def snapshot(self, equity_curve: List[Dict[str, Any]]) -> RiskPortfolio:
        """构造风控用的组合快照。"""
        return RiskPortfolio(
            positions={code: qty for code, qty in self.positions.items() if qty > 0},
            last_prices=dict(self.last_prices),
            cash=self.total_cash,
            industry_map=dict(self.industry_map),
            equity_curve=list(equity_curve),
        )


class RiskGate:
    """风控闸门：唯一的 RiskOrder 构造与 gate() 调用入口。"""

    def __init__(self, risk_engine: Optional[RiskEngine], industry_map: Optional[Dict[str, str]] = None):
        self.risk_engine = risk_engine
        self.industry_map = dict(industry_map or {})

    def check(self, *, ts_code: str, side: str, qty: int, price: float,
              trade_date: Any, book: AccountBook,
              equity_curve: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        审查一笔候选订单。

        Returns:
            None 表示放行；否则返回可直接写入 rejected_orders 的拒单记录。
        """
        if self.risk_engine is None:
            return None

        order = RiskOrder(
            ts_code=ts_code,
            side=side,
            qty=qty,
            price=price,
            industry=self.industry_map.get(ts_code, ''),
            trade_date=trade_date,
        )
        gate = self.risk_engine.gate([order], book.snapshot(equity_curve))
        if not gate.rejected:
            return None

        _, rule_name, reason = gate.rejected[0]
        return {
            'date': trade_date,
            'side': side,
            'ts_code': ts_code,
            'qty': qty,
            'price': price,
            'rule': rule_name,
            'reason': reason,
        }


class EventBacktestRunner:
    """
    通用事件循环：多标的日频回测的唯一撮合实现。

    输入 universe 必须已完成指标预计算（策略按列名读取 ma_* / macd / rsi 等）。
    """

    def __init__(self, cost_model: CostModel, performance_analyzer: PerformanceAnalyzer,
                 risk_engine: Optional[RiskEngine] = None):
        self.cost_model = cost_model
        self.performance_analyzer = performance_analyzer
        self.risk_engine = risk_engine

    def run(self, universe: Dict[str, pl.DataFrame], strategies: Dict[str, Any],
            initial_capital: float = 1_000_000.0, lot_size: int = 100,
            limit_pct: float = 0.10, industry_map: Optional[Dict[str, str]] = None,
            result_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        执行回测并返回结果字典。

        结果固定包含：initial_capital / final_equity / total_return / trades /
        equity_curve / performance / risk_summary / rejected_orders。
        result_meta 里的键会合并进结果（如 strategy_name / strategy_names）。
        """
        if not universe:
            logger.error("回测数据为空")
            return {}

        codes = list(universe.keys())
        if not any(code in strategies for code in codes):
            logger.error("未设置策略")
            return {}

        prepared = {code: self._ensure_sorted(universe[code]) for code in codes}

        # 一次性建立 date -> 行号索引，主循环只做字典查找
        date_positions: Dict[str, Dict[Any, int]] = {}
        all_dates: set = set()
        for code, df in prepared.items():
            positions: Dict[Any, int] = {}
            for i, d in enumerate(df['date'].to_list()):
                if d not in positions:
                    positions[d] = i
            date_positions[code] = positions
            all_dates.update(positions)
        sorted_dates = sorted(all_dates)

        rules = ExecutionRules(limit_pct=limit_pct, lot_size=lot_size)
        book = AccountBook(initial_capital, codes, industry_map)
        gate = RiskGate(self.risk_engine, industry_map)

        pending: Dict[str, Optional[str]] = {code: None for code in codes}
        bar_index: Dict[str, int] = {code: 0 for code in codes}
        prev_close: Dict[str, Optional[float]] = {code: None for code in codes}

        trades: List[Dict[str, Any]] = []
        rejected_orders: List[Dict[str, Any]] = []
        expired_orders: List[Dict[str, Any]] = []
        equity_curve: List[Dict[str, Any]] = []

        for date in sorted_dates:
            for code in codes:
                pos = date_positions[code].get(date)
                if pos is None:
                    # 停牌（无数据行）：挂单顺延至该标的下一个交易日，不视为作废
                    continue

                row = prepared[code].row(pos, named=True)
                close_price = row['close']
                open_price = row.get('open', close_price)
                high = row.get('high')
                low = row.get('low')
                volume = row.get('volume')

                self._execute_pending(
                    code=code, date=date, pending=pending, book=book, gate=gate,
                    rules=rules, cost_model=self.cost_model, trades=trades,
                    rejected_orders=rejected_orders, expired_orders=expired_orders,
                    equity_curve=equity_curve,
                    lot_size=lot_size,
                    prev_close=prev_close[code], open_price=open_price,
                    high=high, low=low, close=close_price, volume=volume,
                )

                # 策略基于当日数据生成信号，延迟到该标的下一个交易日执行
                strategy = strategies.get(code)
                if strategy is not None:
                    signal = strategy.generate_signal(row, bar_index[code])
                    if signal in ('buy', 'sell'):
                        pending[code] = signal

                bar_index[code] += 1
                prev_close[code] = close_price
                book.mark(code, close_price)

            equity_curve.append({'date': date, 'equity': book.total_equity()})

        final_equity = equity_curve[-1]['equity'] if equity_curve else initial_capital
        total_return = (final_equity - initial_capital) / initial_capital * 100

        performance = self.performance_analyzer.analyze(equity_curve, trades)

        risk_summary = None
        if self.risk_engine is not None:
            risk_summary = {
                'rules': [r.name for r in self.risk_engine.rules],
                'total_rejected': len(rejected_orders),
                'reject_detail': [
                    f"[{r['date']}] {r['side']} {r['ts_code']} qty={r['qty']} → {r['rule']}: {r['reason']}"
                    for r in rejected_orders
                ],
            }

        result: Dict[str, Any] = {
            'initial_capital': initial_capital,
            'final_equity': final_equity,
            'total_return': total_return,
            'trades': trades,
            'equity_curve': equity_curve,
            'performance': performance,
            'risk_summary': risk_summary,
            'rejected_orders': rejected_orders,
            'expired_orders': expired_orders,
        }
        if result_meta:
            result.update(result_meta)

        logger.info(
            f"回测完成: 总收益率 = {total_return:.2f}%"
            + (f" | 风控拒单 {len(rejected_orders)} 笔" if risk_summary else "")
            + (f" | 挂单作废 {len(expired_orders)} 笔" if expired_orders else "")
        )
        return result

    # ------------------------------------------------------------------
    # 撮合
    # ------------------------------------------------------------------

    def _execute_pending(self, *, code: str, date: Any, pending: Dict[str, Optional[str]],
                         book: AccountBook, gate: RiskGate, rules: ExecutionRules,
                         cost_model: CostModel, trades: List[Dict[str, Any]],
                         rejected_orders: List[Dict[str, Any]],
                         expired_orders: List[Dict[str, Any]],
                         equity_curve: List[Dict[str, Any]], lot_size: int,
                         prev_close: Optional[float], open_price: Optional[float],
                         high: Optional[float], low: Optional[float],
                         close: Optional[float], volume: Optional[float]) -> None:
        """执行该标的的挂单。

        订单生命周期语义（显式状态机，杜绝静默丢单）：
            - 标的停牌无数据行时不进入本函数，挂单顺延至下一交易日；
            - 进入本函数即消费挂单，当日有效：成交进 trades、被风控拦截进
              rejected_orders、其余不可成交情形一律作废并记入 expired_orders
              （already_holding / no_position / not_tradable / price_missing /
              insufficient_cash），不会残留到下一交易日。
        """
        side = pending.pop(code, None)
        if side is None:
            return

        position = book.positions.get(code, 0)

        def _expire(reason: str, qty: int = 0) -> None:
            expired_orders.append({
                'date': date, 'side': side, 'ts_code': code,
                'reason': reason, 'qty': qty,
            })

        # 持仓前置条件不满足：已持仓的买入、无持仓的卖出
        if side == 'buy' and position > 0:
            _expire('already_holding')
            return
        if side == 'sell' and position == 0:
            _expire('no_position')
            return

        # 涨跌停 / 停牌（零成交量）当日不可按该方向成交
        if not rules.is_tradable(side, prev_close=prev_close, open_price=open_price,
                                 high=high, low=low, close=close, volume=volume,
                                 ts_code=code):
            _expire('not_tradable')
            return

        exec_price = open_price if open_price is not None else close
        if exec_price is None:
            _expire('price_missing')
            return

        if side == 'buy':
            available = book.cash.get(code, 0.0)
            cost_rate = cost_model.get_buy_cost_rate()
            affordable = available / (exec_price * (1 + cost_rate))
            shares = int(affordable // lot_size) * lot_size
            if shares == 0:
                _expire('insufficient_cash')
                return
            buy_cost = cost_model.calculate_buy_cost(shares, exec_price)
            total_pay = shares * exec_price + buy_cost
            if total_pay > available:
                _expire('insufficient_cash', qty=shares)
                return
            rejection = gate.check(
                ts_code=code, side='buy', qty=shares, price=exec_price,
                trade_date=date, book=book, equity_curve=equity_curve,
            )
            if rejection is not None:
                rejected_orders.append(rejection)
                return
            book.buy(code, exec_price, shares, buy_cost)
            trades.append({
                'date': date,
                'stock_code': code,
                'signal': 'buy',
                'price': exec_price,
                'shares': shares,
                'capital': book.cash[code],
                'position': book.positions[code],
                'cost': buy_cost,
            })
            return

        # side == 'sell'（持仓前置条件已在上方校验）
        shares = book.positions[code]
        sell_cost = cost_model.calculate_sell_cost(shares, exec_price)
        rejection = gate.check(
            ts_code=code, side='sell', qty=shares, price=exec_price,
            trade_date=date, book=book, equity_curve=equity_curve,
        )
        if rejection is not None:
            rejected_orders.append(rejection)
            return
        book.sell(code, exec_price, shares, sell_cost)
        trades.append({
            'date': date,
            'stock_code': code,
            'signal': 'sell',
            'price': exec_price,
            'shares': shares,
            'capital': book.cash[code],
            'position': 0,
            'cost': sell_cost,
        })

    @staticmethod
    def _ensure_sorted(df: pl.DataFrame) -> pl.DataFrame:
        """按日期升序，保证日期索引与执行顺序一致。"""
        if 'date' in df.columns and not df['date'].is_sorted():
            return df.sort('date')
        return df
