#!/usr/bin/env python3

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

import math
from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.risk import Order as RiskOrder
from src.risk import Portfolio as RiskPortfolio
from src.risk import RiskEngine


class ExecutionRules:
    """A 股撮合规则：涨跌停幅度推导 + 当日可交易性判定。"""

    #: 开盘价与涨跌停阈值的最小容差（避免浮点误差把贴板成交误判为可成交）
    LIMIT_TOLERANCE = 0.005

    #: 无法从证券代码推导时的兜底涨跌停幅度（主板默认）
    DEFAULT_LIMIT_PCT = 0.10

    def __init__(self, lot_size: int = 100):
        self.lot_size = lot_size

    @staticmethod
    def resolve_limit_pct(ts_code: str = '', is_st: bool = False) -> float:
        """
        按证券代码与 ST 状态推导涨跌停幅度（唯一真源）。

        ST/*ST → 5%；创业板 300/301 → 20%；科创板 68/688 → 20%；
        北交所 4/8/9 开头的 6 位代码 → 30%；其余主板默认 10%。

        注意：is_st 分支已实现但当前无调用方传入 True（数据层尚未提供
        ST 标识），接入 ST 判定时由调用侧补上即可。
        """
        if is_st:
            return 0.05

        code = (ts_code or '').split('.')[0]
        if not code:
            return ExecutionRules.DEFAULT_LIMIT_PCT

        if code.startswith(('300', '301')):
            return 0.20
        if code.startswith(('68', '688')):
            return 0.20
        if code.startswith(('4', '8', '9')) and len(code) == 6:
            return 0.30

        return ExecutionRules.DEFAULT_LIMIT_PCT

    def is_tradable(self, side: str, *, prev_close: float | None, open_price: float | None,
                    high: float | None, low: float | None, close: float | None,
                    volume: float | None, ts_code: str = '', is_st: bool = False) -> bool:
        """
        指定交易方向在某一天是否可成交。

        - 停牌（成交量为 0 / NaN / inf）一律不可交易
        - 有开盘价时：开盘即涨停不可买、开盘即跌停不可卖
          （覆盖一字板 + 开盘封板后盘中打开两种情形）
        - 无开盘价时退化为 high ≈ low（相对容差）的一字板判断，
          此时只做 close 涨跌停判定（偏乐观），调用侧应确保 open 列存在
        - 首根 K 线没有前收盘价，只做停牌判断
        """
        # 缺陷 A 修复：NaN / inf 穿过停牌检查，显式拦截
        if volume is None or not math.isfinite(volume) or volume <= 0:
            return False

        if prev_close is None or prev_close <= 0:
            return True

        limit = self.resolve_limit_pct(ts_code, is_st)

        if open_price is not None and math.isfinite(open_price):
            open_pct = open_price / prev_close - 1
            if side == 'buy' and open_pct >= limit - self.LIMIT_TOLERANCE:
                return False
            return not (side == 'sell' and open_pct <= -(limit - self.LIMIT_TOLERANCE))

        # 缺陷 B 修复：复权后 high / low 可能相差 1e-16，改用相对容差
        # 缺陷 C 修复：open_price 缺失时只退化到此分支，偏乐观
        if high is not None and low is not None and close is not None:
            if abs(high - low) / prev_close < 1e-6:
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
                 industry_map: Dict[str, str] | None = None):
        self.codes = list(codes)
        self.industry_map = dict(industry_map or {})
        per_code = initial_capital / len(self.codes) if self.codes else 0.0
        self.cash: Dict[str, float] = dict.fromkeys(self.codes, per_code)
        self.positions: Dict[str, int] = dict.fromkeys(self.codes, 0)
        self.last_prices: Dict[str, float] = {}

    @property
    def total_cash(self) -> float:
        return sum(self.cash.values())

    def mark(self, code: str, price: float | None) -> None:
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

    def __init__(self, risk_engine: RiskEngine | None, industry_map: Dict[str, str] | None = None):
        self.risk_engine = risk_engine
        self.industry_map = dict(industry_map or {})

    def check(self, *, ts_code: str, side: str, qty: int, price: float,
              trade_date: Any, book: AccountBook,
              equity_curve: List[Dict[str, Any]]) -> Dict[str, Any] | None:
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
                 risk_engine: RiskEngine | None = None):
        self.cost_model = cost_model
        self.performance_analyzer = performance_analyzer
        self.risk_engine = risk_engine

    def run(self, universe: Dict[str, pl.DataFrame], strategies: Dict[str, Any],
            initial_capital: float = 1_000_000.0, lot_size: int = 100,
            industry_map: Dict[str, str] | None = None,
            result_meta: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """
        执行回测并返回结果字典。

        结果固定包含：initial_capital / final_equity / total_return / trades /
        equity_curve / performance / risk_summary / rejected_orders。
        result_meta 里的键会合并进结果（如 strategy_name / strategy_names）。

        universe 价格口径（双价格列契约）：
            - open/high/low/close 为指标与收益计算用价（DataFetcher hfq 出口
              即为后复权价）；
            - 若携带 raw_open / raw_close（hfq 出口自动保留的原始价），
              涨跌停判定一律使用原始价——交易所规则定义在原始价上，除权日
              因子跳变会使复权比值失真；两列必须成对出现；
            - 无 raw_* 列时视为原始价输入（如 TDX 本地数据），直接用主列判定。
        """
        if not universe:
            logger.error("回测数据为空")
            return {}

        codes = list(universe.keys())
        if not any(code in strategies for code in codes):
            logger.error("未设置策略")
            return {}

        prepared = {code: self._ensure_sorted(universe[code]) for code in codes}

        for code, df in prepared.items():
            if ('raw_open' in df.columns) != ('raw_close' in df.columns):
                raise ValueError(f"{code} 的 raw_open / raw_close 必须成对出现")

        # 一次性把各列批量导出为 Python list（to_list 将 null 转为 None，与
        # 原先 .row(named=True) 的取值语义完全一致），主循环只做数组下标取值
        # 与纯 Python dict 组装，避免逐 bar 调用 DataFrame.row() 反复跨越
        # PyO3 边界编组整行
        column_data: Dict[str, Dict[str, list]] = {
            code: {name: df[name].to_list() for name in df.columns}
            for code, df in prepared.items()
        }

        # 缺陷 C：open 列缺失时 is_tradable 退化为 high≈low 路径，
        # 判定偏乐观（遗漏"开盘涨停、盘中打开"等情形）。
        # 整次回测任一标的缺 open 列 → 标记 fill_confidence 为 low。
        _open_missing = any('open' not in cols for cols in column_data.values())
        if _open_missing:
            logger.warning(
                "部分标的缺 open 列，开盘涨停判定将退化为 high≈low 路径，"
                "成交假设偏乐观（fill_confidence=low）。"
                "缺失标的: {}",
                [c for c, cols in column_data.items() if 'open' not in cols],
            )

        # 信号批量预生成：每个标的只调用一次向量化策略接口，主循环退化为
        # 数组下标取值，消除 N_days × N_symbols 次逐行策略调用与行 dict 组装。
        # 信号按 prepared[code] 的行序对齐（_ensure_sorted 保证日期升序），
        # 主循环里的 pos 即该信号在序列中的下标。未提供向量化接口的鸭子类型
        # 策略对象在此逐行适配，主循环保持单一查表路径。
        signal_columns: Dict[str, list] = {}
        for code in codes:
            strategy = strategies.get(code)
            if strategy is None:
                continue
            vec = getattr(strategy, 'generate_signals_vectorized', None)
            if vec is not None:
                signal_columns[code] = vec(prepared[code]).to_list()
            else:
                cols = column_data[code]
                height = len(next(iter(cols.values()))) if cols else 0
                signal_columns[code] = [
                    strategy.generate_signal(
                        {name: col[i] for name, col in cols.items()}, i
                    )
                    for i in range(height)
                ]

        # 一次性建立 date -> 行号索引（用 dict(zip) 替代手动 enumerate 循环），
        # 同时建反向索引 date -> [codes]，主循环按日期直查活跃标的，消除
        # 笛卡尔遍历下大量 date_positions[code].get(date) -> None -> continue
        # 的无效分支（标的上市日期分散时占外层迭代 40~50%）。
        date_positions: Dict[str, Dict[Any, int]] = {}
        date_codes: Dict[Any, List[str]] = {}
        sorted_dates: List[Any] = []
        for code, cols in column_data.items():
            dates = cols['date']
            positions = dict(zip(dates, range(len(dates))))
            date_positions[code] = positions
            for d in positions:
                bucket = date_codes.setdefault(d, [])
                bucket.append(code)
        sorted_dates = sorted(date_codes.keys())

        rules = ExecutionRules(lot_size=lot_size)
        book = AccountBook(initial_capital, codes, industry_map)
        gate = RiskGate(self.risk_engine, industry_map)

        pending: Dict[str, str | None] = dict.fromkeys(codes)
        prev_close: Dict[str, float | None] = dict.fromkeys(codes)
        prev_raw_close: Dict[str, float | None] = dict.fromkeys(codes)

        trades: List[Dict[str, Any]] = []
        rejected_orders: List[Dict[str, Any]] = []
        expired_orders: List[Dict[str, Any]] = []
        equity_curve: List[Dict[str, Any]] = []

        for date in sorted_dates:
            for code in date_codes[date]:
                pos = date_positions[code][date]
                cols = column_data[code]
                close_price = cols['close'][pos]
                # 列不存在时回退默认值，语义同原 row.get(name, default)；
                # 列存在但值为 None（null）时透传 None，不回退
                open_col = cols.get('open')
                open_price = open_col[pos] if open_col is not None else close_price
                high_col = cols.get('high')
                high = high_col[pos] if high_col is not None else None
                low_col = cols.get('low')
                low = low_col[pos] if low_col is not None else None
                volume_col = cols.get('volume')
                volume = volume_col[pos] if volume_col is not None else None
                raw_open_col = cols.get('raw_open')
                raw_open = raw_open_col[pos] if raw_open_col is not None else None
                raw_close_col = cols.get('raw_close')
                raw_close = raw_close_col[pos] if raw_close_col is not None else None

                self._execute_pending(
                    code=code, date=date, pending=pending, book=book, gate=gate,
                    rules=rules, cost_model=self.cost_model, trades=trades,
                    rejected_orders=rejected_orders, expired_orders=expired_orders,
                    equity_curve=equity_curve,
                    lot_size=lot_size,
                    prev_close=prev_close[code], open_price=open_price,
                    high=high, low=low, close=close_price, volume=volume,
                    open_raw=raw_open, prev_close_raw=prev_raw_close[code],
                    close_raw=raw_close,
                )

                # 策略基于当日数据生成信号，延迟到该标的下一个交易日执行；
                # 信号已在主循环前按行序批量生成，此处仅按下标查表
                signals = signal_columns.get(code)
                if signals is not None:
                    signal = signals[pos]
                    if signal in ('buy', 'sell'):
                        pending[code] = signal

                # close 缺失 / 非有限 / ≤0 时，挂单已在 _execute_pending 里过期；
                # 此处不更新 prev_close / book.mark，保留上一个有效值
                if close_price is None or not math.isfinite(close_price) or close_price <= 0:
                    continue
                prev_close[code] = close_price
                prev_raw_close[code] = raw_close if raw_close is not None else close_price
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
            # 缺陷 C：整次回测任一标的缺 open 列 → 开盘涨停判定退化，
            # 成交假设偏乐观，fill_confidence 提示调用方该结果可信度较低
            'fill_confidence': 'low' if _open_missing else 'high',
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

    def _execute_pending(self, *, code: str, date: Any, pending: Dict[str, str | None],
                         book: AccountBook, gate: RiskGate, rules: ExecutionRules,
                         cost_model: CostModel, trades: List[Dict[str, Any]],
                         rejected_orders: List[Dict[str, Any]],
                         expired_orders: List[Dict[str, Any]],
                         equity_curve: List[Dict[str, Any]], lot_size: int,
                         prev_close: float | None, open_price: float | None,
                         high: float | None, low: float | None,
                         close: float | None, volume: float | None,
                         open_raw: float | None = None,
                         prev_close_raw: float | None = None,
                         close_raw: float | None = None) -> None:
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

        # close 缺失 = 无有效行情，挂单作废（主循环已 continue 跳过整行，
        # 此处防御性守卫，确保函数签名 close=None 不会漏进来）
        if close is None or not math.isfinite(close) or close <= 0:
            _expire('price_missing')
            return

        # 涨跌停 / 停牌（零成交量）当日不可按该方向成交。
        # 交易所规则定义在原始价上：数据携带 raw_* 列（hfq 复权输入）时
        # 必须用原始价判定，除权日因子跳变会让复权比值失真（误触发/误放行）；
        # high == low 的一字板等式判断在线性缩放下不变，可直接透传。
        check_open = open_raw if open_raw is not None else open_price
        check_prev = prev_close_raw if prev_close_raw is not None else prev_close
        check_close = close_raw if close_raw is not None else close
        if not rules.is_tradable(side, prev_close=check_prev, open_price=check_open,
                                 high=high, low=low, close=check_close, volume=volume,
                                 ts_code=code):
            _expire('not_tradable')
            return

        # P0-2 修复：开盘价是集合竞价成交价。open_price 为 None（列存在但值缺失，
        # 如停牌复牌日、除权日）时，不能用 close 伪造成交价——回测不应发明价格。
        # 列整体缺失时 open_price 已在主循环回填为 close_price + fill_confidence=low，
        # 此处不会走到 None 分支。
        if open_price is None or not math.isfinite(open_price) or open_price <= 0:
            _expire('price_missing')
            return
        exec_price = open_price

        if side == 'buy':
            available = book.cash.get(code, 0.0)
            cost_rate = cost_model.get_buy_cost_rate()
            affordable = available / (exec_price * (1 + cost_rate))
            shares = int(affordable // lot_size) * lot_size
            # 费率估算不含 min_commission 下限，小单实际成本可能超估算：
            # 按实际成本逐手回退，而不是整笔静默作废
            buy_cost = 0.0
            total_pay = 0.0
            while shares > 0:
                buy_cost = cost_model.calculate_buy_cost(shares, exec_price)
                total_pay = shares * exec_price + buy_cost
                if total_pay <= available:
                    break
                shares -= lot_size
            if shares == 0:
                _expire('insufficient_cash')
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
