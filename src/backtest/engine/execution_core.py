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
    5. 涨跌停幅度按证券代码与交易日自适应（主板 10%、创业板/科创板 20%、
       北交所 30%、ST 5%；创业板注册制改革日 2020-08-24 前为 10%）
"""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.risk import Order as RiskOrder
from src.risk import Portfolio as RiskPortfolio
from src.risk import RiskEngine

#: A 股最小价格变动单位（1 分），交易所涨跌停价的取整单位
PRICE_TICK = Decimal('0.01')


def to_trade_date(value: Any) -> date | None:
    """把回测主循环里的 date（date / datetime / 'YYYY-MM-DD' 字符串）归一为 date。

    无法解析时返回 None，调用侧据此退回到无日期的板块默认幅度，保持旧行为。
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    # polars / pandas 时间戳类对象
    if hasattr(value, 'date') and callable(value.date):
        try:
            return value.date()
        except (TypeError, ValueError):
            return None
    return None


class ExecutionRules:
    """A 股撮合规则：涨跌停幅度推导 + 当日可交易性判定。"""

    #: 价格空间比较的浮点噪声容差（单位：元）。只用于吸收双精度乘法误差
    #: （约 1e-14 量级），不是"允许多涨半个百分点"的业务容差——原实现在比率
    #: 空间直接减 0.005，等于放大了 prev_close 倍，会把涨幅落在
    #: [limit-0.5%, limit) 的开盘价系统性误判为封板不可成交。
    PRICE_EPS = 1e-9

    #: ST/*ST 及"ST 状态未知"时的涨跌停幅度
    ST_LIMIT_PCT = 0.05

    #: 无法从证券代码推导时的兜底涨跌停幅度（主板默认）
    DEFAULT_LIMIT_PCT = 0.10

    #: 创业板注册制改革正式实施日：此前创业板涨跌停幅度为 10%，当日起放宽为 20%。
    #: 历史回测必须按交易日落到改革前后分别取阈值，否则改革前固定用 20% 会把
    #: 实际已封板（>+10%）的开盘价误判为可成交，系统性虚增收益。
    CHINEXT_REG_DATE = date(2020, 8, 24)

    #: 显式代码段表：(代码前缀元组, 涨跌停幅度, 板块名)，按顺序匹配、最长前缀优先。
    #: 900/200 沪深的 B 股必须显式列出为 10%——否则 900xxx 会被更早的北交所
    #: 判定吞掉，30% 阈值等于取消 B 股的涨跌停约束。
    BOARD_RULES = (
        (('688', '689'), 0.20, 'STAR'),        # 科创板（689 为 CDR 段）
        (('300', '301'), 0.20, 'CHINEXT'),     # 创业板：注册制后 20%，改革前需按 trade_date 回退 10%
        (('900',), 0.10, 'SH_B'),              # 沪市 B 股
        (('200',), 0.10, 'SZ_B'),              # 深市 B 股
    )

    #: 无交易所后缀时的北交所号段兜底。不再使用 '4'/'8'/'9' 宽泛前缀：
    #: '9' 会误伤 900xxx 沪市 B 股，'8'/'4' 会误伤新三板/老三板代码。
    #: 920 为北交所新号段（如 920001.BJ），代码段会随时间新增，后缀判定更可靠。
    BJ_PREFIXES = ('83', '87', '88', '920')

    def __init__(self, lot_size: int = 100):
        self.lot_size = lot_size

    @staticmethod
    def resolve_limit_pct(ts_code: str = '', is_st: bool | None = None,
                          trade_date: date | None = None,
                          listing_date: date | None = None) -> float:
        """
        按证券代码、交易日与 ST 状态推导涨跌停幅度（唯一真源）。

        规则：
        - ``is_st=True`` → 5%（ST/*ST 真实幅度）；
        - 创业板 300/301：``trade_date < CHINEXT_REG_DATE``（2020-08-24）按改革
          前的 10%，当日起按 20%。``trade_date`` 缺失时回退到 BOARD_RULES 的
          20%，保持与旧行为一致；
        - 科创板 688/689 → 20%、北交所 → 30%（优先 .BJ 后缀，无后缀按
          83/87/88/920 号段兜底）、沪市 B 股 900xxx / 深市 B 股 200xxx 与其余
          主板 → 10%；
        - ``is_st=None``（数据层未提供 ST 标识）按板块规则判定，未知状态只体现
          为结果里的 st_unknown=True / fill_confidence=low，不改变成交判定本身。

        ``listing_date`` 预留用于新股上市首日不限涨跌幅的判定，当前数据层未
        提供，暂未启用；参数先固化以便未来接入 (code, listing_date) 维表时无需
        改动签名。
        """
        # 仅"明确 ST"按 5%；None 表示数据层无标记，回落到板块默认幅度
        if is_st:
            return ExecutionRules.ST_LIMIT_PCT

        ts_code = (ts_code or '').strip().upper()
        code = ts_code.split('.')[0]
        if not code:
            return ExecutionRules.DEFAULT_LIMIT_PCT

        # 北交所：交易所后缀最可靠（代码段会新增），无后缀时用显式号段兜底
        if ts_code.endswith('.BJ'):
            return 0.30
        if code.startswith(ExecutionRules.BJ_PREFIXES):
            return 0.30

        # 创业板注册制改革前按 10%，改革日及之后按 20%
        if code.startswith(('300', '301')):
            if trade_date is not None and trade_date < ExecutionRules.CHINEXT_REG_DATE:
                return 0.10
            return 0.20

        for prefixes, pct, _board in ExecutionRules.BOARD_RULES:
            if code.startswith(prefixes):
                return pct

        return ExecutionRules.DEFAULT_LIMIT_PCT

    @staticmethod
    def resolve_limit_price(prev_close: float, limit: float, side: str) -> float:
        """按交易所规则计算涨跌停价：round(prev_close × (1±limit), 2)（四舍五入到分）。

        两个关键点：

        - 先转 Decimal 再相乘。二进制浮点下 10.03×1.1 = 11.033000000000001，
          10.05×1.1 = 11.054999999999998，直接用浮点乘积再 round 会在 .005
          边界上少取一分（算出 11.05，交易所公布 11.06），一字板因此被误判
          可成交、系统性虚增收益；
        - 用 ROUND_HALF_UP 而非内置 round()（银行家舍入，.5 向偶数靠拢），
          与交易所"四舍五入"口径一致。

        Args:
            prev_close: 前收盘价（原始价，非复权价）
            limit: 涨跌停幅度（如 0.10 / 0.20）
            side: 'buy' 取涨停价，'sell' 取跌停价
        """
        base = Decimal(str(prev_close))
        pct = Decimal(str(limit))
        factor = Decimal('1') + pct if side == 'buy' else Decimal('1') - pct
        return float((base * factor).quantize(PRICE_TICK, rounding=ROUND_HALF_UP))

    def is_tradable(self, side: str, *, prev_close: float | None, open_price: float | None,
                    high: float | None, low: float | None, close: float | None,
                    volume: float | None, ts_code: str = '',
                    is_st: bool | None = None,
                    trade_date: date | None = None) -> bool:
        """
        指定交易方向在某一天是否可成交。

        - 停牌（成交量为 0 / NaN / inf）一律不可交易
        - 有开盘价时：开盘即涨停不可买、开盘即跌停不可卖
          （覆盖一字板 + 开盘封板后盘中打开两种情形）
        - 无开盘价时退化为 high ≈ low（相对容差）的一字板判断，
          此时只做 close 涨跌停判定（偏乐观），调用侧应确保 open 列存在
        - 首根 K 线没有前收盘价，只做停牌判断
        - is_st 仅 True（明确 ST）按 5% 判定；None（数据层无标记）与 False
          都按板块规则（见 resolve_limit_pct），未知状态只由结果的
          st_unknown / fill_confidence 标记，不再改变成交判定本身
        """
        # 缺陷 A 修复：NaN / inf 穿过停牌检查，显式拦截
        if self._is_suspended(volume):
            return False

        if prev_close is None or prev_close <= 0:
            return True

        limit = self.resolve_limit_pct(ts_code, is_st, trade_date=trade_date)

        if open_price is not None and math.isfinite(open_price):
            # 价格空间比较：涨跌停价 = round(prev_close × (1±limit), 2) 后直接比较。
            # 原写法 (open_pct >= limit - 0.005) 的容差被放大 prev_close 倍，
            # 3 元股等价于允许 1.5% 偏差、200 元股允许 0.25% 偏差，
            # 结果是把 [limit-0.5%, limit) 的正常开盘价误当成封板双向拒绝。
            return not self._limit_breached(
                side, price=open_price, prev_close=prev_close, limit=limit)

        # 缺陷 B 修复：复权后 high / low 可能相差 1e-16，改用相对容差
        # 缺陷 C 修复：open_price 缺失时只退化到此分支，偏乐观
        return not self._is_one_line_board_locked(
            side, high=high, low=low, close=close,
            prev_close=prev_close, limit=limit)

    @staticmethod
    def _is_suspended(volume: float | None) -> bool:
        """成交量为 None / NaN / inf / ≤0 视为停牌，不可交易。"""
        return volume is None or not math.isfinite(volume) or volume <= 0

    def _limit_breached(self, side: str, *, price: float,
                        prev_close: float, limit: float) -> bool:
        """该方向在给定价格是否已触及涨跌停：buy 触涨停、sell 触跌停。

        涨跌停价必须按交易所规则取整到分（见 resolve_limit_price），
        直接比较浮点乘积会把一字板误判为可成交。
        """
        if side == 'buy':
            return price >= self.resolve_limit_price(prev_close, limit, 'buy') - self.PRICE_EPS
        if side == 'sell':
            return price <= self.resolve_limit_price(prev_close, limit, 'sell') + self.PRICE_EPS
        return False

    def _is_one_line_board_locked(
        self, side: str, *, high: float | None, low: float | None,
        close: float | None, prev_close: float, limit: float,
    ) -> bool:
        """无有效开盘价时的退化判定：high≈low（相对容差）且 close 已触板。

        high / low / close 任一缺失时不做封板判定（视为可交易）。
        """
        if high is None or low is None or close is None:
            return False
        if abs(high - low) / prev_close >= 1e-6:
            return False
        return self._limit_breached(side, price=close,
                                    prev_close=prev_close, limit=limit)


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
        """构造风控用的组合快照。

        equity_curve 共享调用方的列表引用、不做复制：风控规则只切片读取尾部
        window（如 MaxDrawdown），快照对象在同一次订单审查结束即被丢弃。
        原实现在这里 list(equity_curve) 整体复制，每笔候选订单一次，
        10 年（T≈2400）× 日均 m 笔时累计复制 O(mT²) 个元素，是长周期
        组合回测的主要纯开销。
        """
        return RiskPortfolio(
            positions={code: qty for code, qty in self.positions.items() if qty > 0},
            last_prices=dict(self.last_prices),
            cash=self.total_cash,
            industry_map=dict(self.industry_map),
            equity_curve=equity_curve,
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
            result_meta: Dict[str, Any] | None = None,
            st_status: Dict[str, bool | None] | None = None) -> Dict[str, Any]:
        """
        执行回测并返回结果字典。

        结果固定包含：initial_capital / final_equity / total_return / trades /
        equity_curve / performance / risk_summary / rejected_orders /
        expired_orders / positions / slow_path_strategies / st_unknown。
        result_meta 里的键会合并进结果（如 strategy_name / strategy_names）。

        st_status 为 (标的名 → ST 三态) 映射：True=ST、False=明确非 ST、
        未出现在映射中=状态未知。未知标的涨跌停按板块默认幅度判定（与
        False 相同），只把结果的 st_unknown 置 True、fill_confidence 降为
        low，作为数据质量标记——若未知即按 5%，主板正常股开盘 +5% 会被
        系统性判成封板，跳空走强信号被剔除、动量策略收益被系统性低估。
        接入 (code, start_date, end_date, is_st) 时间维表后由调用方传此参数。

        universe 价格口径（双价格列契约）：
            - open/high/low/close 为指标与收益计算用价（DataFetcher hfq 出口
              即为后复权价）；
            - 若携带 raw_open / raw_close（hfq 出口自动保留的原始价），
              涨跌停判定一律使用原始价——交易所规则定义在原始价上，除权日
              因子跳变会使复权比值失真；两列必须成对出现；
            - 无 raw_* 列时视为原始价输入（如 TDX 本地数据），直接用主列判定。
        """
        prepared_data = self._prepare_universe(universe, strategies)
        # 空 universe / 无标的配置策略时 _prepare_universe 返回 None，提前返回
        if prepared_data is None:
            return {}
        prepared, column_data = prepared_data
        codes = list(column_data.keys())

        # 缺陷 C：整次回测任一标的缺 open 列 → 标记 fill_confidence 为 low
        _open_missing = self._check_open_column(column_data)

        # ST 三态：True=ST、False=明确非 ST、未提供=未知（按 5% 保守判定）
        st_status, st_unknown = self._resolve_st_status(codes, st_status)

        # 信号批量预生成：每个标的只调用一次向量化策略接口，主循环退化为
        # 数组下标取值，消除 N_days × N_symbols 次逐行策略调用与行 dict 组装。
        # 降级路径必须留痕（slow_path_strategies + warning）
        signal_columns, slow_path_strategies = self._build_signal_columns(
            codes, strategies, prepared, column_data)

        # 一次性建立 date→行号与 date→活跃标的反向索引，主循环按日期直查
        date_positions, date_codes, sorted_dates = self._build_date_index(column_data)

        rules = ExecutionRules(lot_size=lot_size)
        # 现金桶只分配给配置了策略的标的：universe 里加载了但没配策略的"哑标的"
        # 若参与 initial_capital / len(codes) 的等额切分，其份额全程闲置，
        # 冻结比例恰等于无策略标的占比（20 只里只有 5 只配策略 → 75% 本金不动），
        # 净值曲线被压成近水平线、最大回撤被压低，结果会被误读为"策略很稳"。
        active_codes = [c for c in codes if c in strategies]
        book = AccountBook(initial_capital, active_codes, industry_map)
        gate = RiskGate(self.risk_engine, industry_map)

        pending: Dict[str, str | None] = dict.fromkeys(codes)
        prev_close: Dict[str, float | None] = dict.fromkeys(codes)
        prev_raw_close: Dict[str, float | None] = dict.fromkeys(codes)

        trades: List[Dict[str, Any]] = []
        rejected_orders: List[Dict[str, Any]] = []
        expired_orders: List[Dict[str, Any]] = []
        equity_curve: List[Dict[str, Any]] = []

        self._run_trading_loop(
            sorted_dates=sorted_dates, date_codes=date_codes,
            date_positions=date_positions, column_data=column_data,
            pending=pending, prev_close=prev_close, prev_raw_close=prev_raw_close,
            book=book, gate=gate, rules=rules, trades=trades,
            rejected_orders=rejected_orders, expired_orders=expired_orders,
            equity_curve=equity_curve, lot_size=lot_size,
            st_status=st_status, signal_columns=signal_columns,
        )

        return self._build_result(
            initial_capital=initial_capital, equity_curve=equity_curve,
            trades=trades, rejected_orders=rejected_orders,
            expired_orders=expired_orders, book=book,
            slow_path_strategies=slow_path_strategies,
            open_missing=_open_missing, st_unknown=st_unknown,
            result_meta=result_meta,
        )

    # ------------------------------------------------------------------
    # run() 编排子步骤
    # ------------------------------------------------------------------

    def _prepare_universe(
        self, universe: Dict[str, pl.DataFrame], strategies: Dict[str, Any],
    ) -> tuple[Dict[str, pl.DataFrame], Dict[str, Dict[str, list]]] | None:
        """校验 universe 与策略配置，按日期排序并批量导出各列为 Python list。

        universe 为空或没有任何标的配置策略时返回 None，由调用方提前返回空
        结果；raw_open / raw_close 必须成对出现，否则直接抛 ValueError。

        column_data 用 to_list 把 null 转为 None，与原先 ``.row(named=True)``
        的取值语义完全一致：主循环只做数组下标取值与纯 Python dict 组装，
        避免逐 bar 调用 ``DataFrame.row()`` 反复跨越 PyO3 边界编组整行。

        Returns:
            (prepared, column_data)；校验失败返回 None。
        """
        if not universe:
            logger.error("回测数据为空")
            return None

        codes = list(universe.keys())
        if not any(code in strategies for code in codes):
            logger.error("未设置策略")
            return None

        prepared = {code: self._ensure_sorted(universe[code]) for code in codes}
        for code, df in prepared.items():
            if ('raw_open' in df.columns) != ('raw_close' in df.columns):
                raise ValueError(f"{code} 的 raw_open / raw_close 必须成对出现")

        column_data: Dict[str, Dict[str, list]] = {
            code: {name: df[name].to_list() for name in df.columns}
            for code, df in prepared.items()
        }
        return prepared, column_data

    @staticmethod
    def _check_open_column(column_data: Dict[str, Dict[str, list]]) -> bool:
        """返回是否有标的缺 open 列；缺失时按原文案告警（fill_confidence=low）。

        缺陷 C：open 列缺失时 is_tradable 退化为 high≈low 路径，判定偏乐观
        （遗漏"开盘涨停、盘中打开"等情形）。
        """
        _open_missing = any('open' not in cols for cols in column_data.values())
        if _open_missing:
            logger.warning(
                "部分标的缺 open 列，开盘涨停判定将退化为 high≈low 路径，"
                "成交假设偏乐观（fill_confidence=low）。"
                "缺失标的: {}",
                [c for c, cols in column_data.items() if 'open' not in cols],
            )
        return _open_missing

    @staticmethod
    def _resolve_st_status(
        codes: List[str], st_status: Dict[str, bool | None] | None,
    ) -> tuple[Dict[str, bool | None], bool]:
        """解析 ST 三态映射，返回 (st_status, st_unknown)，未知标的按板块默认判定。

        True=ST、False=明确非 ST、未出现在映射中=状态未知。未知不代表 ST：
        涨跌停按板块规则（与 False 相同）判定，只以 st_unknown=True /
        fill_confidence=low 标记数据质量，避免把未知当 ST 后系统性剔除
        主板 +5% 以上的跳空走强成交。
        历史 ST 状态需 (code, start_date, end_date, is_st) 时间维表才能精确
        判定（戴帽/摘帽逐年变化，只取当前名称会在历史区间张冠李戴），
        当前数据层未提供，故默认全部走未知分支。
        """
        st_status = dict(st_status or {})
        unknown_st_codes = [c for c in codes if st_status.get(c) is None]
        st_unknown = bool(unknown_st_codes)
        if st_unknown:
            logger.warning(
                "ST 状态未知的标的 {} 只（如 {}），涨跌停按板块默认幅度判定，"
                "结果标记 st_unknown=True、fill_confidence=low；"
                "接入 ST 时间维表后传入 st_status 即可消除该标记。",
                len(unknown_st_codes), unknown_st_codes[:5],
            )
        return st_status, st_unknown

    def _build_signal_columns(
        self, codes: List[str], strategies: Dict[str, Any],
        prepared: Dict[str, pl.DataFrame],
        column_data: Dict[str, Dict[str, list]],
    ) -> tuple[Dict[str, list], List[Dict[str, str]]]:
        """按行序批量预生成各标的信号，返回 (signal_columns, slow_path_strategies)。

        三段路径：
            1. 向量化：策略实现了 generate_signals_vectorized 且非 BaseStrategy 兜底；
            2. BaseStrategy 的默认兜底实现（is_fallback_impl=True），逐行调用；
            3. 鸭子类型策略对象没有向量化接口，逐行适配 generate_signal。

        信号按 prepared[code] 的行序对齐（_ensure_sorted 保证日期升序），主循环
        里的 pos 即该信号在序列中的下标。降级路径的代价是一次回测
        O(N_days × N_symbols) 次 Python 调用 + 行 dict 构造，必须留痕
        （warning + slow_path_strategies），避免调用方对慢速回测无感知。
        """
        signal_columns: Dict[str, list] = {}
        slow_path_strategies: List[Dict[str, str]] = []
        for code in codes:
            strategy = strategies.get(code)
            if strategy is None:
                continue
            vec = getattr(strategy, 'generate_signals_vectorized', None)
            # BaseStrategy 的默认实现内部就是逐行循环调 generate_signal，
            # 性能与降级路径同级，不能因为方法存在就当成向量化快路径。
            is_fallback = vec is not None and getattr(
                getattr(vec, '__func__', vec), 'is_fallback_impl', False)
            if vec is not None and not is_fallback:
                signal_columns[code] = vec(prepared[code]).to_list()
                continue

            strategy_name = getattr(strategy, 'name', None) or type(strategy).__name__
            cols = column_data[code]
            height = len(next(iter(cols.values()), []))
            rows = height * len(cols)
            slow_path_strategies.append({'ts_code': code, 'strategy': strategy_name})
            logger.warning(
                "策略 {} ({}) 未覆写 generate_signals_vectorized，"
                "信号生成退化为逐 bar Python 调用（{} 行 × {} 列 = {} 次取值），"
                "回测耗时为向量化路径的数量级之上（slow_path_strategies）",
                strategy_name, code, height, len(cols), rows,
            )
            if vec is not None:
                # BaseStrategy 的逐行兜底实现，语义等价于下方的鸭子类型适配
                signal_columns[code] = vec(prepared[code]).to_list()
            else:
                signal_columns[code] = [
                    strategy.generate_signal(
                        {name: col[i] for name, col in cols.items()}, i
                    )
                    for i in range(height)
                ]
        return signal_columns, slow_path_strategies

    @staticmethod
    def _build_date_index(
        column_data: Dict[str, Dict[str, list]],
    ) -> tuple[Dict[str, Dict[Any, int]], Dict[Any, List[str]], List[Any]]:
        """建立 date→行号索引与反向索引 date→[codes]，返回三元组。

        用 dict(zip) 替代手动 enumerate 循环；反向索引让主循环按日期直查
        活跃标的，消除笛卡尔遍历下大量 date_positions[code].get(date) ->
        None -> continue 的无效分支（标的上市日期分散时占外层迭代 40~50%）。
        """
        date_positions: Dict[str, Dict[Any, int]] = {}
        date_codes: Dict[Any, List[str]] = {}
        for code, cols in column_data.items():
            dates = cols['date']
            positions = dict(zip(dates, range(len(dates)), strict=True))
            date_positions[code] = positions
            for d in positions:
                bucket = date_codes.setdefault(d, [])
                bucket.append(code)
        return date_positions, date_codes, sorted(date_codes.keys())

    @staticmethod
    def _extract_bar_prices(
        cols: Dict[str, list], pos: int,
    ) -> tuple[float | None, ...]:
        """按行号取当日价格列，返回 (close, open, high, low, volume, raw_open, raw_close)。

        列不存在时回退默认值，语义同原 ``row.get(name, default)``：open 回退为
        close，其余回退 None；列存在但值为 None（null）时透传 None，不回退。
        """
        close_price = cols['close'][pos]
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
        return close_price, open_price, high, low, volume, raw_open, raw_close

    @staticmethod
    def _record_signal(
        *, code: str, date: Any, pos: int, signal_columns: Dict[str, list],
        pending: Dict[str, str | None], expired_orders: List[Dict[str, Any]],
    ) -> None:
        """把当日信号写入挂单；残留挂单被新信号覆盖时记入 expired_orders。

        P0-4：pending 残留挂单被新信号覆盖时必须记账，否则原始信号彻底消失，
        trades 分母不可对账。场景：停牌日 _execute_pending 不执行，pending
        残留 → 下一有信号日直接覆写，无任何日志。
        """
        signals = signal_columns.get(code)
        if signals is None:
            return
        signal = signals[pos]
        if signal not in ('buy', 'sell'):
            return
        old = pending.get(code)
        if old is not None and old != signal:
            expired_orders.append({
                'date': date, 'side': old, 'ts_code': code,
                'reason': 'overwritten', 'qty': 0,
            })
        pending[code] = signal

    def _run_trading_loop(
        self, *, sorted_dates: List[Any], date_codes: Dict[Any, List[str]],
        date_positions: Dict[str, Dict[Any, int]],
        column_data: Dict[str, Dict[str, list]],
        pending: Dict[str, str | None], prev_close: Dict[str, float | None],
        prev_raw_close: Dict[str, float | None], book: AccountBook,
        gate: RiskGate, rules: ExecutionRules, trades: List[Dict[str, Any]],
        rejected_orders: List[Dict[str, Any]],
        expired_orders: List[Dict[str, Any]],
        equity_curve: List[Dict[str, Any]], lot_size: int,
        st_status: Dict[str, bool | None], signal_columns: Dict[str, list],
    ) -> None:
        """逐日推进主循环：执行挂单、记账信号、更新 prev_close/盯市价、追加权益。"""
        for trade_day in sorted_dates:
            for code in date_codes[trade_day]:
                pos = date_positions[code][trade_day]
                cols = column_data[code]
                (close_price, open_price, high, low,
                 volume, raw_open, raw_close) = self._extract_bar_prices(cols, pos)

                self._execute_pending(
                    code=code, date=trade_day, pending=pending, book=book, gate=gate,
                    rules=rules, cost_model=self.cost_model, trades=trades,
                    rejected_orders=rejected_orders, expired_orders=expired_orders,
                    equity_curve=equity_curve,
                    lot_size=lot_size,
                    prev_close=prev_close[code], open_price=open_price,
                    high=high, low=low, close=close_price, volume=volume,
                    open_raw=raw_open, prev_close_raw=prev_raw_close[code],
                    close_raw=raw_close,
                    is_st=st_status.get(code),
                )

                # 策略基于当日数据生成信号，延迟到该标的下一个交易日执行；
                # 信号已在主循环前按行序批量生成，此处仅按下标查表
                self._record_signal(
                    code=code, date=trade_day, pos=pos, signal_columns=signal_columns,
                    pending=pending, expired_orders=expired_orders,
                )

                # close 缺失 / 非有限 / ≤0 时，挂单已在 _execute_pending 里过期；
                # 此处不更新 prev_close / book.mark，保留上一个有效值
                if close_price is None or not math.isfinite(close_price) or close_price <= 0:
                    continue
                prev_close[code] = close_price
                prev_raw_close[code] = raw_close if raw_close is not None else close_price
                book.mark(code, close_price)

            equity_curve.append({'date': trade_day, 'equity': book.total_equity()})

    def _build_result(
        self, *, initial_capital: float, equity_curve: List[Dict[str, Any]],
        trades: List[Dict[str, Any]], rejected_orders: List[Dict[str, Any]],
        expired_orders: List[Dict[str, Any]], book: AccountBook,
        slow_path_strategies: List[Dict[str, str]], open_missing: bool,
        st_unknown: bool, result_meta: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        """组装结果字典（含 fill_confidence / st_unknown / result_meta 合并）并输出完成日志。"""
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
            # 最终持仓直接取自 AccountBook 账本（唯一真源），
            # 不在引擎侧用 trades 反推，避免账本外再维护一份持仓状态
            'positions': dict(book.positions),
            # 慢速路径标记：非空表示这些策略未实现 generate_signals_vectorized，
            # 信号走了逐 bar Python 调用；空列表表示全部走向量化路径
            'slow_path_strategies': slow_path_strategies,
            # 缺陷 C：整次回测任一标的缺 open 列 → 开盘涨停判定退化，
            # 成交假设偏乐观；ST 状态未知时同样整体偏乐观/保守不可全信
            'fill_confidence': 'low' if (open_missing or st_unknown) else 'high',
            # ST 状态是否未知：True 表示本次回测按 5% 保守判定涨跌停，
            # 逐笔 fill_confidence 也相应为 low，调用方应据此解读成交假设
            'st_unknown': st_unknown,
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
                         close_raw: float | None = None,
                         is_st: bool | None = None) -> None:
        """执行该标的的挂单。

        订单生命周期语义（显式状态机，杜绝静默丢单）：
            - 标的停牌无数据行时不进入本函数，挂单顺延至下一交易日；
            - 进入本函数即消费挂单，当日有效：成交进 trades、被风控拦截进
              rejected_orders、其余不可成交情形一律作废并记入 expired_orders
              （already_holding / no_position / not_tradable / price_missing /
              insufficient_cash），不会残留到下一交易日。

        is_st 为 None 表示 ST 状态未知：涨跌停按板块默认幅度判定（与明确
        非 ST 相同），该笔订单 fill_confidence 标记为 low，作为数据质量提示。
        """
        side = pending.pop(code, None)
        if side is None:
            return

        position = book.positions.get(code, 0)
        # is_st 为 None 表示 ST 状态未知：该笔订单 fill_confidence 标记为 low
        # （仅数据质量提示，涨跌停仍按板块默认幅度判定）
        order_confidence = 'low' if is_st is None else 'high'

        reason = self._check_order_executable(
            code=code, side=side, position=position, rules=rules,
            prev_close=prev_close, open_price=open_price, high=high, low=low,
            close=close, volume=volume, open_raw=open_raw,
            prev_close_raw=prev_close_raw, close_raw=close_raw, is_st=is_st,
            trade_date=to_trade_date(date),
        )
        if reason is not None:
            self._append_expired(
                expired_orders, date=date, side=side, code=code,
                reason=reason, fill_confidence=order_confidence,
            )
            return

        # 校验通过后 open_price 必为有效价，作为成交价
        # （_check_order_executable 已把缺失/非法价判为不可成交并提前返回）
        assert open_price is not None
        exec_price = open_price

        if side == 'buy':
            self._execute_buy(
                code=code, date=date, book=book, gate=gate,
                cost_model=cost_model, trades=trades,
                rejected_orders=rejected_orders, expired_orders=expired_orders,
                equity_curve=equity_curve, lot_size=lot_size,
                exec_price=exec_price, order_confidence=order_confidence,
            )
            return

        # side == 'sell'（持仓前置条件已在 _check_order_executable 校验）
        self._execute_sell(
            code=code, date=date, book=book, gate=gate, cost_model=cost_model,
            trades=trades, rejected_orders=rejected_orders,
            equity_curve=equity_curve, exec_price=exec_price,
            order_confidence=order_confidence,
        )

    @staticmethod
    def _is_valid_price(price: float | None) -> bool:
        """价格是否有效：None / NaN / inf / ≤0 均视为缺失。"""
        return price is not None and math.isfinite(price) and price > 0

    @staticmethod
    def _append_expired(expired_orders: List[Dict[str, Any]], *, date: Any,
                        side: str, code: str, reason: str,
                        fill_confidence: str, qty: int = 0) -> None:
        """追加一条作废订单记录（字段与顺序固定，供对账使用）。"""
        expired_orders.append({
            'date': date, 'side': side, 'ts_code': code,
            'reason': reason, 'qty': qty,
            'fill_confidence': fill_confidence,
        })

    def _check_order_executable(
        self, *, code: str, side: str, position: int, rules: ExecutionRules,
        prev_close: float | None, open_price: float | None,
        high: float | None = None, low: float | None = None,
        close: float | None = None, volume: float | None = None,
        open_raw: float | None = None, prev_close_raw: float | None = None,
        close_raw: float | None = None, is_st: bool | None = None,
        trade_date: date | None = None,
    ) -> str | None:
        """按固定顺序校验订单可执行性，返回作废原因；None 表示可继续撮合。

        判定顺序（与订单生命周期一致，不可调换）：
            1. 持仓前置：买入已持仓 → already_holding；卖出无持仓 → no_position；
            2. close 缺失 / 非有限 / ≤0 → price_missing；
            3. 涨跌停 / 停牌（零成交量）→ not_tradable；
            4. 开盘价缺失 / 非有限 / ≤0 → price_missing（不得用 close 伪造成交价）。

        交易所规则定义在原始价上：携带 raw_* 列（hfq 复权输入）时用原始价判定，
        除权日因子跳变会让复权比值失真；high == low 的一字板等式判断在线性
        缩放下不变，可直接透传。
        """
        # 持仓前置条件不满足：已持仓的买入、无持仓的卖出
        if side == 'buy' and position > 0:
            return 'already_holding'
        if side == 'sell' and position == 0:
            return 'no_position'

        # close 缺失 = 无有效行情，挂单作废（主循环已 continue 跳过整行，
        # 此处防御性守卫，确保函数签名 close=None 不会漏进来）
        if not self._is_valid_price(close):
            return 'price_missing'

        check_open, check_prev, check_close = self._select_check_prices(
            open_price, prev_close, close, open_raw, prev_close_raw, close_raw)
        if not rules.is_tradable(side, prev_close=check_prev, open_price=check_open,
                                 high=high, low=low, close=check_close, volume=volume,
                                 ts_code=code, is_st=is_st, trade_date=trade_date):
            return 'not_tradable'

        # P0-2 修复：开盘价是集合竞价成交价。open_price 为 None（列存在但值缺失，
        # 如停牌复牌日、除权日）时，不能用 close 伪造成交价——回测不应发明价格。
        # 列整体缺失时 open_price 已在主循环回填为 close_price + fill_confidence=low，
        # 此处不会走到 None 分支。
        if not self._is_valid_price(open_price):
            return 'price_missing'
        return None

    @staticmethod
    def _select_check_prices(
        open_price: float | None, prev_close: float | None, close: float | None,
        open_raw: float | None, prev_close_raw: float | None,
        close_raw: float | None,
    ) -> tuple[float | None, float | None, float | None]:
        """涨跌停判定用价：有原始价（hfq 输入）时优先原始价，否则用主列价。"""
        check_open = open_raw if open_raw is not None else open_price
        check_prev = prev_close_raw if prev_close_raw is not None else prev_close
        check_close = close_raw if close_raw is not None else close
        return check_open, check_prev, check_close

    def _execute_buy(
        self, *, code: str, date: Any, book: AccountBook, gate: RiskGate,
        cost_model: CostModel, trades: List[Dict[str, Any]],
        rejected_orders: List[Dict[str, Any]],
        expired_orders: List[Dict[str, Any]],
        equity_curve: List[Dict[str, Any]], lot_size: int,
        exec_price: float, order_confidence: str,
    ) -> None:
        """买入执行：按可用现金估算整手、逐手回退成本、风控审查、记账成交。"""
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
            self._append_expired(
                expired_orders, date=date, side='buy', code=code,
                reason='insufficient_cash', fill_confidence=order_confidence,
            )
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
            'fill_confidence': order_confidence,
        })

    def _execute_sell(
        self, *, code: str, date: Any, book: AccountBook, gate: RiskGate,
        cost_model: CostModel, trades: List[Dict[str, Any]],
        rejected_orders: List[Dict[str, Any]],
        equity_curve: List[Dict[str, Any]], exec_price: float,
        order_confidence: str,
    ) -> None:
        """卖出执行：清空该标的持仓，风控审查通过后记账成交。"""
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
            'fill_confidence': order_confidence,
        })

    @staticmethod
    def _ensure_sorted(df: pl.DataFrame) -> pl.DataFrame:
        """按日期升序，保证日期索引与执行顺序一致。"""
        if 'date' in df.columns and not df['date'].is_sorted():
            return df.sort('date')
        return df
