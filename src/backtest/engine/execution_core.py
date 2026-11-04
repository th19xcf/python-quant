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
    6. 有 raw_open / raw_close（hfq 出口保留的原始价）时，成交与盯市一律
       使用原始价，股数取整与最低佣金的经济口径与交易所名义价一致；
       没有 raw_* 列时退化为使用主列价
    7. 信号既可以是 'buy'/'sell'/'hold' 字符串（全仓进/全仓出），也可以是
       {'action': 'buy'|'sell', 'fraction': 0~1, 'target_weight': 0~1}
       映射（分批建仓 / 金字塔加仓 / 部分减仓）
    8. 除权除息（分红 / 送转 / 配股）只在原始价口径下记账：每个交易日撮合前
       按 ex_date 把现金分红计入现金、送转计入股数、配股扣减现金，权益始终为
       现金 + Σ(股数 × 原始收盘价)。信号/指标用的后复权价已含分红，禁止再
       叠加本事件表（双重计入）；未传事件表时行为完全不变

横截面（组合级）策略可通过 ``portfolio_strategy`` 入口接入：它一次性看到
全部标的，返回 {ts_code: 信号序列}，由内核与单标的信号合并后统一撮合。
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, NamedTuple, Set, Tuple, TypeIs

import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.corporate_actions import (
    RIGHTS_POLICIES,
    CorporateAction,
    CorporateActionSchedule,
)
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.risk import Order as RiskOrder
from src.risk import Portfolio as RiskPortfolio
from src.risk import RiskEngine
from src.risk.rules import DrawdownTracker

#: A 股最小价格变动单位（1 分），交易所涨跌停价的取整单位
PRICE_TICK = Decimal('0.01')

#: 现金/持仓记账的浮点容差（仅吸收乘加误差）
CASH_EPS = 1e-6

#: 挂单在标的无行情行时的默认存活天数：超过该天数仍未获得可撮合 bar
#: 即作废，避免复牌后按数月前的信号成交
DEFAULT_PENDING_TTL_DAYS = 5

#: ST 状态条目：
#:   - bool / None：静态三态（None=未知）
#:   - List[dict]：时间区间列表，每项含 start_date / end_date / is_st，
#:     end_date=None 表示当前仍有效；区间外按未知（None）处理。
StStatusEntry = bool | None | List[Dict[str, Any]]
StStatus = Dict[str, StStatusEntry]


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


def _finite_float(value: Any) -> float | None:
    """把行情值归一为有限 float；None / NaN / inf / 非数值一律返回 None。

    回测数据可能混入 Decimal、字符串或 numpy 标量，直接参与比较或
    math.isfinite 会抛 TypeError；统一在入口处归一，后续热路径只处理 float。
    """
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


@dataclass(frozen=True, slots=True)
class SignalIntent:
    """一条归一化后的委托意图。

    - 字符串 'buy'/'sell' 归一为 ``explicit=False`` 的状态型信号，仍按持仓
      状态过滤（持仓期间持续 buy、空仓期间持续 sell 不构成新委托）；
    - 映射信号归一为 ``explicit=True`` 的显式意图，允许在持仓上加仓、
      在持仓上部分减仓。
    """

    action: str
    fraction: float = 1.0
    target_weight: float | None = None
    explicit: bool = False


class Bar(NamedTuple):
    """单标单单日的撮合上下文（避免 19 个关键字参数层层透传）。"""

    open_price: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    raw_open: float | None
    raw_close: float | None
    prev_close: float | None
    prev_close_raw: float | None
    is_st: bool | None
    suspended: Set[str] | None
    #: 20 日均成交量（手），供 MaxAdvParticipation 容量规则使用
    adv_volume: float | None = None


class ExecutionRules:
    """A 股撮合规则：涨跌停幅度推导 + 当日可交易性判定。"""

    #: ADV 窗口（交易日）：容量规则统一按 20 日均成交量估算
    ADV_WINDOW = 20

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
    def _resolve_board_limit_pct(ts_code: str, trade_date: date | None) -> float:
        """按证券代码与交易日推导该板块常规（非 ST）涨跌停幅度。

        统一以 ts_code 为唯一标识符（A22：不再同时接收 code 与 ts_code），
        代码段由 ts_code 内部派生。

        判定顺序固定：北交所 → 创业板（含注册制改革日切换）→ BOARD_RULES
        显式号段（科创板 / B 股）→ 主板兜底。
        """
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
    def resolve_limit_pct(ts_code: str = '', is_st: bool | None = None,
                          trade_date: date | None = None,
                          listing_date: date | None = None) -> float:
        """
        按交易日 → 板块常规幅度 → ST 状态的顺序推导涨跌停幅度（唯一真源）。

        规则（判断顺序：交易日期 → 板块及当时有效制度 → ST 状态）：
        - 先按代码与交易日推导板块常规幅度：主板 10%、科创板 20%、
          创业板改革前（< 2020-08-24）10% / 改革日起 20%、北交所 30%、
          沪深 B 股 10%；
        - 再叠加 ST 状态：只有常规幅度为 10% 的板块（主板、B 股、改革前
          创业板）的 ST 股票才收窄到 5%；常规幅度为 20%/30% 的板块（改革后
          创业板、科创板、北交所）其风险警示股票涨跌幅与正常股票一致，不另
          设 5%——这与 A 股现行风险警示规则一致；
        - ``is_st=None``（数据层未提供 ST 标识）按板块常规幅度判定，未知状态
          只体现为结果里的 st_unknown=True / fill_confidence=low，不改变成交
          判定本身。

        ``listing_date`` 预留用于新股上市首日不限涨跌幅的判定，当前数据层未
        提供，暂未启用；参数先固化以便未来接入 (code, listing_date) 维表时无需
        改动签名。
        """
        ts_code = (ts_code or '').strip().upper()
        if not ts_code.split('.')[0]:
            return ExecutionRules.DEFAULT_LIMIT_PCT

        regular = ExecutionRules._resolve_board_limit_pct(ts_code, trade_date)

        # 仅常规幅度为 10% 的板块，ST 才收窄到 5%；
        # 20%/30% 板块（改革后创业板、科创板、北交所）的 ST 股票与普通股同幅度
        if is_st and regular == 0.10:
            return ExecutionRules.ST_LIMIT_PCT
        return regular

    @staticmethod
    def resolve_sector(ts_code: str = '', is_st: bool | None = None,
                       trade_date: date | None = None) -> str:
        """
        推导风控侧板块标签（Order.sector 取值），与 resolve_limit_pct 同源。

        PriceLimitTradable 按 sector 查涨跌停档位，因此标签必须满足：
        ``PriceLimitTradable.DEFAULT_LIMITS[sector] == resolve_limit_pct(...)``。
        只有常规幅度为 10% 的板块（主板 / B 股 / 改革前创业板）的 ST 股票
        才映射为 'ST'（5%），20%/30% 板块的 ST 与普通股同档，沿用板块标签。

        无法识别的代码统一按主板（10%）——与 resolve_limit_pct 的兜底一致。
        """
        ts_code = (ts_code or '').strip().upper()
        code = ts_code.split('.')[0]
        if not code:
            return '主板'

        regular = ExecutionRules._resolve_board_limit_pct(ts_code, trade_date)
        if is_st and regular == ExecutionRules.DEFAULT_LIMIT_PCT:
            return 'ST'

        if ts_code.endswith('.BJ') or code.startswith(ExecutionRules.BJ_PREFIXES):
            return '北交所'
        for prefixes, _pct, board in ExecutionRules.BOARD_RULES:
            if code.startswith(prefixes):
                if board == 'STAR':
                    return '科创板'
                if board == 'CHINEXT':
                    # 注册制改革前的创业板常规幅度为 10%，按主板档位审查
                    return '创业板' if regular == 0.20 else '主板'
                # B 股（SH_B / SZ_B）在风控侧按主板 10% 口径
                return '主板'
        return '主板'

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

        - 停牌（成交量为 0 / NaN / inf / 非数值）一律不可交易
        - 有开盘价时：开盘即涨停不可买、开盘即跌停不可卖
          （覆盖一字板 + 开盘封板后盘中打开两种情形）
        - 无开盘价时退化为 high ≈ low（相对容差）的一字板判断，
          此时只做 close 涨跌停判定（偏乐观），调用侧应确保 open 列存在
        - 首根 K 线没有前收盘价，只做停牌判断
        - is_st=True 时，常规幅度为 10% 的板块（主板、B 股、改革前创业板）
          收窄为 5%，20%/30% 板块（改革后创业板、科创板、北交所）与普通股
          同幅度；None（数据层无标记）与 False 都按板块规则（见
          resolve_limit_pct），未知状态只由结果的
          st_unknown / fill_confidence 标记，不再改变成交判定本身
        """
        # 缺陷 A 修复：NaN / inf 穿过停牌检查，显式拦截
        if self._is_suspended(volume):
            return False

        prev = _finite_float(prev_close)
        if prev is None or prev <= 0:
            return True

        limit = self.resolve_limit_pct(ts_code, is_st, trade_date=trade_date)

        open_number = _finite_float(open_price)
        if open_number is not None:
            # 价格空间比较：涨跌停价 = round(prev_close × (1±limit), 2) 后直接比较。
            # 原写法 (open_pct >= limit - 0.005) 的容差被放大 prev_close 倍，
            # 3 元股等价于允许 1.5% 偏差、200 元股允许 0.25% 偏差，
            # 结果是把 [limit-0.5%, limit) 的正常开盘价误当成封板双向拒绝。
            return not self._limit_breached(
                side, price=open_number, prev_close=prev, limit=limit)

        # 缺陷 B 修复：复权后 high / low 可能相差 1e-16，改用相对容差
        # 缺陷 C 修复：open_price 缺失时只退化到此分支，偏乐观
        return not self._is_one_line_board_locked(
            side, high=high, low=low, close=close,
            prev_close=prev, limit=limit)

    @staticmethod
    def _is_suspended(volume: float | None) -> bool:
        """成交量为 None / NaN / inf / 非数值 / ≤0 视为停牌，不可交易。"""
        number = _finite_float(volume)
        return number is None or number <= 0

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

        high / low / close 任一缺失或非数值时不做封板判定（视为可交易）。
        """
        high_number = _finite_float(high)
        low_number = _finite_float(low)
        close_number = _finite_float(close)
        if high_number is None or low_number is None or close_number is None:
            return False
        if abs(high_number - low_number) / prev_close >= 1e-6:
            return False
        return self._limit_breached(side, price=close_number,
                                    prev_close=prev_close, limit=limit)


class AccountBook:
    """
    账户账本：按标的拆分现金桶（组合等权分配），持仓、最新价与权益估值统一在此维护。

    单票回测只有一个现金桶，等价于原来的全局 current_capital。

    权益为增量维护：每次 buy/sell/mark 只更新变化量，total_equity() 为 O(1)
    （原实现每日对所有标的全量求和）。
    """

    def __init__(self, initial_capital: float, codes: List[str],
                 industry_map: Dict[str, str] | None = None):
        self.codes = list(codes)
        self.industry_map = dict(industry_map or {})
        per_code = initial_capital / len(self.codes) if self.codes else 0.0
        self.cash: Dict[str, float] = dict.fromkeys(self.codes, per_code)
        self.positions: Dict[str, int] = dict.fromkeys(self.codes, 0)
        self.last_prices: Dict[str, float] = {}
        self._equity: float = float(sum(self.cash.values()))
        # 回撤增量跟踪：snapshot 时只消费新增 bar，避免每笔订单重扫整条曲线
        self._drawdown = DrawdownTracker()

    @property
    def total_cash(self) -> float:
        return sum(self.cash.values())

    def position(self, code: str) -> int:
        """读取某标的当前持仓（0 表示空仓），替代直接访问原始 positions 字典。"""
        return self.positions.get(code, 0)

    def mark(self, code: str, price: float | None) -> None:
        """更新盯市价（收盘后调用），同时维护增量权益。"""
        number = _finite_float(price)
        if number is None or number <= 0:
            return
        old = self.last_prices.get(code)
        baseline = number if old is None else old
        self._equity += self.position(code) * (number - baseline)
        self.last_prices[code] = number

    def buy(self, code: str, price: float, shares: int, cost: float) -> None:
        """买入：现金扣除成交额与买方成本，持仓增加。

        A25：账本自身校验资金充足性与股数合法性，不再只依赖调用方保证。
        """
        if shares <= 0:
            raise ValueError(f"{code} 买入股数必须为正，实际 {shares}")
        required = shares * price + cost
        if required > self.cash.get(code, 0.0) + CASH_EPS:
            raise ValueError(
                f"{code} 买入资金不足：需要 {required:.6f}，"
                f"可用 {self.cash.get(code, 0.0):.6f}"
            )
        baseline = self.last_prices.get(code)
        if baseline is None:
            baseline = price
            self.last_prices[code] = price
        self.cash[code] -= required
        # 成交价与盯市价之差（滑点/折价）即时体现，成本按笔扣除
        self._equity += shares * (baseline - price) - cost
        self.positions[code] = self.position(code) + shares

    def sell(self, code: str, price: float, shares: int, cost: float) -> None:
        """卖出：所得净额叠加回该标的现金桶（不能覆盖买入后剩余现金）。

        A25 同时支持部分卖出（A12）：按股数递减持仓，而不是一律清零。
        """
        position = self.position(code)
        if shares <= 0:
            raise ValueError(f"{code} 卖出股数必须为正，实际 {shares}")
        if shares > position:
            raise ValueError(
                f"{code} 卖出股数 {shares} 超过当前持仓 {position}"
            )
        baseline = self.last_prices.get(code)
        if baseline is None:
            baseline = price
            self.last_prices[code] = price
        self.cash[code] += shares * price - cost
        self._equity += shares * (price - baseline) - cost
        self.positions[code] = position - shares

    def apply_corporate_action(
        self, code: str, *, cash_per_share: float, share_ratio: float,
        rights_ratio: float = 0.0, rights_price: float = 0.0,
        tax_rate: float = 0.0, rights_policy: str = 'partial',
    ) -> Dict[str, Any]:
        """按除权除息事件调整现金与股数（成本总额不变）。

        记账口径（与权益估值保持同源）：
            - 现金分红：``现金 += 每股派息 × 持仓股数 × (1 - 税率)``；
            - 送转：``股数 += floor(每股送转股数 × 持仓股数)``，成本总额不变；
            - 配股：按配股比例认购、按配股价扣现金；现金不足按
              ``rights_policy`` 部分认购或放弃，未认购部分记录在案。

        权益为增量维护：除权日开盘前持仓仍按「上一交易日收盘价」（含权价）
        计价，因此增加股数必须同时按该含权价补足权益，等收盘按除权价盯市后
        自然回到中性；现金分红同理（现金增加、收盘价下跌相互抵消）。

        Returns:
            Dict[str, Any]: 本次事件的记账明细（写入回测结果的
            ``corporate_actions`` 日志，供逐笔对账）。
        """
        if rights_policy not in RIGHTS_POLICIES:
            raise ValueError(
                f"rights_policy 必须是 {RIGHTS_POLICIES} 之一，实际 {rights_policy!r}")
        if not 0.0 <= tax_rate <= 1.0:
            raise ValueError(f"dividend_tax_rate 必须在 [0, 1]，实际 {tax_rate}")

        position = self.position(code)
        baseline = self.last_prices.get(code)
        record: Dict[str, Any] = {
            'ts_code': code,
            'shares_before': position,
            'cash_per_share': cash_per_share,
            'share_ratio': share_ratio,
            'rights_ratio': rights_ratio,
            'rights_price': rights_price,
            'cash_dividend': 0.0,
            'bonus_shares': 0,
            'rights_shares': 0,
            'rights_cost': 0.0,
            'rights_abandoned_shares': 0,
            'shares_after': position,
        }
        if position <= 0:
            return record

        # 1) 现金分红（按除权日计提应收，税后入账）
        cash_dividend = cash_per_share * position * (1.0 - tax_rate)
        if cash_dividend > 0:
            self.cash[code] = self.cash.get(code, 0.0) + cash_dividend
            self._equity += cash_dividend
        record['cash_dividend'] = cash_dividend

        # 2) 送转：股数增加、成本总额不变
        bonus_shares = int(math.floor(share_ratio * position + CASH_EPS))
        bonus_shares = max(0, bonus_shares)
        added_value = bonus_shares * baseline if baseline is not None else 0.0
        self._equity += added_value
        record['bonus_shares'] = bonus_shares

        # 3) 配股：认购股数受现金约束，现金不足时显式部分认购/放弃
        rights_entitled = int(math.floor(rights_ratio * position + CASH_EPS))
        rights_entitled = max(0, rights_entitled)
        rights_shares = 0
        rights_cost = 0.0
        if rights_entitled > 0:
            available_cash = max(0.0, self.cash.get(code, 0.0))
            affordable = (
                int(math.floor(available_cash / rights_price + CASH_EPS))
                if rights_price > 0 else rights_entitled
            )
            if rights_policy == 'abandon':
                rights_shares = 0
            else:  # 'partial'
                rights_shares = min(rights_entitled, max(0, affordable))
            rights_cost = rights_shares * rights_price
            self.cash[code] = self.cash.get(code, 0.0) - rights_cost
            if baseline is not None:
                self._equity += rights_shares * baseline - rights_cost
            else:
                self._equity -= rights_cost
        record['rights_shares'] = rights_shares
        record['rights_cost'] = rights_cost
        record['rights_abandoned_shares'] = rights_entitled - rights_shares

        self.positions[code] = position + bonus_shares + rights_shares
        record['shares_after'] = self.positions[code]
        return record

    def total_equity(self) -> float:
        """权益 = 各桶现金之和 + 各持仓按最新价盯市（增量维护，O(1)）。"""
        return self._equity

    def snapshot(self, equity_curve: List[Dict[str, Any]],
                 suspended: Set[str] | None = None) -> RiskPortfolio:
        """构造风控用的组合快照。

        equity_curve 共享调用方的列表引用、不做复制：风控规则只切片读取尾部
        window（如 MaxDrawdown），快照对象在同一次订单审查结束即被丢弃。
        原实现在这里 list(equity_curve) 整体复制，每笔候选订单一次，
        10 年（T≈2400）× 日均 m 笔时累计复制 O(mT²) 个元素，是长周期
        组合回测的主要纯开销。

        suspended 为当日停牌标的集合（由撮合循环按成交量口径注入），
        供 NoSuspendedTrade 规则审查；未传时为空集，不改变存量行为。
        """
        # 增量同步权益曲线（通常每日 1 条），回撤规则读取时为 O(1)
        self._drawdown.sync(equity_curve)
        return RiskPortfolio(
            positions={code: qty for code, qty in self.positions.items() if qty > 0},
            last_prices=dict(self.last_prices),
            cash=self.total_cash,
            industry_map=dict(self.industry_map),
            equity_curve=equity_curve,
            suspended=set(suspended or ()),
            drawdown_tracker=self._drawdown,
        )


class RiskGate:
    """风控闸门：唯一的 RiskOrder 构造与 gate() 调用入口。"""

    def __init__(self, risk_engine: RiskEngine | None, industry_map: Dict[str, str] | None = None):
        self.risk_engine = risk_engine
        self.industry_map = dict(industry_map or {})

    def check(self, *, ts_code: str, side: str, qty: int, price: float,
              trade_date: Any, book: AccountBook,
              equity_curve: List[Dict[str, Any]],
              bar_volume: float | None = None,
              adv_volume: float | None = None,
              prev_close: float | None = None,
              sector: str = '',
              suspended: Set[str] | None = None) -> Dict[str, Any] | None:
        """
        审查一笔候选订单。

        bar_volume 为当日 Bar 成交量，供 MaxOrderVolumeRatio 等流动性规则使用；
        prev_close 为原始价口径前收盘价（PriceLimitTradable 用），sector 为
        resolve_sector 推导的板块标签；两者都必须由调用方注入，否则
        PriceLimitTradable 会按 fail-closed 拒绝全部订单。
        suspended 为当日停牌集合，供 NoSuspendedTrade 使用。

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
            sector=sector,
            industry=self.industry_map.get(ts_code, ''),
            trade_date=trade_date,
            prev_close=prev_close,
            bar_volume=bar_volume,
            adv_volume=adv_volume,
        )
        gate = self.risk_engine.gate([order], book.snapshot(equity_curve, suspended))
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


@dataclass(slots=True)
class RunContext:
    """一次回测的可变状态容器：账本、规则、风控与全部日志/曲线列表。

    A23：把原先 14~19 个同构关键字参数在多层函数间反复透传改为单一上下文，
    新增字段（overwritten_orders / filtered_signals / pending_age 等）无需
    再修改每一层签名。
    """

    book: AccountBook
    gate: RiskGate
    rules: ExecutionRules
    cost_model: CostModel
    lot_size: int
    equity_curve: List[Dict[str, Any]]
    trades: List[Dict[str, Any]]
    rejected_orders: List[Dict[str, Any]]
    expired_orders: List[Dict[str, Any]]
    overwritten_orders: List[Dict[str, Any]]
    filtered_signals: List[Dict[str, Any]]
    #: 挂单槽位：None 表示该标的当前无待执行信号（成交/作废后即清空）
    pending: Dict[str, SignalIntent | None]
    pending_age: Dict[str, int]
    prev_close: Dict[str, float | None]
    prev_raw_close: Dict[str, float | None]
    pending_ttl_days: int
    st_unknown_bars: List[Tuple[str, Any]]
    #: 除权除息事件表（None 表示本次回测不记账分红/送转/配股）
    corporate_actions: CorporateActionSchedule | None
    dividend_tax_rate: float
    rights_policy: str
    #: 各标的已处理到的除权事件下标（按 ex_date 升序游标）
    corporate_action_cursor: Dict[str, int]
    #: 逐笔除权记账日志（写入结果，供逐笔对账）
    corporate_action_log: List[Dict[str, Any]]


class EventBacktestRunner:
    """
    通用事件循环：多标的日频回测的唯一撮合实现。

    输入 universe 必须已完成指标预计算（策略按列名读取 ma_* / macd / rsi 等）。
    ``portfolio_strategy`` 可选，用于表达横截面/组合级策略（A11）。
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
            st_status: StStatus | None = None,
            portfolio_strategy: Any | None = None,
            pending_ttl_days: int = DEFAULT_PENDING_TTL_DAYS,
            corporate_actions: CorporateActionSchedule | None = None,
            dividend_tax_rate: float = 0.0,
            rights_policy: str = 'partial') -> Dict[str, Any]:
        """
        执行回测并返回结果字典。

        结果固定包含：initial_capital / final_equity / total_return / trades /
        equity_curve / performance / risk_summary / rejected_orders /
        expired_orders / overwritten_orders / filtered_signals / positions /
        slow_path_strategies / st_unknown / st_unknown_bars。

        result_meta 里的键会合并进结果（如 strategy_name / strategy_names）。

        Args:
            portfolio_strategy: 横截面（组合级）策略对象，一次性看到全部标的。
                - 覆写 ``generate_signals_vectorized(universe)`` 时返回
                  {ts_code: 信号序列}，序列长度必须与该标的行数一致；
                - 只提供 ``generate_signal(universe_rows, index)`` 时按日兜底调用，
                  返回 {ts_code: 信号}，并计入 slow_path_strategies。
                它产出的信号优先于同标的的单标的策略信号。
            pending_ttl_days: 挂单在标的连续无行情行时的最大存活天数（默认 5），
                超过即作废（reason='pending_ttl'），避免复牌后按数月前信号成交。

        st_status 为 (标的名 → ST 三态) 映射：True=ST、False=明确非 ST、
        未出现在映射中=状态未知。未知标的涨跌停按板块默认幅度判定（与
        False 相同），只把结果的 st_unknown 置 True、fill_confidence 降为
        low，并逐条记入 st_unknown_bars。

        universe 价格口径（双价格列契约）：
            - open/high/low/close 为指标与收益计算用价（DataFetcher hfq 出口
              即为后复权价）；
            - 若携带 raw_open / raw_close（hfq 出口自动保留的原始价），
              涨跌停判定与成交/盯市一律使用原始价——交易所规则定义在原始价上，
              且整手取整与最低佣金必须落在名义价上（A16）；两列必须成对出现；
            - 无 raw_* 列时视为原始价输入（如 TDX 本地数据），直接用主列判定。

        除权除息（``corporate_actions``）：
            只在**原始价**口径下启用（成交/盯市本来就是原始价）。传入事件表后，
            每个交易日在撮合前按 ``ex_date ≤ 当日`` 的顺序记账：现金分红入现金、
            送转加股数、配股扣现金。**禁止**在 hfq 价格序列上再叠加本事件表，
            否则分红会被计入两次。未传事件表时行为与此前完全一致。
        """
        if pending_ttl_days < 0:
            raise ValueError(f"pending_ttl_days 必须 ≥ 0，实际 {pending_ttl_days}")
        if not 0.0 <= dividend_tax_rate <= 1.0:
            raise ValueError(f"dividend_tax_rate 必须在 [0, 1]，实际 {dividend_tax_rate}")
        if rights_policy not in RIGHTS_POLICIES:
            raise ValueError(f"rights_policy 必须是 {RIGHTS_POLICIES} 之一，实际 {rights_policy!r}")

        prepared_data = self._prepare_universe(universe, strategies, portfolio_strategy)
        # 空 universe / 无标的配置策略时 _prepare_universe 返回 None，提前返回
        if prepared_data is None:
            return {}
        prepared, column_data = prepared_data
        codes = list(column_data.keys())

        # 缺陷 C：整次回测任一标的缺 open 列 → 标记 fill_confidence 为 low
        _open_missing = self._check_open_column(column_data)

        # ST 三态：True=ST、False=明确非 ST、未提供/区间外=未知（按板块默认判定）
        st_status, st_unknown = self._resolve_st_status(codes, st_status)

        # 信号批量预生成：每个标的只调用一次向量化策略接口，主循环退化为
        # 数组下标取值，消除 N_days × N_symbols 次逐行策略调用与行 dict 组装。
        # 降级路径必须留痕（slow_path_strategies + 聚合 warning）
        signal_columns, slow_path_strategies = self._build_signal_columns(
            codes, strategies, prepared, column_data, portfolio_strategy)

        # 一次性建立按日期对齐的 (活跃标的, 行号) 平行数组，主循环按下标直取
        day_codes, day_positions, sorted_dates = self._build_date_index(column_data)

        # 各标的的列引用预先解析成元组，主循环内不再重复做 dict.get
        bar_columns = {
            code: (cols['close'], cols.get('open'), cols.get('high'), cols.get('low'),
                   cols.get('volume'), cols.get('raw_open'), cols.get('raw_close'),
                   cols.get('adv_volume'))
            for code, cols in column_data.items()
        }

        rules, book, gate = self._create_execution_context(
            codes, strategies, initial_capital, industry_map, lot_size,
            portfolio_strategy,
        )
        ctx = self._init_run_context(codes, rules, book, gate, lot_size,
                                     pending_ttl_days, corporate_actions,
                                     dividend_tax_rate, rights_policy)

        loop = self._run_trading_loop(
            ctx=ctx, sorted_dates=sorted_dates, day_codes=day_codes,
            day_positions=day_positions, column_data=column_data,
            bar_columns=bar_columns, st_status=st_status,
            signal_columns=signal_columns,
        )
        # 区间模式下，区间外日期仍为未知；loop 内逐 bar 判定并回传该标记
        loop_st_unknown, st_unknown_bars = loop
        st_unknown = loop_st_unknown or st_unknown

        return self._build_result(
            initial_capital=initial_capital, ctx=ctx,
            slow_path_strategies=slow_path_strategies,
            open_missing=_open_missing, st_unknown=st_unknown,
            st_unknown_bars=st_unknown_bars,
            result_meta=result_meta,
        )

    # ------------------------------------------------------------------
    # run() 编排子步骤
    # ------------------------------------------------------------------

    def _create_execution_context(self, codes: List[str], strategies: Dict[str, Any],
                                  initial_capital: float, industry_map: Any,
                                  lot_size: int,
                                  portfolio_strategy: Any | None) -> tuple[ExecutionRules, AccountBook, RiskGate]:
        """创建撮合规则、账户账本与风控闸门。"""
        rules = ExecutionRules(lot_size=lot_size)
        # 现金桶只分配给会产生活跃信号的标的：universe 里加载了但没配策略的
        # "哑标的"若参与 initial_capital / len(codes) 的等额切分，其份额全程
        # 闲置，冻结比例恰等于无策略标的占比（20 只里只有 5 只配策略 → 75%
        # 本金不动），净值曲线被压成近水平线、最大回撤被压低。
        # 组合级策略覆盖全部标的，因此其现金桶分配给 universe 全量。
        if portfolio_strategy is not None:
            active_codes = list(codes)
        else:
            active_codes = [c for c in codes if c in strategies]
        book = AccountBook(initial_capital, active_codes, industry_map)
        gate = RiskGate(self.risk_engine, industry_map)
        return rules, book, gate

    def _init_run_context(self, codes: List[str], rules: ExecutionRules,
                          book: AccountBook, gate: RiskGate, lot_size: int,
                          pending_ttl_days: int,
                          corporate_actions: CorporateActionSchedule | None = None,
                          dividend_tax_rate: float = 0.0,
                          rights_policy: str = 'partial') -> RunContext:
        """初始化挂单、前收盘、成交与权益曲线等可变状态。"""
        return RunContext(
            book=book, gate=gate, rules=rules, cost_model=self.cost_model,
            lot_size=lot_size,
            equity_curve=[],
            trades=[],
            rejected_orders=[],
            expired_orders=[],
            overwritten_orders=[],
            filtered_signals=[],
            pending=dict.fromkeys(codes),
            pending_age=dict.fromkeys(codes, 0),
            prev_close=dict.fromkeys(codes),
            prev_raw_close=dict.fromkeys(codes),
            pending_ttl_days=pending_ttl_days,
            st_unknown_bars=[],
            corporate_actions=corporate_actions if not corporate_actions or not corporate_actions.is_empty() else None,
            dividend_tax_rate=dividend_tax_rate,
            rights_policy=rights_policy,
            corporate_action_cursor={},
            corporate_action_log=[],
        )

    def _prepare_universe(
        self, universe: Dict[str, pl.DataFrame], strategies: Dict[str, Any],
        portfolio_strategy: Any | None = None,
    ) -> tuple[Dict[str, pl.DataFrame], Dict[str, Dict[str, list]]] | None:
        """校验 universe 与策略配置，按日期排序并批量导出各列为 Python list。

        universe 为空或没有任何标的配置策略（且无组合级策略）时返回 None，
        由调用方提前返回空结果；raw_open / raw_close 必须成对出现，否则直接
        抛 ValueError。
        """
        if not universe:
            logger.error("回测数据为空")
            return None

        codes = list(universe.keys())
        if portfolio_strategy is None and not any(code in strategies for code in codes):
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
        # ADV 列：20 日均成交量（手）。min_samples=1，避免上市初期整段为空；
        # 缺 volume 列时保留 None，由 MaxAdvParticipation fail-closed。
        for code, df in prepared.items():
            if 'volume' in df.columns:
                adv = (
                    df['volume'].cast(pl.Float64, strict=False)
                    .rolling_mean(window_size=ExecutionRules.ADV_WINDOW, min_samples=1)
                    .to_list()
                )
            else:
                adv = [None] * df.height
            column_data[code]['adv_volume'] = adv
        return prepared, column_data

    @staticmethod
    def _check_open_column(column_data: Dict[str, Dict[str, list]]) -> bool:
        """返回是否有标的缺 open 列；缺失时按原文案告警（fill_confidence=low）。"""
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
        codes: List[str], st_status: StStatus | None,
    ) -> tuple[StStatus, bool]:
        """解析 ST 映射，返回 (st_status, st_unknown)。

        True=ST、False=明确非 ST、None=未知；区间列表表示按日期查询。
        未知不代表 ST：涨跌停按板块规则（与 False 相同）判定，只以
        st_unknown=True / fill_confidence=low 标记数据质量。
        """
        st_status = dict(st_status or {})
        unknown_st_codes = []
        for code in codes:
            value = st_status.get(code)
            if value is None or (isinstance(value, list) and not value):
                unknown_st_codes.append(code)

        st_unknown = bool(unknown_st_codes)
        if st_unknown:
            logger.warning(
                "ST 状态未知的标的 {} 只（如 {}），涨跌停按板块默认幅度判定，"
                "结果标记 st_unknown=True、fill_confidence=low；"
                "接入 ST 时间维表后传入区间列表即可缩小未知范围。",
                len(unknown_st_codes), unknown_st_codes[:5],
            )
        return st_status, st_unknown

    @staticmethod
    def _resolve_st_for_date(value: StStatusEntry, trade_date: Any) -> bool | None:
        """按 trade_date 解析单个标的的 ST 状态。

        - bool/None：静态三态，直接返回；
        - 区间列表：命中区间返回对应 is_st，区间外返回 None（未知）。
        """
        if value is None or isinstance(value, bool):
            return value

        day = to_trade_date(trade_date)
        if day is None:
            return None

        for interval in value:
            start = to_trade_date(interval.get('start_date'))
            if start is None or day < start:
                continue
            end = to_trade_date(interval.get('end_date'))
            if end is not None and day > end:
                continue
            return bool(interval.get('is_st', True))
        return None

    def _build_signal_columns(
        self, codes: List[str], strategies: Dict[str, Any],
        prepared: Dict[str, pl.DataFrame],
        column_data: Dict[str, Dict[str, list]],
        portfolio_strategy: Any | None = None,
    ) -> tuple[Dict[str, list], List[Dict[str, str]]]:
        """按行序批量预生成各标的信号，返回 (signal_columns, slow_path_strategies)。

        三段路径：
            1. 向量化：策略实现了 generate_signals_vectorized 且非 BaseStrategy 兜底；
            2. BaseStrategy 的默认兜底实现（is_fallback_impl=True），逐行调用；
            3. 鸭子类型策略对象没有向量化接口，逐行适配 generate_signal。

        组合级策略（A11）产出的信号优先覆盖同标的的单标的信号。所有降级路径
        只在此处聚合一条 warning（A9：原实现逐标的打一条，5000 标的即 5000 条）。
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
            slow_path_strategies.append({'ts_code': code, 'strategy': strategy_name})
            if vec is not None:
                # BaseStrategy 的逐行兜底实现，语义等价于下方的鸭子类型适配
                signal_columns[code] = vec(prepared[code]).to_list()
            else:
                cols = column_data[code]
                height = len(next(iter(cols.values()), []))
                signal_columns[code] = [
                    strategy.generate_signal(
                        {name: col[i] for name, col in cols.items()}, i
                    )
                    for i in range(height)
                ]

        if portfolio_strategy is not None:
            self._merge_portfolio_signals(
                codes, portfolio_strategy, prepared, column_data,
                signal_columns, slow_path_strategies,
            )

        if slow_path_strategies:
            logger.warning(
                "{} 个策略未覆写 generate_signals_vectorized，信号生成退化为逐 bar "
                "Python 调用（slow_path_strategies）：{}",
                len(slow_path_strategies), slow_path_strategies[:5],
            )
        return signal_columns, slow_path_strategies

    def _merge_portfolio_signals(
        self, codes: List[str], portfolio_strategy: Any,
        prepared: Dict[str, pl.DataFrame],
        column_data: Dict[str, Dict[str, list]],
        signal_columns: Dict[str, list],
        slow_path_strategies: List[Dict[str, str]],
    ) -> None:
        """把组合级（横截面）策略的信号合并进 signal_columns（A11）。

        - 向量化路径：``generate_signals_vectorized(universe)`` 返回
          {ts_code: 与行序对齐的信号序列}；未返回的标的保留原信号。
        - 兜底路径：逐日调用 ``generate_signal(universe_rows, index)``，返回
          {ts_code: 信号}；计入 slow_path_strategies。
        """
        name = getattr(portfolio_strategy, 'name', None) or type(portfolio_strategy).__name__
        vec = getattr(portfolio_strategy, 'generate_signals_vectorized', None)
        is_fallback = vec is not None and getattr(
            getattr(vec, '__func__', vec), 'is_fallback_impl', False)

        if vec is not None and not is_fallback:
            produced = vec(prepared)
            if not isinstance(produced, Mapping):
                raise TypeError(
                    f"组合级策略 {name} 的 generate_signals_vectorized 必须返回 "
                    f"{{ts_code: 信号序列}}，实际 {type(produced).__name__}"
                )
            for code in codes:
                series = produced.get(code)
                if series is None:
                    continue
                expected = self._height(column_data[code])
                values = list(series)
                if len(values) != expected:
                    raise ValueError(
                        f"组合级策略 {name} 给 {code} 的信号长度 {len(values)} "
                        f"与行情行数 {expected} 不一致"
                    )
                signal_columns[code] = values
            return

        if not hasattr(portfolio_strategy, 'generate_signal'):
            raise TypeError(
                f"组合级策略 {name} 缺少 generate_signals_vectorized 或 generate_signal"
            )

        slow_path_strategies.append({'ts_code': '__portfolio__', 'strategy': name})
        heights = {code: self._height(column_data[code]) for code in codes}
        # 原地改写调用方传入的字典，避免局部重绑定导致合并结果丢失
        for code in codes:
            signal_columns[code] = ['hold'] * heights[code]
        day_codes, day_positions, sorted_dates = self._build_date_index(column_data)
        for day_index in range(len(sorted_dates)):
            pos_by_code = dict(zip(day_codes[day_index], day_positions[day_index], strict=True))
            rows = {
                code: {name: col[pos] for name, col in column_data[code].items()}
                for code, pos in pos_by_code.items()
            }
            produced = portfolio_strategy.generate_signal(rows, day_index) or {}
            if not isinstance(produced, Mapping):
                raise TypeError(
                    f"组合级策略 {name} 的 generate_signal 必须返回 "
                    f"{{ts_code: 信号}}，实际 {type(produced).__name__}"
                )
            for code, signal in produced.items():
                pos = pos_by_code.get(code)
                if pos is not None:
                    signal_columns[code][pos] = signal

    @staticmethod
    def _height(cols: Dict[str, list]) -> int:
        """返回该标的的行数（优先用 date 列，缺失时用任一列）。"""
        if 'date' in cols:
            return len(cols['date'])
        return len(next(iter(cols.values()), []))

    @staticmethod
    def _build_date_index(
        column_data: Dict[str, Dict[str, list]],
    ) -> tuple[List[List[str]], List[List[int]], List[Any]]:
        """按日期建立对齐的 (活跃标的, 行号) 平行数组。

        数组化后主循环直接按下标迭代，不再逐 bar 做 ``date_positions[code][trade_day]``
        的双层字典查找；同时对每个标的只构建一次 date→行号映射。

        A4：同一标的的 date 列若存在重复日期，原实现用 dict(zip) 后者覆盖前者，
        该 bar 及其信号被静默吞掉；现在直接抛 ValueError，避免回测结果无声失真。
        """
        codes_by_date: Dict[Any, List[str]] = {}
        positions_by_date: Dict[Any, List[int]] = {}
        for code, cols in column_data.items():
            dates = cols['date']
            positions: Dict[Any, int] = {}
            for index, day in enumerate(dates):
                if day in positions:
                    raise ValueError(
                        f"{code} 的 date 列存在重复日期 {day}（行 {positions[day]} 与 "
                        f"行 {index}），会造成该 bar 及其信号被静默合并"
                    )
                positions[day] = index
            for day, pos in positions.items():
                codes_by_date.setdefault(day, []).append(code)
                positions_by_date.setdefault(day, []).append(pos)

        sorted_dates = sorted(codes_by_date.keys())
        day_codes = [codes_by_date[d] for d in sorted_dates]
        day_positions = [positions_by_date[d] for d in sorted_dates]
        return day_codes, day_positions, sorted_dates

    @staticmethod
    def _daily_suspended_codes(codes: List[str],
                               positions: List[int],
                               column_data: Dict[str, Dict[str, list]]) -> Set[str]:
        """当日停牌集合：成交量为 None / NaN / inf / 非数值 / ≤0 的标的。

        positions 与 codes 按下标对齐，由 _build_date_index 预先生成；
        判定与撮合侧同源（ExecutionRules._is_suspended）。
        """
        suspended: Set[str] = set()
        for code, pos in zip(codes, positions, strict=True):
            volume_col = column_data[code].get('volume')
            volume = volume_col[pos] if volume_col is not None else None
            if ExecutionRules._is_suspended(volume):
                suspended.add(code)
        return suspended

    @staticmethod
    def _normalize_signal(signal: Any) -> SignalIntent | None:
        """把策略输出归一为 SignalIntent；'hold'/None/无法识别返回 None。

        字符串 'buy'/'sell' 为状态型信号（explicit=False）；映射
        {'action': 'buy'|'sell', 'fraction': 0~1, 'target_weight': 0~1}
        为显式意图（explicit=True），允许加仓与部分减仓（A12）。
        """
        if isinstance(signal, SignalIntent):
            return signal
        if isinstance(signal, str):
            if signal == 'buy':
                return SignalIntent('buy')
            if signal == 'sell':
                return SignalIntent('sell')
            return None
        if not isinstance(signal, Mapping):
            return None

        action = signal.get('action')
        if action not in ('buy', 'sell'):
            return None

        fraction = _finite_float(signal.get('fraction', 1.0))
        if fraction is None or not 0 < fraction <= 1:
            raise ValueError(f"信号 fraction 必须在 (0, 1]，实际 {signal.get('fraction')!r}")

        target = signal.get('target_weight')
        if target is not None:
            target = _finite_float(target)
            if target is None or not 0 <= target <= 1:
                raise ValueError(
                    f"信号 target_weight 必须在 [0, 1]，实际 {signal.get('target_weight')!r}"
                )
            if action == 'sell':
                raise ValueError('target_weight 只适用于 buy 信号')

        return SignalIntent(action, fraction, target, explicit=True)

    @staticmethod
    def _record_signal(
        ctx: RunContext, *, code: str, date: Any, pos: int,
        signal_columns: Dict[str, list],
    ) -> None:
        """把当日信号写入挂单；被持仓状态过滤的信号记入 filtered_signals。

        - 状态型字符串信号（explicit=False）按持仓预过滤：持仓期间持续 buy、
          空仓期间持续 sell 不构成新委托，避免逐 bar 派单产生数千条无效日志；
        - 显式映射信号（explicit=True）允许持仓上加仓、持仓上部分减仓；
        - 被过滤的信号不再静默丢弃，逐条记入 filtered_signals（A13），
          可据此区分"策略未发信号"与"信号被持仓过滤"；
        - 挂单被新意图覆盖时记入 overwritten_orders（A20：不再混入
          expired_orders 污染作废统计）。
        """
        signals = signal_columns.get(code)
        if signals is None:
            return
        if pos < 0 or pos >= len(signals):
            # A6：信号列与行情行序本应等长，越界说明上游 build 出错，
            # 记录后跳过该 bar，避免 IndexError 中断整次回测
            logger.warning(
                "{} 在 {} 的信号下标 {} 越界（信号长度 {}），已跳过该 bar",
                code, date, pos, len(signals),
            )
            return

        intent = EventBacktestRunner._normalize_signal(signals[pos])
        if intent is None:
            return

        position = ctx.book.position(code)
        if intent.action == 'buy' and position > 0 and not intent.explicit:
            ctx.filtered_signals.append({
                'date': date, 'ts_code': code, 'side': 'buy',
                'reason': 'already_holding', 'qty': 0,
            })
            return
        if intent.action == 'sell' and position == 0:
            ctx.filtered_signals.append({
                'date': date, 'ts_code': code, 'side': 'sell',
                'reason': 'no_position', 'qty': 0,
            })
            return

        old = ctx.pending.get(code)
        if old is not None and old != intent:
            ctx.overwritten_orders.append({
                'date': date, 'side': old.action, 'ts_code': code,
                'reason': 'overwritten', 'qty': 0,
                'replaced_by': intent.action,
            })
        ctx.pending[code] = intent
        ctx.pending_age[code] = 0

    def _apply_corporate_actions(self, ctx: RunContext, trade_day: Any) -> None:
        """在当日撮合前记账所有已到除权日的事件。

        - 以 ``ex_date ≤ 当日`` 为到期条件：除权日落在非交易日或标的当日完全
          无行情行（数据空洞）时，顺延到下一个回测交易日记账，不会漏记；
        - 权益归属按「记账时点的持仓」判定：除权日当天新买入（撮合在记账之后）
          不享有本次分红/送转，记录日持有者才享有；
        - 持仓为 0 的标的只推进游标，不产生日志，避免后续买入被旧事件误伤。
        """
        schedule = ctx.corporate_actions
        if schedule is None:
            return
        day = to_trade_date(trade_day)
        if day is None:
            return
        for code in schedule.codes():
            events = schedule.events_for(code)
            cursor = ctx.corporate_action_cursor.get(code, 0)
            while cursor < len(events) and events[cursor].ex_date <= day:
                event = events[cursor]
                cursor += 1
                if ctx.book.position(code) <= 0:
                    continue
                record = ctx.book.apply_corporate_action(
                    code,
                    cash_per_share=event.cash_per_share,
                    share_ratio=event.share_ratio,
                    rights_ratio=event.rights_ratio,
                    rights_price=event.rights_price,
                    tax_rate=ctx.dividend_tax_rate,
                    rights_policy=ctx.rights_policy,
                )
                record.update({'date': trade_day, 'ex_date': event.ex_date})
                ctx.corporate_action_log.append(record)
            ctx.corporate_action_cursor[code] = cursor

    def _run_trading_loop(
        self, *, ctx: RunContext, sorted_dates: List[Any],
        day_codes: List[List[str]], day_positions: List[List[int]],
        column_data: Dict[str, Dict[str, list]],
        bar_columns: Dict[str, tuple],
        st_status: StStatus, signal_columns: Dict[str, list],
    ) -> tuple[bool, List[Tuple[str, Any]]]:
        """逐日推进主循环：执行挂单、记账信号、更新 prev_close/盯市价、追加权益。

        Returns:
            (是否出现按日期无法判定 ST 状态的 bar, 逐条 (标的, 日期) 审计记录)
        """
        st_unknown_seen = False
        for day_index, trade_day in enumerate(sorted_dates):
            codes_today = day_codes[day_index]
            positions_today = day_positions[day_index]
            # 除权除息必须在当日撮合之前记账：除权日开盘价已扣权，且送转新增
            # 的股数在当日卖出时必须已经计入持仓（记录日持有者才享有权益）
            self._apply_corporate_actions(ctx, trade_day)
            # 当日停牌集合只在启用风控时构造：NoSuspendedTrade 规则需要它，
            # 未启用风控时该集合无消费方，避免给默认路径增加 O(N_symbols/日) 开销
            suspended_today = (
                self._daily_suspended_codes(
                    codes_today, positions_today, column_data)
                if ctx.gate.risk_engine is not None else None
            )

            # A8：标的当日无行情行时挂单顺延，但必须有存活上限，否则停牌/数据
            # 空洞会让数月前的信号在复牌首日按当时开盘价成交
            if ctx.pending:
                today_set = set(codes_today)
                for code in list(ctx.pending.keys()):
                    intent = ctx.pending[code]
                    if intent is None or code in today_set:
                        continue
                    age = ctx.pending_age.get(code, 0) + 1
                    if age > ctx.pending_ttl_days:
                        ctx.pending[code] = None
                        ctx.pending_age[code] = 0
                        self._append_expired(
                            ctx.expired_orders, date=trade_day,
                            side=intent.action, code=code,
                            reason='pending_ttl', fill_confidence='low',
                        )
                    else:
                        ctx.pending_age[code] = age

            for code, pos in zip(codes_today, positions_today, strict=True):
                # 内联 _extract_bar_prices：热路径少一次函数调用与 6 次 dict.get
                # （列引用已在 run() 中按标的一次性解析为 bar_columns[code] 元组）
                (close_col, open_col, high_col, low_col,
                 volume_col, raw_open_col, raw_close_col,
                 adv_col) = bar_columns[code]
                # A24：行情值统一归一到有限 float，字符串/Decimal/NaN/inf 不再
                # 在后续比较或费用计算中抛 TypeError
                close_price = _finite_float(close_col[pos])
                open_price = (
                    _finite_float(open_col[pos]) if open_col is not None else close_price)
                high = _finite_float(high_col[pos]) if high_col is not None else None
                low = _finite_float(low_col[pos]) if low_col is not None else None
                volume = _finite_float(volume_col[pos]) if volume_col is not None else None
                adv_volume = (
                    _finite_float(adv_col[pos]) if adv_col is not None else None)
                raw_open = (
                    _finite_float(raw_open_col[pos]) if raw_open_col is not None else None)
                raw_close = (
                    _finite_float(raw_close_col[pos]) if raw_close_col is not None else None)

                # 区间模式按 trade_date 查 ST；静态三态直接返回原值
                is_st = self._resolve_st_for_date(st_status.get(code), trade_day)
                if is_st is None:
                    st_unknown_seen = True
                    # A14：保留 (标的, 日期)，可审计哪些 bar 的涨跌停判定不可信
                    ctx.st_unknown_bars.append((code, trade_day))

                pending_intent = ctx.pending.get(code)
                if pending_intent is not None:
                    bar = Bar(
                        open_price=open_price, high=high, low=low,
                        close=close_price, volume=volume, raw_open=raw_open,
                        raw_close=raw_close,
                        prev_close=ctx.prev_close.get(code),
                        prev_close_raw=ctx.prev_raw_close.get(code),
                        is_st=is_st, suspended=suspended_today,
                        adv_volume=adv_volume,
                    )
                    self._execute_pending(
                        ctx, code=code, date=trade_day, bar=bar,
                    )

                # 策略基于当日数据生成信号，延迟到该标的下一个交易日执行；
                # 信号已在主循环前按行序批量生成，此处仅按下标查表
                self._record_signal(
                    ctx, code=code, date=trade_day, pos=pos,
                    signal_columns=signal_columns,
                )

                # close 缺失 / NaN / inf / ≤0 时，挂单已在 _execute_pending 里过期；
                # 此处不更新 prev_close / book.mark，保留上一个有效值
                if close_price is None or close_price <= 0:
                    continue
                ctx.prev_close[code] = close_price
                ctx.prev_raw_close[code] = (
                    raw_close if raw_close is not None else close_price)
                # A16：有原始价时按原始价盯市，与原始价成交的现金记账保持同一口径
                ctx.book.mark(code, raw_close if raw_close is not None else close_price)

            # A10：total_equity 改为增量维护，O(1)
            ctx.equity_curve.append({'date': trade_day, 'equity': ctx.book.total_equity()})

        return st_unknown_seen, ctx.st_unknown_bars

    def _build_result(
        self, *, initial_capital: float, ctx: RunContext,
        slow_path_strategies: List[Dict[str, str]], open_missing: bool,
        st_unknown: bool, st_unknown_bars: List[Tuple[str, Any]],
        result_meta: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        """组装结果字典（含 fill_confidence / st_unknown / result_meta 合并）并输出完成日志。"""
        equity_curve = ctx.equity_curve
        final_equity = equity_curve[-1]['equity'] if equity_curve else initial_capital
        total_return = (final_equity - initial_capital) / initial_capital * 100

        performance = self.performance_analyzer.analyze(equity_curve, ctx.trades)

        risk_summary = None
        if self.risk_engine is not None:
            risk_summary = {
                'rules': [r.name for r in self.risk_engine.rules],
                'total_rejected': len(ctx.rejected_orders),
                'reject_detail': [
                    f"[{r['date']}] {r['side']} {r['ts_code']} qty={r['qty']} → {r['rule']}: {r['reason']}"
                    for r in ctx.rejected_orders
                ],
            }

        result: Dict[str, Any] = {
            'initial_capital': initial_capital,
            'final_equity': final_equity,
            'total_return': total_return,
            'trades': ctx.trades,
            'equity_curve': equity_curve,
            'performance': performance,
            'risk_summary': risk_summary,
            'rejected_orders': ctx.rejected_orders,
            'expired_orders': ctx.expired_orders,
            # A20：被新信号覆盖的挂单独立记账，不再混入 expired_orders
            'overwritten_orders': ctx.overwritten_orders,
            # A13：被持仓状态过滤、未形成委托的信号
            'filtered_signals': ctx.filtered_signals,
            # 最终持仓直接取自 AccountBook 账本（唯一真源），
            # 不在引擎侧用 trades 反推，避免账本外再维护一份持仓状态
            'positions': dict(ctx.book.positions),
            # 慢速路径标记：非空表示这些策略未实现 generate_signals_vectorized，
            # 信号走了逐 bar Python 调用；空列表表示全部走向量化路径
            'slow_path_strategies': slow_path_strategies,
            # 缺陷 C：整次回测任一标的缺 open 列 → 开盘涨停判定退化，
            # 成交假设偏乐观；ST 状态未知时同样整体偏乐观/保守不可全信
            'fill_confidence': 'low' if (open_missing or st_unknown) else 'high',
            # ST 状态是否未知：True 表示本次回测按板块默认幅度判定涨跌停，
            # 逐笔 fill_confidence 也相应为 low，调用方应据此解读成交假设
            'st_unknown': st_unknown,
            # A14：逐条 (标的, 日期) 审计记录，替代单一全局 bool 的信息丢失
            'st_unknown_bars': [
                {'ts_code': code, 'date': day} for code, day in st_unknown_bars
            ],
            # 除权除息逐笔记账日志：现金分红、送转股数、配股认购/放弃，
            # 未传事件表时为空列表（与修复前结果结构保持兼容）
            'corporate_actions': ctx.corporate_action_log,
            # 已实现现金分红合计（税后），供绩效归因直接引用
            'dividend_cash_total': sum(
                float(item.get('cash_dividend', 0.0)) for item in ctx.corporate_action_log
            ),
        }
        if result_meta:
            result.update(result_meta)

        logger.info(
            f"回测完成: 总收益率 = {total_return:.2f}%"
            + (f" | 风控拒单 {len(ctx.rejected_orders)} 笔" if risk_summary else "")
            + (f" | 挂单作废 {len(ctx.expired_orders)} 笔" if ctx.expired_orders else "")
            + (f" | 信号被持仓过滤 {len(ctx.filtered_signals)} 条"
               if ctx.filtered_signals else "")
        )
        return result

    # ------------------------------------------------------------------
    # 撮合
    # ------------------------------------------------------------------

    def _execute_pending(self, ctx: RunContext, *, code: str, date: Any,
                         bar: Bar) -> None:
        """执行该标的的挂单。

        订单生命周期语义（显式状态机，杜绝静默丢单）：
            - 标的当日无行情行时挂单顺延，但超过 pending_ttl_days 即作废；
            - 进入本函数即消费挂单，当日有效：成交进 trades、被风控拦截进
              rejected_orders、其余不可成交情形一律作废并记入 expired_orders
              （already_holding / no_position / not_tradable / price_missing /
              insufficient_cash / zero_order），不会残留到下一交易日。

        is_st 为 None 表示 ST 状态未知：涨跌停按板块默认幅度判定（与明确
        非 ST 相同），该笔订单 fill_confidence 标记为 low，作为数据质量提示。

        A16：成交价优先用原始价（raw_open），使整手取整与最低佣金落在名义价上；
        无 raw 列时退回复权开盘价。
        """
        intent = ctx.pending.get(code)
        ctx.pending[code] = None
        ctx.pending_age[code] = 0
        if intent is None:
            return

        position = ctx.book.position(code)
        # is_st 为 None 表示 ST 状态未知：该笔订单 fill_confidence 标记为 low
        order_confidence = 'low' if bar.is_st is None else 'high'
        trade_day = to_trade_date(date)

        exec_price = bar.raw_open if self._is_valid_price(bar.raw_open) else bar.open_price
        reason = self._check_order_executable(
            code=code, intent=intent, position=position, rules=ctx.rules, bar=bar,
            exec_price=exec_price, trade_date=trade_day,
        )
        if reason is not None:
            self._append_expired(
                ctx.expired_orders, date=date, side=intent.action, code=code,
                reason=reason, fill_confidence=order_confidence,
            )
            return

        # A7：原 assert 在生产路径且 python -O 下被剥离；改为显式校验并作废
        if not self._is_valid_price(exec_price):
            self._append_expired(
                ctx.expired_orders, date=date, side=intent.action, code=code,
                reason='price_missing', fill_confidence=order_confidence,
            )
            return

        self._dispatch_order(
            ctx, intent=intent, code=code, date=date, bar=bar,
            exec_price=exec_price, order_confidence=order_confidence,
            trade_day=trade_day,
        )

    def _dispatch_order(
        self, ctx: RunContext, *, intent: SignalIntent, code: str, date: Any,
        bar: Bar, exec_price: float, order_confidence: str,
        trade_day: date | None,
    ) -> None:
        """按买/卖方向派发已通过可成交校验的订单。"""
        # 风控闸门所需上下文：原始价口径前收盘（交易所涨跌停定义在原始价上）
        # 与板块标签必须由撮合内核注入，缺一 PriceLimitTradable 就会 fail-closed。
        risk_prev_close = (
            bar.prev_close_raw if bar.prev_close_raw is not None else bar.prev_close)
        sector = (ctx.rules.resolve_sector(code, bar.is_st, trade_day)
                  if ctx.gate.risk_engine is not None else '')

        if intent.action == 'buy':
            self._execute_buy(
                ctx, code=code, date=date, bar=bar, exec_price=exec_price,
                intent=intent, order_confidence=order_confidence,
                prev_close=risk_prev_close, sector=sector,
            )
            return

        # action == 'sell'（持仓前置条件已在 _check_order_executable 校验）
        self._execute_sell(
            ctx, code=code, date=date, bar=bar, exec_price=exec_price,
            intent=intent, order_confidence=order_confidence,
            prev_close=risk_prev_close, sector=sector,
        )

    @staticmethod
    def _is_valid_price(price: Any) -> TypeIs[float]:
        """价格是否有效：None / NaN / inf / ≤0 / 非数值均视为缺失（A24）。

        返回类型用 TypeIs 而非 bool：判定为真时调用方可直接按 float 使用该
        价格（涨跌停/成交价计算都要求有限正数），避免下游为通过类型检查再做
        多余判空；运行时仍是同一布尔判定，行为不变。
        """
        number = _finite_float(price)
        return number is not None and number > 0

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
        self, *, code: str, intent: SignalIntent, position: int,
        rules: ExecutionRules, bar: Bar, exec_price: float | None,
        trade_date: date | None = None,
    ) -> str | None:
        """按固定顺序校验订单可执行性，返回作废原因；None 表示可继续撮合。

        判定顺序（与订单生命周期一致，不可调换）：
            1. 持仓前置：状态型买入已持仓 → already_holding；卖出无持仓 → no_position；
               显式买入允许持仓上加仓（A12），不做 this 过滤；
            2. close 缺失 / 非有限 / ≤0 → price_missing；
            3. 涨跌停 / 停牌（零成交量）→ not_tradable；
            4. 成交价缺失 / 非有限 / ≤0 → price_missing（不得用 close 伪造成交价）。

        交易所规则定义在原始价上：携带 raw_* 列（hfq 复权输入）时用原始价判定，
        除权日因子跳变会让复权比值失真；high == low 的一字板等式判断在线性
        缩放下不变，可直接透传。
        """
        # 持仓前置条件不满足：状态型买入已持仓、无持仓的卖出
        if intent.action == 'buy' and position > 0 and not intent.explicit:
            return 'already_holding'
        if intent.action == 'sell' and position == 0:
            return 'no_position'

        # close 缺失 = 无有效行情，挂单作废（主循环已 continue 跳过整行，
        # 此处防御性守卫，确保 bar.close=None 不会漏进来）
        if not self._is_valid_price(bar.close):
            return 'price_missing'

        check_open, check_prev, check_close = self._select_check_prices(
            bar.open_price, bar.prev_close, bar.close,
            bar.raw_open, bar.prev_close_raw, bar.raw_close)
        if not rules.is_tradable(intent.action, prev_close=check_prev,
                                 open_price=check_open, high=bar.high, low=bar.low,
                                 close=check_close, volume=bar.volume,
                                 ts_code=code, is_st=bar.is_st,
                                 trade_date=trade_date):
            return 'not_tradable'

        # P0-2 修复：开盘价是集合竞价成交价。open_price 为 None（列存在但值缺失，
        # 如停牌复牌日、除权日）时，不能用 close 伪造成交价——回测不应发明价格。
        # 列整体缺失时 open_price 已在主循环回填为 close_price + fill_confidence=low，
        # 此处不会走到 None 分支。
        if not self._is_valid_price(exec_price):
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
        self, ctx: RunContext, *, code: str, date: Any, bar: Bar,
        exec_price: float, intent: SignalIntent, order_confidence: str,
        prev_close: float | None, sector: str,
    ) -> None:
        """买入执行：按目标仓位/资金比例估算整手、逐手回退成本、风控、记账。

        A12：``fraction`` 控制本次使用该桶可用现金的比例（默认 1.0 全仓），
        ``target_weight`` 控制买入后该标的市值占桶内权益的目标比例，
        因而可在持仓上继续加仓（金字塔建仓）。
        """
        book = ctx.book
        available = book.cash.get(code, 0.0)
        cost_rate = ctx.cost_model.get_buy_cost_rate(trade_date=date, ts_code=code)

        if intent.target_weight is not None:
            held = book.position(code)
            bucket_equity = available + held * exec_price
            target_value = bucket_equity * intent.target_weight
            budget = target_value - held * exec_price
        else:
            budget = available * intent.fraction
        budget = min(budget, available)

        affordable = (
            budget / (exec_price * (1 + cost_rate)) if exec_price > 0 else 0.0)
        shares = int(affordable // ctx.lot_size) * ctx.lot_size
        # 费率估算不含 min_commission 下限，小单实际成本可能超估算：
        # 按实际成本逐手回退，而不是整笔静默作废
        while shares > 0:
            buy_cost = ctx.cost_model.calculate_buy_cost(
                shares, exec_price, trade_date=date, ts_code=code)
            total_pay = shares * exec_price + buy_cost
            if total_pay <= available + CASH_EPS:
                break
            shares -= ctx.lot_size
        if shares == 0:
            self._append_expired(
                ctx.expired_orders, date=date, side='buy', code=code,
                reason='zero_order' if budget <= 0 else 'insufficient_cash',
                fill_confidence=order_confidence,
            )
            return

        buy_cost = ctx.cost_model.calculate_buy_cost(
            shares, exec_price, trade_date=date, ts_code=code)
        rejection = ctx.gate.check(
            ts_code=code, side='buy', qty=shares, price=exec_price,
            trade_date=date, book=book, equity_curve=ctx.equity_curve,
            bar_volume=bar.volume, adv_volume=bar.adv_volume,
            prev_close=prev_close, sector=sector,
            suspended=bar.suspended,
        )
        if rejection is not None:
            ctx.rejected_orders.append(rejection)
            return
        book.buy(code, exec_price, shares, buy_cost)
        ctx.trades.append({
            'date': date,
            'stock_code': code,
            'signal': 'buy',
            'price': exec_price,
            'shares': shares,
            'capital': book.cash[code],
            'position': book.position(code),
            'cost': buy_cost,
            'fill_confidence': order_confidence,
        })

    def _execute_sell(
        self, ctx: RunContext, *, code: str, date: Any, bar: Bar,
        exec_price: float, intent: SignalIntent, order_confidence: str,
        prev_close: float | None, sector: str,
    ) -> None:
        """卖出执行：按 fraction 计算卖出股数（默认清仓），风控通过后记账。

        A12：``fraction`` 控制本次卖出占当前持仓的比例；部分卖出按整手向下取整，
        fraction=1.0 全数卖出（含零股），与原有全仓卖出语义一致。
        """
        book = ctx.book
        position = book.position(code)
        if intent.fraction >= 1.0:
            shares = position
        else:
            shares = int((position * intent.fraction) // ctx.lot_size) * ctx.lot_size
        if shares <= 0:
            self._append_expired(
                ctx.expired_orders, date=date, side='sell', code=code,
                reason='zero_order', fill_confidence=order_confidence,
            )
            return

        sell_cost = ctx.cost_model.calculate_sell_cost(
            shares, exec_price, trade_date=date, ts_code=code)
        rejection = ctx.gate.check(
            ts_code=code, side='sell', qty=shares, price=exec_price,
            trade_date=date, book=book, equity_curve=ctx.equity_curve,
            bar_volume=bar.volume, adv_volume=bar.adv_volume,
            prev_close=prev_close, sector=sector,
            suspended=bar.suspended,
        )
        if rejection is not None:
            ctx.rejected_orders.append(rejection)
            return
        book.sell(code, exec_price, shares, sell_cost)
        ctx.trades.append({
            'date': date,
            'stock_code': code,
            'signal': 'sell',
            'price': exec_price,
            'shares': shares,
            'capital': book.cash[code],
            'position': book.position(code),
            'cost': sell_cost,
            'fill_confidence': order_confidence,
        })

    @staticmethod
    def _ensure_sorted(df: pl.DataFrame) -> pl.DataFrame:
        """按日期升序，保证日期索引与执行顺序一致。"""
        if 'date' in df.columns and not df['date'].is_sorted():
            return df.sort('date')
        return df
