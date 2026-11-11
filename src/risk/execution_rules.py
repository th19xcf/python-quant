#!/usr/bin/env python3

"""A 股涨跌停 / 可交易性判定规则（唯一真源）。

为什么在 ``src.risk`` 而不是 ``src.backtest.engine``
---------------------------------------------------
本模块只做「给定代码、交易日、ST 状态，算出涨跌停幅度与当日是否可成交」，
不撮合、不记账、不读文件，是纯规则。它的消费方横跨三个层：

* ``src.data``（``st_price_infer`` / ``breadth_service``）用它推导 ST 阈值与
  涨跌停家数；
* ``src.advice``（``eligibility`` / ``factor_eval`` / ``replay``）用它判定
  推荐票当日能否成交；
* ``src.backtest``（``execution_core``）用它做撮合前的封板拦截。

放在回测引擎里会让 ``src.data`` 反向 import ``src.backtest``，而回测引擎会
连带拉起 ``src.risk`` / ``CostModel`` / ``PerformanceAnalyzer``。下沉到
``src.risk`` 后依赖方向统一为 data / advice / backtest → risk，无反向边。

口径边界
--------
* 幅度唯一真源：``resolve_limit_pct``（按交易日 → 板块制度 → ST 状态）。
* 价格取整唯一真源：``resolve_limit_price``（先 Decimal 再乘，ROUND_HALF_UP
  到分）。直接比较浮点乘积会在 .005 边界少取一分，把一字板误判为可成交。
* 风控侧板块标签：``resolve_sector``，必须满足
  ``PriceLimitTradable.DEFAULT_LIMITS[sector] == resolve_limit_pct(...)``。
"""

from __future__ import annotations

import math
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

#: A 股最小价格变动单位（1 分），交易所涨跌停价的取整单位
PRICE_TICK = Decimal('0.01')


def finite_float(value: Any) -> float | None:
    """把行情值归一为有限 float；None / NaN / inf / 非数值一律返回 None。

    回测与数据层可能混入 Decimal、字符串或 numpy 标量，直接参与比较或
    math.isfinite 会抛 TypeError；统一在入口处归一，后续热路径只处理 float。
    """
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


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

        判定逻辑只在 ``block_reason`` 里实现一份，本方法为其布尔投影，
        保证「可成交判定」与「作废原因统计」不会出现两套口径。
        """
        return self.block_reason(
            side, prev_close=prev_close, open_price=open_price, high=high,
            low=low, close=close, volume=volume, ts_code=ts_code,
            is_st=is_st, trade_date=trade_date,
        ) is None

    def block_reason(self, side: str, *, prev_close: float | None,
                     open_price: float | None, high: float | None,
                     low: float | None, close: float | None,
                     volume: float | None, ts_code: str = '',
                     is_st: bool | None = None,
                     trade_date: date | None = None) -> str | None:
        """不可成交的具体原因：'suspended' / 'limit_up' / 'limit_down' / None。

        「因跌停卖不出」要让 ``expired_orders`` 可统计，必须区分跌停与停牌；
        原因由与 ``is_tradable`` 完全相同的分支产生，不新增任何判定规则。
        """
        # 缺陷 A 修复：NaN / inf 穿过停牌检查，显式拦截
        if self._is_suspended(volume):
            return 'suspended'

        prev = finite_float(prev_close)
        if prev is None or prev <= 0:
            return None

        limit = self.resolve_limit_pct(ts_code, is_st, trade_date=trade_date)
        blocked = 'limit_up' if side == 'buy' else 'limit_down'

        open_number = finite_float(open_price)
        if open_number is not None:
            # 价格空间比较：涨跌停价 = round(prev_close × (1±limit), 2) 后直接比较。
            # 原写法 (open_pct >= limit - 0.005) 的容差被放大 prev_close 倍，
            # 3 元股等价于允许 1.5% 偏差、200 元股允许 0.25% 偏差，
            # 结果是把 [limit-0.5%, limit) 的正常开盘价误当成封板双向拒绝。
            if self._limit_breached(
                    side, price=open_number, prev_close=prev, limit=limit):
                return blocked
            return None

        # 缺陷 B 修复：复权后 high / low 可能相差 1e-16，改用相对容差
        # 缺陷 C 修复：open_price 缺失时只退化到此分支，偏乐观
        if self._is_one_line_board_locked(
                side, high=high, low=low, close=close,
                prev_close=prev, limit=limit):
            return blocked
        return None

    @staticmethod
    def _is_suspended(volume: float | None) -> bool:
        """成交量为 None / NaN / inf / 非数值 / ≤0 视为停牌，不可交易。"""
        number = finite_float(volume)
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
        high_number = finite_float(high)
        low_number = finite_float(low)
        close_number = finite_float(close)
        if high_number is None or low_number is None or close_number is None:
            return False
        if abs(high_number - low_number) / prev_close >= 1e-6:
            return False
        return self._limit_breached(side, price=close_number,
                                    prev_close=prev_close, limit=limit)
