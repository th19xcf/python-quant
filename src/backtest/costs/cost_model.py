#!/usr/bin/env python3

"""
交易成本模型，用于计算交易成本

默认参数对齐 A 股现行规则（2023-08-28 起印花税降为 0.05%）：
- 佣金双边 0.05%，单笔最低 5 元
- 印花税卖出侧 0.05%（ETF/LOF 按品种豁免）
- 过户费双边 0.002%（2015-08-01 起沪深统一，ETF/LOF 豁免）
- 滑点 0.02% 双边，可按流动性覆盖

传入 ``trade_date`` 时按历史时间表取费率（印花税/过户费），避免用现行
费率回测历史区间造成系统性成本偏差；不传时使用 ``COST_DEFAULTS`` 中的
现行静态费率（过户费 0.002%，与时间表当日口径一致；ETF/LOF 恒为 0）。
"""

import math
from datetime import date, datetime
from typing import Any, Dict, Tuple, Union

from src.utils.dates import as_date

#: 印花税历史时间表：(生效日, 税率, 征收方向)，按生效日倒序排列。
#: 2023-08-28 减半至 0.05%（卖方单边）；2008-09-19 起 0.1%（卖方单边）；
#: 2007-05-30 上调至 0.3% 且双边；2005-01-24 起 0.1% 双边；
#: 2001-11-16 起 0.2% 双边；1998-06-12 起 0.4% 双边（更早区间按 0.4% 近似）。
STAMP_TAX_SCHEDULE: Tuple[Tuple[date, float, str], ...] = (
    (date(2023, 8, 28), 0.0005, 'sell'),
    (date(2008, 9, 19), 0.001, 'sell'),
    (date(2007, 5, 30), 0.003, 'both'),
    (date(2005, 1, 24), 0.001, 'both'),
    (date(2001, 11, 16), 0.002, 'both'),
    (date(1998, 6, 12), 0.004, 'both'),
)

#: 过户费切换日：2015-08-01 起沪深两市统一按成交金额 0.002%（双向）。
#: 此前仅沪市按成交金额 0.06% 收取，深市不收。
TRANSFER_FEE_SWITCH = date(2015, 8, 1)
TRANSFER_FEE_RATE_UNIFIED = 0.00002
TRANSFER_FEE_RATE_SH_LEGACY = 0.0006

#: 成本单一真源：回测内核、推荐配置（src.advice.config）与因子评估
#: （src.advice.factor_eval，直接使用 CostModel）都从这里取值。
#: 语义映射：推荐侧的 ``impact_rate`` 对应回测侧的 ``slippage``（均为双边）。
COST_DEFAULTS: Dict[str, float] = {
    'commission_rate': 0.0005,       # 佣金费率（双边，按成交额买卖各扣一次）
    'min_commission': 5.0,           # 单笔最低佣金（元）
    'stamp_tax_rate': 0.0005,        # 印花税（仅卖出，2023-08-28 起 0.05%）
    'transfer_fee_rate': TRANSFER_FEE_RATE_UNIFIED,  # 过户费（双边 0.002%）
    'slippage': 0.0002,              # 滑点（双边，未分档时的兜底值）
    # 冲击成本系数：impact = impact_coef × sqrt(成交额 / ADV 成交额)。
    # 默认 0.015 是 2026-11-12 的**显式口径决定**，不是随手取的数：
    # 依据同日的系数扫描（36 次运行）——1 亿档策略收益翻负点 coef≈0.099、
    # 1,000 万档≈0.298；0.015 仅为翻负点的 1/6.6~1/20，而默认 0（旧口径）
    # 的偏乐观幅度实测为 100 万~1,000 万档 0.2~2.0 pp、1 亿档 1.8~6.0 pp。
    # 参与率分母由回测内核传入的 bar.adv20（20 日均成交额，元，lag1）给出；
    # ADV 缺失的标的冲击项为 0（见 impact_rate）。改这个默认值等于改全部
    # 历史回测的收益口径：再动之前必须有新一轮翻负点实测与用户显式批准，
    # 且 trading 侧要给 report/golden 基线同步留证（V-36 决策记录）。
    'impact_coef': 0.015,
}

#: ETF/LOF 滑点分档：按 20 日 ADV（元）从高到低取第一档。
#: 依据：场内基金以做市商报价为主，宽基 ETF 盘口最深；ADV 越低、盘口越薄，
#: 冲击成本越高。行业/主题 ETF 在同一 ADV 档上取相邻更高的档位（见
#: ``slippage_rate`` 的 ``category`` 参数）。
ETF_SLIPPAGE_TIERS: Tuple[Tuple[float, float], ...] = (
    (1.0e8, 0.0005),   # ADV ≥ 1 亿：5bp
    (5.0e7, 0.0007),   # 5,000 万 ~ 1 亿：7bp
    (0.0, 0.0010),     # < 5,000 万：10bp
)

#: 个股滑点分档：按总市值（**亿元**）从高到低取第一档。
#: 依据：总市值是冲击成本最稳定的代理变量（与 ADV 高度相关），分档阈值取
#: A 股总市值分布的大盘/中盘/小盘常用切分（500 亿 / 100 亿）。
STOCK_SLIPPAGE_TIERS_YI: Tuple[Tuple[float, float], ...] = (
    (500.0, 0.0015),   # 大盘：15bp
    (100.0, 0.0020),   # 中盘：20bp
    (0.0, 0.0030),     # 小盘 / 市值缺失：30bp
)


TradeDate = Union[date, datetime, str, None]


def _as_date(value: TradeDate) -> date | None:
    """把 date/datetime/'YYYY-MM-DD'/时间戳对象归一为 date；无法解析时返回 None。

    必须覆盖 Polars / Pandas 时间戳：回测主循环的 ``trade_date`` 由列
    ``to_list()`` 得来，若 date 列是 Datetime 会返回第三方时间戳对象。
    早期实现只认内置 date/datetime/str，此时返回 None 会让 ``_stamp_tax_rate``
    与 ``_transfer_fee_rate`` 静默退回到现行静态费率，历史区间印花税/过户费
    口径整体失真（2015/2023 两次费率切换全部失效）。
    """
    # 统一走 src.utils.dates.as_date：该函数额外覆盖 np.datetime64、NaT、
    # 'YYYYMMDD' 与 pd.Timestamp，避免本模块单独维护一套不完整的解析分支。
    return as_date(value)


class CostModel:
    """
    交易成本模型类，提供交易成本计算功能
    """

    def __init__(self, overrides: Dict[str, Any] | None = None):
        """
        初始化成本模型

        Args:
            overrides: 自定义参数，会覆盖默认值。可选键：
                       commission_rate / min_commission / stamp_tax_rate /
                       transfer_fee_rate / slippage / impact_coef
        """
        self.params = dict(COST_DEFAULTS)
        if overrides:
            self.params.update(overrides)

    def set_params(self, params: Dict[str, Any]) -> None:
        """
        设置成本模型参数
        """
        self.params.update(params)

    def calculate_buy_cost(self, shares: float, price: float,
                           trade_date: TradeDate = None, ts_code: str = '',
                           adv20: float | None = None,
                           total_mv_yi: float | None = None,
                           category: str | None = None,
                           adv_amount: float | None = None) -> float:
        """
        买入侧成本（佣金 + 过户费 + 滑点）；2008-09-19 前印花税双边征收时含税。

        trade_date 传入时按历史时间表取印花税/过户费；未传时沿用静态参数。
        adv20 / total_mv_yi / category 传入时按分档滑点计费（见 ``slippage_rate``）。
        adv_amount（20 日平均成交额，元）传入时额外叠加冲击成本，系数
        ``impact_coef`` 默认 0，故不传或默认值时结果与引入冲击项之前逐位相同。
        """
        commission_rate = self.params['commission_rate']
        min_commission = self.params['min_commission']

        amount = shares * price
        commission = max(amount * commission_rate, min_commission)
        transfer = amount * self._transfer_fee_rate(trade_date, ts_code)
        tax = amount * self._stamp_tax_rate(trade_date, side='buy', ts_code=ts_code)
        slip = amount * (self.slippage_rate(ts_code, adv20, total_mv_yi, category)
                         + self.impact_rate(self._participation(amount, adv_amount)))

        return commission + transfer + tax + slip

    def calculate_sell_cost(self, shares: float, price: float,
                            trade_date: TradeDate = None, ts_code: str = '',
                            adv20: float | None = None,
                            total_mv_yi: float | None = None,
                            category: str | None = None,
                            adv_amount: float | None = None) -> float:
        """
        卖出侧成本（佣金 + 印花税 + 过户费 + 滑点）。

        trade_date 传入时按历史时间表取印花税/过户费；2010 年卖出印花税
        应为 0.1%（2008-09-19 至 2023-08-27），而不是现行 0.05%。
        ETF/LOF 按品种豁免印花税（见 ``_is_fund``）。
        adv_amount 语义同 ``calculate_buy_cost``：系数 ``impact_coef`` 默认 0，
        不传时结果与引入冲击项之前逐位相同。
        """
        commission_rate = self.params['commission_rate']
        min_commission = self.params['min_commission']

        amount = shares * price
        commission = max(amount * commission_rate, min_commission)
        tax = amount * self._stamp_tax_rate(trade_date, side='sell', ts_code=ts_code)
        transfer = amount * self._transfer_fee_rate(trade_date, ts_code)
        slip = amount * (self.slippage_rate(ts_code, adv20, total_mv_yi, category)
                         + self.impact_rate(self._participation(amount, adv_amount)))

        return commission + tax + transfer + slip

    def calculate_cost(self, shares: float, buy_price: float, sell_price: float,
                       trade_date: TradeDate = None, ts_code: str = '',
                       adv20: float | None = None, total_mv_yi: float | None = None,
                       category: str | None = None) -> float:
        """
        一轮完整买卖的总交易成本
        """
        return (self.calculate_buy_cost(shares, buy_price, trade_date, ts_code,
                                        adv20, total_mv_yi, category)
                + self.calculate_sell_cost(shares, sell_price, trade_date, ts_code,
                                           adv20, total_mv_yi, category))

    def get_buy_cost_rate(self, trade_date: TradeDate = None, ts_code: str = '',
                          adv20: float | None = None,
                          total_mv_yi: float | None = None,
                          category: str | None = None) -> float:
        """估算买入侧成本费率（用于计算可负担股数）"""
        return (self.params['commission_rate']
                + self._transfer_fee_rate(trade_date, ts_code)
                + self._stamp_tax_rate(trade_date, side='buy', ts_code=ts_code)
                + self.slippage_rate(ts_code, adv20, total_mv_yi, category))

    def get_sell_cost_rate(self, trade_date: TradeDate = None, ts_code: str = '',
                           adv20: float | None = None,
                           total_mv_yi: float | None = None,
                           category: str | None = None) -> float:
        """估算卖出侧成本费率（佣金 + 印花税 + 过户费 + 分档滑点）。

        与 ``get_buy_cost_rate`` 对称，供评估侧重算逐笔成本时按标的查询费率。
        """
        return (self.params['commission_rate']
                + self._stamp_tax_rate(trade_date, side='sell', ts_code=ts_code)
                + self._transfer_fee_rate(trade_date, ts_code)
                + self.slippage_rate(ts_code, adv20, total_mv_yi, category))

    # ------------------------------------------------------------------
    # 品种判定与滑点分档
    # ------------------------------------------------------------------

    @staticmethod
    def _is_fund(ts_code: str) -> bool:
        """判定是否场内基金（ETF/LOF）：沪市 5 开头，深市 15/16/18 开头。

        ETF 与 LOF 卖出均免征印花税（按品种判定，不用市场统一开关）。
        """
        code = (ts_code or '').upper().split('.')[0]
        return code.startswith('5') or code.startswith(('15', '16', '18'))

    def slippage_rate(self, ts_code: str = '', adv20: float | None = None,
                      total_mv_yi: float | None = None,
                      category: str | None = None) -> float:
        """按品种与流动性返回单边滑点费率；缺少分档输入时退回 ``slippage``。

        - ETF/LOF：按 ``ETF_SLIPPAGE_TIERS``（ADV 门槛，元）；``category='sector'``
          （行业/主题）在同一 ADV 档上取相邻更高的一档。
        - 个股：按 ``STOCK_SLIPPAGE_TIERS_YI``（总市值门槛，亿元）；市值缺失按小盘 30bp。
        """
        if self._is_fund(ts_code):
            if adv20 is None or not (adv20 > 0):
                return self.params['slippage']
            for i, (floor, rate) in enumerate(ETF_SLIPPAGE_TIERS):
                if adv20 >= floor:
                    if str(category or '').lower() in ('sector', 'industry', 'theme'):
                        i = min(i + 1, len(ETF_SLIPPAGE_TIERS) - 1)
                        rate = ETF_SLIPPAGE_TIERS[i][1]
                    return rate
            return ETF_SLIPPAGE_TIERS[-1][1]

        if total_mv_yi is None or not (total_mv_yi > 0):
            return STOCK_SLIPPAGE_TIERS_YI[-1][1]
        for floor, rate in STOCK_SLIPPAGE_TIERS_YI:
            if total_mv_yi >= floor:
                return rate
        return STOCK_SLIPPAGE_TIERS_YI[-1][1]

    def impact_rate(self, participation: float | None) -> float:
        """冲击成本费率：``impact_coef × sqrt(参与率)``，默认系数 0 时恒为 0。

        参与率 = 本笔成交额 / 该标的 20 日平均成交额（均为元），由调用方通过
        ``adv_amount`` 注入。本方法只做纯计算，不解析标的、不读行情；系数为 0
        或参与率缺失/非正时返回精确的 0.0，保证既有回测结果逐位不变。

        明确选择（``impact_coef > 0`` 但 ``adv_amount`` 为 None 时的行为）：
        **返回 0，不抛错**。理由：ADV 成交额缺失属于数据覆盖问题（新上市
        初期、长期停牌后首日、面板没有 ``amount`` 列），抛错会让整次回测因
        个别标的缺列而整体失败，而冲击项本身是可选叠加项；缺失即视为该笔
        不启用冲击成本。这不是静默失效：``execution_core`` 已把 ``adv20``
        作为 adv_amount 无条件传入，缺失只可能来自行情面板本身没有
        ``amount`` 列，而该情形由回测结果的 ``slippage_inputs`` /
        ``cost_inputs`` 审计字段（``impact_missing_symbols`` +
        ``impact_missing_note``）显式列出。
        """
        coef = float(self.params.get('impact_coef') or 0.0)
        if coef <= 0.0 or participation is None or not (participation > 0.0):
            return 0.0
        return coef * math.sqrt(participation)

    @staticmethod
    def _participation(amount: float, adv_amount: float | None) -> float | None:
        """成交额占 20 日均额的比例；未提供 ADV 成交额时返回 None（不启用冲击项）。

        返回 None 而非抛错是刻意的：``impact_rate(None) == 0.0``，使「缺 ADV」
        与「系数为 0」两种情形都落在精确 0 上（见 ``impact_rate`` 的说明）。
        """
        if adv_amount is None or not (adv_amount > 0):
            return None
        return amount / adv_amount

    # ------------------------------------------------------------------
    # 历史费率解析
    # ------------------------------------------------------------------

    def _stamp_tax_rate(self, trade_date: TradeDate, side: str,
                        ts_code: str = '') -> float:
        """按交易日、方向与品种取印花税率；ETF/LOF 买卖均豁免。"""
        if self._is_fund(ts_code):
            return 0.0
        day = _as_date(trade_date)
        if day is None:
            return self.params['stamp_tax_rate'] if side == 'sell' else 0.0
        for effective, rate, direction in STAMP_TAX_SCHEDULE:
            if day >= effective:
                if direction == 'both' or direction == side:
                    return rate
                return 0.0
        return 0.0

    def _transfer_fee_rate(self, trade_date: TradeDate, ts_code: str) -> float:
        """按交易日与市场取过户费率；ETF/LOF 免过户费，未传日期时沿用静态参数。

        ETF/LOF 的登记结算由基金自身承担，投资者买卖场内基金不缴过户费；
        此前 2015-08-01 后对所有代码统一返回 0.002%，把基金也计了 0.2bp，
        对一个 20 年、248 次调仓的 ETF 轮动策略累计约 0.24pp。
        """
        if self._is_fund(ts_code):
            return 0.0
        day = _as_date(trade_date)
        if day is None:
            return self.params['transfer_fee_rate']
        if day >= TRANSFER_FEE_SWITCH:
            return TRANSFER_FEE_RATE_UNIFIED
        return TRANSFER_FEE_RATE_SH_LEGACY if self._is_shanghai(ts_code) else 0.0

    @staticmethod
    def _is_shanghai(ts_code: str) -> bool:
        """判定是否沪市标的：后缀优先，无后缀时按 6/9 开头（含 900 B 股）。"""
        code = (ts_code or '').upper()
        if code.endswith('.SH'):
            return True
        if code.endswith(('.SZ', '.BJ')):
            return False
        return code.split('.')[0].startswith(('6', '9'))
