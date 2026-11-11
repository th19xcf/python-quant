#!/usr/bin/env python3

"""
回测输入契约 —— 行情 / 因子 / 财务三层必需列的唯一声明处。

设计目标（对应代码审查 2.1 / 2.2 / 6.3 / 6.5 / 6.8）：

1. 换数据源只改本文件的 ``ColumnSpec.column``，引擎内核只引用 ``ROLE_*``
   语义角色常量，不再出现文件名与列名字面量；
2. 缺必需列、列名不符、复权因子恒为 1.0（复权价与原始价完全相同）等
   "静默降级"一律抛 ``BacktestInputContractError``，不再告警后继续算；
3. 确有合法的原始层消费方（例如 TDX 本地原始价、只做研究层的裸面板）时，
   用 ``expect=(ROLE_RAW_CLOSE,)`` / ``price_basis='raw'`` 显式豁免，
   豁免必须写在调用点上，而不是放宽全局校验。

契约写法与 ``src/data/dividend_effective.py`` 的 ``DividendSchemaError``
保持一致：列契约常量 + 运行时校验 + 明确指出缺哪一列、可选列有哪些。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Sequence, Tuple

import polars as pl

# ---------------------------------------------------------------------------
# 语义角色：引擎内核只认这些常量
# ---------------------------------------------------------------------------

ROLE_DATE = 'date'
ROLE_OPEN = 'open'
ROLE_HIGH = 'high'
ROLE_LOW = 'low'
ROLE_CLOSE = 'close'
ROLE_VOLUME = 'volume'
ROLE_AMOUNT = 'amount'
ROLE_TOTAL_MV = 'total_mv'
ROLE_TOTAL_MV_YI = 'total_mv_yi'
ROLE_CATEGORY = 'category'
ROLE_RAW_OPEN = 'raw_open'
ROLE_RAW_CLOSE = 'raw_close'
#: 引擎派生列（不在输入里，由 _prepare_universe 生成，仅在此声明语义）
ROLE_ADV_VOLUME = 'adv_volume'
ROLE_ADV20 = 'adv20'

ROLE_MA_SHORT = 'ma_short'
ROLE_MA_LONG = 'ma_long'
ROLE_MACD = 'macd'
ROLE_MACD_SIGNAL = 'macd_signal'
ROLE_MACD_HIST = 'macd_hist'
ROLE_RSI = 'rsi'
ROLE_ATR = 'atr'

# ---------------------------------------------------------------------------
# 财务/估值层语义角色
#
# 这些列名不是凭契约起的名：``src/advice/fundamental_factors.py`` 的
# ``build_fundamentals`` 真实读取的就是下面这些（valuation/shares 直接
# ``select``，income/balance 经 ``ttm_from_cumulative`` / ``balance_metrics``
# 与 ``_dedupe_period`` 读取 ts_code / end_date / ann_date）。
# ---------------------------------------------------------------------------
ROLE_TS_CODE = 'ts_code'
ROLE_TRADE_DATE = 'trade_date'
ROLE_EFFECTIVE_DATE = 'effective_date'
ROLE_END_DATE = 'end_date'
ROLE_ANN_DATE = 'ann_date'
ROLE_CIRC_MV = 'circ_mv'
ROLE_DV_RATIO = 'dv_ratio'
ROLE_FLOAT_SHARE = 'float_share'
ROLE_REVENUE = 'revenue'
ROLE_PARENT_NET_PROFIT = 'parent_net_profit'
ROLE_TOTAL_ASSETS = 'total_assets'
ROLE_TOTAL_EQUITY = 'total_equity'

# ---------------------------------------------------------------------------
# 输出侧记录键名
#
# 回测产物（成交 / 拒单 / 过滤信号 / 权益曲线 / 除权日志 / ST 审计）也是
# 跨模块契约，输出键与输入列名同等对待：只在这里声明一次，引擎内不再写
# 'date' / 'side' / 'ts_code' 裸字符串。
# ---------------------------------------------------------------------------
OUT_DATE = 'date'
OUT_SIDE = 'side'
OUT_TS_CODE = 'ts_code'

#: 价格口径：'auto' 缺省按「有 raw_* 即成对复权」推断；'adjusted' 强制要求
#: 复权价不等于原始价；'raw' 显式声明原始层消费方，跳过复权退化校验
PRICE_BASIS_CHOICES = ('auto', 'adjusted', 'raw')


class BacktestInputContractError(ValueError):
    """回测输入不符合列契约：缺必需列 / 列名不符 / 复权因子全为 1.0。"""


def ma_column(window: int) -> str:
    """均线列名（预计算与策略读取共用同一命名）。"""
    return f'ma_{int(window)}'


def rsi_column(period: int) -> str:
    """自定义周期 RSI 列名。"""
    return f'rsi_{int(period)}'


# ---------------------------------------------------------------------------
# 契约结构
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnSpec:
    """一列的定义：语义角色 → 外部数据源列名 + 是否必需。"""

    role: str
    column: str
    required: bool
    note: str = ''


@dataclass(frozen=True)
class FrameContract:
    """一帧输入（行情 / 因子 / 财务）的列契约。"""

    name: str
    columns: Tuple[ColumnSpec, ...]

    # -- 查询 ---------------------------------------------------------------
    def spec(self, role: str) -> ColumnSpec:
        for item in self.columns:
            if item.role == role:
                return item
        raise KeyError(f'{self.name} 契约未声明角色 {role!r}')

    def column(self, role: str) -> str:
        """返回角色对应的列名（引擎内核取列名的唯一入口）。"""
        return self.spec(role).column

    def required_roles(self) -> Tuple[str, ...]:
        return tuple(item.role for item in self.columns if item.required)

    def optional_roles(self) -> Tuple[str, ...]:
        return tuple(item.role for item in self.columns if not item.required)

    # -- 校验 ---------------------------------------------------------------
    def resolve_columns(
        self, columns: Sequence[str], *, expect: Sequence[str] = (),
    ) -> Dict[str, str]:
        """把实际列名解析成 {角色: 列名}，缺必需列即抛错。

        Args:
            columns: 实际 DataFrame 列名
            expect: 显式豁免的必需角色（合法的原始层消费方）。豁免项只跳过
                必需性检查，不会凭空生成列；调用方必须自己保证后续逻辑可用。
        """
        present = set(columns)
        exempt = set(expect)
        missing = [
            item for item in self.columns
            if item.required and item.role not in exempt and item.column not in present
        ]
        if missing:
            raise BacktestInputContractError(
                f'{self.name} 契约缺少必需列：'
                + ', '.join(f'{item.column}（{item.role}）' for item in missing)
                + f'；实际列 {sorted(present)[:12]}'
                + f'；可选列 {sorted(self.optional_roles())}'
                + '。换数据源请改 input_contract.py 的列映射，'
                  '确有原始层豁免请显式传 expect=()'
            )
        return {
            item.role: item.column
            for item in self.columns
            if item.column in present
        }


# ---------------------------------------------------------------------------
# 三层契约
# ---------------------------------------------------------------------------

#: 行情契约：撮合、涨跌停、停牌、ADV 的唯一输入
BAR_CONTRACT = FrameContract('bar', (
    ColumnSpec(ROLE_DATE, 'date', True, '交易日（升序，不得重复）'),
    ColumnSpec(ROLE_OPEN, 'open', True, '开盘价（成交价；缺列不得回填 close）'),
    ColumnSpec(ROLE_HIGH, 'high', True, '最高价（一字板判定）'),
    ColumnSpec(ROLE_LOW, 'low', True, '最低价（一字板判定）'),
    ColumnSpec(ROLE_CLOSE, 'close', True, '收盘价（盯市 / 策略信号）'),
    ColumnSpec(ROLE_VOLUME, 'volume', True, '成交量（手）：停牌判定与 ADV 唯一输入'),
    ColumnSpec(ROLE_AMOUNT, 'amount', False, '成交额（元）：ETF 分档滑点'),
    ColumnSpec(ROLE_TOTAL_MV, 'total_mv', False, '总市值（元）：个股分档滑点'),
    ColumnSpec(ROLE_TOTAL_MV_YI, 'total_mv_yi', False, '总市值（亿元），优先于 total_mv'),
    ColumnSpec(ROLE_CATEGORY, 'category', False, '品种标签（ETF / 行业主题）'),
    ColumnSpec(ROLE_RAW_OPEN, 'raw_open', False, 'hfq 出口原始开盘价，与 raw_close 成对'),
    ColumnSpec(ROLE_RAW_CLOSE, 'raw_close', False, 'hfq 出口原始收盘价'),
))

#: 因子契约：指标预计算的产物，策略读取前必须存在。
#: ma_short / ma_long 声明的 ma_10 / ma_20 只是默认窗口，实际列由
#: ``validate_factor_frame(ma_columns=...)`` 按策略参数覆盖（ma_5 等）。
FACTOR_CONTRACT = FrameContract('factor', (
    ColumnSpec(ROLE_MA_SHORT, ma_column(10), True, '短期均线（参数化，见 ma_column）'),
    ColumnSpec(ROLE_MA_LONG, ma_column(20), True, '长期均线（参数化，见 ma_column）'),
    ColumnSpec(ROLE_MACD, 'macd', True, 'MACD 快慢线差'),
    ColumnSpec(ROLE_MACD_SIGNAL, 'macd_signal', True, 'MACD 信号线'),
    ColumnSpec(ROLE_MACD_HIST, 'macd_hist', True, 'MACD 柱'),
    ColumnSpec(ROLE_RSI, 'rsi', True, 'RSI（默认周期 14）'),
    ColumnSpec(ROLE_ATR, 'atr', True, 'ATR（默认周期 14）'),
))

#: 财务/估值层契约：四张源表各自独立，列以
#: ``src/advice/fundamental_factors.py::build_fundamentals`` 的真实读取为准。
#: 空帧（确实无数据但 schema 齐全）允许通过，缺列一律抛错。
VALUATION_CONTRACT = FrameContract('valuation', (
    ColumnSpec(ROLE_TS_CODE, 'ts_code', True, '标的代码'),
    ColumnSpec(ROLE_TRADE_DATE, 'trade_date', True, '估值日'),
    ColumnSpec(ROLE_TOTAL_MV, 'total_mv', True, '总市值（元），ff_size 分母'),
    ColumnSpec(ROLE_CIRC_MV, 'circ_mv', True, '流通市值（元），优先于 total_mv'),
    ColumnSpec(ROLE_DV_RATIO, 'dv_ratio', True, '股息率（百分数），ff_div_yield'),
))

SHARES_CONTRACT = FrameContract('shares', (
    ColumnSpec(ROLE_TS_CODE, 'ts_code', True, '标的代码'),
    ColumnSpec(ROLE_EFFECTIVE_DATE, 'effective_date', True, '股本生效日（as-of 连接键）'),
    ColumnSpec(ROLE_FLOAT_SHARE, 'float_share', True, '流通股本（股），换手率分母'),
))

INCOME_CONTRACT = FrameContract('income', (
    ColumnSpec(ROLE_TS_CODE, 'ts_code', True, '标的代码'),
    ColumnSpec(ROLE_END_DATE, 'end_date', True, '报告期'),
    ColumnSpec(ROLE_ANN_DATE, 'ann_date', True, '公告日（可见性 as-of 键）'),
    ColumnSpec(ROLE_REVENUE, 'revenue', True, '营业收入（累计），ff_sales_yield'),
    ColumnSpec(ROLE_PARENT_NET_PROFIT, 'parent_net_profit', True,
               '归母净利润（累计），ff_value_ep'),
))

BALANCE_CONTRACT = FrameContract('balance', (
    ColumnSpec(ROLE_TS_CODE, 'ts_code', True, '标的代码'),
    ColumnSpec(ROLE_END_DATE, 'end_date', True, '报告期'),
    ColumnSpec(ROLE_ANN_DATE, 'ann_date', True, '公告日（可见性 as-of 键）'),
    ColumnSpec(ROLE_TOTAL_ASSETS, 'total_assets', True, '总资产，ff_investment 同比'),
    ColumnSpec(ROLE_TOTAL_EQUITY, 'total_equity', True, '归母净资产，ff_value_bp'),
))

#: 财务层契约集合（按表名索引，供 ``validate_financial_frames`` 使用）
FINANCIAL_CONTRACTS: Tuple[FrameContract, ...] = (
    VALUATION_CONTRACT, SHARES_CONTRACT, INCOME_CONTRACT, BALANCE_CONTRACT,
)

#: 全部输入契约的唯一注册入口：帧名 → 契约。
#: 这里只登记、不复制列定义——列的唯一真源仍是上面各 ``*_CONTRACT``；
#: 新增一类输入必须同时登记到这里，否则 ``contract_for`` 取不到。
ENGINE_CONTRACTS: Dict[str, FrameContract] = {
    contract.name: contract
    for contract in (BAR_CONTRACT, FACTOR_CONTRACT, *FINANCIAL_CONTRACTS)
}

_PRICE_ROLES = (ROLE_OPEN, ROLE_HIGH, ROLE_LOW, ROLE_CLOSE)
_PAIR_ROLES = (ROLE_RAW_OPEN, ROLE_RAW_CLOSE)


def contract_for(name: str) -> FrameContract:
    """按帧名取契约（``ENGINE_CONTRACTS`` 是唯一查表入口）。"""
    try:
        return ENGINE_CONTRACTS[name]
    except KeyError as exc:
        raise KeyError(
            f'未登记的输入契约 {name!r}；已登记 {sorted(ENGINE_CONTRACTS)}。'
            '新增输入类型请先在 input_contract.py 定义契约并登记到 ENGINE_CONTRACTS'
        ) from exc


def describe_contracts() -> Dict[str, Dict[str, object]]:
    """输出契约摘要（写入回测结果 input_contract，供审计与换源对照）。"""
    return {
        contract.name: {
            'required': {
                item.role: item.column for item in contract.columns if item.required
            },
            'optional': {
                item.role: item.column for item in contract.columns if not item.required
            },
        }
        for contract in ENGINE_CONTRACTS.values()
    }


def validate_universe(
    universe: Dict[str, pl.DataFrame],
    *,
    contract: FrameContract = BAR_CONTRACT,
    price_basis: str = 'auto',
    expect: Sequence[str] = (),
    context: str = '',
) -> Dict[str, Dict[str, str]]:
    """逐标的校验行情契约，返回 {标的: {角色: 列名}}。

    校验项（任一不满足即抛 ``BacktestInputContractError``）：
        1. 必需列缺失 / 列名不符（按契约里的 column 精确匹配）；
        2. ``raw_open`` / ``raw_close`` 必须成对；
        3. 复权退化：带了 raw_* 但复权价与原始价完全一致 —— 复权因子恒为
           1.0，等于用未复权价做信号/收益（除非 price_basis='raw' 显式豁免）；
        4. 必需价格列全为空、volume 全为 0/空 —— 数据实际不可用。
    """
    if price_basis not in PRICE_BASIS_CHOICES:
        raise ValueError(
            f'price_basis 必须是 {PRICE_BASIS_CHOICES} 之一，实际 {price_basis!r}')

    resolved_by_code: Dict[str, Dict[str, str]] = {}
    for code, frame in universe.items():
        label = f'{context}{code} ' if context else f'{code} '
        if not isinstance(frame, pl.DataFrame):
            raise BacktestInputContractError(
                f'{label}不是 polars DataFrame（{type(frame).__name__}）')
        try:
            resolved = contract.resolve_columns(frame.columns, expect=expect)
        except BacktestInputContractError as exc:
            # 补上标的标签：组合回测必须能直接看出是哪只票坏了契约
            raise BacktestInputContractError(f'{label}{exc}') from exc
        if frame.height == 0:
            # 空面板没有值可校验，交由上层（无数据分支）处理
            resolved_by_code[code] = resolved
            continue
        _check_raw_pair(frame, resolved, label)
        _check_price_basis(frame, resolved, label, price_basis)
        _check_required_values(frame, resolved, label)
        resolved_by_code[code] = resolved
    return resolved_by_code


#: 因子列全空判定的最短帧长：MACD 慢线为 26 日，短于该长度的帧里长周期
#: 均线与 MACD 本来就可能全空（min_samples 未满），不能判为"预计算未生效"
FACTOR_WARMUP_ROWS = 26


def _ma_window(column: str) -> int | None:
    """从 ``ma_{window}`` 列名解析窗口；非均线列返回 None。"""
    if column.startswith('ma_') and column[3:].isdigit():
        return int(column[3:])
    return None


def validate_factor_frame(
    frame: pl.DataFrame,
    *,
    ma_columns: Sequence[str] = (),
    context: str = '',
) -> None:
    """校验指标预计算后的因子列（策略读取前调用，缺列或整列全空即抛错）。

    Args:
        frame: ``_precompute_indicators`` 的产物；
        ma_columns: 本次策略实际使用的均线列（如 ``ma_5`` / ``ma_20``），
            替代契约里 ma_10 / ma_20 的默认窗口；其余必需列仍由
            ``FACTOR_CONTRACT`` 声明，不存在第二份列清单；
        context: 出错时前缀（策略名），便于定位是哪个策略的读取挂了。

    为什么不能只查列存在：策略读到全 None 会恒返回 hold，回测静默空跑，
    结果看起来正常但完全没有信号。因此列存在 + （帧长足以暖机时）非全空
    两项都查。
    """
    default_ma_columns = {FACTOR_CONTRACT.column(ROLE_MA_SHORT),
                          FACTOR_CONTRACT.column(ROLE_MA_LONG)}
    required = [
        spec.column for spec in FACTOR_CONTRACT.columns
        if spec.required and not (ma_columns and spec.column in default_ma_columns)
    ]
    required.extend(ma_columns)

    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise BacktestInputContractError(
            f'{context} 指标预计算缺少因子列 {missing}：'
            '策略会读到 None 并恒返回 hold，回测将静默空跑'
        )

    if frame.height < FACTOR_WARMUP_ROWS:
        return

    atr_column = FACTOR_CONTRACT.column(ROLE_ATR)
    empty: list[str] = []
    for name in required:
        if name == atr_column:
            continue
        window = _ma_window(name)
        if window is not None and frame.height < window:
            continue
        if frame[name].is_not_null().sum() == 0:
            empty.append(name)
    if empty:
        raise BacktestInputContractError(
            f'{context} 因子列全为空 {empty}：指标预计算未生效'
            '（策略会恒返回 hold，回测静默空跑）'
        )


def validate_financial_frames(
    frames: Mapping[str, pl.DataFrame], *, context: str = '',
) -> Dict[str, Dict[str, str]]:
    """校验财务/估值源表列契约，返回 {表名: {角色: 列名}}。

    Args:
        frames: 键必须是已登记的表名（valuation / shares / income / balance），
            未登记的表名直接 ``KeyError``，避免"校验了但没校验到"；
        context: 出错时前缀。

    空帧允许通过（schema 齐全、确实没有数据由上层分支处理），缺列即抛
    ``BacktestInputContractError``：否则 polars 会在 ``select`` 处抛
    ``ColumnNotFoundError``，既看不出是哪张表的哪一列契约被破坏，也无法一次
    报全所有缺失列。
    """
    resolved: Dict[str, Dict[str, str]] = {}
    for name, frame in frames.items():
        contract = contract_for(name)
        if not isinstance(frame, pl.DataFrame):
            raise BacktestInputContractError(
                f'{context}{name} 不是 polars DataFrame（{type(frame).__name__}）')
        try:
            resolved[name] = contract.resolve_columns(frame.columns)
        except BacktestInputContractError as exc:
            raise BacktestInputContractError(f'{context}{exc}') from exc
    return resolved


# ---------------------------------------------------------------------------
# 内部校验
# ---------------------------------------------------------------------------


def _check_raw_pair(
    frame: pl.DataFrame, resolved: Dict[str, str], label: str,
) -> None:
    has_open = ROLE_RAW_OPEN in resolved
    has_close = ROLE_RAW_CLOSE in resolved
    if has_open == has_close:
        return
    raise BacktestInputContractError(
        f'{label}raw_open / raw_close 必须成对出现（当前只有 '
        f'{"raw_open" if has_open else "raw_close"}）；缺一列会让成交价与'
        '涨跌停判定落在不同价格口径上'
    )


def _check_price_basis(
    frame: pl.DataFrame, resolved: Dict[str, str], label: str, price_basis: str,
) -> None:
    if price_basis == 'raw':
        # 显式声明的原始层消费方：没有复权因子是正常的
        return
    if ROLE_RAW_CLOSE not in resolved:
        if price_basis == 'adjusted':
            raise BacktestInputContractError(
                f'{label}声明 price_basis="adjusted" 但缺 raw_close：'
                '无法证明输入是复权价，也无法用原始价做涨跌停判定'
            )
        # 'auto' 且无 raw_* → 视为原始价输入（如 TDX 本地数据），合法
        return

    close_col, raw_col = resolved[ROLE_CLOSE], resolved[ROLE_RAW_CLOSE]
    pair = pl.DataFrame({
        'adjusted': frame[close_col].cast(pl.Float64, strict=False),
        'raw': frame[raw_col].cast(pl.Float64, strict=False),
    }).drop_nulls()
    if pair.height == 0:
        raise BacktestInputContractError(
            f'{label}close 与 raw_close 没有任何同时非空的行，无法校验复权口径')
    diff = (pair['adjusted'] - pair['raw']).abs().max()
    if diff is not None and float(diff) == 0.0:
        raise BacktestInputContractError(
            f'{label}复权价与原始价完全相同（{pair.height} 行逐值相等）：'
            '复权因子恒为 1.0，等价于未复权价参与信号/收益计算。'
            "确有原始层消费方请显式传 price_basis='raw'"
        )


def _check_required_values(
    frame: pl.DataFrame, resolved: Dict[str, str], label: str,
) -> None:
    empty = [
        resolved[role] for role in (ROLE_DATE, *_PRICE_ROLES, ROLE_VOLUME)
        if frame[resolved[role]].is_not_null().sum() == 0
    ]
    if empty:
        raise BacktestInputContractError(
            f'{label}必需列全为空：{empty}（列存在但没有任何有效值）')
    # 注：volume 全为 0 是合法的「整段停牌」表达，不在此判错；
    # 停牌与 ADV 参与率由撮合层逐笔拒单并写入 rejected_orders。
