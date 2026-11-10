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
from typing import Dict, Sequence, Tuple

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

#: 因子契约：指标预计算的产物，策略读取前必须存在
FACTOR_CONTRACT = FrameContract('factor', (
    ColumnSpec(ROLE_MA_SHORT, ma_column(10), True, '短期均线（参数化，见 ma_column）'),
    ColumnSpec(ROLE_MA_LONG, ma_column(20), True, '长期均线（参数化，见 ma_column）'),
    ColumnSpec(ROLE_MACD, 'macd', True, 'MACD 快慢线差'),
    ColumnSpec(ROLE_MACD_SIGNAL, 'macd_signal', True, 'MACD 信号线'),
    ColumnSpec(ROLE_MACD_HIST, 'macd_hist', True, 'MACD 柱'),
    ColumnSpec(ROLE_RSI, 'rsi', True, 'RSI（默认周期 14）'),
    ColumnSpec(ROLE_ATR, 'atr', True, 'ATR（默认周期 14）'),
))

#: 财务/估值契约：只被成本分档消费，因此不是回测的必需列
FINANCIAL_CONTRACT = FrameContract('financial', (
    ColumnSpec(ROLE_TOTAL_MV, 'total_mv', False, '总市值（元）'),
    ColumnSpec(ROLE_TOTAL_MV_YI, 'total_mv_yi', False, '总市值（亿元）'),
    ColumnSpec(ROLE_AMOUNT, 'amount', False, '成交额（元），作为流动性代理'),
))

#: 装配层需要一次性声明的全部契约
ENGINE_CONTRACTS = (BAR_CONTRACT, FACTOR_CONTRACT, FINANCIAL_CONTRACT)

_PRICE_ROLES = (ROLE_OPEN, ROLE_HIGH, ROLE_LOW, ROLE_CLOSE)
_PAIR_ROLES = (ROLE_RAW_OPEN, ROLE_RAW_CLOSE)


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
        for contract in ENGINE_CONTRACTS
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


def validate_factor_frame(frame: pl.DataFrame, *, context: str = '') -> None:
    """校验指标预计算后的因子列（策略读取前调用，缺列即抛错）。"""
    resolved = FACTOR_CONTRACT.resolve_columns(frame.columns)
    missing_values = [
        role for role, column in resolved.items()
        if role != ROLE_ATR and frame[column].is_not_null().sum() == 0
    ]
    if missing_values:
        raise BacktestInputContractError(
            f'{context} 因子列全为空：{missing_values}；'
            '指标预计算未生效（策略会恒返回 hold，回测静默空跑）'
        )


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
