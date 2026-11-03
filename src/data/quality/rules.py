#!/usr/bin/env python3

"""数据契约巡检规则。

每条规则是一个纯函数：输入一段行情（Polars DataFrame），返回违规行
（统一为 trade_date/field/value/detail 四列），绝不修改输入、绝不写数据。
规则可单独调用，也可由 :mod:`src.data.quality.checker` 批量编排。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Sequence

import polars as pl

#: 行情最小契约列；派生列（pre_close/change/pct_chg）缺失时对应规则自动跳过
REQUIRED_COLUMNS = (
    'date', 'open', 'high', 'low', 'close', 'volume', 'amount',
)
OHLC_COLUMNS = ('open', 'high', 'low', 'close')
NUMERIC_COLUMNS = OHLC_COLUMNS + ('volume', 'amount', 'pre_close', 'change', 'pct_chg')

_VIOLATION_SCHEMA = {
    'trade_date': pl.Date,
    'field': pl.Utf8,
    'value': pl.Float64,
    'detail': pl.Utf8,
}


def empty_violations() -> pl.DataFrame:
    """返回带固定 schema 的空违规表，便于统一拼接。"""
    return pl.DataFrame(schema=_VIOLATION_SCHEMA)


def _cols_detail(columns: Sequence[str]) -> pl.Expr:
    """把若干列的取值拼成 ``open=.. high=..`` 形式，便于人工复核。"""
    parts: list[pl.Expr] = []
    for column in columns:
        parts.extend([pl.lit(f'{column}='), pl.col(column).cast(pl.Utf8), pl.lit(' ')])
    return pl.concat_str(parts, separator='')


def _date_detail() -> pl.Expr:
    return pl.concat_str([pl.lit('date='), pl.col('date').cast(pl.Utf8)], separator='')


def _build_violations(bad: pl.DataFrame, field: str,
                      value: pl.Expr, detail: pl.Expr) -> pl.DataFrame:
    """把过滤后的行转成统一违规表。"""
    if bad.is_empty():
        return empty_violations()
    select: list[pl.Expr] = []
    if 'date' in bad.columns:
        select.append(pl.col('date').cast(pl.Date).alias('trade_date'))
    else:
        select.append(pl.lit(None, dtype=pl.Date).alias('trade_date'))
    select.extend([
        pl.lit(field).alias('field'),
        value.cast(pl.Float64).alias('value'),
        detail.alias('detail'),
    ])
    return bad.select(select)


def _guard_columns(frame: pl.DataFrame, columns: Sequence[str]) -> bool:
    return all(column in frame.columns for column in columns)


def _finite(column: str) -> pl.Expr:
    return pl.col(column).is_not_null() & pl.col(column).is_finite()


@dataclass(frozen=True)
class RuleContext:
    """规则运行上下文（数据类别、容差、最新交易日）。"""

    data_type: str = 'stock'
    latest_trade_date: date | None = None
    price_tolerance: float = 1e-6
    amount_tolerance: float = 0.25
    pre_close_rel_tolerance: float = 1e-6
    change_abs_tolerance: float = 1e-6
    pct_chg_tolerance: float = 1e-6


RuleFunc = Callable[[pl.DataFrame, RuleContext], pl.DataFrame]


@dataclass(frozen=True)
class QualityRule:
    """一条可执行的契约规则。"""

    rule_id: str
    description: str
    severity: str
    data_types: tuple[str, ...]
    func: RuleFunc


# ----------------------------------------------------------------------
# 规则实现
# ----------------------------------------------------------------------
def rule_required_columns(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """关键列必须存在且无空值。"""
    del ctx
    outputs: list[pl.DataFrame] = []
    for column in REQUIRED_COLUMNS:
        if column not in frame.columns:
            outputs.append(pl.DataFrame(
                {
                    'trade_date': [None],
                    'field': [column],
                    'value': [None],
                    'detail': ['缺少必需列'],
                },
                schema=_VIOLATION_SCHEMA,
            ))
            continue
        bad = frame.filter(pl.col(column).is_null())
        if not bad.is_empty():
            outputs.append(_build_violations(
                bad, column, pl.lit(None, dtype=pl.Float64),
                pl.lit(f'{column} 为空值'),
            ))
    if not outputs:
        return empty_violations()
    return pl.concat(outputs, how='vertical_relaxed')


def rule_price_positive(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """OHLC 必须为正数且有限。"""
    del ctx
    if not _guard_columns(frame, OHLC_COLUMNS):
        return empty_violations()
    mask = pl.any_horizontal([
        pl.col(column).is_null()
        | (~pl.col(column).is_finite())
        | (pl.col(column) <= 0)
        for column in OHLC_COLUMNS
    ])
    return _build_violations(
        frame.filter(mask), 'OHLC',
        pl.min_horizontal(*[pl.col(c) for c in OHLC_COLUMNS]),
        _cols_detail(OHLC_COLUMNS),
    )


def rule_high_lt_max_open_close(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """high >= max(open, close)。"""
    if not _guard_columns(frame, OHLC_COLUMNS):
        return empty_violations()
    mask = pl.col('high') < (pl.max_horizontal('open', 'close') - ctx.price_tolerance)
    return _build_violations(
        frame.filter(mask), 'high', pl.col('high'), _cols_detail(OHLC_COLUMNS),
    )


def rule_low_gt_min_open_close(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """low <= min(open, close)。"""
    if not _guard_columns(frame, OHLC_COLUMNS):
        return empty_violations()
    mask = pl.col('low') > (pl.min_horizontal('open', 'close') + ctx.price_tolerance)
    return _build_violations(
        frame.filter(mask), 'low', pl.col('low'), _cols_detail(OHLC_COLUMNS),
    )


def rule_high_lt_low(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """high >= low。"""
    if not _guard_columns(frame, ('high', 'low')):
        return empty_violations()
    mask = pl.col('high') < (pl.col('low') - ctx.price_tolerance)
    return _build_violations(
        frame.filter(mask), 'high', pl.col('high'), _cols_detail(('high', 'low')),
    )


def rule_volume_invalid(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """volume 必须非负且有限（NaN/inf 均视为异常）。"""
    del ctx
    if 'volume' not in frame.columns:
        return empty_violations()
    mask = (
        pl.col('volume').is_null()
        | (~pl.col('volume').is_finite())
        | (pl.col('volume') < 0)
    )
    return _build_violations(
        frame.filter(mask), 'volume', pl.col('volume'),
        pl.concat_str([pl.lit('volume='), pl.col('volume').cast(pl.Utf8)], separator=''),
    )


def rule_amount_dimension(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """amount/(close*volume*100) 量纲比值需在容差内（仅股票；成交量单位为手）。

    指数与基金的 amount/volume 口径不同，套用股票公式会全表误报，故跳过。
    """
    if ctx.data_type != 'stock':
        return empty_violations()
    if not _guard_columns(frame, ('close', 'volume', 'amount')):
        return empty_violations()
    base = _finite('close') & _finite('volume') & (pl.col('close') > 0) & (pl.col('volume') > 0)
    ratio = pl.col('amount') / (pl.col('close') * pl.col('volume') * 100)
    mask = base & (
        pl.col('amount').is_null()
        | (~pl.col('amount').is_finite())
        | (pl.col('amount') <= 0)
        | ((ratio - 1).abs() > ctx.amount_tolerance)
    )
    return _build_violations(
        frame.filter(mask), 'amount', ratio,
        pl.concat_str([
            pl.lit('amount='), pl.col('amount').cast(pl.Utf8),
            pl.lit(' close*volume*100='),
            (pl.col('close') * pl.col('volume') * 100).cast(pl.Utf8),
        ], separator=''),
    )


def rule_future_trade_date(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """交易日不得晚于最新交易日。"""
    if ctx.latest_trade_date is None or 'date' not in frame.columns:
        return empty_violations()
    mask = pl.col('date').cast(pl.Date) > pl.lit(ctx.latest_trade_date, dtype=pl.Date)
    return _build_violations(
        frame.filter(mask), 'date', pl.lit(None, dtype=pl.Float64), _date_detail(),
    )


def rule_weekend_trade_date(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """A 股交易日不应落在周六/周日（早期周线混入时会产生此类行）。"""
    del ctx
    if 'date' not in frame.columns:
        return empty_violations()
    mask = pl.col('date').cast(pl.Date).dt.weekday().is_in([6, 7])
    return _build_violations(
        frame.filter(mask), 'date', pl.lit(None, dtype=pl.Float64), _date_detail(),
    )


def rule_duplicate_key(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """(ts_code, trade_date) 不允许重复。单标的文件内等价于 date 唯一。"""
    del ctx
    if 'date' not in frame.columns:
        return empty_violations()
    key = 'ts_code' if 'ts_code' in frame.columns else 'date'
    keys = [key] if key == 'date' else [key, 'date']
    duplicates = (
        frame.group_by(keys)
        .agg(pl.len().alias('duplicate_rows'))
        .filter(pl.col('duplicate_rows') > 1)
    )
    if duplicates.is_empty():
        return empty_violations()
    return duplicates.select([
        pl.col('date').cast(pl.Date).alias('trade_date'),
        pl.lit('date').alias('field'),
        pl.col('duplicate_rows').cast(pl.Float64).alias('value'),
        pl.concat_str([
            pl.lit('duplicate_rows='), pl.col('duplicate_rows').cast(pl.Utf8),
        ], separator='').alias('detail'),
    ])


def rule_pre_close_mismatch(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """pre_close 应等于上一交易日 close（存在复权因子时按因子折算）。"""
    if not _guard_columns(frame, ('close', 'pre_close')):
        return empty_violations()
    prev_close = pl.col('close').shift(1).alias('_prev_close')
    if 'qfq_factor' in frame.columns:
        prev_factor = pl.col('qfq_factor').shift(1)
        current_factor = pl.col('qfq_factor')
        expected = (
            pl.when(current_factor != 0)
            .then(prev_close * prev_factor / current_factor)
            .otherwise(None)
            .alias('_expected')
        )
    else:
        expected = prev_close.alias('_expected')
    work = frame.with_columns([prev_close, expected])
    mask = (
        pl.col('pre_close').is_not_null()
        & pl.col('_expected').is_not_null()
        & (
            (pl.col('pre_close') <= 0)
            | (
                (pl.col('pre_close') - pl.col('_expected')).abs()
                / pl.col('_expected').abs().clip(lower_bound=1e-12)
                > ctx.pre_close_rel_tolerance
            )
        )
    )
    return _build_violations(
        work.filter(mask), 'pre_close', pl.col('pre_close'),
        pl.concat_str([
            pl.lit('pre_close='), pl.col('pre_close').cast(pl.Utf8),
            pl.lit(' expected='), pl.col('_expected').cast(pl.Utf8),
            pl.lit(' prev_close='), pl.col('_prev_close').cast(pl.Utf8),
        ], separator=''),
    )


def rule_change_mismatch(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """change 应等于 close - pre_close。"""
    if not _guard_columns(frame, ('close', 'pre_close', 'change')):
        return empty_violations()
    expected = (pl.col('close') - pl.col('pre_close')).alias('_expected_change')
    work = frame.with_columns(expected)
    mask = (
        pl.col('pre_close').is_not_null()
        & (pl.col('pre_close') > 0)
        & (
            pl.col('change').is_null()
            | ((pl.col('change') - pl.col('_expected_change')).abs()
               > ctx.change_abs_tolerance)
        )
    )
    return _build_violations(
        work.filter(mask), 'change', pl.col('change'),
        pl.concat_str([
            pl.lit('change='), pl.col('change').cast(pl.Utf8),
            pl.lit(' expected='), pl.col('_expected_change').cast(pl.Utf8),
        ], separator=''),
    )


def rule_pct_chg_mismatch(frame: pl.DataFrame, ctx: RuleContext) -> pl.DataFrame:
    """pct_chg 应等于 (close/pre_close - 1) * 100。"""
    if not _guard_columns(frame, ('close', 'pre_close', 'pct_chg')):
        return empty_violations()
    expected = (
        (pl.col('close') / pl.col('pre_close') - 1) * 100
    ).alias('_expected_pct_chg')
    work = frame.with_columns(expected)
    mask = (
        pl.col('pre_close').is_not_null()
        & (pl.col('pre_close') > 0)
        & (
            pl.col('pct_chg').is_null()
            | ((pl.col('pct_chg') - pl.col('_expected_pct_chg')).abs()
               > ctx.pct_chg_tolerance)
        )
    )
    return _build_violations(
        work.filter(mask), 'pct_chg', pl.col('pct_chg'),
        pl.concat_str([
            pl.lit('pct_chg='), pl.col('pct_chg').cast(pl.Utf8),
            pl.lit(' expected='), pl.col('_expected_pct_chg').cast(pl.Utf8),
        ], separator=''),
    )


#: 全部内置规则；新增规则只需在此追加，巡检与报告自动覆盖
RULES: tuple[QualityRule, ...] = (
    QualityRule('required_columns', '关键列存在且无空值', 'error',
                ('stock', 'index'), rule_required_columns),
    QualityRule('price_non_positive', 'OHLC 为正数且有限', 'error',
                ('stock', 'index'), rule_price_positive),
    QualityRule('high_lt_open_close', 'high >= max(open, close)', 'error',
                ('stock', 'index'), rule_high_lt_max_open_close),
    QualityRule('low_gt_open_close', 'low <= min(open, close)', 'error',
                ('stock', 'index'), rule_low_gt_min_open_close),
    QualityRule('high_lt_low', 'high >= low', 'error',
                ('stock', 'index'), rule_high_lt_low),
    QualityRule('volume_invalid', 'volume 非负且有限', 'error',
                ('stock', 'index'), rule_volume_invalid),
    QualityRule('amount_dimension', 'amount/(close*volume*100) 量纲一致', 'warning',
                ('stock',), rule_amount_dimension),
    QualityRule('future_trade_date', '交易日不晚于最新交易日', 'error',
                ('stock', 'index'), rule_future_trade_date),
    QualityRule('weekend_trade_date', '交易日不落在周末', 'warning',
                ('stock', 'index'), rule_weekend_trade_date),
    QualityRule('duplicate_key', '(ts_code, trade_date) 无重复', 'error',
                ('stock', 'index'), rule_duplicate_key),
    QualityRule('pre_close_mismatch', 'pre_close 与上一交易日收盘一致', 'warning',
                ('stock', 'index'), rule_pre_close_mismatch),
    QualityRule('change_mismatch', 'change 可由 close/pre_close 复算', 'warning',
                ('stock', 'index'), rule_change_mismatch),
    QualityRule('pct_chg_mismatch', 'pct_chg 可由 close/pre_close 复算', 'warning',
                ('stock', 'index'), rule_pct_chg_mismatch),
)


def rule_map() -> dict[str, QualityRule]:
    """规则 ID -> 规则对象。"""
    return {rule.rule_id: rule for rule in RULES}
