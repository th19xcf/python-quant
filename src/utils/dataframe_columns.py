#!/usr/bin/env python3

"""DataFrame 列名判定的共用实现。

项目里同时存在两套日期列：本地列存与多数指标帧用 ``date``，远程库与
部分换手率快照用 ``trade_date``。此前 7 处各自写一遍「有 date 用 date，
否则看 trade_date」，新增一处漏判就会把日期列当成缺失而静默跳过。
本模块是这条判断的唯一实现。
"""

from __future__ import annotations

from collections.abc import Iterable

_DATE_COLUMNS = ('date', 'trade_date')


def resolve_date_column(
    columns: Iterable[str], *, prefer: str = 'date',
) -> str | None:
    """返回帧中的日期列名；两列都没有时返回 ``None``。

    Args:
        columns: 帧的列名集合（``df.columns`` 或 ``collect_schema().names()``）。
        prefer: 两列同时存在时优先返回哪一个，只能是 ``date`` 或
            ``trade_date``。远程库帧用 ``prefer='trade_date'`` 保持原行为。
    """
    if prefer not in _DATE_COLUMNS:
        raise ValueError(f'prefer 只能是 {_DATE_COLUMNS}，收到 {prefer!r}')
    names = columns if isinstance(columns, (set, frozenset)) else set(columns)
    if prefer in names:
        return prefer
    for name in _DATE_COLUMNS:
        if name in names:
            return name
    return None
