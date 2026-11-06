"""统一日期归一化入口。

背景：``src`` 下曾有 19 份各自实现的 ``_as_date``，对 ``pd.Timestamp`` /
``np.datetime64`` / ``'YYYYMMDD'`` / ``pd.NaT`` 的行为互不一致。2026-11-06
的 P0-1 已证明这类差异会让历史印花税与过户费时间表**静默失效**（时间戳
对象解析为 ``None`` 时退回现行静态费率）。本模块提供单一真源，新代码一律
使用 ``as_date``；存量副本按契约测试分批收敛，不做一次性大替换。

设计约定：
- 返回 ``datetime.date``（不是 ``datetime``），``datetime`` 必须先于 ``date``
  判定，否则 ``isinstance(ts, date)`` 会把时间戳当 date 原样返回；
- ``pd.NaT`` / ``np.datetime64('NaT')`` / ``None`` / 不可解析一律返回 ``None``
  （``strict=True`` 时抛 ``ValueError``，供原本就要求严格解析的调用方使用）；
- 不依赖 ``src.backtest`` / ``src.data``，避免引入循环依赖。
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def _invalid(value: Any, strict: bool) -> date | None:
    if strict:
        raise ValueError(f"无法解析为日期: {value!r}")
    return None


def as_date(value: Any, *, strict: bool = False) -> date | None:
    """把常见日期/时间戳表示归一为 ``datetime.date``；失败返回 ``None``。

    支持：``date``、``datetime``、``pd.Timestamp``、``np.datetime64``、
    ``'YYYY-MM-DD'``、``'YYYYMMDD'``、带 ``.date()`` 的第三方时间戳对象。
    ``strict=True`` 时对无效输入抛 ``ValueError`` 而非返回 ``None``。
    """
    if value is None or value is pd.NaT:
        return _invalid(value, strict)
    if isinstance(value, np.datetime64):
        if np.isnat(value):
            return _invalid(value, strict)
        return pd.Timestamp(value).date()
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return _invalid(value, strict)
        return value.date()
    # pd.Timestamp 是 datetime 的子类，已在上面单独处理；此处处理原生 datetime
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        for fmt, width in (("%Y-%m-%d", 10), ("%Y%m%d", 8)):
            try:
                return datetime.strptime(text[:width], fmt).date()
            except ValueError:
                continue
        return _invalid(value, strict)
    # Polars 等第三方时间戳：鸭子类型兜底（P0-1 原有行为，保留兼容）
    if hasattr(value, "date") and callable(value.date):
        try:
            converted = value.date()
        except (TypeError, ValueError):
            return _invalid(value, strict)
        if isinstance(converted, date) and not isinstance(converted, datetime):
            return converted
    return _invalid(value, strict)
