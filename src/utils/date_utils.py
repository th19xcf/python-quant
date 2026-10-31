#!/usr/bin/env python3

"""
日期解析公共工具

统一收口数据层与回测层对 date/字符串日期的解析口径，避免各模块重复实现。
"""

from datetime import date, datetime
from typing import Any


def parse_date(value: Any) -> date | None:
    """将 date/datetime/字符串日期统一转换为 date。

    支持 '%Y-%m-%d' 与 '%Y%m%d' 两种格式；None 或无法解析时返回 None。
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(str(value), fmt).date()
        except ValueError:
            continue
    return None
