#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
复权因子计算工具。
"""

from typing import Optional


def _normalize_ratio_value(value: Optional[float]) -> float:
    """
    标准化送转/配股比例。

    常见数据源有“每股”与“每10股”两种口径。
    - 每股口径通常在 [0, 2] 区间
    - 每10股口径在原值 > 2 时做除以10转换
    """
    if value is None:
        return 0.0
    numeric = float(value)
    if numeric > 2.0:
        return numeric / 10.0
    return numeric


def calculate_qfq_event_factor(
    prev_close: float,
    cash_div: Optional[float],
    share_div: Optional[float],
    rights_issue_price: Optional[float] = None,
    rights_issue_ratio: Optional[float] = None,
) -> Optional[float]:
    """
    计算单次除权除息事件的前复权事件因子。

    公式：
        factor = (P0 - D + R * Pr) / (P0 * (1 + S + R))

    其中：
    - P0: 除权日前收盘价
    - D: 每股现金分红
    - S: 每股送转股
    - R: 每股配股比例
    - Pr: 配股价
    """
    if prev_close is None or prev_close <= 0:
        return None

    cash = float(cash_div or 0.0)
    share = _normalize_ratio_value(share_div)
    rights_ratio = _normalize_ratio_value(rights_issue_ratio)
    rights_price = float(rights_issue_price or 0.0)

    numerator = prev_close - cash + rights_price * rights_ratio
    denominator = prev_close * (1.0 + share + rights_ratio)

    if denominator <= 0 or numerator <= 0:
        return None

    factor = numerator / denominator
    if factor <= 0:
        return None
    return factor
