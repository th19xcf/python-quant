#!/usr/bin/env python3

"""HSL（换手率）两级阈值的统一口径。

结论先行
========
真实换手率中位数约 **0.575%**，历史固定阈值 5%/10% 对主板基本失效
（绝大多数交易日永远不会触发）。本模块改为「个股自身滚动分位优先」：

* 滚动窗口 120 个交易日，至少 60 个有效样本；
* 两级阈值取窗口内 HSL 的 **60% / 85% 分位**；
* 有效样本不足时按板块回退固定值：
  主板 ≈ 1.0% / 3.0%，创业板与科创板 ≈ 3.0% / 8.0%，北交所 ≈ 2.0% / 6.0%。

周/月线的 HSL 是**区间逐日累计口径**（区间内各交易日 turn 直接求和），
数值天然是日线的约 5 倍 / 23 倍，因此温启动期的固定回退值必须按周期放大
（倍数由 4 只样本票「周(月)线 HSL 中位数 ÷ 日线 HSL 中位数」实测中位确定）；
分位阈值则按各周期自身序列的历史分布自适应，**不需要**按周期手工缩放。

``PERIOD_SCALES`` 只服务于「样本不足时的固定回退值」，与 HSL 的计算口径无关：
逐日求和与旧的股本加权口径在股本恒定期数学等价，历史实测倍数因此仍然适用。
它**已被限制为 fallback-only**，不得再用于对任意日线阈值做周期换算
（``percentile_thresholds`` / ``rolling_thresholds`` 的样本充足路径不读它）。

模块只做纯数值计算，不依赖 Polars/UI，供指标层、图表标签与着色
（``chart_manager`` / ``indicator_display`` / ``oscillator``）共用，
保证「阈值判定」只有一个真源。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np

#: 滚动窗口长度（交易日）
HSL_WINDOW = 120
#: 有效样本下限；不足则回退固定阈值
HSL_MIN_SAMPLES = 60
#: 两级阈值分位
HSL_LOW_QUANTILE = 0.60
HSL_HIGH_QUANTILE = 0.85

#: 样本不足时的板块固定阈值（低, 高），单位百分数（日线口径）
FIXED_THRESHOLDS: dict[str, tuple[float, float]] = {
    'main': (1.0, 3.0),      # 主板
    'growth': (3.0, 8.0),    # 创业板 / 科创板
    'bj': (2.0, 6.0),        # 北交所
}

#: 周/月线固定回退值的放大倍数（实测中位数，见模块文档）
#: 周线：4 只样本票「周线中位 ÷ 日线中位」= 4.955 / 4.690 / 5.058 / 5.124，中位 5.007
#: 月线：同法 = 23.798 / 20.266 / 22.015 / 27.724，中位 22.907
#:
#: 状态：**legacy / fallback-only**（deprecated for general reuse）。
#: 仅被 :func:`fixed_thresholds` 用于样本不足时的板块固定阈值放大；
#: 这些倍数本质是「每周期交易日数」（≈5 / ≈22.9），与 HSL 累计口径无关，
#: 故新的逐日求和口径下继续有效，不删除（保留给其它调用方与回滚）。
#: 不得用于把某条日线阈值换算到周/月线——分位阈值按周期自身分布自适应。
PERIOD_SCALES: dict[str, float] = {
    'day': 1.0,
    'week': 5.0,
    'month': 22.9,
}

_PERIOD_ALIASES: dict[str, str] = {
    'day': 'day', 'daily': 'day', '1d': 'day', '日线': 'day', '日': 'day',
    'week': 'week', 'weekly': 'week', '1w': 'week', '周线': 'week', '周': 'week',
    'month': 'month', 'monthly': 'month', '1m': 'month', '月线': 'month', '月': 'month',
}

_GROWTH_PREFIXES = ('300', '301', '688', '689')
_BJ_PREFIXES = ('4', '8', '92')


def board_of(ts_code: str | None) -> str:
    """按代码前缀判断板块：``main`` / ``growth`` / ``bj``。

    无法识别时按主板处理（固定阈值最保守）。
    """
    if not ts_code:
        return 'main'
    text = str(ts_code).strip().upper()
    digits = ''.join(ch for ch in text.split('.')[0] if ch.isdigit())
    if len(digits) != 6:
        digits = ''.join(ch for ch in text if ch.isdigit())[:6]
    if len(digits) != 6:
        return 'main'
    if digits.startswith('92') or digits.startswith(_BJ_PREFIXES):
        return 'bj'
    if digits.startswith(_GROWTH_PREFIXES):
        return 'growth'
    return 'main'


def period_of(period: str | None) -> str:
    """把周期写法归一为 ``day`` / ``week`` / ``month``；无法识别按日线。"""
    if not period:
        return 'day'
    return _PERIOD_ALIASES.get(str(period).strip().lower(), 'day')


def fixed_thresholds(ts_code: str | None,
                     period: str | None = 'day') -> tuple[float, float]:
    """板块固定阈值（样本不足时的回退值），按周期放大 ``PERIOD_SCALES``。"""
    low, high = FIXED_THRESHOLDS[board_of(ts_code)]
    scale = PERIOD_SCALES[period_of(period)]
    return low * scale, high * scale


def _finite(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float).ravel()
    return arr[np.isfinite(arr)]


def _quantile_pair(values: np.ndarray, ts_code: str | None,
                   min_samples: int, period: str | None = 'day',
                   ) -> tuple[float, float]:
    valid = _finite(values)
    if valid.size < min_samples:
        return fixed_thresholds(ts_code, period)
    low, high = np.quantile(valid, [HSL_LOW_QUANTILE, HSL_HIGH_QUANTILE])
    return _normalize_pair(float(low), float(high))


def _normalize_pair(low: float, high: float) -> tuple[float, float]:
    """保证 high > low；常量窗口下给一位小数级的间隔，避免阈值退化。"""
    if high <= low:
        high = low + max(0.01, abs(low) * 0.001)
    return low, high


def percentile_thresholds(values: Sequence[float] | np.ndarray,
                          ts_code: str | None = None,
                          window: int = HSL_WINDOW,
                          min_samples: int = HSL_MIN_SAMPLES,
                          period: str | None = 'day',
                          ) -> tuple[float, float]:
    """用**最新** ``window`` 个有效样本算两级阈值；样本不足回退固定值。

    Args:
        values: HSL 序列（百分数，可含 NaN）
        ts_code: 用于板块回退与代码解析
        window: 滚动窗口长度
        min_samples: 有效样本下限
        period: 周期写法（``日线`` / ``1w`` / ``month`` 等），仅影响固定回退值缩放

    Returns:
        tuple[float, float]: (低阈值, 高阈值)，单位百分数
    """
    arr = np.asarray(values, dtype=float).ravel()
    if window > 0:
        arr = arr[-window:]
    return _quantile_pair(arr, ts_code,
                          max(1, min(min_samples, max(1, window))), period)


def rolling_thresholds(values: Sequence[float] | np.ndarray,
                       ts_code: str | None = None,
                       window: int = HSL_WINDOW,
                       min_samples: int = HSL_MIN_SAMPLES,
                       period: str | None = 'day',
                       ) -> tuple[np.ndarray, np.ndarray]:
    """逐根 K 线的滚动阈值（只看截至当日的 trailing window，避免未来函数）。

    Returns:
        (low, high): 与输入等长的两个 float64 数组；样本不足处取板块固定值
    """
    arr = np.asarray(values, dtype=float).ravel()
    n = arr.size
    fallback_low, fallback_high = fixed_thresholds(ts_code, period)
    low = np.full(n, fallback_low, dtype=float)
    high = np.full(n, fallback_high, dtype=float)
    if n == 0 or window <= 0:
        return low, high

    min_samples = max(1, min(min_samples, window))
    padded = np.concatenate([np.full(window - 1, np.nan), arr])
    windows = np.lib.stride_tricks.sliding_window_view(padded, window)
    valid_counts = np.isfinite(windows).sum(axis=1)
    ready = valid_counts >= min_samples
    if not ready.any():
        return low, high

    quantiles = np.nanquantile(
        windows[ready], [HSL_LOW_QUANTILE, HSL_HIGH_QUANTILE], axis=1)
    low_q = quantiles[0].astype(float)
    high_q = quantiles[1].astype(float)
    degenerate = high_q <= low_q
    high_q[degenerate] = low_q[degenerate] + np.maximum(
        0.01, np.abs(low_q[degenerate]) * 0.001)
    low[ready] = low_q
    high[ready] = high_q
    return low, high


def classify(value: Any, low: float, high: float) -> str:
    """按两级阈值分类：``high`` / ``mid`` / ``low``（供 UI 着色复用）。"""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 'low'
    if not np.isfinite(number):
        return 'low'
    if number > high:
        return 'high'
    if number > low:
        return 'mid'
    return 'low'
