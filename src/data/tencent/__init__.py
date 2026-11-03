#!/usr/bin/env python3

"""腾讯行情离线补数包（只写本地，不进入运行时读路径）。

定位约束：
- 仅由显式 CLI（``python -m src.data.tencent``）触发，默认不联网；
- 数据只能「远程 → 本地列存/快照」，运行时分析路径继续只读本地；
- 不读取、不修改 ``data.allow_remote_sources``，也不被 ``DataFetcher`` 引用。
"""

from src.data.tencent.client import (
    MAX_MINUTE_BARS,
    MINUTE_FREQUENCIES,
    TencentClient,
    TencentError,
    to_tencent_symbol,
)
from src.data.tencent.daily_fill import (
    DEFAULT_DATA_TYPES,
    QuoteCoverage,
    fill_daily_gap,
    scan_quote_files,
    verify_daily,
)
from src.data.tencent.minute import fetch_minute_batch, normalize_volume

__all__ = [
    'MAX_MINUTE_BARS',
    'MINUTE_FREQUENCIES',
    'DEFAULT_DATA_TYPES',
    'QuoteCoverage',
    'TencentClient',
    'TencentError',
    'fill_daily_gap',
    'fetch_minute_batch',
    'normalize_volume',
    'scan_quote_files',
    'to_tencent_symbol',
    'verify_daily',
]
