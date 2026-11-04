#!/usr/bin/env python3

"""财务事实表本地持久化（Parquet）。

对外入口：
- :func:`latest_announced_as_of` / :func:`latest_announced_panel`：点时查询；
- :class:`FinancialsCollector`：全 A 股三表与日频估值采集；
- 命令行入口见 ``tools/collect_financials.py``。
"""

from src.data.financials.collector import (
    CollectResult,
    FinancialsCollector,
    RetryPolicy,
    normalize_bj_statement,
    normalize_bulk_statement,
    normalize_valuation,
    periods_between,
    statutory_deadline,
)
from src.data.financials.store import (
    SCHEMAS,
    STATEMENTS,
    coverage,
    financials_dir,
    latest_announced_as_of,
    latest_announced_panel,
    latest_valuation,
    merge_parts,
    read_table,
    table_path,
)

__all__ = [
    'SCHEMAS',
    'STATEMENTS',
    'CollectResult',
    'FinancialsCollector',
    'RetryPolicy',
    'coverage',
    'financials_dir',
    'latest_announced_as_of',
    'latest_announced_panel',
    'latest_valuation',
    'merge_parts',
    'normalize_bj_statement',
    'normalize_bulk_statement',
    'normalize_valuation',
    'periods_between',
    'read_table',
    'statutory_deadline',
    'table_path',
]
