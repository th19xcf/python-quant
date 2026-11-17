"""本地基金名录读取（唯一入口）。

基金名称只认 ``meta/fund_basic.parquet`` 的 ``name`` 字段，界面与分析页
统一从这里取，避免各自维护一份简称表导致同名标的显示不一致。
只读本地列存，不访问数据库。
"""

from __future__ import annotations

from typing import Any

import polars as pl

from src.data.local_store.paths import resolve_store_root
from src.utils.logger import logger


def load_fund_basic_frame(config: Any = None) -> pl.DataFrame:
    """读取本地基金名录；文件缺失时返回空表并告警。"""
    path = resolve_store_root(config) / 'meta' / 'fund_basic.parquet'
    if not path.exists():
        logger.warning(f'本地基金名录不存在: {path}')
        return pl.DataFrame()
    return pl.read_parquet(path)


def load_fund_name_map(config: Any = None) -> dict[str, str]:
    """``ts_code -> name``，名称取自 ``fund_basic.name``。"""
    frame = load_fund_basic_frame(config)
    if frame.is_empty() or not {'ts_code', 'name'}.issubset(frame.columns):
        return {}
    return {
        row['ts_code']: row['name']
        for row in frame.select(['ts_code', 'name']).iter_rows(named=True)
        if row['name']
    }
