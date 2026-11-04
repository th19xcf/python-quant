#!/usr/bin/env python3

"""
筹码分布应用服务桥接。

UI 层（``src/ui``）只经本模块取得筹码分布的计算入口与结果类型，不再直接
依赖 ``src.tech_analysis.chip``；计算口径、分箱与峰识别仍由
``src.tech_analysis.chip`` 唯一负责，本模块只承担依赖方向上的中转。

A 档所需的真实换手率也在本层补齐：UI 传入 ``(df, ts_code)``，这里调用
``src.data.local_store.turnover.attach_turn`` 按日期左连接 ``turn``，
避免 UI 直连数据层。``turn`` 缺失时保留原帧，由计算层逐行回退到代理口径。
"""

import polars as pl
from loguru import logger

from src.data.local_store.turnover import attach_turn
from src.tech_analysis.chip import (
    CLAMPED_WARN_THRESHOLD,
    DEFAULT_BINS,
    DEFAULT_TURNOVER_FLOOR_MODE,
    TURNOVER_FLOOR_LABELS,
    TURNOVER_FLOOR_PRESETS,
    TURNOVER_SOURCES,
    ChipProfile,
    compute_chip_profile,
)

__all__ = [
    'ChipProfile',
    'CLAMPED_WARN_THRESHOLD',
    'DEFAULT_BINS',
    'DEFAULT_TURNOVER_FLOOR_MODE',
    'DEFAULT_TURNOVER_SOURCE',
    'TURNOVER_FLOOR_LABELS',
    'TURNOVER_FLOOR_PRESETS',
    'TURNOVER_SOURCES',
    'compute_chip_profile',
    'prepare_chip_frame',
]

# UI 默认走 A 档（真实换手率衰减），B 档保留给对照/回退
DEFAULT_TURNOVER_SOURCE = 'turn'


def prepare_chip_frame(
    df: pl.DataFrame | None, ts_code: str, root: str | None = None
) -> pl.DataFrame | None:
    """按 ``ts_code`` 给行情帧补齐真实换手率列 ``turn``（百分数）。

    Args:
        df: 含 ``date`` 的行情帧（Polars DataFrame）
        ts_code: 标准代码，用于股本/换手率事实表查询
        root: 列存根目录；None 用项目默认目录

    Returns:
        带 ``turn`` 列的 Polars DataFrame；已含 ``turn`` 时原样返回；
        数据源不可用或结构不符时回退原帧（计算层将全量走代理口径）。
    """
    if df is None or getattr(df, 'height', 0) == 0 or 'turn' in df.columns:
        return df
    try:
        return attach_turn(df, ts_code, root).collect()
    except (OSError, ValueError, TypeError, pl.exceptions.PolarsError) as exc:
        logger.warning(f'[chip] 真实换手率附加失败，本次回退代理口径 {ts_code}: {exc}')
        return df
