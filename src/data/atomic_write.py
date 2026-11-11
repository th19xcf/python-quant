#!/usr/bin/env python3

"""原子写盘：同目录临时文件 → ``fsync`` → ``os.replace`` 覆盖目标。

支持 eager / lazy parquet、文本与二进制四类写入；写一半失败（断电、进程
被杀、磁盘满）时目标文件保持原样，不会留下半截 parquet / json / csv；
临时文件以 ``.<目标文件名>.tmp`` 命名，失败即清理，不在数据目录里遗留
``.tmp`` 垃圾。全仓正式数据写入必须走本模块（DG-9），禁止手写
「临时文件 + ``os.replace``」——那条路径没有 fsync。

**单文件原子，不等于多文件事务**：多个文件逐个原子写时，中途失败仍会
出现「前几个已更新、后几个还是旧值」的跨文件不一致，``os.replace`` 无法
保证这一层；调用方需自行评估该残留风险。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl


def _tmp_path(path: Path) -> Path:
    return path.with_name(f'.{path.name}.tmp')


def _fsync(path: Path) -> None:
    # Windows 上 fsync 需要可写句柄，故用 ``rb+`` 而不是 ``rb``
    with open(path, 'rb+') as handle:
        os.fsync(handle.fileno())


def write_parquet_atomic(
    frame: pl.DataFrame | pl.LazyFrame, path: Path | str, *,
    compression: Any = 'zstd',
    compression_level: int | None = None,
    statistics: Any = True,
    row_group_size: int | None = None,
) -> None:
    """原子写 parquet：落临时文件、``fsync`` 后替换目标。

    ``frame`` 为 ``LazyFrame`` 时走 ``sink_parquet`` 流式落盘，避免把千万行
    中间结果 collect 回内存；两者走同一套临时文件与 fsync 收尾。

    ``polars`` 延迟导入：只用 :func:`write_text_atomic` /
    :func:`write_bytes_atomic` 的轻量脚本（如巡检）不必为此拉起 polars。
    """
    import polars as pl

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    try:
        if isinstance(frame, pl.LazyFrame):
            frame.sink_parquet(
                str(tmp), compression=compression,
                compression_level=compression_level,
                statistics=statistics, row_group_size=row_group_size)
        else:
            frame.write_parquet(
                tmp, compression=compression,
                compression_level=compression_level,
                statistics=statistics, row_group_size=row_group_size)
        _fsync(tmp)
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def write_bytes_atomic(payload: bytes, path: Path | str) -> None:
    """原子写二进制：落临时文件、``fsync`` 后替换目标。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    try:
        with open(tmp, 'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def write_text_atomic(
    text: str, path: Path | str, *, encoding: str = 'utf-8',
) -> None:
    """原子写文本：落临时文件、``fsync`` 后替换目标。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    try:
        with open(tmp, 'w', encoding=encoding, newline='') as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
