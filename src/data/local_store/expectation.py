#!/usr/bin/env python3

r"""本地列存「缺数据」的判定：预期为空，还是故障。

背景
----
``store.py`` / ``data_manager.py`` 等处在文件不存在时返回 ``None`` / ``{}`` /
空表。路径写错或目录搬迁漏改都不会报错，只在下游结果里体现为「样本少了几只」，
这正是 DG-27 说的「把未接线当成无数据」。本模块把两类缺失分开，供读侧统一调用：

- **预期为空**：未上市 / 已退市 / 请求区间与已构建覆盖区间无交集 → 记 ``warn``，按空返回；
- **理应存在**：上市期内且 ``meta/daily_index.parquet`` 声明有覆盖 → 抛
  :class:`MissingLocalDataError`，报文带路径与判定依据；
- **列存不可用**：根目录或 ``meta/`` / ``quotes/<类型>/`` 目录不在 → 直接抛，指出根路径。

判定口径（按优先级）
--------------------
1. **上市信息**（``meta/stock_basic.parquet`` 的 ``list_date`` / ``delist_date``）：
   请求区间整段早于上市日、或整段晚于退市日 → 预期为空；
2. **覆盖度**（``meta/daily_index.parquet`` 的 ``rows`` / ``start`` / ``end``）：
   声明有行且与请求区间相交 → 理应存在；区间完全落在覆盖区间外 → 预期为空；
3. 两者都查不到 → 该标的从未构建：记 ``warn`` 并按空返回，**不抛错**——否则
   首次构建、新增上市标的采集会被自家断言挡住。

停牌不在本模块判定
------------------
停牌期间行情文件仍然存在，缺失只可能出现在**行级**。把停牌当文件级缺失处理，
会让真实路径错误被「停牌」这个理由掩盖，因此行级空帧由调用方 ``warn``。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

#: 上市信息缓存：只保留最新一份 ``(路径, mtime)`` 对应的表，避免常驻多份
_LISTING_CACHE: dict[tuple[str, int], dict[str, ListingWindow]] = {}
#: 「根目录不存在」已告警过的根，避免逐票重复刷日志
_ROOT_MISSING_WARNED: set[str] = set()


class MissingLocalDataError(RuntimeError):
    """本地列存应有数据却读不到：路径写错、搬迁漏改或构建缺失。"""


@dataclass(frozen=True)
class ListingWindow:
    """单标的上市区间；``None`` 表示该侧无确定日期。"""

    ts_code: str
    list_date: date | None
    delist_date: date | None

    def gap_reason(self, start: date | None, end: date | None) -> str | None:
        """返回「预期为空」的原因；返回 ``None`` 表示请求区间落在上市期内。"""
        if self.list_date is not None and end is not None and end < self.list_date:
            return f'未上市（上市日 {self.list_date}，请求止于 {end}）'
        if self.delist_date is not None and start is not None and start > self.delist_date:
            return f'已退市（退市日 {self.delist_date}，请求起于 {start}）'
        return None


def _as_date(value: Any) -> date | None:
    """把 parquet 日期值（date / datetime / 字符串）归一为 ``date``。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in {'none', 'nat', 'null'}:
        return None
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text[:8], fmt).date()
        except ValueError:
            continue
    return None


def listing_windows(store_root: Path | str) -> dict[str, ListingWindow]:
    """读取 ``meta/stock_basic.parquet`` 的上市区间（按 mtime 缓存）。

    文件不存在或不可读时返回空表——此时缺失分级退化为「按覆盖度判定」。
    """
    path = Path(store_root) / 'meta' / 'stock_basic.parquet'
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        return {}
    key = (str(path), mtime)
    cached = _LISTING_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        frame = pl.read_parquet(path, columns=['ts_code', 'list_date', 'delist_date'])
    except (OSError, ValueError, TypeError, PolarsError) as exc:
        logger.warning(f'[local-store] 上市信息不可读，缺失分级退化为按覆盖度判定: {path}: {exc}')
        return {}
    windows = {
        str(row['ts_code']): ListingWindow(
            ts_code=str(row['ts_code']),
            list_date=_as_date(row.get('list_date')),
            delist_date=_as_date(row.get('delist_date')),
        )
        for row in frame.iter_rows(named=True)
    }
    _LISTING_CACHE.clear()
    _LISTING_CACHE[key] = windows
    return windows


def listing_window(store_root: Path | str, ts_code: str) -> ListingWindow | None:
    return listing_windows(store_root).get(ts_code)


def declared_coverage(store_root: Path | str, data_type: str, ts_code: str) -> tuple[date, date, int] | None:
    """从 ``meta/daily_index.parquet`` 取 ``(start, end, rows)``；无记录返回 ``None``。"""
    path = Path(store_root) / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return None
    try:
        frame = pl.read_parquet(
            path, columns=['ts_code', 'data_type', 'rows', 'start', 'end'])
    except (OSError, ValueError, TypeError, PolarsError) as exc:
        logger.warning(f'[local-store] 覆盖度元数据不可读: {path}: {exc}')
        return None
    hit = frame.filter(
        (pl.col('ts_code') == ts_code) & (pl.col('data_type') == data_type))
    if hit.is_empty():
        return None
    row = hit.row(0, named=True)
    return (_as_date(row['start']) or date.min,
            _as_date(row['end']) or date.min,
            int(row['rows'] or 0))


def ensure_store_root(store_root: Path | str, subdir: str | None = None) -> None:
    """列存根目录（或指定子目录）不存在时直接抛错，指出实际使用的路径。"""
    root = Path(store_root)
    if not root.is_dir():
        raise MissingLocalDataError(
            f'本地列存根目录不存在: {root}；'
            '检查 DATA__LOCAL_STORE_PATH / 配置项 local_store_path 是否指向正确目录')
    if subdir:
        target = root / subdir
        if not target.is_dir():
            raise MissingLocalDataError(
                f'本地列存子目录不存在: {target}；根目录 {root} 存在但缺少 {subdir}，'
                '疑似目录搬迁漏改或构建未完成')


def explain_gap(store_root: Path | str, data_type: str, ts_code: str,
                start: Any = None, end: Any = None) -> str | None:
    """判断缺失是否属于预期。

    Returns:
        ``None`` 表示「理应存在」（调用方应抛 :class:`MissingLocalDataError`）；
        字符串表示预期为空的原因（调用方按空返回并 ``warn``）。
    """
    start_dt = _as_date(start)
    end_dt = _as_date(end)

    window = listing_window(store_root, ts_code)
    if window is not None:
        reason = window.gap_reason(start_dt, end_dt)
        if reason:
            return reason
        # 只有带日期上下文时，上市期内才能直接断定「理应存在」。
        # 不带日期（read_file 整份读取）继续看覆盖度，否则首次构建时
        # 每一只尚未采集的标的都会被当成故障。
        if (start_dt is not None or end_dt is not None) and (
                window.list_date is not None or window.delist_date is not None):
            return None

    coverage = declared_coverage(store_root, data_type, ts_code)
    if coverage is not None:
        cov_start, cov_end, rows = coverage
        if rows > 0:
            if start_dt is not None and end_dt is not None and (
                    end_dt < cov_start or start_dt > cov_end):
                return (f'请求区间 {start_dt}~{end_dt} 与已构建覆盖区间 '
                        f'{cov_start}~{cov_end} 无交集')
            return None
        return f'{ts_code} 在 daily_index 中登记为 0 行（构建时即无数据）'

    return f'{ts_code} 未在 daily_index 中登记（该 {data_type} 尚未构建）'


def check_quote_file(path: Path | str, *, store_root: Path | str, data_type: str,
                     ts_code: str, start: Any = None, end: Any = None,
                     quiet: bool = False, require_root: bool = True) -> str | None:
    """行情文件缺失时的统一处置。

    - 文件存在 → 返回 ``None``；
    - 预期为空 → 记 ``warn`` 并返回原因字符串，调用方按空返回；
    - 理应存在 → 抛 :class:`MissingLocalDataError`，报文带路径。

    ``require_root=False`` 用于采集/构建语义（``read_file``）：首次构建时
    根目录本就尚未创建，不能当故障；但根目录写错也表现为同一现象，故对每个
    根只留一条 ``warn``，让「路径写错」在日志里立即可见。
    """
    target = Path(path)
    if target.is_file():
        return None
    root = Path(store_root)
    if require_root or root.is_dir():
        ensure_store_root(root)
    else:
        # 只校验「根目录是否可用」：quotes/<类型>/ 子目录在首次构建时可能尚未创建，
        # 把它当故障会让新库构建直接失败（误报）。真正的路径写错表现为根目录不存在，
        # 或 daily_index 声明有覆盖而文件不在——后者由下面的 explain_gap 覆盖。
        key = str(root)
        if key not in _ROOT_MISSING_WARNED:
            _ROOT_MISSING_WARNED.add(key)
            logger.warning(
                f'[local-store] 列存根目录不存在，按「尚未构建」返回空: {key}；'
                '首次构建属正常，读取链路请检查 DATA__LOCAL_STORE_PATH / local_store_path')
        return f'列存根目录不存在: {key}'
    reason = explain_gap(root, data_type, ts_code, start, end)
    if reason is not None:
        log = logger.debug if quiet else logger.warning
        log(f'[local-store] 行情文件缺失但属预期为空: {target}（{reason}）')
        return reason
    coverage = declared_coverage(store_root, data_type, ts_code)
    detail = ('daily_index 声明有覆盖' if coverage is not None
              else '上市期内但 daily_index 无记录')
    raise MissingLocalDataError(
        f'本地行情文件缺失: {target}；{ts_code} 在 {start}~{end} 处于上市期，'
        f'{detail}，读不到数据属故障（路径写错 / 搬迁漏改 / 构建缺失）')
