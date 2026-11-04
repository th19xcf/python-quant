#!/usr/bin/env python3

"""财务三表与日频估值的本地 Parquet 事实表（读写 + 点时查询）。

目录结构（根目录默认 ``data/local_store``）::

    meta/financials/income.parquet
    meta/financials/balance.parquet
    meta/financials/cashflow.parquet
    meta/financials/valuation_daily.parquet
    meta/financials/parts/{statement}/{period}.parquet      # 采集分片
    meta/financials/parts/{statement}_bj/{ts_code}.parquet  # 北交所单票分片
    meta/financials/parts/valuation/{batch}.parquet
    meta/financials/ann_date/{period}.parquet               # 公告日映射分片
    meta/financials/progress.json
    meta/financials/failed.csv

点时口径（硬约束）：
- 可见性一律以 ``ann_date`` 判定，**不允许**用 ``end_date`` 代替；
- ``ann_date`` 为空的历史行视为「不可见」，直到调用方显式放宽；
- 同一报告期存在多条公告（更正/重述）时，取 ``ann_date`` 最新的那一版。

本模块不修改 ``src/data/local_store`` 的任何共享文件，也不写 MySQL。
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STORE_ROOT = PROJECT_ROOT / 'data' / 'local_store'
STATEMENTS = ('income', 'balance', 'cashflow')

_COMMON_SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'end_date': pl.Date,
    'ann_date': pl.Date,
    'ann_date_source': pl.Utf8,
    'source': pl.Utf8,
    'fetched_at': pl.Datetime,
}

SCHEMAS: dict[str, dict[str, Any]] = {
    'income': {
        **_COMMON_SCHEMA,
        'revenue': pl.Float64,
        'revenue_yoy': pl.Float64,
        'operating_cost': pl.Float64,
        'gross_profit': pl.Float64,
        'selling_expense': pl.Float64,
        'admin_expense': pl.Float64,
        'finance_expense': pl.Float64,
        'operate_profit': pl.Float64,
        'total_profit': pl.Float64,
        'net_profit': pl.Float64,
        'net_profit_yoy': pl.Float64,
        'parent_net_profit': pl.Float64,
    },
    'balance': {
        **_COMMON_SCHEMA,
        'monetary_funds': pl.Float64,
        'accounts_receivable': pl.Float64,
        'inventory': pl.Float64,
        'total_assets': pl.Float64,
        'total_assets_yoy': pl.Float64,
        'accounts_payable': pl.Float64,
        'advance_receipts': pl.Float64,
        'total_liab': pl.Float64,
        'total_liab_yoy': pl.Float64,
        'debt_ratio': pl.Float64,
        'total_equity': pl.Float64,
    },
    'cashflow': {
        **_COMMON_SCHEMA,
        'net_cash_flow': pl.Float64,
        'net_cash_flow_yoy': pl.Float64,
        'operating_cf': pl.Float64,
        'investing_cf': pl.Float64,
        'financing_cf': pl.Float64,
    },
    'valuation': {
        'ts_code': pl.Utf8,
        'trade_date': pl.Date,
        'close': pl.Float64,
        'pe': pl.Float64,
        'pe_static': pl.Float64,
        'pb': pl.Float64,
        'ps': pl.Float64,
        'dv_ratio': pl.Float64,
        'total_mv': pl.Float64,
        'circ_mv': pl.Float64,
        'source': pl.Utf8,
        'fetched_at': pl.Datetime,
    },
}

_KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    'income': ('ts_code', 'end_date', 'ann_date'),
    'balance': ('ts_code', 'end_date', 'ann_date'),
    'cashflow': ('ts_code', 'end_date', 'ann_date'),
    'valuation': ('ts_code', 'trade_date'),
}


def financials_dir(store_root: Path | str | None = None) -> Path:
    """返回财务事实表目录（``<store_root>/meta/financials``）。"""
    root = Path(store_root) if store_root is not None else DEFAULT_STORE_ROOT
    return root / 'meta' / 'financials'


def table_path(table: str, store_root: Path | str | None = None) -> Path:
    """返回某张事实表的最终文件路径。"""
    if table not in SCHEMAS:
        raise ValueError(f'未知财务表: {table}')
    filename = 'valuation_daily' if table == 'valuation' else table
    return financials_dir(store_root) / f'{filename}.parquet'


def parts_dir(table: str, store_root: Path | str | None = None) -> Path:
    """返回某张事实表的分片目录（采集器使用，允许 ``balance_bj`` 等变体）。"""
    if not table:
        raise ValueError('table 不能为空')
    return financials_dir(store_root) / 'parts' / table


def schema_of(table: str) -> dict[str, Any]:
    """返回表的列与类型定义。"""
    return dict(SCHEMAS[table])


def as_date(value: Any) -> date | None:
    """把 str/date/datetime 统一成 ``date``（无法解析返回 None）。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()[:10]
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text if '-' in text else text[:8], fmt).date()
        except ValueError:
            continue
    return None


def coerce_schema(frame: pl.DataFrame, table: str) -> pl.DataFrame:
    """把任意来源的帧对齐到目标 schema（缺列补 null，列序固定）。"""
    schema = SCHEMAS[table]
    exprs: list[pl.Expr] = []
    for name, dtype in schema.items():
        if name in frame.columns:
            exprs.append(pl.col(name).cast(dtype, strict=False).alias(name))
        else:
            exprs.append(pl.lit(None, dtype=dtype).alias(name))
    return frame.with_columns(exprs).select(list(schema))


def _atomic_write(frame: pl.DataFrame, path: Path) -> None:
    """原子写：先写 ``*.tmp`` 再替换，避免中断留下半个文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    frame.write_parquet(tmp, compression='zstd', compression_level=3, statistics=True)
    os.replace(tmp, path)


def write_table(frame: pl.DataFrame, table: str,
                store_root: Path | str | None = None) -> int:
    """写最终事实表（覆盖，原子替换），返回行数。"""
    if frame.is_empty():
        logger.warning(f'[financials] {table} 写入空帧，跳过')
        return 0
    aligned = coerce_schema(frame, table)
    key = list(_KEY_COLUMNS[table])
    aligned = aligned.unique(subset=key, keep='last').sort(key)
    _atomic_write(aligned, table_path(table, store_root))
    logger.info(f'[financials] {table} 写入 {aligned.height:,} 行 → '
                f'{table_path(table, store_root)}')
    return aligned.height


def read_table(table: str, store_root: Path | str | None = None, *,
               ts_code: str | None = None, codes: Iterable[str] | None = None,
               start: Any = None, end: Any = None,
               date_column: str | None = None) -> pl.DataFrame:
    """读取事实表（可选按代码与日期区间过滤）。

    ``start/end`` 作用于 ``date_column``（默认：三表用 ``end_date``，
    估值用 ``trade_date``）；只做切片，不做可见性判断。
    """
    path = table_path(table, store_root)
    if not path.exists():
        return pl.DataFrame(schema=SCHEMAS[table])
    col = date_column or ('trade_date' if table == 'valuation' else 'end_date')
    lazy = pl.scan_parquet(str(path))
    if ts_code is not None:
        lazy = lazy.filter(pl.col('ts_code') == ts_code)
    if codes is not None:
        code_list = list(codes)
        lazy = lazy.filter(pl.col('ts_code').is_in(code_list))
    start_dt = as_date(start)
    end_dt = as_date(end)
    if start_dt is not None:
        lazy = lazy.filter(pl.col(col) >= start_dt)
    if end_dt is not None:
        lazy = lazy.filter(pl.col(col) <= end_dt)
    return lazy.collect()


def latest_announced_as_of(ts_code: str, as_of: Any, statement: str = 'income',
                           store_root: Path | str | None = None) -> dict[str, Any] | None:
    """返回「截至 ``as_of`` 已公告」的最新一期报表。

    - 只认 ``ann_date``：``ann_date <= as_of`` 才可见；
    - ``ann_date`` 为空的行永远不可见（不能让 ``end_date`` 蒙混过关）；
    - 同日多期公告时按 ``end_date`` 取更新的一期，同报告期多条公告取最新公告。
    """
    if statement not in STATEMENTS:
        raise ValueError(f'statement 必须是 {STATEMENTS} 之一，收到 {statement!r}')
    as_of_dt = as_date(as_of)
    if as_of_dt is None:
        raise ValueError(f'as_of 无法解析为日期: {as_of!r}')
    frame = read_table(statement, store_root, ts_code=ts_code)
    if frame.is_empty():
        return None
    visible = frame.filter(
        pl.col('ann_date').is_not_null() & (pl.col('ann_date') <= as_of_dt))
    if visible.is_empty():
        return None
    row = visible.sort(['ann_date', 'end_date'], descending=True).head(1)
    return row.to_dicts()[0]


def latest_announced_panel(codes: Iterable[str], as_of: Any, statement: str = 'income',
                           store_root: Path | str | None = None) -> pl.DataFrame:
    """横截面：每个代码返回截至 ``as_of`` 已公告的最新一期报表。"""
    if statement not in STATEMENTS:
        raise ValueError(f'statement 必须是 {STATEMENTS} 之一，收到 {statement!r}')
    as_of_dt = as_date(as_of)
    if as_of_dt is None:
        raise ValueError(f'as_of 无法解析为日期: {as_of!r}')
    frame = read_table(statement, store_root, codes=list(codes))
    if frame.is_empty():
        return pl.DataFrame(schema=SCHEMAS[statement])
    visible = frame.filter(
        pl.col('ann_date').is_not_null() & (pl.col('ann_date') <= as_of_dt))
    if visible.is_empty():
        return pl.DataFrame(schema=SCHEMAS[statement])
    visible = visible.sort(['ts_code', 'ann_date', 'end_date'])
    return visible.group_by('ts_code', maintain_order=True).agg(pl.all().last())


def latest_valuation(as_of: Any = None, codes: Iterable[str] | None = None,
                     store_root: Path | str | None = None) -> pl.DataFrame:
    """返回每个代码截至 ``as_of``（含）的最后一个交易日估值行。"""
    frame = read_table('valuation', store_root, codes=codes)
    if frame.is_empty():
        return frame
    as_of_dt = as_date(as_of)
    if as_of_dt is not None:
        frame = frame.filter(pl.col('trade_date') <= as_of_dt)
    if frame.is_empty():
        return frame
    frame = frame.sort(['ts_code', 'trade_date'])
    return frame.group_by('ts_code', maintain_order=True).agg(pl.all().last())


def merge_parts(table: str, store_root: Path | str | None = None,
                extra_dirs: Iterable[Path | str] = ()) -> int:
    """合并分片为最终事实表（懒扫描 + sink，避免整表进内存）。

    同一主键保留最后写入的分片（分片按报告期/批次顺序传入，
    因此后采的更正公告会覆盖先采的旧版本）。
    """
    search_dirs = [parts_dir(table, store_root)]
    search_dirs.extend(Path(p) for p in extra_dirs)
    files = sorted(
        str(p) for d in search_dirs if Path(d).exists() for p in Path(d).glob('*.parquet'))
    existing = table_path(table, store_root)
    if existing.exists():
        # 已有最终表作为最早输入；分片按顺序覆盖同主键旧版本
        files.insert(0, str(existing))
    if not files:
        logger.warning(f'[financials] {table} 没有可合并的分片')
        return 0
    frames = [pl.scan_parquet(f) for f in files]
    combined = pl.concat(frames, how='diagonal_relaxed')
    names = combined.collect_schema().names()
    exprs: list[pl.Expr] = [
        pl.col(name).cast(dtype, strict=False).alias(name)
        if name in names else pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in SCHEMAS[table].items()
    ]
    key = list(_KEY_COLUMNS[table])
    plan = combined.with_columns(exprs).select(list(SCHEMAS[table])).unique(
        subset=key, keep='last').sort(key)
    dest = table_path(table, store_root)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + '.tmp')
    try:
        plan.sink_parquet(tmp, compression='zstd', statistics=True)
        os.replace(tmp, dest)
        rows = pl.scan_parquet(str(dest)).select(pl.len()).collect().item()
    # 兜底理由：流式引擎不支持 unique/sort 时退回内存合并（表本身可控）
    except (pl.exceptions.PolarsError, NotImplementedError):
        merged = plan.collect()
        _atomic_write(merged, dest)
        rows = merged.height
    logger.info(f'[financials] {table} 合并 {len(files)} 个分片 → {rows:,} 行')
    return rows


def coverage(table: str, store_root: Path | str | None = None) -> dict[str, Any]:
    """汇总事实表覆盖度：行数、票数、时间跨度、公告日缺失量。"""
    path = table_path(table, store_root)
    if not path.exists():
        return {'table': table, 'exists': False}
    frame = pl.read_parquet(path)
    if frame.is_empty():
        return {'table': table, 'exists': True, 'rows': 0}
    result: dict[str, Any] = {
        'table': table,
        'exists': True,
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'columns': frame.columns,
    }
    if 'ann_date' in frame.columns:
        result['ann_date_null'] = int(frame['ann_date'].is_null().sum())
        result['start'] = str(frame['end_date'].min())
        result['end'] = str(frame['end_date'].max())
        if 'ann_date_source' in frame.columns:
            result['ann_date_source'] = dict(
                frame.group_by('ann_date_source').len().iter_rows())
    else:
        result['start'] = str(frame['trade_date'].min())
        result['end'] = str(frame['trade_date'].max())
    return result
