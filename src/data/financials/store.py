#!/usr/bin/env python3

"""财务三表与日频估值的本地 Parquet 事实表（读写 + 点时查询）。

目录结构（根目录默认 ``data/local_store``）::

    meta/financials/income.parquet
    meta/financials/balance.parquet
    meta/financials/cashflow.parquet
    meta/financials/valuation_daily.parquet
    meta/financials/ann_date/{period}.parquet               # 公告日映射分片

采集分片与断点**不在** ``data/`` 之下（DG-5）：落在仓库根
``_staging/financials_parts_<YYYYMMDD>/``，见 :func:`parts_root`::

    _staging/financials_parts_<YYYYMMDD>/{income,balance,cashflow}/{period}.parquet
    _staging/financials_parts_<YYYYMMDD>/{income,balance,cashflow}_bj/{ts_code}.parquet
    _staging/financials_parts_<YYYYMMDD>/valuation/{batch}.parquet
    _staging/financials_parts_<YYYYMMDD>/progress.json
    _staging/financials_parts_<YYYYMMDD>/failed.csv

点时口径（硬约束）：
- 可见性一律以 ``ann_date`` 判定，**不允许**用 ``end_date`` 代替；
- ``ann_date`` 为空的历史行视为「不可见」，直到调用方显式放宽；
- 同一报告期存在多条公告（更正/重述）时，取 ``ann_date`` 最新的那一版。

本模块不修改 ``src/data/local_store`` 的任何共享文件，也不写 MySQL。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic
from src.data.staging import STAGING_ROOT, staging_dir

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STORE_ROOT = PROJECT_ROOT / 'data' / 'local_store'
PARTS_TASK = 'financials_parts'
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


def parts_root(store_root: Path | str | None = None) -> Path:
    """采集任务目录（DG-5）：仓库根 ``_staging/financials_parts_<YYYYMMDD>/``。

    分片与断点（``progress.json`` / ``failed.csv``）都落在该目录，**不再写进**
    ``data/local_store/meta/financials/``：正式层只保留合并后的四张事实表。

    跨日续跑：已存在任一 ``financials_parts_*`` 时复用最新一个，避免第二天启动
    时看不到昨天的分片而重复采集；要开新任务须先把旧目录移走。
    自定义 ``store_root``（测试）时落在该根下的 ``_staging``，不写进真实仓库。
    """
    root = DEFAULT_STORE_ROOT if store_root is None else Path(store_root)
    base = STAGING_ROOT if root.resolve() == DEFAULT_STORE_ROOT.resolve() else root / '_staging'
    existing = sorted(p for p in base.glob(f'{PARTS_TASK}_*') if p.is_dir())
    if existing:
        return existing[-1]
    if base == STAGING_ROOT:
        return staging_dir(PARTS_TASK, with_date=True)
    return base / f'{PARTS_TASK}_{date.today():%Y%m%d}'


def parts_dir(table: str, store_root: Path | str | None = None) -> Path:
    """返回某张事实表的分片目录（采集器使用，允许 ``balance_bj`` 等变体）。"""
    if not table:
        raise ValueError('table 不能为空')
    return parts_root(store_root) / table


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
    """原子写：临时文件 → fsync → 替换，避免中断留下半个文件。"""
    write_parquet_atomic(
        frame, path, compression='zstd', compression_level=3, statistics=True)


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
    """返回每个代码截至 ``as_of``（含）的最后一个交易日估值行。

    实现为「两列键 argmax + semi join」：只对 ``(ts_code, trade_date)`` 两组键
    求每个代码的最大交易日，再半连接回原表取整行。原实现先把整张宽表
    ``collect()`` 进内存（1,800 万行 × 12 列）再排序分组，独立进程实测
    0.71 s / 峰值增量 6.5–6.8 GB；本实现 0.18 s / 2.3 GB（3.9× 快、2.9× 省内存），
    且逐值等价。内存仍有 2.3 GB 是因为返回整行必须解码全部 12 列，
    semi join 只省掉 sort/group_by 的副本。

    行数不会放大：``(ts_code, trade_date)`` 是表主键，由 ``merge_parts`` 的
    ``unique(subset=key, keep='last')`` 保证唯一；``unique`` 兜底防止外部
    手工写入重复行时半连接放大。
    """
    path = table_path('valuation', store_root)
    if not path.exists():
        return pl.DataFrame(schema=SCHEMAS['valuation'])
    lazy = pl.scan_parquet(str(path))
    if codes is not None:
        lazy = lazy.filter(pl.col('ts_code').is_in(list(codes)))
    as_of_dt = as_date(as_of)
    if as_of_dt is not None:
        lazy = lazy.filter(pl.col('trade_date') <= as_of_dt)
    keys = lazy.group_by('ts_code').agg(
        pl.col('trade_date').max().alias('trade_date'))
    result = lazy.join(keys, on=['ts_code', 'trade_date'], how='semi').collect()
    if result.is_empty():
        return result
    # 排序只为与旧实现的输出顺序（ts_code 升序）保持一致
    return result.unique(subset=['ts_code', 'trade_date'], keep='last').sort('ts_code')


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
    try:
        write_parquet_atomic(plan, dest, compression='zstd', statistics=True)
        rows = pl.scan_parquet(str(dest)).select(pl.len()).collect().item()
    # 兜底理由：流式引擎不支持 unique/sort 时退回内存合并（表本身可控）
    except (pl.exceptions.PolarsError, NotImplementedError):
        merged = plan.collect()
        _atomic_write(merged, dest)
        rows = merged.height
    logger.info(f'[financials] {table} 合并 {len(files)} 个分片 → {rows:,} 行')
    return rows


def coverage(table: str, store_root: Path | str | None = None) -> dict[str, Any]:
    """汇总事实表覆盖度：行数、票数、时间跨度、公告日缺失量。

    全部走惰性聚合（行数 / 去重票数 / 空值数 / 时间跨度一次扫描出结果），
    不再 ``pl.read_parquet`` 整表进内存。估值表 1,800 万行实测内存增量
    2.2 GB → 数十 MB 级，输出与原实现逐值等价。
    """
    path = table_path(table, store_root)
    if not path.exists():
        return {'table': table, 'exists': False}
    lazy = pl.scan_parquet(str(path))
    names = lazy.collect_schema().names()
    has_ann = 'ann_date' in names
    date_col = 'end_date' if has_ann else 'trade_date'
    aggs: list[pl.Expr] = [
        pl.len().alias('rows'),
        pl.col('ts_code').n_unique().alias('codes'),
        pl.col(date_col).min().alias('start'),
        pl.col(date_col).max().alias('end'),
    ]
    if has_ann:
        aggs.append(pl.col('ann_date').is_null().sum().alias('ann_null'))
    stats = lazy.select(aggs).collect().row(0, named=True)
    rows = int(stats['rows'])
    if rows == 0:
        return {'table': table, 'exists': True, 'rows': 0}
    result: dict[str, Any] = {
        'table': table,
        'exists': True,
        'rows': rows,
        'codes': int(stats['codes']),
        'columns': names,
    }
    if has_ann:
        result['ann_date_null'] = int(stats['ann_null'])
        result['start'] = str(stats['start'])
        result['end'] = str(stats['end'])
        if 'ann_date_source' in names:
            src = lazy.group_by('ann_date_source').len().collect()
            result['ann_date_source'] = dict(
                zip(src['ann_date_source'].to_list(), src['len'].to_list(), strict=False))
    else:
        result['start'] = str(stats['start'])
        result['end'] = str(stats['end'])
    return result
