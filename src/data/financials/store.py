#!/usr/bin/env python3

"""财务三表与日频估值的本地 Parquet 事实表（读写 + 点时查询）。

目录结构（根目录默认 ``data/local_store``）::

    meta/financials/income.parquet
    meta/financials/balance.parquet
    meta/financials/cashflow.parquet
    meta/financials/valuation_daily.parquet
    meta/financials/ann_date_map.parquet                    # 公告日映射（键 period, symbol）

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

``ann_date`` 分段来源（已知缺陷 G-090，只读禁令）：
- **1990–2009 段的公告日只存在于三表自身的 ``ann_date`` 列**（值）+ ``ann_date_source``
  列（来源标记，全部为 ``sina_publish``），没有独立文件；
  ``meta/financials/ann_date_map.parquet`` 的 ``period`` 从 ``2010-03-31`` 起，
  **对 pre-2010 零覆盖**，不是三表 ``ann_date`` 的唯一来源。
- 两段不同源：pre-2010 为新浪 ``publish_date`` 直写，2010+ 为 EM
  ``RPT_LICO_FN_CPD`` 的 ``NOTICE_DATE`` 经公告日映射表落盘；pre-2010 还存在
  分表分年的系统性日期错位（三表同 ``(symbol, end_date)`` 的 ``ann_date`` 全等率
  仅 28.39%，post-2010 为 100%），成因未取证。
- **硬约束：任何研究/回测窗口早于 2010-01，必须先重取源端并统一为同一来源，
  不得直接使用现有 pre-2010 ``ann_date``**；详情见
  ``docs/数据缺口登记_正式表_2026-11-12.md`` 的 G-090。

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

#: 公告日映射合并表（``meta/financials/ann_date_map.parquet``）的 schema。
#: 键 ``(period, symbol)``；``period`` 即原分片文件名解析出的报告期末。
#: 该表只是 2010+ 的公告日采集缓存，不是三表 ``ann_date`` 的唯一来源。
ANN_DATE_MAP_SCHEMA: dict[str, Any] = {
    'period': pl.Date,
    'symbol': pl.Utf8,
    'ann_date': pl.Date,
    'update_date': pl.Date,
    'parent_net_profit': pl.Float64,
}

ANN_DATE_MAP_KEY: tuple[str, ...] = ('period', 'symbol')


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


def ann_date_map_path(store_root: Path | str | None = None) -> Path:
    """公告日映射合并表的唯一正式路径。"""
    return financials_dir(store_root) / 'ann_date_map.parquet'


def read_ann_date_map(period_end: Any,
                      store_root: Path | str | None = None) -> pl.DataFrame:
    """读取合并表中某一报告期的公告日映射，列与旧分片一致（不含 ``period``）。

    表不存在返回空帧（schema 同上），由调用方决定是否联网重采。
    """
    schema = {name: dtype for name, dtype in ANN_DATE_MAP_SCHEMA.items()
              if name != 'period'}
    empty = pl.DataFrame(schema=schema)
    path = ann_date_map_path(store_root)
    if not path.exists():
        return empty
    period = as_date(period_end)
    if period is None:
        raise ValueError(f'period 无法解析为日期: {period_end!r}')
    frame = pl.read_parquet(path).filter(pl.col('period') == period)
    return frame.select(list(schema)).sort('symbol')


def upsert_ann_date_map(frame: pl.DataFrame,
                        store_root: Path | str | None = None) -> int:
    """把若干报告期的映射并入合并表，按 ``(period, symbol)`` 覆盖，返回总行数。

    单文件原子替换（``write_parquet_atomic``）；未出现在 ``frame`` 里的报告期
    原样保留，因此可逐期增量补齐。
    """
    aligned = _align(frame, ANN_DATE_MAP_SCHEMA)
    if aligned.is_empty():
        path = ann_date_map_path(store_root)
        return (pl.scan_parquet(str(path)).select(pl.len()).collect().item()
                if path.exists() else 0)
    periods = aligned['period'].unique().to_list()
    path = ann_date_map_path(store_root)
    if path.exists():
        existing = pl.read_parquet(path)
        existing = existing.filter(~pl.col('period').is_in(periods))
        aligned = pl.concat([_align(existing, ANN_DATE_MAP_SCHEMA), aligned],
                            how='vertical_relaxed')
    aligned = aligned.unique(subset=list(ANN_DATE_MAP_KEY), keep='last').sort(
        list(ANN_DATE_MAP_KEY))
    _atomic_write(aligned, path)
    return aligned.height


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


def _align(frame: pl.DataFrame, schema: dict[str, Any]) -> pl.DataFrame:
    """把帧对齐到给定 schema（缺列补 null，列序固定）。"""
    exprs: list[pl.Expr] = []
    for name, dtype in schema.items():
        if name in frame.columns:
            exprs.append(pl.col(name).cast(dtype, strict=False).alias(name))
        else:
            exprs.append(pl.lit(None, dtype=dtype).alias(name))
    return frame.with_columns(exprs).select(list(schema))


def coerce_schema(frame: pl.DataFrame, table: str) -> pl.DataFrame:
    """把任意来源的帧对齐到目标 schema（缺列补 null，列序固定）。"""
    return _align(frame, SCHEMAS[table])


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


#: 合并顺序辅助列：分片下标，只在 ``merge_parts`` 内部存在，不落盘
_ORDER_COLUMN = '__merge_ord'

#: 估值 ``source`` 的字段级语法（DG-19）：(列名, 串里使用的段名) 及规范顺序。
#: 注意段名与列名不总是一致：``dv_ratio`` 的段名是 ``dv``、``total_mv`` 是 ``mv``。
_SOURCE_SEGMENT_FIELDS: tuple[tuple[str, str], ...] = (
    ('close', 'close'), ('pe', 'pe'), ('pe_static', 'pe_static'), ('pb', 'pb'),
    ('ps', 'ps'), ('dv_ratio', 'dv'), ('total_mv', 'mv'), ('circ_mv', 'circ_mv'))


def _merge_agg_exprs(table: str) -> list[pl.Expr]:
    """``merge_parts`` 的分组聚合式：逐字段「最后非空」+ ``source`` 按字段跟随。

    修复 G-080：旧实现用 ``unique(keep='last')`` 整行取最后一片，只要最后一片
    该列为空，历史非空值就被降级为 null，``source`` 串也随整行被替换。这里改为
    逐列取「最后一个非空值」，并让字段级 ``source`` 段跟着该字段的来源行走：
    新值为空时字段与它的 ``source`` 段都保持原值。

    ``source`` 三种形态的处置：
    - 能解析出 ``field=来源`` 段（估值）→ 按 :data:`_SOURCE_SEGMENT_FIELDS`
      顺序重组成 ``close=…|pe=…|pb=…``，只保留确有来源的字段段；
    - 整串无 ``=`` 段（三表的 ``local:…`` 这类整体来源）→ 退化为「最后非空整串」；
    - 字段有值但该行走的 ``source`` 缺这个段 → 只保留值、不造来源段（不猜测）。

    字段值为空时（含 ``none`` 段）：沿用组内「最后一条声明了该字段段」的那条
    ``source`` 里**该字段自己的段**（G-080 第二段实测：不沿用会让 2,282,126 行的
    ``dv=none`` 等显式「无来源」标记、287,879 行空值字段的来源声明丢失）。
    只沿用已存在的段，不新造、不跨字段挪用；段顺序仍按规范序；
    组内没有任何段的整串形态照旧原样保留，不会被拼出段。
    """
    key = set(_KEY_COLUMNS[table])
    schema = SCHEMAS[table]
    aggs: list[pl.Expr] = [
        pl.col(name).drop_nulls().last().alias(name)
        for name in schema if name not in key and name != 'source'
    ]
    if 'source' not in schema:
        return aggs
    fields = [(col, seg_key) for col, seg_key in _SOURCE_SEGMENT_FIELDS
              if col in schema and col not in key]
    fallback = pl.col('source').drop_nulls().last()
    if not fields:
        aggs.append(fallback.alias('source'))
        return aggs
    segments: list[pl.Expr] = []
    for column, seg_key in fields:
        pattern = rf'(?:^|\|){seg_key}=([^|]*)'
        from_value_row = (pl.when(pl.col(column).is_not_null())
                          .then(pl.col('source').str.extract(pattern, 1))
                          .drop_nulls().last())
        # 字段值为空：沿用组内「最后一条声明了该字段段」的声明（含 ``none``），
        # 只取本字段自己的段，不新造、不跨字段挪用
        from_last_declaration = pl.col('source').str.extract(pattern, 1).drop_nulls().last()
        seg = (pl.when(pl.col(column).is_not_null().any())
               .then(from_value_row)
               .otherwise(from_last_declaration))
        segments.append(
            pl.when(seg.is_not_null())
            .then(pl.concat_str([pl.lit(f'{seg_key}='), seg]))
            .alias(f'__seg_{column}'))
    merged = pl.concat_str(segments, separator='|', ignore_nulls=True)
    aggs.append(pl.when(merged.is_null() | (merged == '')).then(fallback)
                .otherwise(merged).alias('source'))
    return aggs


def merge_parts(table: str, store_root: Path | str | None = None,
                extra_dirs: Iterable[Path | str] = ()) -> int:
    """合并分片为最终事实表（懒扫描 + sink，避免整表进内存）。

    同主键按分片顺序合并，但不是整行 last-wins：每个字段取「最后一个非空值」，
    字段级 ``source`` 段随该字段的来源行走（见 :func:`_merge_agg_exprs`）。
    这样后来采到的「只有部分列为空」的分片不会把历史非空值抹掉（G-080）。
    行数、键集合与 ``sort(key)`` 排序语义与旧实现一致。
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
    frames = [
        pl.scan_parquet(f).with_columns(
            pl.lit(i, dtype=pl.UInt32).alias(_ORDER_COLUMN))
        for i, f in enumerate(files)
    ]
    combined = pl.concat(frames, how='diagonal_relaxed')
    names = combined.collect_schema().names()
    exprs: list[pl.Expr] = [
        pl.col(name).cast(dtype, strict=False).alias(name)
        if name in names else pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in SCHEMAS[table].items()
    ]
    key = list(_KEY_COLUMNS[table])
    plan = (combined.with_columns(exprs)
            .select([*SCHEMAS[table], _ORDER_COLUMN])
            .sort([*key, _ORDER_COLUMN])
            .group_by(key, maintain_order=True)
            .agg(_merge_agg_exprs(table))
            .select(list(SCHEMAS[table]))
            .sort(key))
    dest = table_path(table, store_root)
    try:
        write_parquet_atomic(plan, dest, compression='zstd', statistics=True)
        rows = pl.scan_parquet(str(dest)).select(pl.len()).collect().item()
    # 兜底理由：流式引擎不支持 group_by 聚合/sort 时退回内存合并（表本身可控）
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
