#!/usr/bin/env python3

r"""构建股票池分层矩阵 L0–L5（``(date, ts_code, in_layer)`` 布尔长表）。

层定义
======
- L0 全历史域：``list_date <= t`` 且（``delist_date > t`` 或为空）；保留退市股；
- L1 剔除次新：上市满 ``MIN_LISTED_TRADING_DAYS``（默认 120）个交易日，按交易日历计；
- L2 可交易：t 日有成交（bar 存在且 ``volume > 0``、``amount > 0``）且不在停牌并集内；
- L3 风险剔除：不在 ST 区间（``meta/st/st_intervals.parquet`` 区间版）内，且不在退市整理期；
- L4 流动性：前 ``ADV_WINDOW``（默认 20）个交易日成交额均值 >= ``MIN_AMOUNT``（默认 2,000 万）；
- L5 池化：按现有准入规则（板块 / 旧码）组合，规则常数与 ``src/data/universe_filter.py``
  的 ``UniverseFilterConfig`` 默认值一致。

``meta/pools/`` 目前只有 ETF 权益池（``etf_equity_pool_<日期>.csv``），没有股票池文件，
因此 L5 的股票准入规则取 ``UniverseFilterConfig`` 默认值，并逐条写进 manifest。

输出
====
候选先写 ``<仓库根>/_staging/universe_layers_20261111/``，``--apply`` 才用原子写
提升到 ``<store>/meta/universe/layers/``；同目录写 ``manifest.json``、
``counts_by_year.csv`` 与 ``st_snapshot_vs_interval.json``。路径不带版本号。

复现::

    .venv\\Scripts\\python.exe tools\\build_universe_layers.py            # 只建候选
    .venv\\Scripts\\python.exe tools\\build_universe_layers.py --apply    # 校验并提升
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data.atomic_write import write_bytes_atomic, write_parquet_atomic  # noqa: E402
from src.data.quality.code_migration import load_entity_index  # noqa: E402
from src.data.quality.suspension_union import load_suspension_union  # noqa: E402
from src.data.staging import PROJECT_ROOT, staging_dir  # noqa: E402
from src.data.trading_calendar import get_calendar  # noqa: E402
from src.data.universe_filter import board_of  # noqa: E402

TASK_NAME = 'universe_layers_20261111'
FORMAL_REL = Path('meta') / 'universe' / 'layers'

#: 层文件名（不带版本号）
LAYER_FILES: dict[str, str] = {
    'L0': 'l0_all_history.parquet',
    'L1': 'l1_listed_120d.parquet',
    'L2': 'l2_tradable.parquet',
    'L3': 'l3_risk_excluded.parquet',
    'L4': 'l4_liquid.parquet',
    'L5': 'l5_pool.parquet',
}
LAYER_ORDER = tuple(LAYER_FILES)

#: 阈值参数（全部写进 manifest）
MIN_LISTED_TRADING_DAYS = 120
ADV_WINDOW = 20
MIN_AMOUNT = 2.0e7
DELISTING_WINDOW_TRADING_DAYS = 30
POOL_RULES: dict[str, Any] = {
    'include_bse': False,
    'include_star': True,
    'include_chinext': True,
    'exclude_b_share': True,
    'exclude_old_three_board': True,
    'exclude_superseded': True,
}

UPSTREAM_FILES = (
    'meta/stock_basic.parquet',
    'meta/universe/listing_intervals.parquet',
    'meta/suspension/suspension_daily.parquet',
    'meta/suspension/implied_suspension.parquet',
    'meta/st/st_intervals.parquet',
    'meta/st/st_coverage.parquet',
    'meta/name_history.parquet',
    'meta/code_migration.parquet',
    'meta/pools/etf_equity_pool.csv',
    'quotes/index/sh/000001.SH.parquet',
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _mtime_iso(path: Path) -> str | None:
    if not path.is_file():
        return None
    stamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return stamp.isoformat()


def _read_stock_basic(root: Path) -> pl.DataFrame:
    frame = pl.read_parquet(
        root / 'meta' / 'stock_basic.parquet',
        columns=['ts_code', 'name', 'list_date', 'delist_date', 'status'],
    )
    return frame.filter(pl.col('list_date').is_not_null()) \
        .unique(subset=['ts_code'], keep='first').sort('ts_code')


def _calendar_dates(root: Path, as_of: date | None) -> tuple[list[date], date]:
    calendar = get_calendar(root)
    last = calendar.authoritative_end or calendar.dates[-1]
    end = min(last, as_of) if as_of else last
    return [d for d in calendar.dates if d <= end], end


def _build_grid(
    basic: pl.DataFrame, dates: list[date], limit_codes: int | None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """按 list_date/delist_date 展开 (cal_idx, ts_code) 全历史域。"""
    n = len(dates)
    codes: list[str] = []
    los: list[int] = []
    his: list[int] = []
    skipped: list[str] = []
    for row in basic.iter_rows(named=True):
        if limit_codes is not None and len(codes) >= limit_codes:
            break
        code = str(row['ts_code'])
        lo = bisect.bisect_left(dates, row['list_date'])
        if lo >= n:
            skipped.append(code)
            continue
        hi = bisect.bisect_left(dates, row['delist_date']) if row['delist_date'] else n
        if hi <= lo:
            skipped.append(code)
            continue
        codes.append(code)
        los.append(lo)
        his.append(hi)

    spans = pl.DataFrame({
        'ts_code': pl.Series(codes, dtype=pl.String),
        'lo': pl.Series(los, dtype=pl.Int32),
        'hi': pl.Series(his, dtype=pl.Int32),
    })
    grid = spans.with_columns(pl.int_ranges('lo', 'hi').alias('cal_idx')).explode('cal_idx')
    grid = grid.with_columns(pl.col('cal_idx').cast(pl.Int32))
    grid = grid.with_columns(
        (pl.col('cal_idx') >= pl.col('lo') + (MIN_LISTED_TRADING_DAYS - 1)).alias('l1')
    ).drop(['lo', 'hi'])
    stats = {
        'codes': len(codes),
        'rows': grid.height,
        'skipped_codes': skipped,
        'spans': {c: (lo, hi) for c, lo, hi in zip(codes, los, his)},
    }
    return grid, stats


def _quote_panel(
    root: Path, dates: list[date], universe: set[str], as_of: date,
) -> pl.DataFrame:
    """读取本地个股日线，返回 (cal_idx, ts_code, amount, traded) 面板。"""
    cal_array = np.array(dates, dtype='datetime64[D]')
    files = sorted((root / 'quotes' / 'stock').glob('*/*.parquet'))
    parts: list[pl.DataFrame] = []
    for number, path in enumerate(files, 1):
        code = path.stem.upper()
        if code not in universe:
            continue
        try:
            frame = pl.read_parquet(path, columns=['date', 'volume', 'amount'])
        except Exception as exc:  # noqa: BLE001 - 单文件坏不阻断整批
            logger.warning(f'[layers] 行情读取失败 {path}: {exc}')
            continue
        if frame.is_empty():
            continue
        frame = frame.filter(pl.col('date') <= as_of)
        if frame.is_empty():
            continue
        day_values = frame['date'].to_numpy().astype('datetime64[D]')
        cal_idx = np.searchsorted(cal_array, day_values)
        frame = frame.with_columns(
            pl.Series('cal_idx', cal_idx, dtype=pl.Int32),
            pl.lit(code).alias('ts_code'),
            (pl.col('volume').fill_null(0) > 0)
            .and_(pl.col('amount').fill_null(0) > 0).alias('traded'),
        ).select(['cal_idx', 'ts_code', 'amount', 'traded'])
        parts.append(frame)
        if number % 1000 == 0:
            logger.info(f'[layers] 行情读取 {number}/{len(files)}')
    if not parts:
        return pl.DataFrame(schema={
            'cal_idx': pl.Int32, 'ts_code': pl.String,
            'amount': pl.Float64, 'traded': pl.Boolean,
        })
    panel = pl.concat(parts, how='vertical_relaxed')
    # 同一 (交易日, 代码) 只保留一行，避免下游 left join 放大网格
    return panel.unique(subset=['cal_idx', 'ts_code'], keep='first', maintain_order=True)


def _expand_intervals(
    path: Path, dates: list[date], end: date, columns: list[str],
) -> pl.DataFrame:
    frame = pl.read_parquet(path, columns=columns)
    return _expand_intervals_frame(frame, dates, end)


def _expand_intervals_frame(frame: pl.DataFrame, dates: list[date],
                            end: date) -> pl.DataFrame:
    rows_idx: list[np.ndarray] = []
    rows_code: list[np.ndarray] = []
    n = len(dates)
    for row in frame.iter_rows(named=True):
        start = row['start_date']
        stop = row.get('end_date') or end
        lo = bisect.bisect_left(dates, start)
        hi = bisect.bisect_right(dates, min(stop, end))
        if hi <= lo:
            continue
        rows_idx.append(np.arange(lo, min(hi, n), dtype=np.int32))
        rows_code.append(np.full(len(rows_idx[-1]), str(row['ts_code']), dtype=object))
    if not rows_idx:
        return pl.DataFrame(schema={'cal_idx': pl.Int32, 'ts_code': pl.String})
    return pl.DataFrame({
        'cal_idx': pl.Series(np.concatenate(rows_idx), dtype=pl.Int32),
        'ts_code': pl.Series(np.concatenate(rows_code).tolist(), dtype=pl.String),
    }).unique()


def _delisting_keys(basic: pl.DataFrame, dates: list[date]) -> pl.DataFrame:
    n = len(dates)
    rows_idx: list[np.ndarray] = []
    rows_code: list[np.ndarray] = []
    for row in basic.iter_rows(named=True):
        delist_day = row['delist_date']
        if delist_day is None:
            continue
        delist_at = bisect.bisect_left(dates, delist_day)
        if delist_at >= n:
            continue
        lo = max(0, delist_at - DELISTING_WINDOW_TRADING_DAYS)
        if lo >= delist_at:
            continue
        rows_idx.append(np.arange(lo, delist_at, dtype=np.int32))
        rows_code.append(np.full(delist_at - lo, str(row['ts_code']), dtype=object))
    if not rows_idx:
        return pl.DataFrame(schema={'cal_idx': pl.Int32, 'ts_code': pl.String})
    return pl.DataFrame({
        'cal_idx': pl.Series(np.concatenate(rows_idx), dtype=pl.Int32),
        'ts_code': pl.Series(np.concatenate(rows_code).tolist(), dtype=pl.String),
    }).unique()


def _delisting_name_keys(root: Path, dates: list[date], end: date) -> pl.DataFrame:
    path = root / 'meta' / 'name_history.parquet'
    if not path.is_file():
        return pl.DataFrame(schema={'cal_idx': pl.Int32, 'ts_code': pl.String})
    frame = pl.read_parquet(
        path, columns=['ts_code', 'name', 'start_date', 'end_date']
    ).filter(pl.col('name').str.contains('退'))
    if frame.is_empty():
        return pl.DataFrame(schema={'cal_idx': pl.Int32, 'ts_code': pl.String})
    return _expand_intervals_frame(frame, dates, end)


def _counts(grid: pl.DataFrame, dates: list[date]) -> tuple[dict, pl.DataFrame, dict]:
    n_days = len(dates)
    year_map = {i: d.year for i, d in enumerate(dates)}
    grid = grid.with_columns(
        pl.col('cal_idx').replace_strict(year_map, return_dtype=pl.Int32).alias('year')
    )
    summary: dict[str, Any] = {'grid_rows': grid.height, 'calendar_days': n_days,
                               'nested': True}
    for layer in LAYER_ORDER:
        sub = grid.filter(pl.col(layer))
        days = sub['cal_idx'].n_unique()
        summary[layer] = {
            'rows': sub.height,
            'codes': sub['ts_code'].n_unique(),
            'member_days': days,
            'avg_codes_per_day': round(sub.height / days, 2) if days else 0.0,
            'first_date': str(dates[int(sub['cal_idx'].min())]) if sub.height else None,
            'last_date': str(dates[int(sub['cal_idx'].max())]) if sub.height else None,
        }
    transitions: dict[str, Any] = {}
    for prev, cur in zip(LAYER_ORDER, LAYER_ORDER[1:]):
        prev_codes = set(grid.filter(pl.col(prev))['ts_code'].unique().to_list())
        cur_codes = set(grid.filter(pl.col(cur))['ts_code'].unique().to_list())
        dropped = grid.filter(pl.col(prev) & ~pl.col(cur))
        transitions[f'{prev}->{cur}'] = {
            'rows_dropped': dropped.height,
            'codes_dropped': len(prev_codes - cur_codes),
            'rows_kept': summary[cur]['rows'],
        }
        del dropped
    parts: list[pl.DataFrame] = []
    for layer in LAYER_ORDER:
        parts.append(
            grid.filter(pl.col(layer)).group_by('year').agg(
                pl.len().alias('member_days'),
                pl.col('ts_code').n_unique().alias('codes'),
            ).with_columns(pl.lit(layer).alias('layer'))
        )
    by_year = pl.concat(parts).select(['layer', 'year', 'member_days', 'codes']) \
        .sort(['layer', 'year'])
    return summary, by_year, transitions


def _snapshot_comparison(grid: pl.DataFrame, root: Path, dates: list[date],
                         st_keys: pl.DataFrame) -> dict:
    """L3 区间版 vs 快照版（st_status_<最新>.parquet）差异（只比 ST，不含整理期）。"""
    snapshot_files = sorted((root / 'meta' / 'st_status').glob('st_status_*.parquet'))
    snapshot_file = snapshot_files[-1] if snapshot_files else None
    if snapshot_file is None:
        return {'available': False, 'reason': 'no_snapshot'}
    snap = pl.read_parquet(snapshot_file, columns=['code', 'is_st'])
    st_codes = snap.filter(pl.col('is_st') == True)['code'].unique().to_list()  # noqa: E712
    unknown_codes = snap.filter(pl.col('is_st').is_null())['code'].unique().to_list()
    year_map = {i: d.year for i, d in enumerate(dates)}

    l2 = grid.filter(pl.col('L2')).select(['cal_idx', 'ts_code'])
    l3 = grid.filter(pl.col('L3')).select(['cal_idx', 'ts_code'])
    interval_st = l2.join(st_keys.select(['cal_idx', 'ts_code']),
                          on=['cal_idx', 'ts_code'], how='inner')
    delisting_removed = l2.join(l3, on=['cal_idx', 'ts_code'], how='anti')
    snap_static = l2.filter(pl.col('ts_code').is_in(st_codes))
    only_interval = interval_st.join(snap_static, on=['cal_idx', 'ts_code'], how='anti')
    only_snapshot = snap_static.join(interval_st, on=['cal_idx', 'ts_code'], how='anti')
    both = interval_st.join(snap_static, on=['cal_idx', 'ts_code'], how='inner')

    def _year_counts(frame: pl.DataFrame) -> dict[str, int]:
        if frame.is_empty():
            return {}
        out = frame.with_columns(
            pl.col('cal_idx').replace_strict(year_map, return_dtype=pl.Int32).alias('year')
        ).group_by('year').len().sort('year')
        return {str(row['year']): int(row['len']) for row in out.iter_rows(named=True)}

    def _sample(frame: pl.DataFrame, size: int = 10) -> list[dict[str, str]]:
        if frame.is_empty():
            return []
        idx_map = {i: str(d) for i, d in enumerate(dates)}
        head = frame.sort(['cal_idx', 'ts_code']).head(size)
        return [{'date': idx_map[int(r['cal_idx'])], 'ts_code': r['ts_code']}
                for r in head.iter_rows(named=True)]

    return {
        'available': True,
        'snapshot_file': snapshot_file.name,
        'snapshot_st_codes': len(st_codes),
        'snapshot_unknown_codes': len(unknown_codes),
        'l2_rows': l2.height,
        'l3_rows': l3.height,
        'l3_removed_rows_total': delisting_removed.height,
        'interval_st_rows': interval_st.height,
        'snapshot_static_rows': snap_static.height,
        'both_st_rows': both.height,
        'only_interval_st_rows': only_interval.height,
        'only_interval_st_codes': only_interval['ts_code'].n_unique(),
        'only_snapshot_st_rows': only_snapshot.height,
        'only_snapshot_st_codes': only_snapshot['ts_code'].n_unique(),
        'only_interval_by_year': _year_counts(only_interval),
        'only_snapshot_by_year': _year_counts(only_snapshot),
        'only_interval_sample': _sample(only_interval),
        'only_snapshot_sample': _sample(only_snapshot),
    }


def _manifest(root: Path, staging: Path, outputs: dict[str, Path],
              summary: dict, transitions: dict, snapshot: dict,
              end: date, stats: dict, panel: pl.DataFrame) -> dict:
    upstream = {}
    for rel in UPSTREAM_FILES:
        path = root / rel
        upstream[rel] = {
            'exists': path.is_file(),
            'mtime': _mtime_iso(path),
            'size': path.stat().st_size if path.is_file() else None,
        }
    files = {}
    for layer, path in outputs.items():
        frame = pl.read_parquet(path, columns=['in_layer'])
        files[LAYER_FILES[layer]] = {
            'rows': frame.height,
            'true_rows': int(frame['in_layer'].sum()),
            'sha256': _sha256(path),
            'bytes': path.stat().st_size,
        }
    panel_codes = set(panel['ts_code'].unique().to_list())
    quote_gaps = sorted(set(stats['spans']) - panel_codes)
    return {
        'semantic': 'stock_universe_layers_l0_l5',
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'as_of': str(end),
        'columns': ['date', 'ts_code', 'in_layer'],
        'grid': '六层共用同一 L0 网格，in_layer 为布尔成员标记',
        'layer_order': list(LAYER_ORDER),
        'parameters': {
            'min_listed_trading_days': MIN_LISTED_TRADING_DAYS,
            'adv_window': ADV_WINDOW,
            'min_amount': MIN_AMOUNT,
            'delisting_window_trading_days': DELISTING_WINDOW_TRADING_DAYS,
            'pool_rules': POOL_RULES,
            'calendar': 'src/data/trading_calendar.py（基准 000001.SH，权威末日）',
        },
        'sources': {
            'stock_domain': 'meta/stock_basic.parquet（list_date/delist_date）',
            'calendar': 'quotes/index/sh/000001.SH.parquet',
            'quotes': 'quotes/stock/**/*.parquet（date/volume/amount）',
            'suspension': 'meta/suspension/suspension_daily.parquet + implied_suspension.parquet',
            'st_interval': 'meta/st/st_intervals.parquet（区间版，非快照）',
            'delisting_name': 'meta/name_history.parquet（名称含「退」的历史段）',
            'pool_rules': 'src/data/universe_filter.py UniverseFilterConfig 默认值'
                          '（meta/pools 当前仅 ETF 权益池，无股票池文件）',
        },
        'counts': summary,
        'transitions': transitions,
        'st_snapshot_vs_interval': snapshot,
        'build_stats': {
            'grid_rows': stats['rows'],
            'codes': stats['codes'],
            'skipped_codes': stats['skipped_codes'],
            'quote_panel_rows': panel.height,
            'codes_without_local_quotes': len(quote_gaps),
            'codes_without_local_quotes_list': quote_gaps,
            'codes_without_local_quotes_note': (
                'L0 域内但本地 quotes/stock 无行情文件，L2 起无法进入；'
                '均为已退市老股，属数据覆盖缺口，未用占位值补齐'),
        },
        'upstream': upstream,
        'files': files,
        'staging': str(staging),
        'final_dir': str(root / FORMAL_REL),
    }


def _verify_outputs(outputs: dict[str, Path], summary: dict) -> None:
    for layer, path in outputs.items():
        frame = pl.read_parquet(path)
        if frame.height != summary['grid_rows']:
            raise RuntimeError(f'{layer} 网格行数不一致: {frame.height} != {summary["grid_rows"]}')
        if int(frame['in_layer'].sum()) != summary[layer]['rows']:
            raise RuntimeError(f'{layer} 成员行数不一致')
    sets: dict[str, pl.DataFrame] = {}
    for layer, path in outputs.items():
        frame = pl.read_parquet(path, columns=['date', 'ts_code', 'in_layer']) \
            .filter(pl.col('in_layer'))
        sets[layer] = frame.select(['date', 'ts_code'])
    for prev, cur in zip(LAYER_ORDER, LAYER_ORDER[1:]):
        missing = sets[cur].join(sets[prev], on=['date', 'ts_code'], how='anti')
        if missing.height:
            raise RuntimeError(f'{cur} 有 {missing.height} 行不在 {prev} 内')
    logger.info('[layers] 嵌套性与行数校验通过')


def _promote(root: Path, staging: Path, outputs: dict[str, Path]) -> None:
    target_dir = root / FORMAL_REL
    target_dir.mkdir(parents=True, exist_ok=True)
    for layer, path in outputs.items():
        target = target_dir / LAYER_FILES[layer]
        write_bytes_atomic(path.read_bytes(), target)
        if _sha256(target) != _sha256(path):
            raise RuntimeError(f'{layer} 提升后 SHA256 不一致')
    for name in ('manifest.json', 'counts_by_year.csv', 'st_snapshot_vs_interval.json'):
        write_bytes_atomic((staging / name).read_bytes(), target_dir / name)
    logger.info(f'[layers] 正式路径 {target_dir} 已写入（{len(outputs)} 个 parquet）')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建股票池分层矩阵 L0–L5')
    parser.add_argument('--store-root', default=str(PROJECT_ROOT / 'data' / 'local_store'))
    parser.add_argument('--as-of', default=None, help='截至日期（YYYY-MM-DD），默认取日历权威末端')
    parser.add_argument('--task-dir', default=TASK_NAME)
    parser.add_argument('--limit-codes', type=int, default=None, help='调试用：只取前 N 只')
    parser.add_argument('--apply', action='store_true', help='校验通过后原子提升到正式路径')
    args = parser.parse_args(argv)

    root = Path(args.store_root).resolve()
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    dates, end = _calendar_dates(root, as_of)
    staging = staging_dir(args.task_dir)
    staging.mkdir(parents=True, exist_ok=True)

    basic = _read_stock_basic(root)
    grid, stats = _build_grid(basic, dates, args.limit_codes)
    logger.info(f'[layers] L0 网格 {grid.height:,} 行 / {stats["codes"]} 只 / 截至 {end}')

    universe = set(grid['ts_code'].unique().to_list())
    panel = _quote_panel(root, dates, universe, end)
    logger.info(f'[layers] 行情面板 {panel.height:,} 行')

    traded = panel.filter(pl.col('traded')).select(['cal_idx', 'ts_code']).unique()
    union = load_suspension_union(root)
    susp = union.frame[['ts_code', 'trade_date']]
    if len(susp):
        idx_map = pl.DataFrame({
            'trade_date': pl.Series(dates, dtype=pl.Date),
            'cal_idx': pl.Series(list(range(len(dates))), dtype=pl.Int32),
        })
        suspend_keys = (
            pl.from_pandas(susp)
            .with_columns(pl.col('trade_date').cast(pl.Date))
            .join(idx_map, on='trade_date', how='inner')
            .select(['cal_idx', 'ts_code']).unique()
        )
    else:
        suspend_keys = pl.DataFrame(schema={'cal_idx': pl.Int32, 'ts_code': pl.String})

    grid = grid.join(traded.with_columns(pl.lit(True).alias('traded_bar')),
                     on=['cal_idx', 'ts_code'], how='left')
    grid = grid.join(suspend_keys.with_columns(pl.lit(True).alias('suspended')),
                     on=['cal_idx', 'ts_code'], how='left')
    grid = grid.with_columns(
        (pl.col('l1') & pl.col('traded_bar').fill_null(False)
         & ~pl.col('suspended').fill_null(False)).alias('l2')
    ).drop(['traded_bar', 'suspended'])

    st_keys = _expand_intervals(
        root / 'meta' / 'st' / 'st_intervals.parquet', dates, end,
        columns=['ts_code', 'start_date', 'end_date'],
    )
    delist_keys = _delisting_keys(basic, dates)
    name_keys = _delisting_name_keys(root, dates, end)
    grid = grid.join(st_keys.with_columns(pl.lit(True).alias('is_st')),
                     on=['cal_idx', 'ts_code'], how='left')
    grid = grid.join(delist_keys.with_columns(pl.lit(True).alias('delist_period')),
                     on=['cal_idx', 'ts_code'], how='left')
    grid = grid.join(name_keys.with_columns(pl.lit(True).alias('name_delist')),
                     on=['cal_idx', 'ts_code'], how='left')
    grid = grid.with_columns(
        (pl.col('l2')
         & ~pl.col('is_st').fill_null(False)
         & ~pl.col('delist_period').fill_null(False)
         & ~pl.col('name_delist').fill_null(False)).alias('l3')
    ).drop(['is_st', 'delist_period', 'name_delist'])

    amount_panel = panel.select(['cal_idx', 'ts_code', 'amount']).unique(
        subset=['cal_idx', 'ts_code'], keep='first')
    grid = grid.join(amount_panel, on=['cal_idx', 'ts_code'], how='left')
    grid = grid.sort(['ts_code', 'cal_idx'])
    grid = grid.with_columns(
        pl.col('amount').rolling_mean(window_size=ADV_WINDOW, min_samples=1)
        .over('ts_code').alias('adv20')
    )
    grid = grid.with_columns(
        (pl.col('l3') & pl.col('adv20').is_not_null()
         & (pl.col('adv20') >= MIN_AMOUNT)).alias('l4')
    ).drop('amount')

    entity = load_entity_index(root)
    boards = {code: board_of(code) for code in universe}
    allowed_boards = ['main', 'star', 'chinext']
    superseded = list(set(entity.superseded) & universe)
    grid = grid.with_columns(
        pl.col('ts_code').replace_strict(boards, return_dtype=pl.String).alias('board'),
        pl.col('ts_code').is_in(superseded).alias('superseded'),
    )
    grid = grid.with_columns(
        (pl.col('l4') & pl.col('board').is_in(allowed_boards)
         & ~pl.col('superseded')).alias('l5')
    ).drop(['board', 'superseded'])

    grid = grid.with_columns(
        pl.lit(True).alias('L0'),
        pl.col('l1').cast(pl.Boolean).alias('L1'),
        pl.col('l2').cast(pl.Boolean).alias('L2'),
        pl.col('l3').cast(pl.Boolean).alias('L3'),
        pl.col('l4').cast(pl.Boolean).alias('L4'),
        pl.col('l5').cast(pl.Boolean).alias('L5'),
    ).drop(['l1', 'l2', 'l3', 'l4', 'l5'])
    grid = grid.with_columns(
        pl.col('cal_idx').replace_strict(
            {i: d for i, d in enumerate(dates)}, return_dtype=pl.Date).alias('date')
    ).sort(['cal_idx', 'ts_code'])
    if grid.height != stats['rows']:
        raise RuntimeError(f'网格被 join 放大: {grid.height} != {stats["rows"]}')

    outputs: dict[str, Path] = {}
    for layer in LAYER_ORDER:
        out = staging / LAYER_FILES[layer]
        frame = grid.select([pl.col('date'), pl.col('ts_code'),
                             pl.col(layer).alias('in_layer')])
        write_parquet_atomic(frame, out)
        outputs[layer] = out
        logger.info(f'[layers] 写出 {layer} -> {out.name}（成员 {int(frame["in_layer"].sum()):,} 行）')

    summary, by_year, transitions = _counts(grid, dates)
    snapshot = _snapshot_comparison(grid, root, dates, st_keys)
    manifest = _manifest(root, staging, outputs, summary, transitions,
                         snapshot, end, stats, panel)
    (staging / 'manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    by_year.write_csv(staging / 'counts_by_year.csv')
    (staging / 'st_snapshot_vs_interval.json').write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2), encoding='utf-8')

    print('=== LAYER SUMMARY ===')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print('=== TRANSITIONS ===')
    print(json.dumps(transitions, ensure_ascii=False, indent=2))
    print('=== ST SNAPSHOT VS INTERVAL ===')
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))

    if not args.apply:
        logger.info(f'[layers] 候选已写入 {staging}；加 --apply 提升到正式路径')
        return 0
    _verify_outputs(outputs, summary)
    _promote(root, staging, outputs)
    logger.info(f'[layers] 已提升到 {root / FORMAL_REL}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
