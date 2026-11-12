#!/usr/bin/env python3

r"""市场宽度派生表：站上 MA20 / MA60 / MA120 的个股占比（``breadth_ma_daily``）。

产物（``--apply`` 后）
=====================
- ``data/local_store/meta/breadth/breadth_ma_daily.parquet``：一个交易日一行
- ``data/local_store/meta/breadth/manifest.json``：口径全量、行数、只数、覆盖区间、SHA256

候选先写 ``_staging/breadth_ma_<YYYYMMDD>/``（``parts/`` 逐块、``progress.csv`` 断点、
``failed.csv`` 失败清单），校验通过后 ``--apply`` 原子替换到正式路径。

与 ``breadth_daily.parquet`` 的关系
===================================
两者互不覆盖：``breadth_daily`` 是 ``src/data/services/breadth_service.py`` 的产出
（涨跌家数 / 涨跌停 / 250 日新高新低 / ADL），本表只新增「均线占比」语义。
``new_high_250`` / ``new_low_250`` 本表自行计算（口径见下），不读现有表。

口径（逐条，与 manifest 内 ``method`` 一致）
==========================================
1. 复权方式：``adj_close = close × hfq_factor``（后复权收盘价）。均线、新高新低都用它，
   避免除权造成的假跌破。原始不复权 ``close`` 只用于「当日是否成交」判定（配合 ``volume``）。
2. 样本纳入：``quotes/stock/**`` 全市场，含 ST，不做 ST 过滤；
   剔除 ``meta/code_migration.parquet`` 判定的旧码（``superseded``，避免同一实体双计）；
   **剔除上市不足 60 个交易日的**（``meta/stock_basic.parquet`` 的 ``list_date``，
   缺失回退 ``meta/universe/listing_intervals.parquet`` 的 ``first_trade_date``）；
   **剔除当日无成交的**（``volume`` 为空或 ≤ 0）；退市标的 ``date >= delist_date`` 起不再进分母。
3. 停牌处理：当日无 bar 或 ``volume<=0`` 的标的当日不进分母；停牌日也不为均线提供 bar，
   即 MA 取「最近 N 个有成交的交易日」，停牌日不占窗口。
4. 起始日期：由数据决定——MA120 连续可得的第一个交易日（最后一个「当日无任何 MA120」日之后），
   不硬编码年份。
5. 均线：``rolling_mean(window, min_samples=window)``，window = 20 / 60 / 120，
   按标的自身的有效 bar 序列滚动。
6. 站上判定：``adj_close − ma > 1e-9 × |ma|``（严格大于，相对容差只用于消除「恰好收在均线上」
   时浮点求和顺序的末位漂移；等于均线不算站上）。
7. 分母：``sample_size`` = 当日有 MA20 的只数；MA60 / MA120 各自的可得只数分别记在
   ``sample_size_ma60`` / ``sample_size_ma120``；``sample_size_250`` 为 250 日窗口满的只数。
   ``pct_above_maN = above_maN / sample_size_maN × 100``。
8. ``new_high_250`` / ``new_low_250``：``adj_close`` 相对自身近 250 个有效 bar 的
   ``rolling_max/min(min_samples=250)``，``>= max`` 记新高、``<= min`` 记新低——
   与 ``breadth_service`` 同口径（后者用 ``close × qfq_factor``，与 ``hfq_factor`` 只差
   每只标的的常数倍，比较结果不变）。

用法::

    .venv\\Scripts\\python.exe tools\\build_breadth_ma.py            # 只构建候选
    .venv\\Scripts\\python.exe tools\\build_breadth_ma.py --apply    # 构建 + 落正式路径
    .venv\\Scripts\\python.exe tools\\build_breadth_ma.py --rebuild  # 忽略断点重算
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORE = ROOT / 'data' / 'local_store'

#: 口径版本；任一口径项变化都必须 +1（写进 manifest，不写进文件名）
METHOD_VERSION = 'breadth_ma_v1'

#: 同目录 ``breadth_daily.parquet`` 首次补登进本 manifest 的日期（只作记账，不随重建变化）
REGISTERED_DATE = '2026-11-12'

#: 均线窗口
MA_WINDOWS = (20, 60, 120)
#: 250 日新高新低窗口
HIGH_LOW_WINDOW = 250
#: 上市门槛：至少满 60 个交易日（含首日）
MIN_LISTED_TD = 60

#: 分块大小（按文件数）；每块独立落 parquet，便于断点续跑
DEFAULT_CHUNK_SIZE = 400

#: 「站上」判定的相对容差。恰好收在均线上（``adj == ma``）时，浮点求和顺序的末位差异
#: 会把同一只标的判到两边；用相对容差消除这种不确定性，实测每 5,500 只有 1 只踩线。
ABOVE_TOL = 1e-9

_PRICE_COLUMNS = ['date', 'ts_code', 'close', 'volume', 'hfq_factor']

_PART_COLUMNS = [
    'date',
    'n_valid',
    'above_ma20', 'n_ma20',
    'above_ma60', 'n_ma60',
    'above_ma120', 'n_ma120',
    'new_high_250', 'new_low_250', 'n_250',
]

_OUTPUT_COLUMNS = [
    'date', 'sample_size',
    'above_ma20', 'above_ma60', 'above_ma120',
    'pct_above_ma20', 'pct_above_ma60', 'pct_above_ma120',
    'new_high_250', 'new_low_250',
    'sample_size_ma60', 'sample_size_ma120', 'sample_size_250',
]


# ----------------------------------------------------------------------
# 路径
# ----------------------------------------------------------------------
def staging_dir(day: date | None = None) -> Path:
    return ROOT / '_staging' / f'breadth_ma_{(day or date.today()):%Y%m%d}'


def target_path(store: Path) -> Path:
    return store / 'meta' / 'breadth' / 'breadth_ma_daily.parquet'


def manifest_path(store: Path) -> Path:
    return store / 'meta' / 'breadth' / 'manifest.json'


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


# ----------------------------------------------------------------------
# 校准：交易日历、上市日、退市日
# ----------------------------------------------------------------------
def build_calendar(store: Path) -> list[date]:
    """交易日历 = 个股行情里实际出现过的全部日期（升序去重）。

    不读 ``breadth_daily``（避免跨链依赖），也不用交易日历模块的「工作日近似尾部」，
    以免把未来日期当历史。
    """
    glob = str(store / 'quotes' / 'stock' / '**' / '*.parquet')
    # 只取 date：投影子集读取，多余列不应让日历构建失败
    frame = (pl.scan_parquet(glob, extra_columns='ignore')
             .select(pl.col('date').cast(pl.Date))
             .unique().sort('date').collect())
    return frame['date'].to_list()


def build_code_meta(
    store: Path, calendar: list[date], quote_codes: list[str],
) -> pl.DataFrame:
    """逐只标的的 (list_pos, delist_date)：用于上市门槛与退市剔除。"""
    basic = pl.read_parquet(
        store / 'meta' / 'stock_basic.parquet',
        columns=['ts_code', 'list_date', 'delist_date'])
    intervals = pl.read_parquet(
        store / 'meta' / 'universe' / 'listing_intervals.parquet',
        columns=['ts_code', 'data_type', 'first_trade_date'])
    intervals = (intervals.filter(pl.col('data_type') == 'stock')
                 .select(['ts_code', pl.col('first_trade_date').alias('_first_trade')]))
    basic = basic.join(intervals, on='ts_code', how='left')
    basic = basic.with_columns(
        pl.coalesce([pl.col('list_date'), pl.col('_first_trade')]).alias('list_date'))

    basic = basic.filter(pl.col('ts_code').is_in(quote_codes))
    missing = sorted(basic.filter(pl.col('list_date').is_null())['ts_code'].to_list())
    uncovered = sorted(set(quote_codes) - set(basic['ts_code'].to_list()))
    if missing or uncovered:
        # 上市日是纳入门槛的唯一依据，缺了就无从判断「是否满 60 个交易日」；
        # 宁可停下要求补 stock_basic，也不用行情首行冒充上市日（DG-16）
        raise SystemExit(
            f'[breadth_ma] 上市日缺失，拒绝继续：缺 list_date {len(missing)} 只 {missing[:10]}；'
            f'不在 stock_basic {len(uncovered)} 只 {uncovered[:10]}')

    rows: list[tuple[str, int, date | None]] = []
    for item in basic.select(['ts_code', 'list_date', 'delist_date']).iter_rows(named=True):
        pos = bisect.bisect_left(calendar, item['list_date'])
        rows.append((item['ts_code'], pos, item['delist_date']))
    return pl.DataFrame(
        rows, schema={'ts_code': pl.Utf8, 'list_pos': pl.Int32, 'delist_date': pl.Date},
        orient='row')


def superseded_codes(store: Path) -> set[str]:
    """旧码集合（已被新码完整覆盖），与 ``breadth_service`` 的去重口径一致。"""
    from src.data.quality.code_migration import load_entity_index

    return set(load_entity_index(store).superseded)


# ----------------------------------------------------------------------
# 分块计算
# ----------------------------------------------------------------------
def _read_chunk(
    files: Iterable[Path], failed: list[tuple[str, str]],
) -> pl.DataFrame | None:
    """逐文件读取，坏文件记 failed 后跳过，不中断整块。"""
    frames: list[pl.DataFrame] = []
    for path in files:
        try:
            frames.append(pl.read_parquet(path, columns=_PRICE_COLUMNS))
        except Exception as exc:  # noqa: BLE001 - 单只坏文件不应中断全量
            failed.append((path.name.removesuffix('.parquet'), f'{type(exc).__name__}: {exc}'))
    if not frames:
        return None
    return pl.concat(frames, how='vertical_relaxed')


def _empty_part() -> pl.DataFrame:
    """空块的统一 schema，保证跨块 concat 不炸。"""
    schema = {name: (pl.Date if name == 'date' else pl.Int64) for name in _PART_COLUMNS}
    return pl.DataFrame(schema=schema)


def process_chunk(
    frame: pl.DataFrame,
    *,
    calendar_pos: pl.DataFrame,
    meta: pl.DataFrame,
    drop_codes: set[str],
) -> pl.DataFrame:
    """一个块 → 逐日计数（未做跨块合并）。"""
    if 'ts_code' in frame.columns and drop_codes:
        frame = frame.filter(~pl.col('ts_code').is_in(sorted(drop_codes)))
    frame = frame.with_columns([
        pl.col('date').cast(pl.Date),
        # 后复权收盘价：均线与新高新低都基于它
        (pl.col('close') * pl.col('hfq_factor')).alias('adj_close'),
    ])
    # 当日无成交 / 价格缺失 → 不进分母，也不为均线提供 bar
    frame = frame.filter(
        pl.col('volume').is_not_null() & (pl.col('volume') > 0)
        & pl.col('adj_close').is_not_null() & (pl.col('adj_close') > 0)
    )
    if frame.height == 0:
        return _empty_part()

    frame = frame.join(calendar_pos, on='date', how='left')
    frame = frame.join(meta, on='ts_code', how='left')

    # 均线必须先按「全部有效 bar」滚动，再按纳入条件筛行：
    # 若先剔除上市初期 60 个交易日的行，新股的 MA20/MA60 会被截断成不可得
    frame = frame.sort(['ts_code', 'date'])
    exprs: list[pl.Expr] = []
    for window in MA_WINDOWS:
        exprs.append(
            pl.col('adj_close').rolling_mean(window, min_samples=window)
            .over('ts_code').alias(f'ma{window}'))
    exprs.append(
        pl.col('adj_close').rolling_max(HIGH_LOW_WINDOW, min_samples=HIGH_LOW_WINDOW)
        .over('ts_code').alias('hi250'))
    exprs.append(
        pl.col('adj_close').rolling_min(HIGH_LOW_WINDOW, min_samples=HIGH_LOW_WINDOW)
        .over('ts_code').alias('lo250'))
    frame = frame.with_columns(exprs)

    # 上市门槛：上市第 N 个交易日 = (pos - list_pos + 1)，要求 N >= 60
    gate = (pl.col('date_pos') - pl.col('list_pos') + 1) >= MIN_LISTED_TD
    # 退市：delist_date 起不再进分母
    alive = pl.col('delist_date').is_null() | (pl.col('date') < pl.col('delist_date'))
    frame = frame.filter(gate & alive)
    if frame.height == 0:
        return _empty_part()

    aggs: list[pl.Expr] = [pl.len().alias('n_valid')]
    for window in MA_WINDOWS:
        aggs.append(
            ((pl.col('adj_close') - pl.col(f'ma{window}'))
             > (pl.col(f'ma{window}').abs() * ABOVE_TOL)).fill_null(False).sum()
            .alias(f'above_ma{window}'))
        aggs.append(pl.col(f'ma{window}').is_not_null().sum().alias(f'n_ma{window}'))
    aggs.append(
        ((pl.col('adj_close') >= pl.col('hi250')).fill_null(False)).sum()
        .alias('new_high_250'))
    aggs.append(
        ((pl.col('adj_close') <= pl.col('lo250')).fill_null(False)).sum()
        .alias('new_low_250'))
    aggs.append(pl.col('hi250').is_not_null().sum().alias('n_250'))
    return frame.group_by('date').agg(aggs).sort('date')


# ----------------------------------------------------------------------
# 断点
# ----------------------------------------------------------------------
def _read_progress(path: Path) -> dict[int, dict[str, Any]]:
    if not path.is_file():
        return {}
    done: dict[int, dict[str, Any]] = {}
    with open(path, encoding='utf-8', newline='') as handle:
        for row in csv.DictReader(handle):
            try:
                done[int(row['chunk'])] = row
            except (KeyError, ValueError):
                continue
    return done


def _append_progress(path: Path, row: dict[str, Any]) -> None:
    header = ['chunk', 'part', 'codes', 'rows', 'valid_rows', 'status', 'elapsed_s', 'updated_at']
    write_header = not path.is_file()
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _append_failed(path: Path, rows: list[tuple[str, str]]) -> None:
    if not rows:
        return
    write_header = not path.is_file()
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(['ts_code', 'reason', 'logged_at'])
        stamp = datetime.now().isoformat(timespec='seconds')
        for code, reason in rows:
            writer.writerow([code, reason, stamp])


def _config_payload(store: Path) -> dict[str, Any]:
    return {
        'method_version': METHOD_VERSION,
        'ma_windows': list(MA_WINDOWS),
        'high_low_window': HIGH_LOW_WINDOW,
        'min_listed_td': MIN_LISTED_TD,
        'price': 'adj_close = close * hfq_factor',
        'gate': 'volume>0 and listed>=60td and date<delist_date',
        'store_root': str(store),
    }


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def build(
    store: Path, *, chunk_size: int = DEFAULT_CHUNK_SIZE, rebuild: bool = False,
) -> tuple[pl.DataFrame, Path]:
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    stage = staging_dir()
    parts = stage / 'parts'
    parts.mkdir(parents=True, exist_ok=True)
    progress_path = stage / 'progress.csv'
    failed_path = stage / 'failed.csv'
    config_path = stage / 'config.json'

    config = _config_payload(store)
    if config_path.is_file():
        existing = json.loads(config_path.read_text(encoding='utf-8'))
        if existing != config and not rebuild:
            raise SystemExit(
                f'[breadth_ma] 口径配置与上次构建不一致，拒绝续跑；确认后加 --rebuild\n'
                f'  旧: {json.dumps(existing, ensure_ascii=False)}\n'
                f'  新: {json.dumps(config, ensure_ascii=False)}')
    config_path.write_text(
        json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
    if rebuild:
        for stale in parts.glob('part_*.parquet'):
            stale.unlink()
        progress_path.unlink(missing_ok=True)
        failed_path.unlink(missing_ok=True)

    files = sorted((store / 'quotes' / 'stock').glob('**/*.parquet'))
    if not files:
        raise SystemExit(f'[breadth_ma] 未找到个股行情: {store / "quotes" / "stock" / "**"}')
    logger.info(f'[breadth_ma] 个股行情 {len(files)} 个文件，分块 {chunk_size} 只/块')

    calendar = build_calendar(store)
    logger.info(f'[breadth_ma] 交易日历 {len(calendar)} 天：{calendar[0]} ~ {calendar[-1]}')
    calendar_pos = pl.DataFrame(
        {'date': calendar, 'date_pos': list(range(len(calendar)))},
        schema={'date': pl.Date, 'date_pos': pl.Int32})
    quote_codes = [path.stem for path in files]
    meta = build_code_meta(store, calendar, quote_codes)
    drop = superseded_codes(store)
    logger.info(f'[breadth_ma] 旧码剔除 {len(drop)} 只：{sorted(drop)}')

    chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
    done = _read_progress(progress_path)
    for index, chunk in enumerate(chunks):
        part = parts / f'part_{index:04d}.parquet'
        record = done.get(index)
        if record is not None and record.get('status') == 'ok' and part.is_file():
            logger.info(f'[breadth_ma] 块 {index} 已完成，跳过（断点续跑）')
            continue

        started = time.time()
        failures: list[tuple[str, str]] = []
        frame = _read_chunk(chunk, failures)
        rows = frame.height if frame is not None else 0
        result = (process_chunk(frame, calendar_pos=calendar_pos, meta=meta,
                                drop_codes=drop)
                  if frame is not None else None)
        if result is not None and result.height:
            write_parquet_atomic(result.select(_PART_COLUMNS), part)
            valid = int(result['n_valid'].sum())
        else:
            valid = 0
            write_parquet_atomic(_empty_part(), part)
        _append_failed(failed_path, failures)
        _append_progress(progress_path, {
            'chunk': index,
            'part': part.name,
            'codes': len(chunk),
            'rows': rows,
            'valid_rows': valid,
            'status': 'ok' if not failures else f'ok_with_{len(failures)}_failed',
            'elapsed_s': f'{time.time() - started:.1f}',
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        })
        logger.info(f'[breadth_ma] 块 {index + 1}/{len(chunks)}: {len(chunk)} 只 / '
                    f'{rows} 行 / 有效 {valid} 行 / 失败 {len(failures)} 只')

    parts_files = sorted(parts.glob('part_*.parquet'))
    if len(parts_files) != len(chunks):
        raise SystemExit(f'[breadth_ma] 分块不足：{len(parts_files)}/{len(chunks)}')
    merged = pl.read_parquet(parts_files)
    daily = assemble(merged)
    out = stage / 'breadth_ma_daily.parquet'
    write_parquet_atomic(daily, out)
    logger.info(f'[breadth_ma] 候选完成 {daily.height} 行 -> {out}')
    return daily, stage


def assemble(parts: pl.DataFrame) -> pl.DataFrame:
    """跨块按日期求和 → 占比 → 由数据决定起点。"""
    summed = parts.group_by('date').agg([pl.col(name).sum() for name in _PART_COLUMNS[1:]])
    summed = summed.sort('date')
    # 起点由数据决定：MA120 连续可得的第一个交易日（最后一个「无 MA120」日之后）
    zero_dates = summed.filter(pl.col('n_ma120') == 0)['date']
    if zero_dates.len():
        start = zero_dates.max()
        summed = summed.filter(pl.col('date') > start)
    if summed.height == 0:
        raise SystemExit('[breadth_ma] 序列为空：没有任何交易日满足 MA120 可得')

    def ratio(above: str, base: str) -> pl.Expr:
        return (pl.when(pl.col(base) > 0)
                .then(pl.col(above) / pl.col(base) * 100.0)
                .otherwise(None)).round(6).alias(f'pct_{above}')

    out = summed.with_columns([
        pl.col('n_ma20').alias('sample_size'),
        pl.col('n_ma60').alias('sample_size_ma60'),
        pl.col('n_ma120').alias('sample_size_ma120'),
        pl.col('n_250').alias('sample_size_250'),
        ratio('above_ma20', 'n_ma20'),
        ratio('above_ma60', 'n_ma60'),
        ratio('above_ma120', 'n_ma120'),
    ])
    out = out.select([
        pl.col(name).cast(pl.Int64) if name != 'date' and not name.startswith('pct_')
        else pl.col(name).cast(pl.Float64) if name.startswith('pct_') else pl.col(name)
        for name in _OUTPUT_COLUMNS
    ])
    return out.sort('date')


def _sibling_tables(store: Path) -> dict:
    """同目录 ``breadth_daily.parquet`` 的逐文件记账（与本表互不覆盖）。

    该表由 ``src/data/services/breadth_service.py::refresh`` 维护，本脚本
    不读它（独立取数链），但 manifest 归本脚本整份重写，必须一并登记，
    否则每跑一次就抹掉这条记账。
    """
    directory = Path(store) / 'meta' / 'breadth'
    daily = directory / 'breadth_daily.parquet'
    files: list[dict] = []
    if daily.is_file():
        frame = pl.read_parquet(daily)
        entry = {
            'path': str(daily.relative_to(ROOT)).replace('\\', '/'),
            'semantics': '全市场涨跌家数、涨跌停家数、250 日新高新低与 ADL 的逐日表，一行 = 一个交易日',
            'produced_by': 'src/data/services/breadth_service.py::refresh'
                           '（python -m src.data.local_store build 收口调用）',
            'rows': frame.height,
            'cols': frame.width,
            'columns': list(frame.columns),
            'date_range': [str(frame['trade_date'].min()), str(frame['trade_date'].max())],
            'bytes': daily.stat().st_size,
            'sha256': _sha256(daily),
            'state_file': 'data/local_store/meta/breadth/coverage.json',
            'registered_at': REGISTERED_DATE,
            'registered_reason': '补登：本 manifest 此前只描述 breadth_ma_daily.parquet，'
                                 '同目录 breadth_daily.parquet 无目录级记账'
                                 '（其自身状态只有 coverage.json，无 SHA）',
        }
        coverage = directory / 'coverage.json'
        if coverage.is_file():
            meta = json.loads(coverage.read_text(encoding='utf-8'))
            entry['coverage'] = {
                key: meta.get(key) for key in
                ('version', 'first_trade_date', 'last_trade_date', 'rows', 'built_at')
            }
        files.append(entry)
    return {
        'note': '同目录另有一张由 breadth_service.py 维护的逐日宽度表；与本表语义不同、互不覆盖'
                '（本表不读它，属独立取数链），本条只作目录级记账',
        'files': files,
    }


def write_manifest(
    daily: pl.DataFrame, store: Path, stage: Path, *, chunk_size: int, codes: int,
    failed_rows: list[dict[str, str]],
) -> Path:
    target = target_path(store)
    sources = [
        'quotes/stock/**/*.parquet',
        'meta/stock_basic.parquet',
        'meta/universe/listing_intervals.parquet',
        'meta/code_migration.parquet',
    ]
    payload = {
        'path': str(target.relative_to(ROOT)).replace('\\', '/'),
        'semantics': '市场宽度：站上 MA20 / MA60 / MA120 的个股占比（后复权口径）',
        'method_version': METHOD_VERSION,
        'produced_by': 'tools/build_breadth_ma.py',
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'rows': daily.height,
        'date_range': [str(daily['date'].min()), str(daily['date'].max())],
        'universe': {
            'quote_files': codes,
            'chunk_size': chunk_size,
            'excludes_superseded': sorted(superseded_codes(store)),
            'failed_files': failed_rows,
        },
        'columns': _OUTPUT_COLUMNS,
        'method': {
            'price': 'adj_close = close × hfq_factor（后复权）；原始 close 只用于「当日是否成交」判定',
            'adjustment_reason': '后复权避免除权造成的假跌破；hfq_factor 与 qfq_factor 只差每只标的的常数倍，'
                                 '比较类指标（站上均线 / 新高新低）结果不受该常数影响',
            'sample_inclusion': '全市场含 ST；剔除 code_migration 旧码；剔除上市不足 60 个交易日的；'
                                '剔除当日无成交的',
            'listed_gate': 'list_date（缺失回退 listing_intervals.first_trade_date）起第 60 个交易日'
                           '（含首日）及以后才计入',
            'delist_gate': 'date >= delist_date 起不再进分母（delist_date 与末个行情日相等的标的，'
                           '其最后一个交易日同样剔除；影响面为 335 只各 1 天）',
            'suspension': '当日无 bar 或 volume<=0 的标的当日不进分母，也不为均线提供 bar；'
                          '即 MA 取「最近 N 个有成交的交易日」，停牌日不占窗口。'
                          '序列连续性影响：停牌不产生缺口，但分母当日缩小，占比的日间波动会略放大',
            'ma': 'rolling_mean(window, min_samples=window)，window ∈ {20,60,120}，按标的自身有效 bar 滚动',
            'above_rule': 'adj_close − ma > 1e-9 × |ma|（严格大于，相对容差消除「恰好收在均线上」时'
                          '浮点求和顺序导致的判定漂移）',
            'denominator': 'sample_size=有 MA20 的只数；pct_above_maN = above_maN / sample_size_maN × 100，'
                           '保留 6 位小数',
            'start': '由数据决定：MA120 连续可得的第一个交易日（最后一个「当日无任何 MA120」日之后），'
                     '不硬编码年份',
            'high_low': 'new_high_250/new_low_250 由本脚本自算：adj_close 相对自身近 250 个有效 bar 的 '
                        'rolling_max/min（min_samples=250）；与 breadth_daily 的同类列同口径'
                        '（后者用 close×qfq_factor，与 hfq 只差每只标的常数倍，比较结果不变），'
                        '并非从该表抄录',
        },
        'sources': sources,
        'rebuild': 'python tools/build_breadth_ma.py --rebuild --apply',
        'notes': [
            '本表与 breadth_daily.parquet 语义不同，互不覆盖；breadth_daily 由 breadth_service.py 维护',
            '尚未接入代码消费者；读取方式 pl.read_parquet(store/"meta"/"breadth"/"breadth_ma_daily.parquet")',
        ],
        'sha256': _sha256(target),
        'bytes': target.stat().st_size,
        'sibling_tables': _sibling_tables(store),
    }
    path = manifest_path(store)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)
    return path


def apply(daily: pl.DataFrame, store: Path, stage: Path, *, chunk_size: int, codes: int) -> None:
    from src.data.atomic_write import write_parquet_atomic

    failed_rows: list[dict[str, str]] = []
    failed_path = stage / 'failed.csv'
    if failed_path.is_file():
        with open(failed_path, encoding='utf-8', newline='') as handle:
            failed_rows = list(csv.DictReader(handle))

    target = target_path(store)
    write_parquet_atomic(daily, target)
    written = pl.read_parquet(target)
    assert written.height == daily.height, '落盘行数与候选不一致'
    assert written.equals(daily), '落盘内容与候选不一致'
    path = write_manifest(daily, store, stage, chunk_size=chunk_size, codes=codes,
                          failed_rows=failed_rows)
    logger.info(f'[breadth_ma] 已落正式路径 {target}（SHA256 {_sha256(target)[:16]}…）')
    logger.info(f'[breadth_ma] manifest -> {path}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建 breadth_ma_daily（均线占比宽度）')
    parser.add_argument('--store-root', default=str(DEFAULT_STORE))
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument('--rebuild', action='store_true', help='忽略断点，清空本次分块重算')
    parser.add_argument('--apply', action='store_true', help='校验通过后原子写正式路径')
    args = parser.parse_args(argv)

    store = Path(args.store_root)
    daily, stage = build(store, chunk_size=args.chunk_size, rebuild=args.rebuild)
    codes = len(sorted((store / 'quotes' / 'stock').glob('**/*.parquet')))
    if args.apply:
        apply(daily, store, stage, chunk_size=args.chunk_size, codes=codes)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
