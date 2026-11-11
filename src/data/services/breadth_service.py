#!/usr/bin/env python3

"""全市场涨跌家数（breadth）统一取数服务。

数据口径（与 ``ExecutionRules`` 同源）：

- 有效样本：``volume > 0`` 且 ``pre_close`` 非空；
- 涨/跌/平 = ``close > / < / == pre_close``；
- ``total_count`` = 当日有成交的股票数（不是上市总数）；
- 涨跌停用价格口径：``round_half_up(pre_close × (1 ± limit_pct), 2)``，
  幅度由 ``ExecutionRules.resolve_limit_pct`` 推导；新股首个交易日不计；
- ``new_high_250`` / ``new_low_250`` 用 ``close × qfq_factor`` 的 250 日
  ``rolling max/min``（``min_samples=250``）；
- ``adl`` 从本地行情首个交易日开始全历史累计，不随显示窗口变化。

落盘位置::

    data/local_store/meta/breadth/breadth_daily.parquet
    data/local_store/meta/breadth/coverage.json

读取入口 ``get_breadth()``：快照存在且不落后直接读；缺失/落后则用本地日线
补齐并原子写回。增量更新只重算快照最后 2 个自然日之后的行，同时为 250 日
rolling 预留足够的交易日预热窗口。
"""

from __future__ import annotations

import bisect
import json
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.quality.code_migration import load_entity_index
from src.data.st_history import load_intervals
from src.risk.execution_rules import ExecutionRules

#: 快照口径版本号；口径变化时必须改版本，触发全量重建
BREADTH_VERSION = 'breadth_v1'

#: 250 日新高/新低窗口
ROLLING_WINDOW = 250

#: 增量更新时向前多取的自然日，用于覆盖迟到数据；rolling 预热另按交易日计算
OVERLAP_DAYS = 2

#: rolling 预热需要的交易日数量（250 日窗口 + 缓冲）
ROLLING_WARMUP_BARS = ROLLING_WINDOW + 10

_SNAPSHOT_SCHEMA = {
    'trade_date': pl.Date,
    'up_count': pl.Int64,
    'down_count': pl.Int64,
    'flat_count': pl.Int64,
    'total_count': pl.Int64,
    'limit_up_count': pl.Int64,
    'limit_down_count': pl.Int64,
    'new_high_250': pl.Int64,
    'new_low_250': pl.Int64,
    'adl': pl.Int64,
}

_QUOTE_COLUMNS = ['date', 'ts_code', 'close', 'pre_close', 'volume', 'qfq_factor']

_LOCK = threading.RLock()
_SNAPSHOT_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}
_ROOT_CACHE: dict[str, Path] = {}


# ----------------------------------------------------------------------
# 路径与配置
# ----------------------------------------------------------------------
def resolve_root(store_root: Path | str | None = None) -> Path:
    """解析本地列存根目录；未显式传入时读取项目配置。"""
    if store_root is not None:
        return Path(store_root)
    cached = _ROOT_CACHE.get('root')
    if cached is not None:
        return cached
    from src.data.local_store.store import resolve_store_root
    from src.utils.config import get_config

    root = resolve_store_root(get_config())
    _ROOT_CACHE['root'] = root
    return root


def breadth_dir(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'breadth'


def snapshot_path(store_root: Path | str) -> Path:
    return breadth_dir(store_root) / 'breadth_daily.parquet'


def coverage_path(store_root: Path | str) -> Path:
    return breadth_dir(store_root) / 'coverage.json'


def _quotes_glob(store_root: Path | str) -> str:
    """只扫个股行情：breadth 统计的是 A 股涨跌家数，指数/基金不参与。

    2026-11-07 复核结论（避免后续误加过滤）：94 只 ``close=1.0`` 占位桩
    全部是基金，不在本 glob 内；隔离台账里的 117 只虽是个股，但其坏行
    ``volume`` 为 null，已被 ``_daily_from_quotes`` 的 ``volume > 0`` 挡掉。
    因此这里无需再接 ``drop_quarantined``。若将来 glob 扩展到 fund/index，
    必须同步接入行级隔离与占位桩剔除。
    """
    return str(Path(store_root) / 'quotes' / 'stock' / '**' / '*.parquet')


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    write_parquet_atomic(frame, path)


# ----------------------------------------------------------------------
# 快照读取与本地最新交易日
# ----------------------------------------------------------------------
def _read_coverage(store_root: Path | str) -> dict[str, Any]:
    path = coverage_path(store_root)
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def _load_snapshot(store_root: Path | str) -> pl.DataFrame | None:
    """读取快照；版本/列不一致时视为缺失，触发重建。"""
    path = snapshot_path(store_root)
    if not path.is_file():
        return None
    coverage = _read_coverage(store_root)
    if coverage.get('version') != BREADTH_VERSION:
        return None
    mtime = path.stat().st_mtime_ns
    cached = _SNAPSHOT_CACHE.get(str(path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    frame = pl.read_parquet(path)
    missing = [name for name in _SNAPSHOT_SCHEMA if name not in frame.columns]
    if missing:
        logger.warning(f'[breadth] 快照缺列 {missing}，将重建')
        return None
    frame = frame.sort('trade_date')
    _SNAPSHOT_CACHE[str(path)] = (mtime, frame)
    return frame


def _local_max_date(store_root: Path | str) -> date | None:
    """本地行情最新交易日；优先读 daily_index（毫秒级），缺失时扫行情。"""
    index_path = Path(store_root) / 'meta' / 'daily_index.parquet'
    if index_path.is_file():
        try:
            index = pl.read_parquet(index_path, columns=['data_type', 'end'])
            stock = index.filter(pl.col('data_type') == 'stock')
            value = stock['end'].max()
            if value is not None:
                return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
            logger.warning(f'[breadth] 读取 daily_index 失败，回退扫描行情: {exc}')
    lf = pl.scan_parquet(_quotes_glob(store_root))
    value = lf.select(pl.col('date').max()).collect().item()
    return _as_date(value)


# ----------------------------------------------------------------------
# 计算
# ----------------------------------------------------------------------
def _st_pairs(store_root: Path | str, dates: list[date]) -> pl.DataFrame:
    """把 ST 区间展开成 (ts_code, date) 明细，用于逐行判定 is_st。"""
    intervals = load_intervals(store_root)
    empty = pl.DataFrame(schema={'ts_code': pl.Utf8, 'date': pl.Date})
    if intervals is None or intervals.height == 0 or not dates:
        return empty
    intervals = intervals.filter(
        pl.col('flag').is_in(['st', '*st'])
        & (pl.col('start_date') <= dates[-1])
        & (pl.col('end_date').is_null() | (pl.col('end_date') >= dates[0]))
    )
    if intervals.height == 0:
        return empty
    rows: list[tuple[str, date]] = []
    last = dates[-1]
    for item in intervals.to_dicts():
        start = item['start_date']
        end = item['end_date'] or last
        lo = bisect.bisect_left(dates, start)
        hi = bisect.bisect_right(dates, end)
        code = item['ts_code']
        rows.extend((code, day) for day in dates[lo:hi])
    if not rows:
        return empty
    return pl.DataFrame(rows, schema={'ts_code': pl.Utf8, 'date': pl.Date}, orient='row').unique()


def _limit_pct_map(codes: Iterable[str]) -> pl.DataFrame:
    """按 (ts_code, era, is_st) 复用 ExecutionRules.resolve_limit_pct 生成幅度映射。

    era=0 代表创业板注册制改革前，era=1 代表改革日及之后；其余板块两档相同。
    """
    rows: list[tuple[str, int, bool, int]] = []
    pre_date = ExecutionRules.CHINEXT_REG_DATE - timedelta(days=1)
    post_date = ExecutionRules.CHINEXT_REG_DATE
    for code in set(codes):
        for era, sample in ((0, pre_date), (1, post_date)):
            for is_st in (False, True):
                pct = ExecutionRules.resolve_limit_pct(code, is_st=is_st, trade_date=sample)
                rows.append((code, era, is_st, int(round(pct * 100))))
    return pl.DataFrame(
        rows, schema={'ts_code': pl.Utf8, 'era': pl.Int8, 'is_st': pl.Boolean,
                      'limit_pct': pl.Int64}, orient='row')


def _daily_from_quotes(
    hist: pl.DataFrame,
    store_root: Path | str,
    *,
    with_rolling: bool = True,
) -> pl.DataFrame:
    """由行情明细计算逐日 breadth 行（不含 ADL 累计）。"""
    if hist.height == 0:
        return pl.DataFrame(schema=_SNAPSHOT_SCHEMA)

    hist = hist.with_columns(pl.col('date').cast(pl.Date))
    # 同一实体的旧码已被新码完整覆盖时先剔除，避免涨跌家数重复计数
    superseded = sorted(load_entity_index(store_root).superseded)
    if superseded:
        hist = hist.filter(~pl.col('ts_code').is_in(superseded))
    dates = sorted(hist['date'].unique().to_list())

    st_pairs = _st_pairs(store_root, dates)
    if st_pairs.height:
        hist = hist.join(st_pairs.with_columns(pl.lit(True).alias('is_st')),
                         on=['ts_code', 'date'], how='left')
        hist = hist.with_columns(pl.col('is_st').fill_null(False))
    else:
        hist = hist.with_columns(pl.lit(False).alias('is_st'))

    first_dates = hist.group_by('ts_code').agg(pl.col('date').min().alias('first_date'))
    hist = hist.join(first_dates, on='ts_code', how='left').with_columns(
        (pl.col('date') == pl.col('first_date')).alias('is_first_day'))

    era = (pl.col('date') >= pl.lit(ExecutionRules.CHINEXT_REG_DATE)).cast(pl.Int8)
    hist = hist.with_columns(era.alias('era'))
    pct_map = _limit_pct_map(hist['ts_code'].unique().to_list())
    hist = hist.join(pct_map, on=['ts_code', 'era', 'is_st'], how='left')
    hist = hist.with_columns(pl.col('limit_pct').fill_null(10).cast(pl.Int64))

    # 价格口径：以「分」为单位的整数四舍五入（等价 Decimal ROUND_HALF_UP）
    pre_cents = (pl.col('pre_close') * 100).round(0).cast(pl.Int64)
    close_cents = (pl.col('close') * 100).round(0).cast(pl.Int64)
    up_cents = (pre_cents * (100 + pl.col('limit_pct')) + 50) // 100
    down_cents = (pre_cents * (100 - pl.col('limit_pct')) + 50) // 100

    valid = hist.filter(
        (pl.col('volume') > 0) & pl.col('pre_close').is_not_null() & pl.col('close').is_not_null()
    ).with_columns([
        pre_cents.alias('_pre_cents'),
        close_cents.alias('_close_cents'),
        up_cents.alias('_up_cents'),
        down_cents.alias('_down_cents'),
    ])

    # 新股首日 pre_close=0（如 001381.SZ 2026-11-03）：无有效昨收，
    # 涨跌归类无意义，按 pct_chg=0 的既有约定计入平盘，但总数仍计入。
    positive_pre = pl.col('pre_close') > 0
    daily = valid.group_by('date').agg([
        ((pl.col('close') > pl.col('pre_close')) & positive_pre).sum().alias('up_count'),
        ((pl.col('close') < pl.col('pre_close')) & positive_pre).sum().alias('down_count'),
        ((pl.col('close') == pl.col('pre_close')) | ~positive_pre).sum().alias('flat_count'),
        pl.len().alias('total_count'),
    ])

    limitable = valid.filter(~pl.col('is_first_day') & positive_pre)
    limit_daily = limitable.group_by('date').agg([
        (pl.col('_close_cents') >= pl.col('_up_cents')).sum().alias('limit_up_count'),
        (pl.col('_close_cents') <= pl.col('_down_cents')).sum().alias('limit_down_count'),
    ])
    daily = daily.join(limit_daily, on='date', how='left')

    if with_rolling:
        adj = (valid.select(['date', 'ts_code', 'close', 'qfq_factor'])
               .with_columns(pl.col('qfq_factor').fill_null(1.0))
               .with_columns((pl.col('close') * pl.col('qfq_factor')).alias('adj_close'))
               .sort(['ts_code', 'date']))
        adj = adj.with_columns([
            pl.col('adj_close').rolling_max(ROLLING_WINDOW, min_samples=ROLLING_WINDOW)
            .over('ts_code').alias('_hi'),
            pl.col('adj_close').rolling_min(ROLLING_WINDOW, min_samples=ROLLING_WINDOW)
            .over('ts_code').alias('_lo'),
        ])
        rolling = adj.group_by('date').agg([
            (pl.col('adj_close') >= pl.col('_hi')).sum().alias('new_high_250'),
            (pl.col('adj_close') <= pl.col('_lo')).sum().alias('new_low_250'),
        ])
        daily = daily.join(rolling, on='date', how='left')
    else:
        daily = daily.with_columns([
            pl.lit(0, dtype=pl.Int64).alias('new_high_250'),
            pl.lit(0, dtype=pl.Int64).alias('new_low_250'),
        ])

    daily = daily.rename({'date': 'trade_date'}).sort('trade_date')
    for name, dtype in _SNAPSHOT_SCHEMA.items():
        if name == 'adl':
            continue
        if name not in daily.columns:
            daily = daily.with_columns(pl.lit(0, dtype=dtype).alias(name))
        else:
            daily = daily.with_columns(pl.col(name).cast(dtype))
    return daily.select([name for name in _SNAPSHOT_SCHEMA if name != 'adl'])


def _with_adl(frame: pl.DataFrame) -> pl.DataFrame:
    """按 trade_date 全历史累计 ADL（Int64，避免 UInt32 下溢）。"""
    if frame.height == 0:
        return frame.with_columns(pl.lit(0, dtype=pl.Int64).alias('adl'))
    frame = frame.sort('trade_date').with_columns(
        (pl.col('up_count').cast(pl.Int64) - pl.col('down_count').cast(pl.Int64))
        .cum_sum().alias('adl'))
    return frame.select(list(_SNAPSHOT_SCHEMA))


def _scan_quotes(store_root: Path | str, start: date | None = None) -> pl.DataFrame:
    lf = pl.scan_parquet(_quotes_glob(store_root)).select(_QUOTE_COLUMNS)
    # 本地列存 date 为 ISO 字符串，统一转 Date 后过滤/比较
    lf = lf.with_columns(pl.col('date').cast(pl.Date))
    if start is not None:
        lf = lf.filter(pl.col('date') >= start)
    return lf.collect()


def build_full(store_root: Path | str | None = None) -> pl.DataFrame:
    """全历史重建 breadth 快照并原子写回。"""
    root = resolve_root(store_root)
    hist = _scan_quotes(root)
    frame = _with_adl(_daily_from_quotes(hist, root))
    _write_snapshot(root, frame)
    logger.info(f'[breadth] 全量构建完成: {frame.height} 个交易日 -> {snapshot_path(root)}')
    return frame


def _write_snapshot(root: Path, frame: pl.DataFrame) -> None:
    path = snapshot_path(root)
    _write_parquet_atomic(frame, path)
    first = _as_date(frame['trade_date'].min()) if frame.height else None
    last = _as_date(frame['trade_date'].max()) if frame.height else None
    coverage = {
        'version': BREADTH_VERSION,
        'last_trade_date': last.isoformat() if last else None,
        'first_trade_date': first.isoformat() if first else None,
        'rows': frame.height,
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'rolling_window': ROLLING_WINDOW,
        'note': '本地日线收盘口径；MySQL market_breadth 为盘中实时快照，不可作为收盘日频广度来源',
    }
    write_text_atomic(
        json.dumps(coverage, ensure_ascii=False, indent=2), coverage_path(root))
    _SNAPSHOT_CACHE[str(path)] = (path.stat().st_mtime_ns, frame)


def _lookback_start(existing: pl.DataFrame, cutoff: date) -> date:
    """取 cutoff 之前 ROLLING_WARMUP_BARS 个交易日作为增量扫描起点。"""
    dates = existing.filter(pl.col('trade_date') <= cutoff)['trade_date'].sort().to_list()
    if len(dates) <= ROLLING_WARMUP_BARS:
        start = _as_date(existing['trade_date'].min())
        return start if start is not None else cutoff
    start = _as_date(dates[-ROLLING_WARMUP_BARS])
    return start if start is not None else cutoff


def refresh(store_root: Path | str | None = None) -> pl.DataFrame:
    """增量补齐快照：只重算最后 2 个自然日之后的行，并保留 rolling 预热窗口。"""
    root = resolve_root(store_root)
    existing = _load_snapshot(root)
    local_max = _local_max_date(root)
    if local_max is None:
        if existing is not None:
            return existing
        return pl.DataFrame(schema=_SNAPSHOT_SCHEMA)
    if existing is None:
        return build_full(root)
    last = _as_date(existing['trade_date'].max())
    if last is None or local_max <= last:
        return existing

    cutoff = last - timedelta(days=OVERLAP_DAYS)
    scan_start = _lookback_start(existing, cutoff)
    hist = _scan_quotes(root, start=scan_start)
    fresh = _daily_from_quotes(hist, root, with_rolling=True)
    fresh = fresh.filter(pl.col('trade_date') > cutoff)

    kept = existing.filter(pl.col('trade_date') <= cutoff)
    combined = pl.concat([kept.select(fresh.columns), fresh], how='vertical_relaxed')
    combined = _with_adl(combined)
    _write_snapshot(root, combined)
    logger.info(f'[breadth] 增量更新完成: {kept.height} + {fresh.height} -> {combined.height} 行')
    return combined


def get_breadth(
    start_date: Any = None,
    end_date: Any = None,
    *,
    warmup: int = 0,
    store_root: Path | str | None = None,
) -> pl.DataFrame:
    """读取 breadth 快照（必要时增量补齐），按日期区间返回。

    Args:
        start_date/end_date: 需要的日期区间（含端点）；None 表示不限。
        warmup: 向前额外保留的交易日数，用于 ADR/OBOS 等滚动指标预热。
    """
    root = resolve_root(store_root)
    with _LOCK:
        frame = _load_snapshot(root)
        local_max = _local_max_date(root)
        stale = frame is None
        if frame is not None and local_max is not None:
            snapshot_last = _as_date(frame['trade_date'].max())
            stale = snapshot_last is None or snapshot_last < local_max
        if stale:
            frame = refresh(root)
    if frame is None or frame.height == 0:
        return pl.DataFrame(schema=_SNAPSHOT_SCHEMA)

    start = _as_date(start_date)
    end = _as_date(end_date)
    dates = frame['trade_date'].to_list()
    if start is not None:
        index = bisect.bisect_left(dates, start)
        index = max(0, index - max(0, int(warmup)))
        frame = frame.slice(index, frame.height - index)
    if end is not None:
        frame = frame.filter(pl.col('trade_date') <= end)
    return frame


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None
