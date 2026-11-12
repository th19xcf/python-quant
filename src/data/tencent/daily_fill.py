#!/usr/bin/env python3

"""腾讯日线补缺口（只写本地列存，不进入运行时读路径）。

设计要点：

- **原始价契约**：使用 ``newfqkline`` 的 ``day``（不复权），实测与 TDX
  ``.day`` 原始价逐行一致；严禁使用 ``fqkline`` 的 ``qfq`` 口径。
- **量/额口径**：volume=手、amount=万元→元；与本地列存契约一致，不做二次换算。
- **幂等**：只追加 ``date > 本地末根`` 的行，重复执行不会产生重复行。
- **可续跑**：失败标的不写入，下次执行按「本地末根 < 最新交易日」重新识别。
- **显式开关**：CLI 必须传 ``--allow-remote`` 才会联网；本模块不读取也不修改
  ``data.allow_remote_sources``。
- **元数据兜底**：写入后按磁盘实况重建 ``meta/daily_index.parquet``，避免
  ``build`` 的 ``flush_meta`` 只落当前批次条目导致其他标的失去覆盖度记录。
"""

from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from datetime import time as dtime
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic
from src.data.local_store.store import (
    _META_SCHEMA,
    STORE_VERSION,
    LocalQuoteStore,
    market_of,
)
from src.data.tdx_day_parser import parse_day_file
from src.data.tencent.client import TencentClient, TencentError
from src.utils.code_converter import to_tdx_file_stem, to_ts_code

#: 默认只补个股与指数；基金口径相同但历史上多为已退市标的，按需 --data-type all
DEFAULT_DATA_TYPES = ('stock', 'index')
#: 收盘后与盘中分界：盘中（<15:05）丢弃当天的未完成日 K
_CLOSE_CUTOFF = dtime(15, 5)
#: 价格对拍容差（指数接口按 0.01 取整，故用 0.011）
_PRICE_TOLERANCE = 0.011
_VOLUME_TOLERANCE = 1.0
_AMOUNT_REL_TOLERANCE = 1e-5
#: 写前闸门只挑历史足够长的标的（太短的标的与腾讯请求窗口可能无重叠）
_GUARD_MIN_ROWS = 120
#: 连续失败达到该值即中止本轮（避免在限流/封禁时继续施压）
_MAX_CONSECUTIVE_FAILURES = 20

_EVIDENCE_KLINE_COLUMNS = (
    'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'volume',
    'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor',
)


@dataclass(frozen=True)
class QuoteCoverage:
    """单标的本地覆盖度（由磁盘实况扫描得到，不依赖 meta）。"""

    ts_code: str
    data_type: str
    rows: int
    start: date
    end: date
    file_size: int


# ----------------------------------------------------------------------
# 盘点：磁盘实况 + 现有 meta
# ----------------------------------------------------------------------
def scan_quote_files(root: Path | str, workers: int = 8) -> dict[tuple[str, str], QuoteCoverage]:
    """扫描 ``quotes/{type}/{market}/*.parquet``，返回按 (类型, 代码) 索引的覆盖度。"""
    base = Path(root) / 'quotes'
    files = sorted(base.glob('*/*/*.parquet')) if base.is_dir() else []

    def read_one(path: Path) -> tuple[tuple[str, str], QuoteCoverage] | None:
        try:
            frame = pl.read_parquet(path, columns=['date'])
        except (OSError, pl.exceptions.PolarsError):
            logger.warning(f'[tencent-fill] 读取 {path} 失败，跳过')
            return None
        if frame.is_empty():
            return None
        data_type = path.parents[1].name
        ts_code = path.stem
        first = frame['date'].min()
        last = frame['date'].max()
        return (data_type, ts_code), QuoteCoverage(
            ts_code=ts_code, data_type=data_type, rows=frame.height,
            start=cast(date, first), end=cast(date, last),
            file_size=path.stat().st_size,
        )

    result: dict[tuple[str, str], QuoteCoverage] = {}
    if not files:
        return result
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for item in pool.map(read_one, files):
            if item is not None:
                result[item[0]] = item[1]
    return result


def read_meta_rows(store: LocalQuoteStore) -> dict[tuple[str, str], dict[str, Any]]:
    """读取现有覆盖度元数据（用于保留 full / source_mtime_ns / built_at）。"""
    path = store.meta_path
    if not path.exists():
        return {}
    try:
        frame = pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError):
        logger.warning(f'[tencent-fill] 读取 {path} 失败，将按磁盘实况重建')
        return {}
    return {(row['data_type'], row['ts_code']): row for row in frame.to_dicts()}


def _tdx_mtime(tdx_path: Path | None, ts_code: str) -> int | None:
    if tdx_path is None:
        return None
    stem = to_tdx_file_stem(ts_code)
    path = tdx_path / stem[:2] / 'lday' / f'{stem}.day'
    try:
        return path.stat().st_mtime_ns if path.exists() else None
    except OSError:
        return None


def refresh_meta(store: LocalQuoteStore,
                 coverages: dict[tuple[str, str], QuoteCoverage],
                 updated: dict[tuple[str, str], QuoteCoverage],
                 tdx_path: Path | None = None) -> int:
    """按磁盘实况重建覆盖度元数据（原子替换），返回条目数。

    保留已有条目的 ``full`` / ``source_mtime_ns`` / ``built_at``；新条目按
    「整文件构建」处理，并用 TDX ``.day`` 的 mtime 作为源端水位。
    """
    existing = read_meta_rows(store)
    now = datetime.now()
    rows: list[dict[str, Any]] = []
    for key, cov in coverages.items():
        old = existing.get(key, {})
        current = updated.get(key, cov)
        rows.append({
            'ts_code': current.ts_code,
            'data_type': current.data_type,
            'market': market_of(current.ts_code),
            'rows': current.rows,
            'start': current.start,
            'end': current.end,
            'built_at': now if key in updated else (old.get('built_at') or now),
            'file_size': current.file_size,
            'version': STORE_VERSION,
            'full': bool(old.get('full', True)),
            'source_mtime_ns': (
                old.get('source_mtime_ns')
                if old.get('source_mtime_ns') is not None
                else _tdx_mtime(tdx_path, current.ts_code)
            ),
        })
    if not rows:
        return 0
    frame = pl.DataFrame(rows, schema_overrides=_META_SCHEMA).sort(['data_type', 'ts_code'])
    write_parquet_atomic(frame, store.meta_path, compression='zstd',
                         statistics=True)
    return frame.height


# ----------------------------------------------------------------------
# 最新交易日
# ----------------------------------------------------------------------
def resolve_latest_trade_date(client: TencentClient, now: datetime | None = None,
                              benchmarks: Sequence[str] = (
                                  '600000.SH', '000001.SZ', '600519.SH',
                                  '601318.SH', '000858.SZ')) -> date:
    """用若干高流动性标的推断最新**已完成**交易日。

    盘中（15:05 前）腾讯会返回当天的未完成日 K，必须丢弃，否则会把
    半根 K 线写进日线列存。
    """
    now = now or datetime.now()
    candidates: list[date] = []
    for code in benchmarks:
        try:
            frame = client.fetch_daily(code, count=10)
        except (TencentError, OSError) as exc:
            logger.warning(f'[tencent-fill] 推断最新交易日时 {code} 失败: {exc}')
            continue
        if frame.is_empty():
            continue
        dates = frame['date'].to_list()
        if dates and dates[-1] == now.date() and now.time() < _CLOSE_CUTOFF:
            dates = dates[:-1]
        if dates:
            candidates.append(max(dates))
    if not candidates:
        raise TencentError('无法从腾讯日线推断最新交易日，请用 --latest-trade-date 显式指定')
    return max(candidates)


# ----------------------------------------------------------------------
# 行构造 / 合并
# ----------------------------------------------------------------------
def _estimated_count(last_local: date, latest: date) -> int:
    """按缺口长度估算请求行数（交易日约占自然日的 5/7，另加余量）。"""
    days = (latest - last_local).days
    estimate = int(days * 5 / 7) + 10
    return max(12, min(800, estimate))


def build_new_rows(new: pl.DataFrame, existing: pl.DataFrame,
                   ts_code: str) -> pl.DataFrame:
    """把腾讯原始行构造成与本地列存逐列一致的增量片段。

    ``pre_close`` 沿用构建器口径：``close.shift(1) * (qfq.shift(1) / qfq)``，
    用本地最后一根做种子，保证增量首行与全量构建结果一致。复权因子沿用
    本地末值（前向填充）；若缺口区间发生除权，需要另行刷新因子快照。
    """
    seed = existing.tail(1).select([
        'date', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'qfq_factor', 'hfq_factor',
    ])
    last_qfq = float(seed['qfq_factor'][0]) if 'qfq_factor' in seed.columns else 1.0
    last_hfq = float(seed['hfq_factor'][0]) if 'hfq_factor' in seed.columns else 1.0
    prepared = new.with_columns([
        pl.lit(ts_code).alias('ts_code'),
        pl.lit(last_qfq).alias('qfq_factor'),
        pl.lit(last_hfq).alias('hfq_factor'),
    ])
    combined = pl.concat([seed, prepared], how='diagonal_relaxed').sort('date')
    combined = combined.with_columns([
        (pl.col('close').shift(1)
         * (pl.col('qfq_factor').shift(1) / pl.col('qfq_factor'))).alias('pre_close'),
    ])
    combined = combined.with_columns([
        (pl.col('close') - pl.col('pre_close')).alias('change'),
        pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
        .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
        .otherwise(None).alias('pct_chg'),
    ])
    prepared = combined.slice(1)
    return prepared.select(list(_EVIDENCE_KLINE_COLUMNS))


def merge_incremental(existing: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    """按日期去重（保留新值）并升序，保证幂等。"""
    merged = pl.concat([existing, fresh], how='diagonal_relaxed')
    merged = merged.unique(subset=['date'], keep='last').sort('date')
    return merged.select([c for c in _EVIDENCE_KLINE_COLUMNS if c in merged.columns])


def _sanitize(frame: pl.DataFrame) -> pl.DataFrame:
    """剔除 OHLC 非法/成交量为负的行（正常数据不会触发，作为写前闸门）。"""
    if frame.is_empty():
        return frame
    valid = (
        pl.col('open').is_finite() & (pl.col('open') > 0)
        & pl.col('high').is_finite() & (pl.col('high') > 0)
        & pl.col('low').is_finite() & (pl.col('low') > 0)
        & pl.col('close').is_finite() & (pl.col('close') > 0)
        & pl.col('volume').is_finite() & (pl.col('volume') >= 0)
    )
    dropped = frame.filter(~valid).height
    if dropped:
        logger.warning(f'[tencent-fill] 剔除 {dropped} 行非法 OHLC/VOL')
    return frame.filter(valid)


# ----------------------------------------------------------------------
# 对拍（远程 vs TDX / 本地）
# ----------------------------------------------------------------------
def compare_with_tdx(remote: pl.DataFrame, tdx: pl.DataFrame) -> dict[str, Any]:
    """逐字段对拍：返回重叠行数与每字段最大绝对/相对偏差。"""
    if remote.is_empty() or tdx.is_empty():
        return {'overlap_rows': 0, 'fields': {}}
    fields = ('open', 'high', 'low', 'close', 'volume', 'amount')
    left = remote.select(['date', *[f for f in fields if f in remote.columns]])
    right = tdx.select(['date', *[f for f in fields if f in tdx.columns]])
    joined = left.join(right, on='date', how='inner', suffix='_tdx')
    if joined.is_empty():
        return {'overlap_rows': 0, 'fields': {}}
    result: dict[str, Any] = {'overlap_rows': joined.height, 'fields': {}}
    for name in fields:
        if f'{name}_tdx' not in joined.columns or name not in joined.columns:
            continue
        both = (
            pl.col(name).is_not_null() & pl.col(f'{name}_tdx').is_not_null()
            & pl.col(name).is_finite() & pl.col(f'{name}_tdx').is_finite()
        )
        diff = (pl.col(name) - pl.col(f'{name}_tdx')).abs()
        rel = diff / pl.max_horizontal(
            pl.col(name).abs(), pl.col(f'{name}_tdx').abs(), pl.lit(1e-12))
        subset = joined.filter(both)
        if subset.is_empty():
            continue
        max_abs = float(subset.select(diff.max()).item() or 0.0)
        max_rel = float(subset.select(rel.max()).item() or 0.0)
        if name == 'amount':
            over = rel > _AMOUNT_REL_TOLERANCE
        elif name == 'volume':
            over = diff > _VOLUME_TOLERANCE
        else:
            over = diff > _PRICE_TOLERANCE
        diff_rows = int(subset.filter(over).height)
        result['fields'][name] = {
            'max_abs': max_abs, 'max_rel': max_rel, 'diff_rows': diff_rows,
        }
    return result


def _tdx_frame(tdx_path: Path | None, ts_code: str) -> pl.DataFrame:
    if tdx_path is None:
        return pl.DataFrame()
    stem = to_tdx_file_stem(ts_code)
    path = tdx_path / stem[:2] / 'lday' / f'{stem}.day'
    if not path.exists():
        return pl.DataFrame()
    return parse_day_file(path)


def guard_sample(client: TencentClient, stale: Sequence[QuoteCoverage],
                 tdx_path: Path | None,
                 sample_size: int = 5) -> dict[str, Any]:
    """写前闸门：抽样对拍腾讯原始价与 TDX，一致性不达标则拒绝写入。

    抽样发生在**本地已存在区间**（缺口之外），用于确认「远程原始价」与
    「本地原始价」同口径；缺口内没有 TDX 数据，只能靠这个闸门做间接担保。

    抽样策略：优先挑历史足够长的标的（``rows >= _GUARD_MIN_ROWS``），并按
    ``data_type`` 轮转覆盖各类资产——长期停更的小额基金往往与腾讯无重叠行，
    若只用列表头部会把闸门判成「无样本」而误拒。
    """
    eligible = [cov for cov in stale if cov.rows >= _GUARD_MIN_ROWS]
    if not eligible:
        eligible = [cov for cov in stale if cov.rows > 0]
    buckets: dict[str, list[QuoteCoverage]] = {}
    for cov in eligible:
        buckets.setdefault(cov.data_type, []).append(cov)
    ordered: list[QuoteCoverage] = []
    depth = max((len(items) for items in buckets.values()), default=0)
    for index in range(depth):
        for kind in sorted(buckets):
            if index < len(buckets[kind]):
                ordered.append(buckets[kind][index])

    checks: list[dict[str, Any]] = []
    max_price = 0.0
    max_amount_rel = 0.0
    compared = 0
    compared_symbols = 0
    attempts = 0
    for cov in ordered:
        if compared_symbols >= max(1, sample_size):
            break
        attempts += 1
        if attempts > max(20, sample_size * 4):
            break
        tdx = _tdx_frame(tdx_path, cov.ts_code)
        if tdx.is_empty():
            continue
        try:
            remote = client.fetch_daily(cov.ts_code, count=200)
        except (TencentError, OSError) as exc:
            checks.append({'ts_code': cov.ts_code, 'error': str(exc)[:120]})
            continue
        # 只比较本地已存在区间（缺口外）
        overlap = compare_with_tdx(remote, tdx)
        if overlap['overlap_rows']:
            compared += overlap['overlap_rows']
            compared_symbols += 1
            price = max(
                (overlap['fields'].get(f, {}).get('max_abs', 0.0)
                 for f in ('open', 'high', 'low', 'close')), default=0.0)
            max_price = max(max_price, price)
            max_amount_rel = max(
                max_amount_rel,
                overlap['fields'].get('amount', {}).get('max_rel', 0.0))
        checks.append({'ts_code': cov.ts_code, 'data_type': cov.data_type,
                       'rows': cov.rows, **overlap})
    ok = compared_symbols > 0 and max_price <= _PRICE_TOLERANCE
    return {
        'ok': ok, 'sample_size': compared_symbols, 'attempts': attempts,
        'overlap_rows': compared,
        'max_price_abs': max_price, 'max_amount_rel': max_amount_rel,
        'checks': checks,
    }


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def _normalize_codes(codes: Iterable[str] | None) -> set[str] | None:
    if not codes:
        return None
    wanted: set[str] = set()
    for code in codes:
        try:
            wanted.add(to_ts_code(code))
        except (TypeError, ValueError):
            wanted.add(str(code).strip().upper())
    return wanted


@dataclass
class _FillJob:
    """单标的抓取作业的共享上下文（供线程池内的 :func:`_fetch_one` 只读使用）。"""

    quote_store: LocalQuoteStore
    client: TencentClient
    latest: date
    since: date | None
    meta_rows: dict[tuple[str, str], dict[str, Any]]
    stop: bool = False


def _resolve_tdx_path(config: Any) -> Path | None:
    """解析通达信数据根目录；未配置或不是目录时返回 None。"""
    raw = getattr(getattr(config, 'data', None), 'tdx_data_path', '') or ''
    path = Path(str(raw)) if raw else None
    return path if path is not None and path.is_dir() else None


def _filter_candidates(coverages: dict[tuple[str, str], QuoteCoverage],
                       data_types: Sequence[str],
                       wanted: set[str] | None) -> list[QuoteCoverage]:
    """按资产类型与标的白名单过滤本地覆盖记录。"""
    allowed = set(data_types)
    return [
        cov for cov in coverages.values()
        if cov.data_type in allowed
        and (wanted is None or cov.ts_code.upper() in wanted)
    ]


def _split_by_gap(behind: Sequence[QuoteCoverage], latest: date,
                  max_gap_days: int | None) -> tuple[list[QuoteCoverage], list[QuoteCoverage]]:
    """把落后标的拆成「待补」与「长期停更/退市跳过」两组，两组均按 (类型, 代码) 排序。"""
    if max_gap_days is not None and max_gap_days > 0:
        skipped = sorted(
            (cov for cov in behind if (latest - cov.end).days > max_gap_days),
            key=lambda cov: (cov.data_type, cov.ts_code))
        stale = sorted(
            (cov for cov in behind if (latest - cov.end).days <= max_gap_days),
            key=lambda cov: (cov.data_type, cov.ts_code))
        return stale, skipped
    return sorted(behind, key=lambda cov: (cov.data_type, cov.ts_code)), []


def _run_guard(client: TencentClient, stale: Sequence[QuoteCoverage],
               tdx_path: Path | None, sample_size: int) -> dict[str, Any]:
    """写前抽样对拍腾讯原始价与 TDX，返回闸门结果字典。"""
    logger.info('[tencent-fill] 写前抽样对拍（腾讯原始价 vs TDX）...')
    guard = guard_sample(client, stale, tdx_path, sample_size=sample_size)
    guard['skipped'] = False
    logger.info(
        f'[tencent-fill] 闸门: overlap={guard["overlap_rows"]} '
        f'max_price_abs={guard["max_price_abs"]:.4f} '
        f'max_amount_rel={guard["max_amount_rel"]:.2e}')
    return guard


def _fetch_one(cov: QuoteCoverage,
               job: _FillJob) -> tuple[str, int, QuoteCoverage | None]:
    """抓取单标的缺失区间并增量合并写回列存，返回 (状态, 新增行数, 新覆盖度)。"""
    if job.stop:
        return 'aborted', 0, None
    count = _estimated_count(cov.end, job.latest)
    remote = job.client.fetch_daily(cov.ts_code, count=count)
    if remote.is_empty():
        return 'no_new', 0, None
    remote = remote.filter(
        (pl.col('date') > cov.end) & (pl.col('date') <= job.latest))
    if job.since is not None:
        remote = remote.filter(pl.col('date') >= job.since)
    remote = _sanitize(remote)
    if remote.is_empty():
        return 'no_new', 0, None
    existing = job.quote_store.read_file(cov.data_type, cov.ts_code)
    if existing is None or existing.is_empty():
        return 'no_local', 0, None
    fresh = build_new_rows(remote, existing, cov.ts_code)
    merged = merge_incremental(existing, fresh)
    if merged.is_empty():
        return 'no_new', 0, None
    old_meta = job.meta_rows.get((cov.data_type, cov.ts_code), {})
    job.quote_store.write_daily(
        cov.data_type, cov.ts_code, merged,
        full=bool(old_meta.get('full', True)),
        source_mtime_ns=old_meta.get('source_mtime_ns'))
    new_cov = QuoteCoverage(
        ts_code=cov.ts_code, data_type=cov.data_type, rows=merged.height,
        start=cast(date, merged['date'].min()), end=cast(date, merged['date'].max()),
        file_size=job.quote_store.quote_path(cov.data_type, cov.ts_code).stat().st_size)
    return 'ok', fresh.height, new_cov


def _fetch_and_merge(job: _FillJob, stale: Sequence[QuoteCoverage], workers: int,
                     started: float) -> tuple[dict[tuple[str, str], QuoteCoverage],
                                             dict[str, dict[str, int]],
                                             list[str], list[str]]:
    """并发抓取全部待补标的并汇总，返回 (更新覆盖表, 分类统计, 失败码, 无新增码)。"""
    updated: dict[tuple[str, str], QuoteCoverage] = {}
    by_type: dict[str, dict[str, int]] = {}
    failed: list[str] = []
    no_new: list[str] = []
    processed = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_fetch_one, cov, job): cov for cov in stale}
        for future in as_completed(futures):
            processed += 1
            try:
                status, added, new_cov = future.result()
            except (TencentError, OSError, ValueError, pl.exceptions.PolarsError) as exc:
                failed.append(futures[future].ts_code)
                logger.warning(f'[tencent-fill] {futures[future].ts_code} 补数失败: {exc}')
                status, added, new_cov = 'failed', 0, None
            if status == 'ok' and new_cov is not None:
                updated[(new_cov.data_type, new_cov.ts_code)] = new_cov
                bucket = by_type.setdefault(new_cov.data_type, {'symbols': 0, 'rows': 0})
                bucket['symbols'] += 1
                bucket['rows'] += added
            elif status == 'no_new':
                no_new.append(futures[future].ts_code)
            if job.client.consecutive_failures() >= _MAX_CONSECUTIVE_FAILURES and not job.stop:
                job.stop = True
                logger.error(
                    f'[tencent-fill] 连续失败 {job.client.consecutive_failures()} 次，中止本轮')
            if processed % 200 == 0 or processed == len(stale):
                logger.info(
                    f'[tencent-fill] 进度 {processed}/{len(stale)}，'
                    f'已补 {sum(v["rows"] for v in by_type.values()):,} 行，'
                    f'耗时 {time.perf_counter() - started:.1f}s')
    return updated, by_type, failed, no_new


def fill_daily_gap(
    config: Any,
    quote_store: LocalQuoteStore,
    client: TencentClient,
    *,
    codes: Iterable[str] | None = None,
    data_types: Sequence[str] = DEFAULT_DATA_TYPES,
    since: date | None = None,
    latest_trade_date: date | None = None,
    workers: int = 6,
    dry_run: bool = False,
    max_symbols: int | None = None,
    max_gap_days: int | None = 90,
    guard_sample_size: int = 5,
    force: bool = False,
) -> dict[str, Any]:
    """把本地列存补齐到最新已完成交易日（幂等、可续跑）。

    Args:
        codes: 限定标的（支持 ``600000`` / ``600000.SH`` 两种写法）
        data_types: 参与补数的资产类型
        since: 只追加该日期之后的行（默认只追加本地末根之后）
        latest_trade_date: 显式指定目标交易日；缺省自动探测
        workers: 并发线程数
        dry_run: 只盘点不联网、不落盘
        max_symbols: 调试用，最多处理多少个标的
        max_gap_days: 末根距目标交易日超过该天数视为长期停更/退市，不回填历史
            （``None`` 表示不设闸门，仅用于显式历史回填场景）
        guard_sample_size: 写前抽样对拍数量（0 表示跳过）
        force: 抽样一致性不达标时仍强制写入（默认禁止）

    Returns:
        统计字典（标的数、行数、覆盖交易日、耗时、失败清单、闸门结果等）。
    """
    started = time.perf_counter()
    root = quote_store.root
    tdx_path = _resolve_tdx_path(config)

    logger.info('[tencent-fill] 扫描本地列存覆盖度 ...')
    coverages = scan_quote_files(root)
    logger.info(f'[tencent-fill] 本地标的 {len(coverages)} 个')

    wanted = _normalize_codes(codes)
    candidates = _filter_candidates(coverages, data_types, wanted)
    latest = latest_trade_date or resolve_latest_trade_date(client)
    behind = [cov for cov in candidates if cov.end < latest]
    stale, skipped_gap = _split_by_gap(behind, latest, max_gap_days)
    if max_symbols is not None:
        stale = stale[:max(0, int(max_symbols))]

    logger.info(
        f'[tencent-fill] 目标交易日 {latest}，落后标的 {len(behind)} 个'
        f'（跳过长期停更 {len(skipped_gap)} 个），待补 {len(stale)} 个'
        + ('（dry-run，不落盘）' if dry_run else ''))
    base_stats: dict[str, Any] = {
        'latest_trade_date': str(latest),
        'scanned_symbols': len(coverages),
        'candidate_symbols': len(candidates),
        'stale_symbols': len(stale),
        'skipped_gap_symbols': len(skipped_gap),
        'skipped_gap_codes': [cov.ts_code for cov in skipped_gap[:50]],
        'max_gap_days': max_gap_days,
        'dry_run': dry_run,
    }
    if dry_run or not stale:
        base_stats.update({
            'filled_symbols': 0, 'rows_added': 0, 'by_type': {},
            'failed_symbols': 0, 'elapsed_s': round(time.perf_counter() - started, 3),
            'meta_entries': 0,
        })
        return base_stats

    guard: dict[str, Any] = {'ok': True, 'skipped': True}
    if guard_sample_size > 0:
        guard = _run_guard(client, stale, tdx_path, guard_sample_size)
        if not guard['ok'] and not force:
            logger.error('[tencent-fill] 一致性不达标，按契约拒绝写入（可 --force 覆盖）')
            base_stats.update({
                'filled_symbols': 0, 'rows_added': 0, 'by_type': {},
                'failed_symbols': 0, 'guard': guard,
                'aborted': 'guard_failed',
                'elapsed_s': round(time.perf_counter() - started, 3),
                'meta_entries': 0,
            })
            return base_stats

    job = _FillJob(
        quote_store=quote_store, client=client, latest=latest,
        since=since, meta_rows=read_meta_rows(quote_store))
    updated, by_type, failed, no_new = _fetch_and_merge(job, stale, workers, started)

    refreshed = dict(coverages)
    refreshed.update(updated)
    meta_entries = refresh_meta(quote_store, refreshed, updated, tdx_path)

    rows_added = sum(bucket['rows'] for bucket in by_type.values())
    filled = sum(bucket['symbols'] for bucket in by_type.values())
    stats = {
        **base_stats,
        'filled_symbols': filled,
        'rows_added': rows_added,
        'by_type': by_type,
        'failed_symbols': len(failed),
        'failed_codes': failed[:50],
        'no_new_symbols': len(no_new),
        'guard': guard,
        'meta_entries': meta_entries,
        'elapsed_s': round(time.perf_counter() - started, 3),
    }
    logger.info(
        f'[tencent-fill] 完成：补 {filled} 只 / {rows_added:,} 行 / 覆盖至 '
        f'{max((c.end for c in updated.values()), default=latest)}，'
        f'耗时 {stats["elapsed_s"]}s，失败 {len(failed)}，无新增 {len(no_new)}')
    return stats


# ----------------------------------------------------------------------
# 抽样对拍（验收用）
# ----------------------------------------------------------------------
def sample_stock_codes(coverages: dict[tuple[str, str], QuoteCoverage],
                       count: int = 20, seed: int = 42,
                       core_codes: Sequence[str] = (
                           '600000.SH', '600519.SH', '000001.SZ', '000858.SZ',
                           '601318.SH', '600036.SH', '300750.SZ', '002594.SZ')) -> list[str]:
    """确定性抽样：核心大盘股 + 固定种子随机补充（仅沪深个股）。"""
    candidates = sorted(
        cov.ts_code for (kind, _code), cov in coverages.items()
        if kind == 'stock' and cov.ts_code.endswith(('.SH', '.SZ')))
    selected = [code for code in core_codes if code in candidates][:count]
    remaining = [code for code in candidates if code not in selected]
    if len(selected) < count and remaining:
        rng = random.Random(seed)  # noqa: S311 - 抽样可复现性要求，非加密用途
        selected.extend(rng.sample(remaining, min(count - len(selected), len(remaining))))
    return sorted(selected)


def verify_daily(config: Any, quote_store: LocalQuoteStore, client: TencentClient,
                 *, codes: Sequence[str] | None = None, sample: int = 20,
                 start: date | None = None, end: date | None = None,
                 count: int = 800) -> dict[str, Any]:
    """抽样对拍腾讯原始价与 TDX ``.day``（只读，不写任何数据）。"""
    coverages = scan_quote_files(quote_store.root)
    if codes:
        target_codes = []
        for code in codes:
            try:
                target_codes.append(to_ts_code(code))
            except (TypeError, ValueError):
                target_codes.append(str(code).strip().upper())
    else:
        target_codes = sample_stock_codes(coverages, count=sample)
    tdx_raw = getattr(getattr(config, 'data', None), 'tdx_data_path', '') or ''
    tdx_path = Path(str(tdx_raw)) if tdx_raw else None
    started = time.perf_counter()
    results: list[dict[str, Any]] = []
    totals = {name: {'max_abs': 0.0, 'max_rel': 0.0, 'diff_rows': 0}
              for name in ('open', 'high', 'low', 'close', 'volume', 'amount')}
    total_overlap = 0
    for ts_code in target_codes:
        tdx = _tdx_frame(tdx_path, ts_code)
        if tdx.is_empty():
            results.append({'ts_code': ts_code, 'status': 'tdx_missing'})
            continue
        if start is not None:
            tdx = tdx.filter(pl.col('date') >= start)
        if end is not None:
            tdx = tdx.filter(pl.col('date') <= end)
        try:
            remote = client.fetch_daily(ts_code, count=count)
        except (TencentError, OSError) as exc:
            results.append({'ts_code': ts_code, 'status': 'source_error',
                            'error': str(exc)[:160]})
            continue
        comparison = compare_with_tdx(remote, tdx)
        if comparison['overlap_rows'] == 0:
            results.append({'ts_code': ts_code, 'status': 'no_overlap'})
            continue
        total_overlap += comparison['overlap_rows']
        for name, stat in comparison['fields'].items():
            totals[name]['max_abs'] = max(totals[name]['max_abs'], stat['max_abs'])
            totals[name]['max_rel'] = max(totals[name]['max_rel'], stat['max_rel'])
            totals[name]['diff_rows'] += stat['diff_rows']
        worst_price = max(
            (comparison['fields'].get(f, {}).get('max_abs', 0.0)
             for f in ('open', 'high', 'low', 'close')), default=0.0)
        results.append({
            'ts_code': ts_code, 'status': 'ok' if worst_price <= _PRICE_TOLERANCE
            else 'price_mismatch',
            'overlap_rows': comparison['overlap_rows'],
            'max_price_abs': worst_price,
            'max_amount_rel': comparison['fields'].get('amount', {}).get('max_rel', 0.0),
        })
    return {
        'codes': len(target_codes),
        'overlap_rows': total_overlap,
        'fields': totals,
        'results': results,
        'elapsed_s': round(time.perf_counter() - started, 3),
    }
