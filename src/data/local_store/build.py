#!/usr/bin/env python3

"""本地列存构建器与 CLI。

用法（项目根目录执行）::

    # 全市场首建（个股 + 指数 + 基金），只读 .day，不写库
    .venv\\Scripts\\python.exe -m src.data.local_store build --all

    # 预演，只统计不落盘
    .venv\\Scripts\\python.exe -m src.data.local_store build --all --dry-run

    # 增量：指定标的 / 指定日期之后
    .venv\\Scripts\\python.exe -m src.data.local_store build --codes 600000.SH,000001.SZ
    .venv\\Scripts\\python.exe -m src.data.local_store build --since 2026-10-01

    # 危险操作：用远程库整表覆盖本地因子与分红快照（默认禁止，需二次确认）
    .venv\\Scripts\\python.exe -m src.data.local_store refresh-factors --allow-remote \
        --force-overwrite --confirm OVERWRITE-LOCAL-SNAPSHOT

    # 用本地行情+分红补齐全市场因子快照（不写远程库，断点续跑）
    .venv\\Scripts\\python.exe -m src.data.local_store rebuild-factors

    # 查看列存规模
    .venv\\Scripts\\python.exe -m src.data.local_store stats

构建口径与 ``TdxHandler`` 完全一致：同一份 ``parse_day_file`` 原始记录 →
同一套复权因子来源（本地因子快照优先；缺票时按本地分红现场计算；远程预计算
因子仅作最后兜底，且全为 1.0 时跳过）→ 同一个
``DataSourceBase._normalize_kline`` 出口，保证数值逐行等价。
"""

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, cast

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.local_store.session_guard import filter_intraday_rows, guarded_source_date
from src.data.local_store.store import (
    STORE_VERSION,
    LocalFactorStore,
    LocalQuoteStore,
    refresh_quote_factor_columns,
    resolve_store_root,
    verify_quote_factor_consistency,
)
from src.data.sources.base import DataSourceBase
from src.data.tdx_day_parser import (
    parse_day_file,
    read_first_last_trade_date,
    read_recent_records,
)
from src.utils.code_converter import is_fund_code, is_index_code

_KLINE_COLUMN_MAP = {
    'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
    'close': 'close', 'pre_close': 'pre_close', 'volume': 'volume',
    'amount': 'amount', 'pct_chg': 'pct_chg', 'change': 'change',
    'qfq_factor': 'qfq_factor', 'hfq_factor': 'hfq_factor',
}
_DIVIDEND_COLUMNS = [
    'ts_code', 'symbol', 'name', 'dividend_year', 'report_date', 'record_date',
    'ex_date', 'pay_date', 'cash_div', 'share_div', 'total_div',
    'rights_issue_price', 'rights_issue_ratio',
]


# ----------------------------------------------------------------------
# 文件发现
# ----------------------------------------------------------------------
def _valid_stock_code(code: str, market: str) -> bool:
    """复用 TdxHandler 的股票/基金代码口径，避免两套判定分叉。"""
    from src.data.tdx_handler import _is_valid_tdx_stock_code  # noqa: PLC0415

    return _is_valid_tdx_stock_code(code, market)


def discover_local_files(tdx_path: Path, data_type: str = 'all') -> list[tuple[str, str, Path]]:
    """扫描通达信 lday 目录，返回 (data_type, ts_code, 文件路径)。

    ``data_type`` 取 ``stock`` / ``index`` / ``fund``：基金（ETF/LOF/封闭式/
    REITs）与个股共用 lday 目录和 32 字节布局，但价格缩放为 ÷1000，必须
    单独成类，不能混入 ``stock`` 目录（否则回测会拿到 10 倍价格）。
    """
    found: list[tuple[str, str, Path]] = []
    for market in ('sh', 'sz', 'bj'):
        lday = tdx_path / market / 'lday'
        if not lday.is_dir():
            continue
        suffix = market.upper()
        for path in lday.glob(f'{market}*.day'):
            code = path.stem[2:]
            if not (code.isdigit() and len(code) == 6):
                continue
            ts_code = f'{code}.{suffix}'
            if is_index_code(ts_code):
                kind = 'index'
            elif is_fund_code(code, market):
                kind = 'fund'
            elif _valid_stock_code(code, market):
                kind = 'stock'
            else:
                continue
            if data_type != 'all' and data_type != kind:
                continue
            found.append((kind, ts_code, path))
    return found


# ----------------------------------------------------------------------
# 单标的构建
# ----------------------------------------------------------------------
def _factor_arrays(
    factor_store: LocalFactorStore,
    ts_code: str,
    dates: list[date],
    closes: list[float],
) -> tuple[pl.Series, pl.Series] | tuple[None, None]:
    """按与 ``TdxHandler._get_adj_factors`` 相同的优先级产出因子数组。

    1. 本地快照有预计算因子 → 按日期左连接 + 前向填充 + 兜底 1.0；
    2. 否则用本地分红快照按 ``AdjFactorCalculator`` 本地计算；
    3. 二者都没有 → 返回 None（调用方填 1.0）。
    """
    snapshot = factor_store.factors(ts_code)
    if snapshot is not None and not snapshot.is_empty():
        joined = pl.DataFrame({'trade_date': dates}).join(
            snapshot, on='trade_date', how='left')
        qfq = joined['qfq_factor'].fill_null(strategy='forward').fill_null(1.0)
        hfq = joined['hfq_factor'].fill_null(strategy='forward').fill_null(1.0)
        return qfq, hfq

    dividends = factor_store.dividends(ts_code)
    if dividends is None or dividends.is_empty():
        return None, None

    div = dividends.filter(pl.col('ex_date').is_not_null())
    if div.is_empty():
        return None, None
    div = div.with_columns([
        pl.col('cash_div').cast(pl.Float64).fill_null(0.0),
        pl.col('share_div').cast(pl.Float64).fill_null(0.0),
        pl.col('rights_issue_ratio').cast(pl.Float64).fill_null(0.0),
        pl.col('rights_issue_price').cast(pl.Float64).fill_null(0.0),
    ])

    prices = pl.DataFrame({
        'ts_code': [ts_code] * len(dates),
        'trade_date': dates,
        'close': closes,
    })
    result = AdjFactorCalculator.calculate_factors(prices, div)
    return result['qfq_factor'], result['hfq_factor']


def build_frame(
    data_type: str,
    ts_code: str,
    day_path: Path,
    factor_store: LocalFactorStore,
) -> pl.DataFrame:
    """把一个 ``.day`` 文件构造成与现有 TDX 出口逐值一致的 DataFrame。"""
    raw = parse_day_file(day_path)
    if raw.is_empty():
        return raw
    raw = raw.sort('date').with_columns(pl.col('date').cast(pl.Date))

    qfq: pl.Series | None = None
    hfq: pl.Series | None = None
    if data_type == 'stock':
        qfq, hfq = _factor_arrays(
            factor_store, ts_code, raw['date'].to_list(), raw['close'].to_list())
    # data_type == 'fund'：本地因子快照与分红快照当前均无基金记录
    # （stock_adj_factor / stock_dividend 里基金行为 0），因此基金不查因子，
    # 统一按因子 1.0（原始价）写入；请求复权时由 DataFetcher 显式告警，
    # 不做「伪复权」也不把因子伪造成非 1 值。

    frame = raw.with_columns(pl.lit(ts_code).alias('ts_code'))
    if qfq is not None and hfq is not None:
        frame = frame.with_columns([
            qfq.alias('qfq_factor'),
            hfq.alias('hfq_factor'),
        ])
        pre_close = pl.col('close').shift(1) * (
            pl.col('qfq_factor').shift(1) / pl.col('qfq_factor'))
    else:
        pre_close = pl.col('close').shift(1)

    frame = frame.with_columns(pre_close.alias('pre_close'))
    frame = frame.with_columns([
        (pl.col('close') - pl.col('pre_close')).alias('change'),
        pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
        .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
        .otherwise(None).alias('pct_chg'),
    ])
    return DataSourceBase._normalize_kline(
        frame,
        column_map=_KLINE_COLUMN_MAP,
        ts_code=ts_code,
        volume_unit='hand',
        date_format=None,
        include_factors=True,
    )


def _merge_incremental(existing: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    """增量合并：按日期去重（保留最新构建值）后升序。"""
    combined = pl.concat([existing, fresh], how='diagonal_relaxed')
    return combined.unique(subset=['date'], keep='last').sort('date')


# ----------------------------------------------------------------------
# 构建入口
# ----------------------------------------------------------------------
def _last_row_matches(local_last: dict[str, Any], source_row: dict[str, Any]) -> bool:
    """比较本地末行与源末记录是否逐字段一致（OHLC/成交量/成交额）。

    价格与成交额源端为 float32 宽化，本地按 float64 原样落地，因此用
    极小的相对容差即可；成交量两端都是 float64，同源重解析应完全一致。
    """
    if local_last['date'] != source_row['date']:
        return False
    for field in ('open', 'high', 'low', 'close', 'volume', 'amount'):
        a = float(local_last[field])
        b = float(source_row[field])
        if abs(a - b) > max(1e-6, abs(b) * 1e-9):
            return False
    return True


def _source_record_at(path: Path, day: date) -> dict[str, Any] | None:
    """读取源文件中日期为 ``day`` 的末条记录（只读末尾少量记录）。"""
    try:
        records = read_recent_records(path, 5)
    except (OSError, ValueError):
        return None
    for record in reversed(records):
        if record.get('date') == day:
            return record
    return None


def _target_is_stale(
    quote_store: LocalQuoteStore, kind: str, ts_code: str, path: Path, *,
    max_date: date | None = None,
) -> bool:
    """目标是否需要重建：本地缺失、源日期更新、或同日但末行内容已变化。"""
    cov = quote_store.coverage(kind, ts_code)
    if cov is None or not cov.full:
        return True
    _, source_last = read_first_last_trade_date(path)
    effective = guarded_source_date(source_last)
    if effective is None:
        return False
    if max_date is not None and effective > max_date:
        effective = max_date
    if cov.end < effective:
        return True
    if cov.end > effective:
        # 源文件比本地还旧（停更/回退），重建只会截断本地历史，保留现有数据。
        return False
    # 日期相同：用末行内容比对，避免源端修订后静默停在旧值。
    source_row = _source_record_at(path, effective)
    if source_row is None:
        return False
    local = quote_store.read_file(kind, ts_code)
    if local is None or local.is_empty():
        return True
    local_last = local.sort('date').tail(1).to_dicts()[0]
    return not _last_row_matches(local_last, source_row)


def _incremental_plan(
    targets: list[tuple[str, str, Path]], quote_store: LocalQuoteStore,
    max_date: date | None = None,
) -> tuple[int, int]:
    """统计落后标的数量与需重读的源行数（用于 dry-run）。"""
    stale_count = 0
    planned_rows = 0
    for kind, ts_code, path in targets:
        if _target_is_stale(quote_store, kind, ts_code, path, max_date=max_date):
            stale_count += 1
            planned_rows += path.stat().st_size // 32
    return stale_count, planned_rows


def _merge_source_with_existing(
    quote_store: LocalQuoteStore, kind: str, ts_code: str,
    fresh: pl.DataFrame, since: date | None,
) -> tuple[pl.DataFrame, bool]:
    """把源端新帧与存量合并，返回（待写入帧, 是否完整覆盖）。

    源端 ``.day`` 并不保证是存量的超集：代码复用换段、停更后回退、同步
    进程盘中重写历史，都会让源端区间比本地短。因此重建一律按
    ``(date)`` 做 upsert —— 保留存量所有行，同日以源端为准 —— 而不是
    用源端整只覆盖本地。源端缺失的历史行只可能来自此前的缺口回补，
    整只覆盖会把它静默删掉。

    ``since`` 只影响从源端取哪一段（无存量时写入的是尾段，标记非完整）；
    有存量时始终合并。
    """
    existing = quote_store.read_file(kind, ts_code)
    if existing is not None and not existing.is_empty():
        tail = fresh.filter(pl.col('date') >= since) if since is not None else fresh
        return _merge_incremental(existing, tail), True
    if since is not None:
        return fresh.filter(pl.col('date') >= since), False
    return fresh, True


def _rebuild_target(
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    kind: str,
    ts_code: str,
    path: Path,
    *,
    since: date | None,
    max_date: date | None,
    force_intraday: bool,
) -> int | None:
    """重建单标的并返回写入行数；源记录为空时返回 None。"""
    fresh = build_frame(kind, ts_code, path, factor_store)
    fresh = filter_intraday_rows(fresh, force=force_intraday)
    if max_date is not None:
        fresh = fresh.filter(pl.col('date') <= max_date)
    if fresh.is_empty():
        return None
    fresh, full = _merge_source_with_existing(
        quote_store, kind, ts_code, fresh, since)
    if fresh.is_empty():
        return None
    return quote_store.write_daily(
        kind, ts_code, fresh, full=full,
        source_mtime_ns=path.stat().st_mtime_ns)


def _align_written_factor_copies(
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    written_by_kind: dict[str, list[str]],
) -> dict[str, Any]:
    """把本次写入标的的行情内嵌因子副本对齐到权威快照。

    增量补数追加的新行可能带着默认因子 1.0（尤其基金：源 ``.day`` 没有
    因子列），而权威快照只到前一交易日。若不在同一次构建内对齐，行情副本
    就会与快照漂移，直到有人手工跑 ``sync-quote-factors`` /
    ``factor-governance``。只处理本次写入的标的，避免每次构建全市场重扫。
    """
    aligned: dict[str, Any] = {}
    for kind, written_codes in written_by_kind.items():
        try:
            aligned[kind] = refresh_quote_factor_columns(
                quote_store, factor_store, codes=written_codes, kind=kind)
        except (OSError, ValueError, RuntimeError, PolarsError) as exc:
            logger.warning(
                f'[local-store] {kind} 因子副本对齐失败'
                f'（{len(written_codes)} 只）：{exc}'
            )
    if aligned:
        logger.info(f'[local-store] 构建收尾因子副本对齐：{aligned}')
    return aligned


def build(
    config: Any,
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    *,
    data_type: str = 'all',
    codes: Iterable[str] | None = None,
    since: date | None = None,
    incremental: bool = False,
    target_date: date | None = None,
    max_date: date | None = None,
    force_intraday: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """执行构建（全量或增量），返回统计信息（供 CLI 与测试使用）。

    ``incremental=True`` 时先用源 ``.day`` 末行日期与本地 ``end`` 比对，
    只重建真正落后的标的；已到源末行的标的直接跳过（不再全量重跑）。
    """
    tdx_path_raw = getattr(getattr(config, 'data', None), 'tdx_data_path', '') or ''
    tdx_path = Path(str(tdx_path_raw))
    if not tdx_path.is_dir():
        raise FileNotFoundError(f'通达信数据路径不存在: {tdx_path}')

    targets = discover_local_files(tdx_path, data_type)
    if codes:
        wanted = {c.strip().upper() for c in codes if c.strip()}
        targets = [t for t in targets if t[1].upper() in wanted]
    if not targets:
        logger.warning('未发现可构建的通达信文件')
        return {'symbols': 0, 'rows': 0, 'elapsed_s': 0.0, 'files': 0}

    started = time.perf_counter()
    written_rows = 0
    written_files = 0
    #: 本次实际写入的标的，用于收尾做因子副本对齐（不重扫全市场）
    written_by_kind: dict[str, list[str]] = {}
    skipped = 0
    skipped_uptodate = 0
    total = len(targets)

    if dry_run:
        planned_rows = sum(t[2].stat().st_size // 32 for t in targets)
        if incremental:
            stale_count, planned_rows = _incremental_plan(
                targets, quote_store, max_date=max_date)
            total = stale_count
            logger.info(
                f'[dry-run] 增量模式：{len(targets)} 个标的中 {total} 个落后，'
                f'预计重读约 {planned_rows:,} 行，目标目录 {quote_store.root}')
        else:
            logger.info(
                f'[dry-run] 计划构建 {total} 个文件，约 {planned_rows:,} 行，'
                f'目标目录 {quote_store.root}')
        return {
            'symbols': total,
            'rows': planned_rows,
            'elapsed_s': round(time.perf_counter() - started, 3),
            'files': 0,
            'dry_run': True,
        }

    for index, (kind, ts_code, path) in enumerate(targets, start=1):
        try:
            if incremental and not _target_is_stale(
                    quote_store, kind, ts_code, path, max_date=max_date):
                skipped_uptodate += 1
                if index % 500 == 0 or index == total:
                    elapsed = time.perf_counter() - started
                    logger.info(
                        f'[local-store] 增量进度 {index}/{total}，落后重跑 '
                        f'{written_files} 个 / 跳过已最新 {skipped_uptodate} 个，'
                        f'已用 {elapsed:.1f}s')
                continue
            written = _rebuild_target(
                quote_store, factor_store, kind, ts_code, path,
                since=since, max_date=max_date, force_intraday=force_intraday)
            if written is None:
                skipped += 1
                continue
            written_rows += written
            written_files += 1
            written_by_kind.setdefault(kind, []).append(ts_code)
        except (OSError, ValueError, RuntimeError) as exc:
            skipped += 1
            logger.warning(f'[local-store] 构建 {ts_code} 失败: {exc}')

        if index % 500 == 0 or index == total:
            elapsed = time.perf_counter() - started
            logger.info(
                f'[local-store] 进度 {index}/{total}，已写 {written_files} 文件 / '
                f'{written_rows:,} 行，已用 {elapsed:.1f}s')

    quote_store.flush_meta()

    # 增量补数追加的新行可能带着默认因子 1.0（尤其基金：源 .day 没有因子列），
    # 而权威快照只到前一交易日。若不在同一次构建内对齐，行情内嵌副本就会
    # 与快照漂移，直到有人手工跑 sync-quote-factors / factor-governance。
    # 这里只处理本次写入的标的，避免每次构建全市场重扫。
    factor_alignment = _align_written_factor_copies(
        quote_store, factor_store, written_by_kind)

    elapsed = time.perf_counter() - started
    stats = quote_store.stats()
    quote_store.write_state({
        'last_build': 'incremental' if (codes or since or incremental) else 'full',
        'data_type': data_type,
        'symbols': stats['symbols'],
        'rows': stats['rows'],
        'files': stats['files'],
        'disk_bytes': stats['disk_bytes'],
        'elapsed_s': round(elapsed, 3),
        'skipped': skipped,
        'skipped_uptodate': skipped_uptodate,
        'target_date': target_date.isoformat() if target_date else None,
    })
    logger.info(
        f'[local-store] 构建完成：{written_files} 文件 / {written_rows:,} 行，'
        f'跳过已最新 {skipped_uptodate} 个，耗时 {elapsed:.1f}s，'
        f'列存共 {stats["rows"]:,} 行 / {stats["files"]} 文件 / '
        f'{stats["disk_bytes"] / 1024 / 1024:.1f} MB')
    if target_date is not None:
        verify_incremental_target(
            config, quote_store, target_date=target_date, data_type=data_type)
    return {
        'symbols': written_files,
        'rows': written_rows,
        'elapsed_s': round(elapsed, 3),
        'files': written_files,
        'skipped': skipped,
        'skipped_uptodate': skipped_uptodate,
        'factor_alignment': factor_alignment,
        'stats': stats,
    }


def _recent_dates(path: Path, count: int = 15) -> set[date]:
    """读取源文件末尾若干条记录的日期集合（用于判断源端是否含目标日）。"""
    try:
        records = read_recent_records(path, count)
    except (OSError, ValueError):
        return set()
    return {r['date'] for r in records if r.get('date') is not None}


def _classify_target(
    quote_store: LocalQuoteStore, kind: str, ts_code: str, path: Path,
    target_date: date,
) -> tuple[str, dict[str, Any]]:
    """按「源是否含目标日 + 本地末行内容」给单个标的分类。"""
    if not path.exists():
        return 'source_missing', {}
    _, source_last = read_first_last_trade_date(path)
    if source_last is None:
        return 'unreadable', {}
    cov = quote_store.coverage(kind, ts_code)
    if cov is None:
        return 'not_built', {}
    info: dict[str, Any] = {
        'ts_code': ts_code, 'data_type': kind,
        'local_end': cov.end.isoformat(),
        'source_last': source_last.isoformat()}
    if cov.end >= target_date:
        if source_last < target_date:
            # 本地已有目标日但源文件没有：源端停更/回退，保留本地数据。
            return 'local_ahead_of_source', info
        if cov.end == target_date:
            source_row = _source_record_at(path, target_date)
            local = quote_store.read_file(kind, ts_code)
            if (source_row is not None and local is not None
                    and not local.is_empty()):
                local_last = local.sort('date').tail(1).to_dicts()[0]
                if not _last_row_matches(local_last, source_row):
                    return 'content_mismatch', info
        return 'at_target', info
    # 源末行早于目标日，或源最近记录里根本没有目标日（如同步只写了
    # 更晚的日期、中间断档），都属于源端自身缺数据，不是本地漏建。
    recent = _recent_dates(path)
    if source_last < target_date or target_date not in recent:
        if source_last < target_date:
            info['reason'] = 'source_lagging'
        else:
            info['reason'] = 'source_missing_target'
        info['gap_days'] = (target_date - cov.end).days
        return 'source_stale', info
    return 'local_behind', info


def verify_incremental_target(
    config: Any,
    quote_store: LocalQuoteStore,
    *,
    target_date: date,
    data_type: str = 'all',
) -> dict[str, Any]:
    """显式校验「本地末行日期 vs 目标日期」，并对异常分类告警。

    分类口径：
    - ``at_target``：本地已到目标日；
    - ``source_stale``：源文件本身缺目标日（末行早于目标日或中间断档），
      属于停牌/退市/源未更新，不是本地构建遗漏；
    - ``local_behind``：源文件最近记录中确实含目标日、但本地落后
      （构建遗漏，需告警）；
    - ``not_built``：有源文件但列存无记录；
    - ``source_missing``：扫描到的标的中源文件不存在（理论不会出现）。
    """
    tdx_path_raw = getattr(getattr(config, 'data', None), 'tdx_data_path', '') or ''
    tdx_path = Path(str(tdx_path_raw))
    targets = discover_local_files(tdx_path, data_type)
    counters = {
        'at_target': 0, 'source_stale': 0, 'local_behind': 0,
        'not_built': 0, 'source_missing': 0, 'unreadable': 0,
        'local_ahead_of_source': 0, 'content_mismatch': 0,
    }
    behind: list[str] = []
    stale_samples: list[dict[str, Any]] = []
    ahead_samples: list[dict[str, Any]] = []
    stale_content: list[str] = []
    for kind, ts_code, path in targets:
        status, info = _classify_target(
            quote_store, kind, ts_code, path, target_date)
        counters[status] += 1
        if status == 'source_stale' and len(stale_samples) < 20:
            stale_samples.append(info)
        elif status == 'local_ahead_of_source' and len(ahead_samples) < 20:
            ahead_samples.append(info)
        elif status == 'content_mismatch' and len(stale_content) < 20:
            stale_content.append(f'{ts_code}({kind})')
        elif status == 'local_behind' and len(behind) < 20:
            behind.append(f'{ts_code}(本地 {info["local_end"]} / 源 {info["source_last"]})')

    if counters['local_behind'] or counters['not_built'] or counters['content_mismatch']:
        logger.warning(
            f'[local-store] 目标日 {target_date} 校验异常：本地落后 '
            f'{counters["local_behind"]} 个，未构建 {counters["not_built"]} 个，'
            f'同日末行内容不一致 {counters["content_mismatch"]} 个，'
            f'样例 {behind[:10] or stale_content[:10]}')
    logger.info(f'[local-store] 目标日 {target_date} 校验结果: {counters}')
    return {
        **counters,
        'target_date': target_date.isoformat(),
        'local_behind_samples': behind,
        'source_stale_samples': stale_samples,
        'local_ahead_samples': ahead_samples,
        'content_mismatch_samples': stale_content,
    }



_OVERWRITE_CONFIRM_TOKEN = 'OVERWRITE-LOCAL-SNAPSHOT'  # noqa: S105


def _load_snapshot_columns(path: Path, columns: list[str]) -> pl.DataFrame | None:
    """读取现有快照中的指定列；文件缺失或损坏时按「无快照」处理。"""
    if not path.exists():
        return None
    try:
        frame = pl.read_parquet(path)
    except (OSError, ValueError) as exc:  # 损坏的快照不应阻塞校验，交给上层决定
        logger.warning(f'读取本地快照失败，按无快照校验：{path} -> {exc}')
        return None
    return frame.select([c for c in columns if c in frame.columns])


def validate_remote_overwrite(
    factor_store: LocalFactorStore,
    factors: pl.DataFrame,
    dividends: pl.DataFrame,
    *,
    max_event_drop_rows: int = 0,
    max_event_drop_codes: int = 0,
    max_latest_date_regression_days: int = 0,
    max_factor_mismatch_codes: int = 0,
) -> dict[str, Any]:
    """写前差异校验：远程数据不得让本地快照「变差」。

    逐项对比现有快照与远程库：分红事件条数（按票）、最新除权日、因子末值。
    默认阈值全为 0，即任何事件减少、除权日倒退、因子末值不一致都会中止写入；
    快照不存在（首次冷启动）时不做回退判定。
    """
    factor_dir = factor_store.root / 'factors'
    snapshot_factors = _load_snapshot_columns(
        factor_dir / 'stock_adj_factor.parquet',
        ['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor'])
    snapshot_dividends = _load_snapshot_columns(
        factor_dir / 'stock_dividend.parquet', ['ts_code', 'ex_date'])

    stats: dict[str, Any] = {
        'snapshot_factor_rows': 0 if snapshot_factors is None else snapshot_factors.height,
        'snapshot_dividend_rows': 0 if snapshot_dividends is None else snapshot_dividends.height,
        'remote_factor_rows': factors.height,
        'remote_dividend_rows': dividends.height,
        'event_drop_rows': 0,
        'event_drop_codes': 0,
        'latest_date_regression_codes': 0,
        'max_latest_date_regression_days': 0,
        'factor_mismatch_codes': 0,
        'violations': [],
        'ok': True,
    }

    if snapshot_dividends is not None and not snapshot_dividends.is_empty():
        snap = (snapshot_dividends.filter(pl.col('ex_date').is_not_null())
                .group_by('ts_code')
                .agg(pl.len().alias('snap_n'),
                     pl.col('ex_date').max().alias('snap_max')))
        remote = (dividends.filter(pl.col('ex_date').is_not_null())
                  .group_by('ts_code')
                  .agg(pl.len().alias('remote_n'),
                       pl.col('ex_date').max().alias('remote_max')))
        joined = snap.join(remote, on='ts_code', how='left').with_columns(
            pl.col('remote_n').fill_null(0),
            pl.col('remote_max').fill_null(pl.date(1900, 1, 1)))
        dropped = joined.with_columns(
            (pl.col('snap_n') - pl.col('remote_n')).clip(lower_bound=0).alias('lost'))
        stats['event_drop_rows'] = int(dropped['lost'].sum())
        stats['event_drop_codes'] = int(dropped.filter(pl.col('lost') > 0).height)

        regressed = joined.with_columns(
            (pl.col('snap_max') - pl.col('remote_max')).dt.total_days().alias('gap')
        ).filter(pl.col('gap') > max_latest_date_regression_days)
        stats['latest_date_regression_codes'] = regressed.height
        if regressed.height:
            stats['max_latest_date_regression_days'] = int(
                cast(int, regressed['gap'].max()))

        if stats['event_drop_rows'] > max_event_drop_rows:
            stats['violations'].append(
                f'分红事件将减少 {stats["event_drop_rows"]} 行（阈值 {max_event_drop_rows}）')
        if stats['event_drop_codes'] > max_event_drop_codes:
            stats['violations'].append(
                f'{stats["event_drop_codes"]} 只票的分红事件减少（阈值 {max_event_drop_codes}）')
        if regressed.height:
            stats['violations'].append(
                f'{regressed.height} 只票最新除权日倒退，最大 '
                f'{stats["max_latest_date_regression_days"]} 天'
                f'（阈值 {max_latest_date_regression_days} 天）')

    if (snapshot_factors is not None and not snapshot_factors.is_empty()
            and not factors.is_empty()):
        snap_last = (snapshot_factors.sort('trade_date').group_by('ts_code')
                     .agg(pl.col('qfq_factor').last().alias('snap_qfq'),
                          pl.col('hfq_factor').last().alias('snap_hfq')))
        remote_last = (factors.sort('trade_date').group_by('ts_code')
                       .agg(pl.col('qfq_factor').last().alias('remote_qfq'),
                            pl.col('hfq_factor').last().alias('remote_hfq')))
        compared = snap_last.join(remote_last, on='ts_code', how='inner')
        mismatch = compared.filter(
            ((pl.col('snap_qfq') - pl.col('remote_qfq')).abs() > 1e-6)
            | ((pl.col('snap_hfq') - pl.col('remote_hfq')).abs() > 1e-6))
        stats['factor_mismatch_codes'] = mismatch.height
        if mismatch.height > max_factor_mismatch_codes:
            stats['violations'].append(
                f'{mismatch.height} 只票因子末值不一致（阈值 {max_factor_mismatch_codes}）')

    stats['ok'] = not stats['violations']
    return stats


def refresh_factors(
    config: Any,
    factor_store: LocalFactorStore,
    *,
    allow_remote: bool = False,
    force_overwrite: bool = False,
    confirm: str = '',
    dry_run: bool = False,
    max_event_drop_rows: int = 0,
    max_event_drop_codes: int = 0,
    max_latest_date_regression_days: int = 0,
    max_factor_mismatch_codes: int = 0,
) -> dict[str, Any]:
    """危险操作：用远程库整表覆盖本地因子/分红快照（默认禁止）。

    本地快照是唯一权威数据源。该入口默认拒绝覆盖，即使显式
    ``allow_remote`` 也必须同时提供 ``force_overwrite`` 与
    ``confirm='OVERWRITE-LOCAL-SNAPSHOT'``；写前还会做差异校验，
    远程数据相对快照出现任何事件减少/除权日倒退/因子末值不一致即中止。

    仅用于首次冷启动（快照不存在）或经确认的全量重建。
    """
    if not allow_remote:
        raise PermissionError(
            '刷新因子/分红需要显式 --allow-remote，避免运行期偷偷访问远程库')
    if not force_overwrite:
        raise PermissionError(
            '默认禁止用远程库覆盖本地因子/分红快照（本地快照为唯一权威源）；'
            '确需冷启动重建请显式 --force-overwrite --confirm '
            f'{_OVERWRITE_CONFIRM_TOKEN}')
    if confirm != _OVERWRITE_CONFIRM_TOKEN:
        raise PermissionError(
            f'危险操作二次确认失败：--confirm 必须为 {_OVERWRITE_CONFIRM_TOKEN}')

    from src.database.db_manager import DatabaseManager  # noqa: PLC0415

    db = DatabaseManager(config)
    if not db.connect():
        raise RuntimeError('数据库连接失败，无法刷新因子/分红快照')

    started = time.perf_counter()
    try:
        with db.engine.connect() as conn:
            factors = pl.read_database(
                'SELECT ts_code, trade_date, qfq_factor, hfq_factor '
                'FROM stock_adj_factor ORDER BY ts_code, trade_date', conn)
            dividends = pl.read_database(
                'SELECT ' + ', '.join(f'`{c}`' for c in _DIVIDEND_COLUMNS) + ' '
                'FROM stock_dividend ORDER BY ts_code, ex_date', conn)
    finally:
        db.disconnect()

    elapsed = time.perf_counter() - started
    validation = validate_remote_overwrite(
        factor_store, factors, dividends,
        max_event_drop_rows=max_event_drop_rows,
        max_event_drop_codes=max_event_drop_codes,
        max_latest_date_regression_days=max_latest_date_regression_days,
        max_factor_mismatch_codes=max_factor_mismatch_codes)
    planned = {'factors': factors.height, 'dividends': dividends.height,
               'elapsed_s': round(elapsed, 3), 'validation': validation,
               'forced': True}

    out_dir = factor_store.root / 'factors'
    factor_path = out_dir / 'stock_adj_factor.parquet'
    dividend_path = out_dir / 'stock_dividend.parquet'

    if not validation['ok']:
        message = '远程数据相对本地快照存在回退，已中止写入：' + '；'.join(
            validation['violations'])
        if dry_run:
            planned['would_abort'] = True
            logger.warning(f'[dry-run] {message}')
            return planned
        raise RuntimeError(message)

    if dry_run:
        logger.info(
            f'[dry-run] 校验通过：因子 {factors.height:,} 行 / 分红 {dividends.height:,} 行')
        return planned

    out_dir.mkdir(parents=True, exist_ok=True)
    factors.write_parquet(factor_path, compression='zstd', statistics=True)
    dividends.write_parquet(dividend_path, compression='zstd', statistics=True)

    meta = {
        'source': 'remote_mysql',
        'forced_overwrite': True,
        'validation': validation,
        'factor_rows': factors.height,
        'dividend_rows': dividends.height,
        'factor_file_bytes': factor_path.stat().st_size,
        'dividend_file_bytes': dividend_path.stat().st_size,
        'elapsed_s': round(elapsed, 3),
        'updated_at': datetime.now().isoformat(timespec='seconds'),
        'version': STORE_VERSION,
    }
    (out_dir / 'meta.json').write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(
        f'[local-store] 危险覆盖完成：{factors.height:,} / {dividends.height:,} 行，'
        f'耗时 {elapsed:.1f}s')
    return meta


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _freshness_check_step(root: Path, *, dry_run: bool = False) -> None:
    """补数收尾：跑一次全量新鲜度巡检，落报告并对 warn/critical 告警。

    只读 ``data/**``，报告写 ``artifacts/data_freshness``（已被 .gitignore
    排除）。非致命：需要卡口时用 ``tools/data_freshness_check.py --fail-on``。
    """
    from src.data.local_store.paths import freshness_artifacts_dir  # noqa: PLC0415
    from src.data.quality import freshness  # noqa: PLC0415

    results = freshness.check_all(root)
    summary = freshness.summarize(results)
    logger.info(
        f'[freshness] 收尾巡检: ok={summary["ok"]} warn={summary["warn"]} '
        f'critical={summary["critical"]}'
    )
    for result in results:
        if result.severity in ('warn', 'critical'):
            logger.warning(
                f'[freshness] {result.key} {result.severity}: '
                f'{result.reasons[:2]}'
            )
    # 扩展数据契约（停复牌 / 行业 PIT / 指数成分历史）目前均为占位，
    # 这里只把「未就绪」暴露在收尾日志里，不阻断构建、也不静默当成约束成立。
    from src.data.quality.extended_contracts import check_contracts  # noqa: PLC0415

    gaps = [item for item in check_contracts(root) if item['status'] != 'ready']
    if gaps:
        logger.warning(
            f'[contracts] {len(gaps)} 项扩展数据契约未就绪：'
            f'{[item["key"] for item in gaps]}；'
            '相关业务约束（停牌可交易性/行业 PIT/指数成分历史）仍需人工复核'
        )

    if not dry_run:
        today = datetime.now().date()
        freshness.write_outputs(freshness_artifacts_dir(), results, today)


def _factor_mismatch_totals(result: dict[str, Any]) -> tuple[int, int]:
    codes = rows = 0
    for stats in result.values():
        if isinstance(stats, dict):
            codes += int(stats.get('mismatch_codes', 0) or 0)
            rows += int(stats.get('mismatch_rows', 0) or 0)
    return codes, rows


def _factor_snapshot_gate(
    factor_store: LocalFactorStore,
    root: Path,
    *,
    allow_missing: bool = False,
) -> int:
    """``build`` 前置门禁：权威因子快照缺失时拒绝写入行情。

    ``_factor_arrays`` 在「快照与分红表都缺失」时返回 ``(None, None)``，
    调用方会把新写入行的 ``qfq_factor/hfq_factor`` 填成 1.0，把正确的
    复权价悄悄改成未复权价，且构建当时不会报任何错。此处在写盘前拦住，
    避免这类不可逆的历史数据污染。

    同时提示快照末日落后于行情末日的情况（增量补数 + 新分红事件未进快照
    会漏记除权），但不阻断：是否需要重算由人工判断。
    """
    required = {
        'stock_adj_factor': factor_store.factor_path,
        'stock_dividend': factor_store.dividend_path,
        'fund_adj_factor': factor_store.fund_factor_path,
    }
    missing = [f'{name}: {path}' for name, path in required.items()
               if not path.exists()]
    if missing:
        message = (
            '权威因子快照缺失，拒绝构建：' + '；'.join(missing)
            + '。请先恢复 data/local_store/factors/ 快照，或执行 '
              'python -m src.data.local_store rebuild-factors；'
              '确需无快照构建时加 --allow-missing-factors（复权因子将被填成 1.0）'
        )
        if allow_missing:
            logger.warning(message + ' [已由 --allow-missing-factors 放行]')
            return 0
        logger.error(message)
        return 1

    try:
        index = pl.read_parquet(
            root / 'meta' / 'daily_index.parquet',
            columns=['data_type', 'end'],
        )
        quote_end = index.filter(pl.col('data_type') == 'stock')['end'].max()
        quote_end_label = (
            quote_end.decode('utf-8', errors='replace')
            if isinstance(quote_end, bytes) else quote_end
        )
        snap_end = pl.scan_parquet(str(factor_store.factor_path)).select(
            pl.col('trade_date').max()).collect().item()
        if snap_end is not None and quote_end is not None and snap_end < quote_end:
            logger.warning(
                f'[factor-gate] 个股因子快照末日 {snap_end} 早于行情末日 '
                f'{quote_end_label}；若期间有新的除权除息事件，请先执行 '
                'rebuild-factors 再补数，否则新增行会漏记除权'
            )
    except (OSError, ValueError, RuntimeError, PolarsError) as exc:
        logger.warning(f'[factor-gate] 快照新鲜度检查失败（不阻断）：{exc}')
    return 0


def _factor_governance_step(
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    *,
    sync: bool = False,
    commit: bool = False,
    fail_on_mismatch: bool = False,
    json_output: bool = False,
) -> int:
    """半自动治理入口：只读校验 →（可选）同步 → 复验 → 可选门禁。

    默认只做校验，不写任何文件；``--sync`` 不加 ``--commit`` 也只是 dry-run
    计划。因子快照是唯一权威源，本入口只把行情内嵌副本对齐到快照，绝不
    反向修改快照。
    """
    before = verify_quote_factor_consistency(quote_store, factor_store)
    if 'error' in before:
        logger.error(f'[factor-governance] 无法校验：{before["error"]}')
        return 1
    before_codes, before_rows = _factor_mismatch_totals(before)
    logger.info(
        f'[factor-governance] 校验：不一致标的 {before_codes} 只 / '
        f'{before_rows:,} 行；明细 {before}'
    )

    sync_stats: dict[str, Any] = {}
    if sync and before_codes:
        for kind, stats in before.items():
            if not isinstance(stats, dict) or not stats.get('mismatch_codes'):
                continue
            sync_stats[kind] = refresh_quote_factor_columns(
                quote_store, factor_store, kind=kind, dry_run=not commit)
            logger.info(
                f'[factor-governance] {kind} 副本同步'
                f'{"（dry-run）" if not commit else ""}：{sync_stats[kind]}'
            )
    elif sync:
        logger.info('[factor-governance] 无差异，跳过同步')

    after = before
    if sync and commit and before_codes:
        after = verify_quote_factor_consistency(quote_store, factor_store)
    after_codes, after_rows = _factor_mismatch_totals(after)
    if sync and commit and before_codes:
        logger.info(
            f'[factor-governance] 复验：不一致标的 {after_codes} 只 / '
            f'{after_rows:,} 行'
        )

    if json_output:
        sys.stdout.write(json.dumps({
            'before': before,
            'sync': sync_stats,
            'after': after,
            'committed': bool(sync and commit),
        }, ensure_ascii=False, indent=2) + '\n')

    if fail_on_mismatch and after_codes:
        logger.error(
            f'[factor-governance] 门禁失败：仍有 {after_codes} 只标的的行情因子'
            '副本与权威快照不一致；请执行 --sync --commit 后复验'
        )
        return 1
    return 0


def _derive_turnover_step(root: Path, *, skip: bool = False,
                          dry_run: bool = False) -> dict[str, Any] | None:
    """补数收尾：用本地行情派生换手率缺口（免上游、幂等、秒级）。

    ``derive_turnover_local`` 按 ``(ts_code, trade_date)`` anti join 只补缺失行，
    不覆盖既有值；无股本记录或 ``volume<=0``（停牌）自然跳过。因此可以无条件
    挂在每日补数末尾——重复执行不会改变已有数据，也不会因为上游断供而失败。

    非致命：股本快照缺失、parquet 读写异常时只告警，退出码仍为 0。
    """
    if skip:
        logger.info('已跳过换手率本地派生（--skip-derive-turnover）')
        return None

    from src.data.local_store.turnover import (  # noqa: PLC0415
        derive_turnover_local,
    )

    try:
        result = derive_turnover_local(root, dry_run=dry_run)
    except (OSError, ValueError, RuntimeError, PolarsError) as exc:
        logger.warning(f'[turnover] 本地派生失败（不影响本次补数）: {exc}')
        return None

    gap = int(result.get('gap_rows') or 0)
    if gap == 0:
        logger.info('[turnover] 换手率无缺口，跳过派生')
        return result

    derived = int(result.get('derived_rows') or 0)
    no_share = int(result.get('no_share_rows') or 0)
    if dry_run:
        logger.info(
            f'[turnover] dry-run：缺口 {gap:,} 行，可派生 {derived:,} 行，'
            f'无股本跳过 {no_share:,} 行')
        return result
    if not result.get('written'):
        logger.info(
            f'[turnover] 缺口 {gap:,} 行中无可派生行（无股本跳过 {no_share:,} 行）')
        return result

    added = int(result.get('rows_added') or 0)
    codes = int(result.get('codes') or 0)
    span = f'{result.get("date_min")}~{result.get("date_max")}'
    logger.info(
        f'[turnover] 本地派生换手率 {added:,} 行 / {codes} 只（{span}），'
        f'表 {int(result.get("before_rows") or 0):,} → '
        f'{int(result.get("after_rows") or 0):,} 行')
    return result


def _refresh_anomaly_lists_step(root: Path, *, skip: bool,
                                dry_run: bool = False) -> dict[str, Any] | None:
    """数据写入完成后的末步：刷新异常台账清单 + 新鲜度巡检（均非致命）。"""
    result: dict[str, Any] | None = None
    if dry_run:
        logger.info('dry-run：跳过异常台账清单刷新')
    elif skip:
        logger.info('已跳过异常台账清单刷新（--skip-anomaly-lists）')
    else:
        from src.data.quality.anomaly_lists import (  # noqa: PLC0415
            refresh_anomaly_lists,
        )

        result = refresh_anomaly_lists(root)
        if result['status'] != 'ok':
            logger.warning(
                f'异常台账清单刷新失败（不影响本次补数，退出码仍为 0）: '
                f'{result["error"]}')
    # 新鲜度巡检是只读的，dry-run 也执行（但不落报告），保证每日补数后
    # 断更类问题（例如公告事件停留在去年）能立刻在收尾日志里暴露。
    _freshness_check_step(root, dry_run=dry_run)
    return result


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog='python -m src.data.local_store')
    sub = parser.add_subparsers(dest='command', required=True)

    build_cmd = sub.add_parser('build', help='构建/增量更新本地行情列存')
    build_cmd.add_argument('--all', action='store_true', help='全市场全量重建（默认）')
    build_cmd.add_argument('--codes', default='', help='逗号分隔的 ts_code 列表')
    build_cmd.add_argument('--since', default='', help='只更新该日期之后的记录（YYYY-MM-DD）')
    build_cmd.add_argument('--data-type', default='all',
                           choices=['all', 'stock', 'index', 'fund'])
    build_cmd.add_argument('--incremental', action='store_true',
                           help='只重建源 .day 末行日期比本地新的标的')
    build_cmd.add_argument('--target-date', default='',
                           help='构建后校验本地末行是否到达该日期（YYYY-MM-DD）')
    build_cmd.add_argument('--max-date', default='',
                           help='只写入不晚于该日期的记录（YYYY-MM-DD）')
    build_cmd.add_argument('--force-intraday', action='store_true',
                           help='交易时段也写入当日未收盘 K 线（默认跳过）')
    build_cmd.add_argument('--dry-run', action='store_true', help='只统计不落盘')
    build_cmd.add_argument('--skip-anomaly-lists', action='store_true',
                           help='数据写入完成后不刷新异常台账清单（默认刷新）')
    build_cmd.add_argument('--skip-derive-turnover', action='store_true',
                           help='收尾不做换手率本地派生（默认用行情 volume + 流通股本补齐缺口）')
    build_cmd.add_argument('--allow-missing-factors', action='store_true',
                           help='应急逃生：因子快照缺失时仍允许构建（会把复权因子填成 1.0，仅用于恢复快照）')
    build_cmd.add_argument('--skip-factor-verify', action='store_true',
                           help='收尾不做行情副本与因子快照的全量一致性门禁（默认做，失败返回非零）')

    factor_cmd = sub.add_parser(
        'refresh-factors', help='危险操作：用远程库整表覆盖本地快照（默认拒绝）')
    factor_cmd.add_argument('--allow-remote', action='store_true', help='显式允许访问远程库')
    factor_cmd.add_argument('--force-overwrite', action='store_true',
                            help='危险：允许远程库覆盖本地快照（需配合 --confirm）')
    factor_cmd.add_argument('--confirm', default='',
                            help='二次确认口令，必须为 OVERWRITE-LOCAL-SNAPSHOT')
    factor_cmd.add_argument('--max-event-drop-rows', type=int, default=0,
                            help='允许的分红事件减少行数（默认 0，即不允许）')
    factor_cmd.add_argument('--max-event-drop-codes', type=int, default=0,
                            help='允许的分红事件减少票数（默认 0）')
    factor_cmd.add_argument('--max-latest-date-regression-days', type=int, default=0,
                            help='允许的最新除权日倒退天数（默认 0）')
    factor_cmd.add_argument('--max-factor-mismatch-codes', type=int, default=0,
                            help='允许的因子末值不一致票数（默认 0）')
    factor_cmd.add_argument('--dry-run', action='store_true')
    factor_cmd.add_argument('--skip-anomaly-lists', action='store_true',
                            help='不刷新异常台账清单（默认刷新）')

    rebuild_cmd = sub.add_parser(
        'rebuild-factors', help='用本地行情+分红批量重算因子快照（不写远程库）')
    rebuild_cmd.add_argument('--codes', default='', help='逗号分隔的 ts_code 列表')
    rebuild_cmd.add_argument('--chunk-size', type=int, default=200,
                             help='每个分块处理的标的数（默认 200）')
    rebuild_cmd.add_argument('--include-existing', action='store_true',
                             help='连同快照里已有的标的一起重算（默认只补缺）')
    rebuild_cmd.add_argument('--no-resume', action='store_true',
                             help='忽略断点，从头开始')
    rebuild_cmd.add_argument('--dry-run', action='store_true', help='只统计不落盘')
    rebuild_cmd.add_argument('--skip-anomaly-lists', action='store_true',
                             help='不刷新异常台账清单（默认刷新）')

    sync_cmd = sub.add_parser(
        'sync-quote-factors', help='把行情 parquet 内嵌的 qfq/hfq 副本对齐到因子快照')
    sync_cmd.add_argument('--codes', default='', help='逗号分隔的 ts_code 列表（默认全市场）')
    sync_cmd.add_argument('--kind', default='stock', choices=['stock', 'index', 'fund'],
                          help='行情类型（默认 stock）')
    sync_cmd.add_argument('--dry-run', action='store_true', help='只统计需要重写的标的/行数')

    verify_cmd = sub.add_parser(
        'verify-quote-factors', help='全量核对行情副本与因子快照的一致性')
    verify_cmd.add_argument('--json', action='store_true', help='以 JSON 输出')

    gov_cmd = sub.add_parser(
        'factor-governance',
        help='因子副本一致性治理：只读校验 →（可选）同步 → 复验 → 门禁')
    gov_cmd.add_argument('--sync', action='store_true',
                         help='对有差异的类型执行副本同步（不加 --commit 时仍是 dry-run）')
    gov_cmd.add_argument('--commit', action='store_true',
                         help='与 --sync 配合才真正改写行情 parquet')
    gov_cmd.add_argument('--fail-on-mismatch', action='store_true',
                         help='复验后仍有差异则返回非零退出码（可挂 CI）')
    gov_cmd.add_argument('--json', action='store_true', help='以 JSON 输出校验结果')

    sub.add_parser('rescan-meta', help='从现有 Parquet 重建覆盖度元数据（不重算行情）')
    sub.add_parser('stats', help='查看列存规模')

    turnover_cmd = sub.add_parser(
        'sync-turnover', help='从 baostock 回补换手率事实表（幂等、断点续跑）')
    turnover_cmd.add_argument('--codes', default='', help='逗号分隔的 ts_code 列表（默认全市场）')
    turnover_cmd.add_argument('--chunk-size', type=int, default=200,
                              help='每批写盘的标的数（默认 200）')
    turnover_cmd.add_argument('--workers', type=int, default=1,
                              help='并发进程数；默认 1 串行（实测并发登录会被 baostock 拉黑）')
    turnover_cmd.add_argument('--max-codes', type=int, default=None,
                              help='只处理前 N 只（调试用）')
    turnover_cmd.add_argument('--no-resume', action='store_true',
                              help='忽略断点，从头开始')
    turnover_cmd.add_argument('--dry-run', action='store_true', help='只统计不落盘')

    derive_cmd = sub.add_parser(
        'derive-turnover',
        help='用本地行情 volume + 流通股本派生换手率（免上游，只补缺失行）')
    derive_cmd.add_argument('--start', default='', help='起始交易日 YYYY-MM-DD（含）')
    derive_cmd.add_argument('--end', default='', help='结束交易日 YYYY-MM-DD（含）')
    derive_cmd.add_argument('--codes', default='', help='逗号分隔的 ts_code 列表（默认全市场）')
    derive_cmd.add_argument('--commit', action='store_true', help='真正写入（默认 dry-run）')
    return parser.parse_args(argv)


def _run_build_command(
    args: argparse.Namespace,
    config: Any,
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    root: Path,
) -> int:
    """build 子命令：前置快照门禁 → 构建 → 收尾因子一致性复验。"""
    # 快照缺失会把复权因子写成 1.0，必须在写盘前拦住。
    # dry-run 不落盘，跳过门禁以便先看清计划规模。
    if not args.dry_run:
        gate_code = _factor_snapshot_gate(
            factor_store, root,
            allow_missing=args.allow_missing_factors,
        )
        if gate_code:
            return gate_code

    codes = [c for c in args.codes.split(',') if c.strip()] if args.codes else None
    since = datetime.strptime(args.since, '%Y-%m-%d').date() if args.since else None
    target_date = (
        datetime.strptime(args.target_date, '%Y-%m-%d').date()
        if args.target_date else None)
    max_date = (
        datetime.strptime(args.max_date, '%Y-%m-%d').date()
        if args.max_date else None)
    result = build(
        config, quote_store, factor_store,
        data_type=args.data_type, codes=codes, since=since,
        incremental=args.incremental, target_date=target_date,
        max_date=max_date, force_intraday=args.force_intraday,
        dry_run=args.dry_run,
    )
    logger.info(f'构建结果: {result}')
    # 换手率派生必须在新鲜度巡检之前：巡检会读 factors/stock_turnover.parquet，
    # 若先巡检后派生，刚刚补进来的当日数据仍会被判成「缺最新交易日」。
    # dry-run 时照样跑一次只读统计，把缺口规模暴露在日志里。
    _derive_turnover_step(
        root, skip=args.skip_derive_turnover, dry_run=args.dry_run)
    _refresh_anomaly_lists_step(
        root, skip=args.skip_anomaly_lists, dry_run=args.dry_run)
    # 收尾门禁：确认行情内嵌副本与权威快照逐行一致（stock/index/fund 0/0/0）。
    # 与 factor-governance 同口径，默认只读、失败即返回非零，避免静默漂移。
    if (not args.dry_run and not args.skip_factor_verify
            and _factor_governance_step(
                quote_store, factor_store, fail_on_mismatch=True)):
        logger.error(
            '[local-store] build 收尾因子一致性门禁失败；'
            '请执行 python -m src.data.local_store factor-governance '
            '--sync --commit 后复验'
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 入口。"""
    args = _parse_args(argv)
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    root = resolve_store_root(config)
    quote_store = LocalQuoteStore(root, enabled=True)
    factor_store = LocalFactorStore(root, enabled=True)

    if args.command == 'stats':
        stats = quote_store.stats()
        logger.info(f'列存根目录: {stats["root"]}')
        logger.info(f'标的 {stats["symbols"]} 个，行数 {stats["rows"]:,}，'
                    f'文件 {stats["files"]} 个，磁盘 {stats["disk_bytes"] / 1024 / 1024:.1f} MB')
        logger.info(f'分类: {stats["by_type"]}')
        logger.info(f'因子/分红快照: {factor_store.meta()}')
        return 0

    if args.command == 'refresh-factors':
        refresh_factors(config, factor_store,
                        allow_remote=args.allow_remote,
                        force_overwrite=args.force_overwrite,
                        confirm=args.confirm,
                        dry_run=args.dry_run,
                        max_event_drop_rows=args.max_event_drop_rows,
                        max_event_drop_codes=args.max_event_drop_codes,
                        max_latest_date_regression_days=(
                            args.max_latest_date_regression_days),
                        max_factor_mismatch_codes=args.max_factor_mismatch_codes)
        _refresh_anomaly_lists_step(
            root, skip=args.skip_anomaly_lists, dry_run=args.dry_run)
        return 0

    if args.command == 'rebuild-factors':
        from src.data.local_store.factor_rebuild import (  # noqa: PLC0415
            rebuild_factors_from_local,
        )

        codes = [c for c in args.codes.split(',') if c.strip()] if args.codes else None
        result = rebuild_factors_from_local(
            quote_store, factor_store,
            codes=codes, chunk_size=args.chunk_size,
            include_existing=args.include_existing,
            resume=not args.no_resume, dry_run=args.dry_run,
        )
        logger.info(f'因子重建结果: {result}')
        _refresh_anomaly_lists_step(
            root, skip=args.skip_anomaly_lists, dry_run=args.dry_run)
        return 0

    if args.command == 'sync-quote-factors':
        codes = [c.strip() for c in args.codes.split(',') if c.strip()] if args.codes else None
        stats = refresh_quote_factor_columns(
            quote_store, factor_store, codes=codes, kind=args.kind,
            dry_run=args.dry_run)
        logger.info(f'行情因子副本同步: {stats}')
        return 0

    if args.command == 'verify-quote-factors':
        result = verify_quote_factor_consistency(quote_store, factor_store)
        if args.json:
            sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        else:
            for kind, stats in result.items():
                logger.info(f'[{kind}] {stats}')
        return 0

    if args.command == 'factor-governance':
        return _factor_governance_step(
            quote_store, factor_store,
            sync=args.sync, commit=args.commit,
            fail_on_mismatch=args.fail_on_mismatch,
            json_output=args.json)

    if args.command == 'sync-turnover':
        from src.data.local_store.turnover import backfill_turnover  # noqa: PLC0415

        codes = [c.strip() for c in args.codes.split(',') if c.strip()] if args.codes else None
        result = backfill_turnover(
            quote_store, root,
            codes=codes, chunk_size=args.chunk_size, workers=args.workers,
            resume=not args.no_resume, dry_run=args.dry_run,
            max_codes=args.max_codes)
        logger.info(f'换手率回补结果: {result}')
        return 0

    if args.command == 'derive-turnover':
        from src.data.local_store.turnover import derive_turnover_local  # noqa: PLC0415

        codes = [c.strip() for c in args.codes.split(',') if c.strip()] if args.codes else None
        result = derive_turnover_local(
            root,
            start=date.fromisoformat(args.start) if args.start else None,
            end=date.fromisoformat(args.end) if args.end else None,
            codes=codes, dry_run=not args.commit)
        logger.info(f'换手率本地派生结果: {result}')
        return 0

    if args.command == 'rescan-meta':
        tdx_raw = getattr(getattr(config, 'data', None), 'tdx_data_path', '') or ''
        tdx = Path(str(tdx_raw)) if str(tdx_raw) else None
        result = quote_store.rescan_meta(tdx_path=tdx)
        logger.info(f'元数据重建结果: {result}')
        return 0

    return _run_build_command(args, config, quote_store, factor_store, root)


if __name__ == '__main__':
    sys.exit(main())
