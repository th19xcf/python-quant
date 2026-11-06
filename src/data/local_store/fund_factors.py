#!/usr/bin/env python3

"""基金（ETF/LOF/REITs 等）分红与份额折算事件表、复权因子重建。

背景
----
个股复权走 ``factors/stock_dividend.parquet`` + ``stock_adj_factor.parquet``，
基金此前完全没有事件数据，2,615 只基金因子恒为 1.0，导致除息/折算当日的
价格跳空被当成真实下跌。本模块为基金补齐同语义的事件与因子。

产物（均落在 ``data/local_store/factors/``）
-------------------------------------------
* ``fund_dividend.parquet``      现金分红事件
  ``ts_code / ex_date / record_date / cash_amount / source / fetched_at``
* ``fund_share_change.parquet``  份额分拆 / 份额折算事件
  ``ts_code / ex_date / split_type / share_ratio / source / fetched_at``
  单独成表的原因：份额折算没有现金流出，语义是「1 份变成 x 份」的乘性变换，
  与现金分红的加性口径不同；混在一张表里会让下游分红展示出现 x=0 的伪事件。
* ``fund_adj_factor.parquet``    复权因子（与个股完全同一口径）
  ``ts_code / trade_date / qfq_factor / hfq_factor``
  单独文件而非并入个股因子快照：个股快照由 ``factor_rebuild`` 用
  ``covered_codes.txt`` 维护「已覆盖个股集合」，混入基金代码会破坏该
  sidecar 语义；同 schema 便于读侧统一处理。

事件生效日对齐（实测结论，见报告）
--------------------------------
* ETF（159/51x/56x/58x/520/530）现金分红：公告除息日当天生效；
* 其余品种（LOF 16x/15x、REITs 508/180、50x 等）现金分红：公告除息日的
  下一个交易日生效（场内价格比净值晚一天反映）；
* 份额分拆/折算：公告折算日的下一个交易日生效（折算在当日收盘后完成，
  交易价格次日切换）。
对齐只影响因子挂载的交易日，事件表里保留公告原始日期。

复权公式与个股一致
----------------
``factor = (prev_close - cash_amount) / (prev_close * share_ratio)``
其中 ``share_ratio`` 即「1:x」里的 x（无份额变动时为 1）。份额反向折算
（1:0.01）会使因子达到 100，因此基金路径把因子上限放宽到 1000。

用法::

    python -m src.data.local_store.fund_factors --dry-run
    python -m src.data.local_store.fund_factors
"""

from __future__ import annotations

import argparse
import bisect
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import polars as pl
from loguru import logger

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.local_store.store import (
    LocalFactorStore,
    LocalQuoteStore,
    refresh_quote_factor_columns,
)
from src.utils.dates import as_date

FUND_DIVIDEND_FILENAME = 'fund_dividend.parquet'
FUND_SHARE_CHANGE_FILENAME = 'fund_share_change.parquet'
FUND_FACTOR_FILENAME = 'fund_adj_factor.parquet'
FUND_COVERED_SIDECAR = 'fund_covered_codes.txt'
FUND_META_FILENAME = 'fund_meta.json'

DIVIDEND_COLUMNS = (
    'ts_code', 'ex_date', 'record_date', 'cash_amount', 'source', 'fetched_at')
SHARE_CHANGE_COLUMNS = (
    'ts_code', 'ex_date', 'split_type', 'share_ratio', 'source', 'fetched_at')
FACTOR_COLUMNS = ('ts_code', 'trade_date', 'qfq_factor', 'hfq_factor')

# ETF 场内交易代码前缀：现金分红在公告除息日当天反映到价格上。
_ETF_PREFIXES = ('159', '51', '56', '58', '520', '530')
FUND_FACTOR_MAX = 1000.0

#: 行情源已对份额折算做过前复权的基金：``quotes/fund`` 在折算日**没有**价格
#: 跳空（原始价连续），再乘折算因子会构成二次复权。实测 159930.SZ 2013-12-12
#: 原始收益 -0.32%，因子台阶 2.588957→1.054741 却造出 -59.4% 的伪跳变；
#: 2025-09-22 同理（原始 -1.19% vs 报表 pct_chg -6.31%）。这类标的跳过份额
#: 折算事件，现金分红仍按原逻辑处理。
QUOTE_SPLIT_ADJUSTED_CODES: tuple[str, ...] = ('159930.SZ',)


# ----------------------------------------------------------------------
# 代码与日期工具
# ----------------------------------------------------------------------
def fund_ts_code(symbol: str) -> str:
    """6 位基金代码 → 标准 ts_code（5 开头为上交所，其余深交所）。"""
    code = str(symbol).strip().zfill(6)
    return f'{code}.SH' if code.startswith('5') else f'{code}.SZ'


def is_etf_code(ts_code: str) -> bool:
    """是否 ETF 场内代码（现金分红当日生效）。"""
    return ts_code.split('.')[0].startswith(_ETF_PREFIXES)


def _as_date(value: Any) -> date | None:
    """统一日期归一；新增 'YYYYMMDD' 与 numpy 时间戳支持，行为向真源对齐。"""
    return as_date(value)


def _parse_split_ratio(value: Any) -> float | None:
    """解析 ``1:0.2803`` / ``0.2803`` 形式的分拆比例。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if ':' in text:
        text = text.rsplit(':', 1)[-1].strip()
    try:
        ratio = float(text)
    except ValueError:
        return None
    return ratio if ratio > 0 else None


def _next_trade_date(trade_dates: Sequence[date], ex_date: date) -> date | None:
    """返回严格晚于 ``ex_date`` 的第一个交易日。"""
    pos = bisect.bisect_right(trade_dates, ex_date)
    if pos >= len(trade_dates):
        return None
    return trade_dates[pos]


def _call_with_retry(fn, *, attempts: int = 3, base_delay: float = 0.6,
                     label: str = '') -> Any:
    """带指数退避的远程调用；全部失败返回 None。"""
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - 东财接口抛错类型不稳定
            last_error = exc
            time.sleep(base_delay * (2 ** attempt))
    logger.warning(f'[fund] {label} 连续 {attempts} 次失败: {last_error}')
    return None


# ----------------------------------------------------------------------
# 远程事件抓取
# ----------------------------------------------------------------------
def _normalize_dividends(raw: pl.DataFrame, fetched_at: datetime) -> pl.DataFrame:
    """把 ``fund_fh_em`` 的原始列标准化为事件表 schema。"""
    frame = raw.select([
        pl.col('基金代码').cast(pl.Utf8).str.zfill(6).alias('symbol'),
        pl.col('除息日期').cast(pl.Utf8).str.slice(0, 10)
        .str.strptime(pl.Date, '%Y-%m-%d', strict=False).alias('ex_date'),
        pl.col('权益登记日').cast(pl.Utf8).str.slice(0, 10)
        .str.strptime(pl.Date, '%Y-%m-%d', strict=False).alias('record_date'),
        pl.col('分红').cast(pl.Float64, strict=False).alias('cash_amount'),
    ])
    frame = frame.filter(
        pl.col('symbol').is_not_null()
        & pl.col('ex_date').is_not_null()
        & pl.col('cash_amount').is_not_null()
        & (pl.col('cash_amount') > 0))
    frame = frame.with_columns([
        (pl.col('symbol')
         + pl.when(pl.col('symbol').str.starts_with('5'))
         .then(pl.lit('.SH')).otherwise(pl.lit('.SZ'))).alias('ts_code'),
        pl.lit('akshare:fund_fh_em').alias('source'),
        pl.lit(fetched_at).alias('fetched_at'),
    ])
    # 同一年度表内偶发重复行：按 (代码, 除息日) 取最大金额，避免重复累加
    frame = (frame.sort('cash_amount', descending=True)
             .unique(subset=['ts_code', 'ex_date'], keep='first', maintain_order=True))
    return frame.select(DIVIDEND_COLUMNS).sort(['ts_code', 'ex_date'])


def fetch_dividend_events(
    years: Iterable[int] | None = None,
    *,
    cache_dir: Path | str = 'tmp/fund_cache',
    refresh: bool = False,
) -> pl.DataFrame:
    """抓取并汇总全市场基金现金分红事件（AkShare ``fund_fh_em``）。"""
    import akshare as ak  # noqa: PLC0415 - 延迟导入，避免测试与离线场景加载远程库

    cache_root = Path(cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.now()
    frames: list[pl.DataFrame] = []

    for year in years or range(2004, date.today().year + 1):
        cache_path = cache_root / f'fh_{year}.parquet'
        if cache_path.exists() and not refresh:
            frames.append(pl.read_parquet(cache_path))
            continue
        raw = _call_with_retry(
            lambda y=year: ak.fund_fh_em(year=str(y)),
            label=f'fund_fh_em({year})')
        if raw is None or len(raw) == 0:
            continue
        frame = pl.from_pandas(raw)
        tmp = cache_path.with_suffix('.parquet.tmp')
        frame.write_parquet(tmp)
        os.replace(tmp, cache_path)
        frames.append(frame)

    if not frames:
        return pl.DataFrame(schema={
            'ts_code': pl.Utf8, 'ex_date': pl.Date, 'record_date': pl.Date,
            'cash_amount': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
        })
    return _normalize_dividends(
        pl.concat(frames, how='diagonal_relaxed'), fetched_at)


def _fetch_one_share_change(symbol: str,
                            cache: dict[str, Any]) -> tuple[str, Any, float]:
    import akshare as ak  # noqa: PLC0415

    t0 = time.perf_counter()
    for attempt in range(3):
        try:
            df = ak.fund_open_fund_info_em(symbol=symbol, indicator='拆分详情')
            rows = pl.from_pandas(df).to_dicts() if len(df) else []
            return symbol, rows, time.perf_counter() - t0
        except Exception as exc:  # noqa: BLE001 - 东财接口抛错类型不稳定
            if attempt == 2:
                return symbol, {'error': f'{type(exc).__name__}: {exc}'}, (
                    time.perf_counter() - t0)
            time.sleep(0.5 * (2 ** attempt))
    return symbol, {'error': 'unknown'}, time.perf_counter() - t0


def fetch_share_change_events(
    ts_codes: Sequence[str],
    *,
    cache_path: Path | str = 'tmp/fund_cache/splits.json',
    refresh: bool = False,
    max_workers: int = 6,
) -> pl.DataFrame:
    """抓取基金份额分拆/折算事件（AkShare ``拆分详情``），带本地缓存。"""
    path = Path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cache: dict[str, Any] = {}
    if path.exists() and not refresh:
        try:
            cache = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            cache = {}

    symbols = sorted({code.split('.')[0] for code in ts_codes})
    pending = [
        sym for sym in symbols
        if not isinstance(cache.get(sym), list)
    ]
    logger.info(
        f'[fund] 份额折算抓取：目标 {len(symbols)} 只，缓存命中 '
        f'{len(symbols) - len(pending)} 只，待抓 {len(pending)} 只')

    started = time.perf_counter()
    if pending:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_fetch_one_share_change, sym, cache)
                       for sym in pending]
            for index, fut in enumerate(as_completed(futures), 1):
                sym, rows, elapsed = fut.result()
                cache[sym] = rows
                if index % 200 == 0 or index == len(pending):
                    logger.info(
                        f'[fund] 份额折算进度 {index}/{len(pending)}，'
                        f'已用 {time.perf_counter() - started:.1f}s')
        tmp = path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(cache, ensure_ascii=False), encoding='utf-8')
        os.replace(tmp, path)

    fetched_at = datetime.now()
    records: list[dict[str, Any]] = []
    failed = 0
    for sym in symbols:
        rows = cache.get(sym)
        if not isinstance(rows, list):
            failed += 1
            continue
        for row in rows:
            ex_date = _as_date(row.get('拆分折算日'))
            ratio = _parse_split_ratio(row.get('拆分折算比例'))
            if ex_date is None or ratio is None:
                continue
            records.append({
                'ts_code': fund_ts_code(sym),
                'ex_date': ex_date,
                'split_type': str(row.get('拆分类型') or 'unknown'),
                'share_ratio': ratio,
                'source': 'akshare:fund_open_fund_info_em/拆分详情',
                'fetched_at': fetched_at,
            })
    if failed:
        logger.warning(f'[fund] 份额折算抓取失败 {failed} 只（按无事件处理）')
    if not records:
        return pl.DataFrame(schema={
            'ts_code': pl.Utf8, 'ex_date': pl.Date, 'split_type': pl.Utf8,
            'share_ratio': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
        })
    return pl.DataFrame(records).select(SHARE_CHANGE_COLUMNS).unique(
        subset=['ts_code', 'ex_date', 'split_type'], keep='first'
    ).sort(['ts_code', 'ex_date'])


# ----------------------------------------------------------------------
# 事件合成与因子计算
# ----------------------------------------------------------------------
def prepare_events_for_code(
    ts_code: str,
    trade_dates: Sequence[date],
    dividends: pl.DataFrame | None,
    share_changes: pl.DataFrame | None,
) -> pl.DataFrame:
    """把现金分红与份额折算合成单只基金的「事件日 + 因子参数」表。"""
    if not trade_dates:
        return pl.DataFrame(schema={
            'ts_code': pl.Utf8, 'ex_date': pl.Date, 'cash_div': pl.Float64,
            'share_div': pl.Float64, 'rights_issue_ratio': pl.Float64,
            'rights_issue_price': pl.Float64,
        })
    last_date = trade_dates[-1]
    etf = is_etf_code(ts_code)
    combined: dict[date, list[float]] = {}

    if dividends is not None and not dividends.is_empty():
        for row in dividends.iter_rows(named=True):
            cash = float(row['cash_amount'] or 0.0)
            if cash <= 0:
                continue
            ex_date = _as_date(row['ex_date'])
            if ex_date is None:
                continue
            # ETF 分红当日生效；其余品种顺延到下一个交易日。
            event_date = ex_date if etf else _next_trade_date(trade_dates, ex_date)
            if event_date is None or event_date > last_date:
                continue
            entry = combined.setdefault(event_date, [0.0, 1.0])
            entry[0] += cash

    if (share_changes is not None and not share_changes.is_empty()
            and ts_code not in QUOTE_SPLIT_ADJUSTED_CODES):
        for row in share_changes.iter_rows(named=True):
            ratio = float(row['share_ratio'] or 0.0)
            if ratio <= 0:
                continue
            ex_date = _as_date(row['ex_date'])
            if ex_date is None:
                continue
            # 份额折算在公告日收盘后完成，价格于下一交易日切换。
            event_date = _next_trade_date(trade_dates, ex_date)
            if event_date is None or event_date > last_date:
                continue
            combined.setdefault(event_date, [0.0, 1.0])[1] *= ratio

    if not combined:
        return pl.DataFrame(schema={
            'ts_code': pl.Utf8, 'ex_date': pl.Date, 'cash_div': pl.Float64,
            'share_div': pl.Float64, 'rights_issue_ratio': pl.Float64,
            'rights_issue_price': pl.Float64,
        })

    rows = [
        {'ts_code': ts_code, 'ex_date': ex_date,
         'cash_div': cash, 'share_div': ratio - 1.0,
         'rights_issue_ratio': 0.0, 'rights_issue_price': 0.0}
        for ex_date, (cash, ratio) in sorted(combined.items())
        if cash > 0 or abs(ratio - 1.0) > 1e-12
    ]
    return pl.DataFrame(rows, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'cash_div': pl.Float64,
        'share_div': pl.Float64, 'rights_issue_ratio': pl.Float64,
        'rights_issue_price': pl.Float64,
    })


def _compute_code_factors(
    ts_code: str,
    quote_store: LocalQuoteStore,
    dividends: pl.DataFrame | None,
    share_changes: pl.DataFrame | None,
) -> pl.DataFrame | None:
    frame = quote_store.read_file('fund', ts_code)
    if frame is None or frame.is_empty() or 'close' not in frame.columns:
        return None
    prices = frame.select([
        pl.lit(ts_code).alias('ts_code'),
        pl.col('date').alias('trade_date'),
        pl.col('close').cast(pl.Float64),
    ]).sort('trade_date')
    trade_dates = prices['trade_date'].to_list()
    events = prepare_events_for_code(
        ts_code, trade_dates, dividends, share_changes)
    if events.is_empty():
        result = prices.with_columns([
            pl.lit(1.0).alias('qfq_factor'),
            pl.lit(1.0).alias('hfq_factor'),
        ])
    else:
        result = AdjFactorCalculator.calculate_factors(
            prices, events, max_factor=FUND_FACTOR_MAX)
    return result.select(FACTOR_COLUMNS).sort('trade_date')


def rebuild_fund_factors(
    store_root: Path | str,
    dividends: pl.DataFrame,
    share_changes: pl.DataFrame,
    *,
    codes: Sequence[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """按事件重算全部基金复权因子并原子替换快照文件。"""
    root = Path(store_root)
    quote_store = LocalQuoteStore(root, enabled=True)
    fund_codes = quote_store.codes('fund')
    if codes is not None:
        wanted = {c.strip().upper() for c in codes if c.strip()}
        fund_codes = [c for c in fund_codes if c in wanted]

    div_by_code = {
        str(group['ts_code'][0]): group
        for group in dividends.partition_by('ts_code')} if not dividends.is_empty() else {}
    split_by_code = {
        str(group['ts_code'][0]): group
        for group in share_changes.partition_by('ts_code')
    } if not share_changes.is_empty() else {}

    planned_rows = 0
    for code in fund_codes:
        cov = quote_store.coverage('fund', code)
        if cov is not None:
            planned_rows += cov.rows
    if dry_run:
        return {
            'fund_codes': len(fund_codes),
            'rows': planned_rows,
            'dividend_events': dividends.height,
            'share_change_events': share_changes.height,
            'dry_run': True,
        }

    started = time.perf_counter()
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    for index, code in enumerate(fund_codes, 1):
        try:
            frame = _compute_code_factors(
                code, quote_store, div_by_code.get(code), split_by_code.get(code))
        except (OSError, ValueError, RuntimeError, TypeError) as exc:
            failed.append(code)
            logger.warning(f'[fund] {code} 因子计算失败: {exc}')
            continue
        if frame is not None and not frame.is_empty():
            frames.append(frame)
        if index % 500 == 0:
            logger.info(
                f'[fund] 因子进度 {index}/{len(fund_codes)}，'
                f'已用 {time.perf_counter() - started:.1f}s')

    if not frames:
        raise RuntimeError('基金因子重建失败：没有可写入的标的')

    merged = pl.concat(frames, how='vertical_relaxed').sort(['ts_code', 'trade_date'])
    target = root / 'factors' / FUND_FACTOR_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix('.parquet.tmp')
    merged.write_parquet(
        tmp, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    os.replace(tmp, target)

    covered = sorted({str(code) for code in merged['ts_code'].unique().to_list()})
    sidecar = root / 'factors' / FUND_COVERED_SIDECAR
    sidecar.write_text('\n'.join(covered) + '\n', encoding='utf-8', newline='\n')

    # 因子快照重建后必须立即对齐行情副本，否则 quotes/fund 会停在旧口径，
    # 界面切复权看不到变化（与个股 920008.BJ 同类问题）。
    quote_refresh = None
    try:
        quote_refresh = refresh_quote_factor_columns(
            quote_store, LocalFactorStore(root, enabled=True),
            codes=covered, kind='fund')
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning(f'[fund] 行情因子副本同步失败，需手工重跑: {exc}')

    meta = {
        'source': 'akshare fund_fh_em + 拆分详情',
        'factor_rows': merged.height,
        'factor_codes': len(covered),
        'dividend_rows': dividends.height,
        'dividend_codes': dividends['ts_code'].n_unique() if not dividends.is_empty() else 0,
        'share_change_rows': share_changes.height,
        'share_change_codes': (
            share_changes['ts_code'].n_unique() if not share_changes.is_empty() else 0),
        'failed_codes': len(failed),
        'quote_factor_refresh': quote_refresh,
        'elapsed_s': round(time.perf_counter() - started, 3),
        'updated_at': datetime.now().isoformat(timespec='seconds'),
        'event_alignment': {
            'etf_dividend': 'ex_date',
            'non_etf_dividend': 'next_trade_date',
            'share_change': 'next_trade_date',
        },
    }
    meta_path = root / 'factors' / FUND_META_FILENAME
    tmp_meta = meta_path.with_suffix('.json.tmp')
    tmp_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp_meta, meta_path)
    logger.info(
        f'[fund] 因子重建完成：{len(covered)} 只 / {merged.height:,} 行，'
        f'耗时 {meta["elapsed_s"]}s')
    return {**meta, 'dry_run': False, 'file_bytes': target.stat().st_size}


# ----------------------------------------------------------------------
# 落盘与备份
# ----------------------------------------------------------------------
def _backup_to_csv(path: Path, log_dir: Path) -> None:
    if not path.exists():
        return
    log_dir.mkdir(parents=True, exist_ok=True)
    out = log_dir / f'{path.stem}.csv'
    pl.read_parquet(path).write_csv(out)
    logger.info(f'[fund] 已备份 {path.name} → {out}')


def build_fund_store(
    store_root: Path | str = 'data/local_store',
    *,
    years: Iterable[int] | None = None,
    codes: Sequence[str] | None = None,
    cache_dir: Path | str = 'tmp/fund_cache',
    log_dir: Path | str = 'logs/fund_factor_20261104',
    refresh: bool = False,
    dry_run: bool = False,
    skip_fetch: bool = False,
) -> dict[str, Any]:
    """抓事件 → 落事件表 → 重算因子 → 写 sidecar / meta。"""
    root = Path(store_root)
    quote_store = LocalQuoteStore(root, enabled=True)
    fund_codes = quote_store.codes('fund')
    if not fund_codes:
        raise RuntimeError(f'{root}/quotes/fund 下没有基金行情，无法重建')
    if codes is not None:
        wanted = {c.strip().upper() for c in codes if c.strip()}
        fund_codes = [c for c in fund_codes if c in wanted]
        if not fund_codes:
            raise RuntimeError(f'指定的基金代码不在本地行情中: {codes}')

    if skip_fetch:
        div_path = root / 'factors' / FUND_DIVIDEND_FILENAME
        split_path = root / 'factors' / FUND_SHARE_CHANGE_FILENAME
        dividends = pl.read_parquet(div_path) if div_path.exists() else pl.DataFrame()
        share_changes = pl.read_parquet(split_path) if split_path.exists() else pl.DataFrame()
    else:
        dividends = fetch_dividend_events(years, cache_dir=cache_dir, refresh=refresh)
        share_changes = fetch_share_change_events(
            fund_codes, cache_path=Path(cache_dir) / 'splits.json', refresh=refresh)

    # 只保留本地有行情的基金；事件表不记录「有事件但无行情」的标的。
    dividends = dividends.filter(pl.col('ts_code').is_in(fund_codes)) \
        if not dividends.is_empty() else dividends
    share_changes = share_changes.filter(pl.col('ts_code').is_in(fund_codes)) \
        if not share_changes.is_empty() else share_changes

    stats = rebuild_fund_factors(
        root, dividends, share_changes, codes=fund_codes, dry_run=True)
    stats['dividend_codes_all'] = (
        dividends['ts_code'].n_unique() if not dividends.is_empty() else 0)
    stats['share_change_codes_all'] = (
        share_changes['ts_code'].n_unique() if not share_changes.is_empty() else 0)
    if dry_run:
        return stats

    # 写前备份：已有快照 → CSV，避免覆盖后无法回滚。
    log_root = Path(log_dir)
    for name in (FUND_FACTOR_FILENAME, FUND_DIVIDEND_FILENAME,
                 FUND_SHARE_CHANGE_FILENAME):
        _backup_to_csv(root / 'factors' / name, log_root)

    # 先落事件表（含 CSV 审计副本），再按事件重算因子。
    for frame, name, columns in (
        (dividends, FUND_DIVIDEND_FILENAME, DIVIDEND_COLUMNS),
        (share_changes, FUND_SHARE_CHANGE_FILENAME, SHARE_CHANGE_COLUMNS),
    ):
        path = root / 'factors' / name
        tmp = path.with_suffix('.parquet.tmp')
        frame.select(columns).write_parquet(
            tmp, compression='zstd', statistics=True)
        os.replace(tmp, path)
        if not frame.is_empty():
            log_root.mkdir(parents=True, exist_ok=True)
            frame.select(columns).write_csv(log_root / f'{path.stem}_new.csv')

    final = rebuild_fund_factors(
        root, dividends, share_changes, codes=fund_codes, dry_run=False)
    final['dividend_codes_all'] = stats['dividend_codes_all']
    final['share_change_codes_all'] = stats['share_change_codes_all']
    return final


# ----------------------------------------------------------------------
# 验证
# ----------------------------------------------------------------------
def verify_reversibility(
    store_root: Path | str,
    codes: Sequence[str],
    *,
    tolerance: float = 1e-12,
) -> dict[str, Any]:
    """事件→因子→复权价→反推原始价，返回最大相对误差。"""
    root = Path(store_root)
    quote_store = LocalQuoteStore(root, enabled=True)
    factor_path = root / 'factors' / FUND_FACTOR_FILENAME
    factors = pl.read_parquet(factor_path) if factor_path.exists() else pl.DataFrame()
    max_error = 0.0
    checked_rows = 0
    checked_codes: list[str] = []
    for code in codes:
        frame = quote_store.read_file('fund', code)
        snap = factors.filter(pl.col('ts_code') == code)
        if frame is None or frame.is_empty() or snap.is_empty():
            continue
        merged = frame.select(['date', 'close']).join(
            snap.select(['trade_date', 'qfq_factor']),
            left_on='date', right_on='trade_date', how='left').sort('date')
        merged = merged.with_columns(
            pl.col('qfq_factor').fill_null(1.0).cast(pl.Float64))
        qfq_close = merged['close'].cast(pl.Float64) * merged['qfq_factor']
        restored = qfq_close / merged['qfq_factor']
        raw = merged['close'].cast(pl.Float64)
        rel = ((restored - raw).abs() / raw.abs().clip(1e-300)).max()
        # Polars 标量联合类型里含 None（空序列才会出现，此处已由上方
        # is_empty 分支排除）；显式收窄为数值，不改变语义。
        if isinstance(rel, (int, float)):
            max_error = max(max_error, float(rel))
        checked_rows += merged.height
        checked_codes.append(code)
    return {
        'codes': len(checked_codes),
        'rows': checked_rows,
        'max_relative_error': max_error,
        'tolerance': tolerance,
        'passed': max_error <= tolerance,
    }


def verify_gap_digestion(
    store_root: Path | str,
    dividends: pl.DataFrame,
    *,
    min_raw_gap: float = 0.01,
    max_adjusted_gap: float = 0.02,
    limit: int = 40,
) -> dict[str, Any]:
    """对比除息事件前后的原始涨跌幅与复权后涨跌幅。"""
    root = Path(store_root)
    quote_store = LocalQuoteStore(root, enabled=True)
    factor_path = root / 'factors' / FUND_FACTOR_FILENAME
    if not factor_path.exists() or dividends.is_empty():
        return {'events': 0, 'before_over_1pct': 0, 'after_over_2pct': 0}
    factors = pl.read_parquet(factor_path)

    checked = 0
    before_big = 0
    after_big = 0
    worst_before: list[dict[str, Any]] = []
    for row in dividends.iter_rows(named=True):
        code = row['ts_code']
        frame = quote_store.read_file('fund', code)
        if frame is None or frame.is_empty():
            continue
        frame = frame.select(['date', 'close']).sort('date')
        trade_dates = frame['date'].to_list()
        ex_date = _as_date(row['ex_date'])
        if ex_date is None:
            continue
        event_date = ex_date if is_etf_code(code) else _next_trade_date(
            trade_dates, ex_date)
        if event_date is None:
            continue
        pos = bisect.bisect_left(trade_dates, event_date)
        if pos <= 0 or pos >= len(trade_dates):
            continue
        prev_date, cur_date = trade_dates[pos - 1], trade_dates[pos]
        prev_close = float(frame.filter(pl.col('date') == prev_date)['close'][0])
        cur_close = float(frame.filter(pl.col('date') == cur_date)['close'][0])
        if prev_close <= 0:
            continue
        snap = factors.filter(pl.col('ts_code') == code)
        hfq_prev = float(snap.filter(pl.col('trade_date') == prev_date)
                         ['hfq_factor'][0]) if not snap.is_empty() else 1.0
        hfq_cur = float(snap.filter(pl.col('trade_date') == cur_date)
                        ['hfq_factor'][0]) if not snap.is_empty() else 1.0
        raw_gap = cur_close / prev_close - 1
        adj_close_ratio = (cur_close * hfq_cur) / (prev_close * hfq_prev)
        adj_gap = adj_close_ratio - 1
        checked += 1
        if abs(raw_gap) >= min_raw_gap:
            before_big += 1
            if abs(adj_gap) >= max_adjusted_gap:
                after_big += 1
            if len(worst_before) < limit:
                worst_before.append({
                    'ts_code': code,
                    'event_date': str(cur_date),
                    'cash_amount': row['cash_amount'],
                    'raw_gap_pct': round(raw_gap * 100, 2),
                    'adjusted_gap_pct': round(adj_gap * 100, 3),
                })
    return {
        'events_checked': checked,
        'before_over_1pct': before_big,
        'after_over_2pct': after_big,
        'samples': worst_before,
    }


def verify_coverage(store_root: Path | str) -> dict[str, Any]:
    """因子覆盖与 ``quotes/fund`` 标的数对齐情况。"""
    root = Path(store_root)
    quote_store = LocalQuoteStore(root, enabled=True)
    quote_codes = set(quote_store.codes('fund'))
    factor_path = root / 'factors' / FUND_FACTOR_FILENAME
    factor_codes = set()
    if factor_path.exists():
        factor_codes = set(
            pl.scan_parquet(str(factor_path)).select('ts_code')
            .collect()['ts_code'].unique().to_list())
    return {
        'quote_codes': len(quote_codes),
        'factor_codes': len(factor_codes),
        'missing_factor': sorted(quote_codes - factor_codes)[:20],
        'missing_factor_count': len(quote_codes - factor_codes),
        'extra_factor_count': len(factor_codes - quote_codes),
    }


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _parse_years(text: str | None) -> list[int] | None:
    if not text:
        return None
    if '-' in text:
        start, end = text.split('-', 1)
        return list(range(int(start), int(end) + 1))
    return [int(part) for part in text.split(',') if part.strip()]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store-root', default='data/local_store')
    parser.add_argument('--cache-dir', default='tmp/fund_cache')
    parser.add_argument('--log-dir', default='logs/fund_factor_20261104')
    parser.add_argument('--years', default='2004-2026')
    parser.add_argument('--codes', default='',
                        help='逗号分隔的 ts_code，仅处理这些标的')
    parser.add_argument('--refresh', action='store_true', help='忽略本地抓取缓存')
    parser.add_argument('--skip-fetch', action='store_true', help='复用已有事件表')
    parser.add_argument('--dry-run', action='store_true', help='只估算与检查，不落盘')
    parser.add_argument('--verify', action='store_true', help='跑可逆性/跳空/覆盖校验')
    args = parser.parse_args(argv)

    codes = [c.strip().upper() for c in args.codes.split(',') if c.strip()] or None
    started = time.perf_counter()
    stats = build_fund_store(
        args.store_root,
        years=_parse_years(args.years),
        codes=codes,
        cache_dir=args.cache_dir,
        log_dir=args.log_dir,
        refresh=args.refresh,
        dry_run=args.dry_run,
        skip_fetch=args.skip_fetch,
    )
    logger.info(f'[fund] 结果: {json.dumps(stats, ensure_ascii=False, default=str)}')

    if args.verify and not args.dry_run:
        div_path = Path(args.store_root) / 'factors' / FUND_DIVIDEND_FILENAME
        dividends = pl.read_parquet(div_path) if div_path.exists() else pl.DataFrame()
        sample = sorted(set(codes or [])) or [
            '510880.SH', '515100.SH', '510050.SH', '510300.SH', '510500.SH',
            '159915.SZ', '161907.SZ', '508000.SH', '180101.SZ', '159207.SZ']
        logger.info('[fund] 可逆性: %s', verify_reversibility(
            args.store_root, sample))
        logger.info('[fund] 跳空消化: %s', verify_gap_digestion(
            args.store_root, dividends))
        logger.info('[fund] 覆盖对齐: %s', verify_coverage(args.store_root))

    logger.info(f'[fund] 总耗时 {time.perf_counter() - started:.1f}s')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
