#!/usr/bin/env python3

"""本地列存构建器与 CLI。

用法（项目根目录执行）::

    # 全市场首建（个股 + 指数），只读 .day，不写库
    .venv\\Scripts\\python.exe -m src.data.local_store build --all

    # 预演，只统计不落盘
    .venv\\Scripts\\python.exe -m src.data.local_store build --all --dry-run

    # 增量：指定标的 / 指定日期之后
    .venv\\Scripts\\python.exe -m src.data.local_store build --codes 600000.SH,000001.SZ
    .venv\\Scripts\\python.exe -m src.data.local_store build --since 2026-10-01

    # 从远程库刷新因子与分红快照（必须显式 --allow-remote）
    .venv\\Scripts\\python.exe -m src.data.local_store refresh-factors --allow-remote

    # 用本地行情+分红补齐全市场因子快照（不写远程库，断点续跑）
    .venv\\Scripts\\python.exe -m src.data.local_store rebuild-factors

    # 查看列存规模
    .venv\\Scripts\\python.exe -m src.data.local_store stats

构建口径与 ``TdxHandler`` 完全一致：同一份 ``parse_day_file`` 原始记录 →
同一套复权因子来源（预计算因子优先，缺失时按分红本地计算）→ 同一个
``DataSourceBase._normalize_kline`` 出口，保证数值逐行等价。
"""

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.local_store.store import (
    STORE_VERSION,
    LocalFactorStore,
    LocalQuoteStore,
    resolve_store_root,
)
from src.data.sources.base import DataSourceBase
from src.data.tdx_day_parser import parse_day_file
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

    ``data_type`` 取 ``stock`` / ``index``；基金、债券等非行情标的跳过，
    保持与画图/回测实际读取的资产类型一致。
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
                continue
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
def build(
    config: Any,
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    *,
    data_type: str = 'all',
    codes: Iterable[str] | None = None,
    since: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """执行构建（全量或增量），返回统计信息（供 CLI 与测试使用）。"""
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
    skipped = 0
    total = len(targets)

    if dry_run:
        planned_rows = sum(t[2].stat().st_size // 32 for t in targets)
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
            fresh = build_frame(kind, ts_code, path, factor_store)
            if fresh.is_empty():
                skipped += 1
                continue
            full = True
            if since is not None:
                existing = quote_store.read_file(kind, ts_code)
                if existing is not None and not existing.is_empty():
                    fresh = _merge_incremental(existing, fresh.filter(pl.col('date') >= since))
                else:
                    # 无存量时只写入 tail，前缀不完整 → 标记为增量片段
                    fresh = fresh.filter(pl.col('date') >= since)
                    full = False
            if fresh.is_empty():
                skipped += 1
                continue
            written_rows += quote_store.write_daily(
                kind, ts_code, fresh, full=full,
                source_mtime_ns=path.stat().st_mtime_ns)
            written_files += 1
        except (OSError, ValueError, RuntimeError) as exc:
            skipped += 1
            logger.warning(f'[local-store] 构建 {ts_code} 失败: {exc}')

        if index % 500 == 0 or index == total:
            elapsed = time.perf_counter() - started
            logger.info(
                f'[local-store] 进度 {index}/{total}，已写 {written_files} 文件 / '
                f'{written_rows:,} 行，已用 {elapsed:.1f}s')

    quote_store.flush_meta()
    elapsed = time.perf_counter() - started
    stats = quote_store.stats()
    quote_store.write_state({
        'last_build': 'incremental' if (codes or since) else 'full',
        'data_type': data_type,
        'symbols': stats['symbols'],
        'rows': stats['rows'],
        'files': stats['files'],
        'disk_bytes': stats['disk_bytes'],
        'elapsed_s': round(elapsed, 3),
        'skipped': skipped,
    })
    logger.info(
        f'[local-store] 构建完成：{written_files} 文件 / {written_rows:,} 行，'
        f'耗时 {elapsed:.1f}s，列存共 {stats["rows"]:,} 行 / {stats["files"]} 文件 / '
        f'{stats["disk_bytes"] / 1024 / 1024:.1f} MB')
    return {
        'symbols': written_files,
        'rows': written_rows,
        'elapsed_s': round(elapsed, 3),
        'files': written_files,
        'skipped': skipped,
        'stats': stats,
    }


def refresh_factors(
    config: Any,
    factor_store: LocalFactorStore,
    *,
    allow_remote: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """从远程库一次性拉取复权因子与分红快照；未显式允许时拒绝联网。"""
    if not allow_remote:
        raise PermissionError(
            '刷新因子/分红需要显式 --allow-remote，避免运行期偷偷访问远程库')

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
    planned = {'factors': factors.height, 'dividends': dividends.height,
               'elapsed_s': round(elapsed, 3)}
    if dry_run:
        logger.info(f'[dry-run] 因子 {factors.height:,} 行 / 分红 {dividends.height:,} 行')
        return planned

    out_dir = factor_store.root / 'factors'
    out_dir.mkdir(parents=True, exist_ok=True)
    factor_path = out_dir / 'stock_adj_factor.parquet'
    dividend_path = out_dir / 'stock_dividend.parquet'
    factors.write_parquet(factor_path, compression='zstd', statistics=True)
    dividends.write_parquet(dividend_path, compression='zstd', statistics=True)

    meta = {
        'source': 'remote_mysql',
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
        f'[local-store] 因子/分红快照已刷新：{factors.height:,} / {dividends.height:,} 行，'
        f'耗时 {elapsed:.1f}s')
    return meta


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog='python -m src.data.local_store')
    sub = parser.add_subparsers(dest='command', required=True)

    build_cmd = sub.add_parser('build', help='构建/增量更新本地行情列存')
    build_cmd.add_argument('--all', action='store_true', help='全市场全量重建（默认）')
    build_cmd.add_argument('--codes', default='', help='逗号分隔的 ts_code 列表')
    build_cmd.add_argument('--since', default='', help='只更新该日期之后的记录（YYYY-MM-DD）')
    build_cmd.add_argument('--data-type', default='all', choices=['all', 'stock', 'index'])
    build_cmd.add_argument('--dry-run', action='store_true', help='只统计不落盘')

    factor_cmd = sub.add_parser('refresh-factors', help='从远程库刷新因子/分红快照')
    factor_cmd.add_argument('--allow-remote', action='store_true', help='显式允许访问远程库')
    factor_cmd.add_argument('--dry-run', action='store_true')

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

    sub.add_parser('stats', help='查看列存规模')
    return parser.parse_args(argv)


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
                        allow_remote=args.allow_remote, dry_run=args.dry_run)
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
        return 0

    codes = [c for c in args.codes.split(',') if c.strip()] if args.codes else None
    since = None
    if args.since:
        since = datetime.strptime(args.since, '%Y-%m-%d').date()
    result = build(
        config, quote_store, factor_store,
        data_type=args.data_type, codes=codes, since=since,
        dry_run=args.dry_run,
    )
    logger.info(f'构建结果: {result}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
