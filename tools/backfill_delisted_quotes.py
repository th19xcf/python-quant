#!/usr/bin/env python3

r"""补采停更/退市个股的尾部行情（AkShare，默认只落 staging）。

背景：``meta/universe/stale_status.parquet`` 标记了 241 只停更个股。实测
本地 TDX 已保留它们**直到停更日**的完整历史，但少数标的在停更日之后
仍有零星交易（例如 000005.SZ 本地止于 2024-03-05，BaoStock 有到
2024-04-26 的记录）。本工具只补 ``date > 本地末日`` 的新行，不覆盖历史。

默认只把待合并分片写到 ``_staging/p1_backfill/delisted_quotes/``（仓库根，不在
``data/`` 下）并出报告；
``--commit`` 才追加进 ``quotes/stock/**/*.parquet`` 并同步 daily_index。
因子列按本地末行前向填充（退市后无新除权），commit 后应执行
``rescan-meta`` 与 ``verify-quote-factors`` 复验。

用法::

    python tools/backfill_delisted_quotes.py                  # staging + 报告
    python tools/backfill_delisted_quotes.py --max-codes 30   # 小批量试点
    python tools/backfill_delisted_quotes.py --commit         # 合并进生产列存
    python tools/backfill_delisted_quotes.py --from-staging --commit
    python tools/backfill_delisted_quotes.py --normalize-precision --commit

新行的 ``pct_chg`` 一律由 ``close/pre_close`` 派生全精度值（源端只给 2 位小数，
直接用会触发 ``quality`` 的 ``pct_chg_mismatch`` 告警）；``--normalize-precision``
用于修复早期已落盘的补采行。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402
from src.data.staging import staging_dir  # noqa: E402

DEFAULT_ROOT = ROOT / 'data' / 'local_store'
#: 候选目录统一由 ``src/data/staging.py`` 定义，不再另立一套 staging 根
DEFAULT_STAGING = staging_dir('p1_backfill') / 'delisted_quotes'

COLS = ['date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor']
RENAME = {
    '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high',
    '最低': 'low', '成交量': 'volume', '成交额': 'amount',
    '涨跌幅': 'pct_chg', '涨跌额': 'change',
}


def _quote_path(root: Path, code: str) -> Path:
    market = 'sh' if code.endswith('.SH') else 'sz' if code.endswith('.SZ') else 'bj'
    return root / 'quotes' / 'stock' / market / f'{code}.parquet'


def _stale_codes(root: Path, limit: int | None) -> list[str]:
    frame = pl.read_parquet(root / 'meta' / 'universe' / 'stale_status.parquet')
    codes = frame.filter(
        (pl.col('action') == 'exclude') & (pl.col('asset_class') == 'stock')
    )['ts_code'].to_list()
    return sorted(codes)[:limit] if limit else sorted(codes)


def _fetch(code: str, start: date, end: date):
    import akshare as ak  # noqa: PLC0415

    raw = ak.stock_zh_a_hist(
        symbol=code[:6], period='daily',
        start_date=start.strftime('%Y%m%d'), end_date=end.strftime('%Y%m%d'),
        adjust='')
    if raw is None or raw.empty:
        return None
    pdf = raw.rename(columns=RENAME)
    keep = [c for c in RENAME.values() if c in pdf.columns]
    df = pl.from_pandas(pdf[keep])
    if 'date' in df.columns:
        df = df.with_columns(pl.col('date').cast(pl.Date))
    return df


def _new_rows(
    code: str, local: pl.DataFrame, remote: pl.DataFrame,
) -> tuple[pl.DataFrame, float | None]:
    """构造新行；返回 (新行, 首行 pre_close 与本地末收的缺口)。

    用源端「涨跌额」反推 pre_close，而不是直接沿用本地末收：若远端起点
    与本地末行之间还有缺口或除权，两者会对不上，必须靠这个差额拦住。
    """
    last = local['date'].max()
    tail = local.tail(1)
    qfq = float(tail['qfq_factor'][0])
    hfq = float(tail['hfq_factor'][0])
    last_close = float(tail['close'][0])
    fresh = remote.filter(pl.col('date') > last).sort('date')
    if fresh.is_empty():
        empty = pl.DataFrame(schema=dict.fromkeys(COLS, pl.Float64) | {
            'date': pl.Date, 'ts_code': pl.Utf8})
        return empty, None
    rows = []
    first_gap: float | None = None
    for index, row in enumerate(fresh.iter_rows(named=True)):
        close = float(row['close'])
        change = float(row.get('change') or 0.0)
        implied_pre = close - change
        if index == 0:
            first_gap = abs(implied_pre - last_close)
        source_pct = float(row.get('pct_chg') or 0.0)
        # 源端「涨跌幅」只保留 2 位小数，而本地列存与 quality 契约要求
        # pct_chg 能由 close/pre_close 以 1e-6 精度复算。落盘用派生全精度值，
        # 源端值仅留在 source_pct_chg 供校验（commit 时会被裁掉）。
        derived_pct = (
            (close / implied_pre - 1) * 100
            if implied_pre > 0 else source_pct
        )
        rows.append({
            'date': row['date'],
            'ts_code': code,
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': close,
            'pre_close': implied_pre,
            'volume': float(row.get('volume') or 0.0),
            'amount': float(row.get('amount') or 0.0),
            'pct_chg': derived_pct,
            'source_pct_chg': source_pct,
            'change': change,
            'qfq_factor': qfq,
            'hfq_factor': hfq,
        })
    return pl.DataFrame(rows, schema_overrides={'date': pl.Date}), first_gap


def _validate(rows: pl.DataFrame, first_gap: float | None) -> list[str]:
    if rows.is_empty():
        return []
    problems: list[str] = []
    bad_price = rows.filter(
        (pl.col('open') <= 0) | (pl.col('high') <= 0)
        | (pl.col('low') <= 0) | (pl.col('close') <= 0)).height
    bad_ohlc = rows.filter(
        (pl.col('high') < pl.col('open')) | (pl.col('high') < pl.col('close'))
        | (pl.col('low') > pl.col('open')) | (pl.col('low') > pl.col('close'))
    ).height
    future = rows.filter(pl.col('date') > date.today()).height
    dup = rows.height - rows['date'].n_unique()
    derived = (pl.col('close') / pl.col('pre_close') - 1) * 100
    valid_pre = pl.col('pre_close') > 0
    bad_pct = rows.filter(
        valid_pre & ((derived - pl.col('pct_chg')).abs() > 1e-6)
    ).height
    # 源端 2 位小数涨跌幅只做粗校验（>1 个百分点视为源数据自相矛盾），
    # 不再拿它当落盘值，否则会触发 quality 的 pct_chg_mismatch 告警。
    bad_source = 0
    if 'source_pct_chg' in rows.columns:
        bad_source = rows.filter(
            valid_pre
            & ((derived - pl.col('source_pct_chg')).abs() > 1.0)
        ).height
    if bad_price:
        problems.append(f'非正价 {bad_price} 行')
    if bad_ohlc:
        problems.append(f'OHLC 越界 {bad_ohlc} 行')
    if future:
        problems.append(f'未来日期 {future} 行')
    if dup:
        problems.append(f'重复日期 {dup} 行')
    if bad_pct:
        problems.append(f'派生涨跌幅与 pre_close 不自洽 {bad_pct} 行')
    if bad_source:
        problems.append(f'源端涨跌幅与复算值偏离过大 {bad_source} 行')
    if first_gap is not None and first_gap > 0.02:
        problems.append(
            f'首行与本地末收缺口 {first_gap:.4f}，疑似中间缺交易日或除权')
    return problems


def run(
    root: Path, staging: Path, max_codes: int | None,
    commit: bool, end: date, sleep: float,
) -> dict[str, Any]:
    import akshare  # noqa: F401,PLC0415  # 提前暴露缺失依赖

    staging.mkdir(parents=True, exist_ok=True)
    codes = _stale_codes(root, max_codes)
    summary: dict[str, Any] = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'end': end.isoformat(), 'commit': commit,
        'scanned_codes': len(codes), 'codes_with_new_rows': 0,
        'new_rows': 0, 'failed_codes': [], 'validation_problems': {},
        'per_code': [],
    }
    for index, code in enumerate(codes, start=1):
        path = _quote_path(root, code)
        if not path.is_file():
            continue
        try:
            local = pl.read_parquet(path)
            last = local['date'].max()
            if last >= end:
                continue
            remote = _fetch(code, last + timedelta(days=1), end)
            if remote is None:
                continue
            rows, first_gap = _new_rows(code, local, remote)
            if rows.is_empty():
                continue
            problems = _validate(rows, first_gap)
            if problems:
                summary['validation_problems'][code] = problems
                continue
            shard = staging / f'{code}.parquet'
            write_parquet_atomic(rows, shard)
            summary['codes_with_new_rows'] += 1
            summary['new_rows'] += rows.height
            summary['per_code'].append({
                'code': code, 'local_last': str(last),
                'new_last': str(rows['date'].max()), 'rows': rows.height,
            })
            if commit:
                merged = pl.concat([local, rows.select(local.columns)],
                                   how='vertical_relaxed').unique(
                    subset=['date'], keep='last').sort('date')
                write_parquet_atomic(
                    merged, path, compression='zstd', statistics=True)
        except Exception as exc:  # noqa: BLE001
            summary['failed_codes'].append(f'{code}: {type(exc).__name__}: {exc}')
        if sleep:
            time.sleep(sleep)
        if index % 50 == 0:
            print(f'  进度 {index}/{len(codes)}，已补 {summary["new_rows"]} 行', flush=True)

    report = staging / 'backfill_report.json'
    report.write_text(json.dumps(summary, ensure_ascii=False, indent=2),
                      encoding='utf-8')
    summary['report'] = str(report)
    return summary


def commit_from_staging(root: Path, staging: Path) -> dict[str, Any]:
    """把已校验的 staging 分片确定性合并进生产行情（不联网、不重抓）。"""
    merged_codes: list[dict[str, Any]] = []
    for shard in sorted(staging.glob('*.parquet')):
        code = shard.stem
        path = _quote_path(root, code)
        if not path.is_file():
            continue
        rows = pl.read_parquet(shard)
        if rows.is_empty():
            continue
        local = pl.read_parquet(path)
        yesterday = local['date'].max()
        added = rows.filter(pl.col('date') > yesterday)
        if added.is_empty():
            continue
        frame = pl.concat([local, added.select(local.columns)],
                          how='vertical_relaxed').unique(
            subset=['date'], keep='last').sort('date')
        write_parquet_atomic(frame, path, compression='zstd', statistics=True)
        merged_codes.append({
            'code': code, 'rows_before': local.height,
            'rows_after': frame.height, 'added': added.height,
            'last': str(frame['date'].max()),
        })
    return {'merged_codes': len(merged_codes), 'added_rows': sum(
        item['added'] for item in merged_codes), 'detail': merged_codes}


def normalize_committed_precision(
    root: Path, staging: Path, commit: bool = False,
) -> dict[str, Any]:
    """把早期补采已落盘行的 2 位小数 pct_chg 归一为 close/pre_close 全精度。

    只针对 staging 分片里出现过的 ``(code, date)``，历史行一律不动。
    默认 dry-run，``commit=True`` 才写回生产 parquet。
    """
    details: list[dict[str, Any]] = []
    for shard in sorted(staging.glob('*.parquet')):
        code = shard.stem
        path = _quote_path(root, code)
        if not path.is_file():
            continue
        shard_frame = pl.read_parquet(shard)
        dates = shard_frame['date'].to_list()
        if not dates:
            continue
        local = pl.read_parquet(path)
        target = local.filter(pl.col('date').is_in(dates))
        if target.is_empty():
            continue
        derived = (pl.col('close') / pl.col('pre_close') - 1) * 100
        fixable = pl.col('date').is_in(dates) & (pl.col('pre_close') > 0)
        mismatched = target.filter(
            fixable & ((derived - pl.col('pct_chg')).abs() > 1e-6)
        ).height
        if not mismatched:
            continue
        if commit:
            frame = local.with_columns(
                pl.when(fixable).then(derived)
                .otherwise(pl.col('pct_chg')).alias('pct_chg')
            )
            write_parquet_atomic(frame, path, compression='zstd', statistics=True)
            # 同步修正 staging 分片，避免日后 --from-staging --commit 把舍入值写回。
            if {'pct_chg', 'close', 'pre_close'} <= set(shard_frame.columns):
                shard_derived = (
                    pl.col('close') / pl.col('pre_close') - 1
                ) * 100
                shard_frame = shard_frame.with_columns(
                    pl.when(pl.col('pre_close') > 0).then(shard_derived)
                    .otherwise(pl.col('pct_chg')).alias('pct_chg')
                )
                write_parquet_atomic(shard_frame, shard)
        details.append({
            'code': code, 'rows': target.height,
            'mismatched_rows': mismatched,
        })
    return {
        'mode': 'normalize-precision', 'commit': commit,
        'codes': len(details),
        'rows': sum(item['mismatched_rows'] for item in details),
        'detail': details,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='停更/退市个股尾部行情补采')
    parser.add_argument('--store-root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--staging', type=Path, default=DEFAULT_STAGING)
    parser.add_argument('--max-codes', type=int, default=None)
    parser.add_argument('--end', default=date.today().isoformat())
    parser.add_argument('--sleep', type=float, default=0.15)
    parser.add_argument('--commit', action='store_true',
                        help='追加到 quotes/stock；默认只写 staging')
    parser.add_argument('--from-staging', action='store_true',
                        help='用已校验的 staging 分片落盘，不联网重抓（需 --commit）')
    parser.add_argument('--normalize-precision', action='store_true',
                        help='把已落盘补采行的 pct_chg 归一为全精度（默认 dry-run，'
                             '加 --commit 才写回）')
    args = parser.parse_args(argv)
    if args.normalize_precision:
        result = normalize_committed_precision(
            args.store_root, args.staging, args.commit)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.from_staging:
        if not args.commit:
            parser.error('--from-staging 必须同时给 --commit')
        result = commit_from_staging(args.store_root, args.staging)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    result = run(args.store_root, args.staging, args.max_codes,
                 args.commit, date.fromisoformat(args.end), args.sleep)
    print(json.dumps({k: v for k, v in result.items() if k != 'per_code'},
                     ensure_ascii=False, indent=2))
    print(f'前 10 只: {result["per_code"][:10]}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
