#!/usr/bin/env python3

r"""P1 参考数据历史补采（AkShare，可续跑，默认只写 staging）。

三个 target：

* ``industry``    —— 巨潮按代码的历史行业分类变更，构造 PIT 区间；
* ``suspension``  —— 按交易日的权威停复牌公告；
* ``membership``  —— 中证指数当日成分快照（累积后差分可得进出区间）。

**重要**：这些源都不提供完整历史（停牌源端只保留近年、指数成分只有当日
快照、退市股无行业变更记录）。试点/分批结果一律标 ``complete=false``，
默认只落 ``artifacts/p1_backfill/reference/``；只有全量补齐并人工确认后
才应 ``--commit`` 写进生产 ``meta/``，否则会让扩展契约误判数据已就绪。

用法::

    python tools/backfill_reference_history.py --target industry --limit 50
    python tools/backfill_reference_history.py --target suspension --limit 20
    python tools/backfill_reference_history.py --target membership
    python tools/backfill_reference_history.py --target industry --commit   # 全量确认后
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

DEFAULT_ROOT = ROOT / 'data' / 'local_store'
DEFAULT_STAGING = ROOT / 'artifacts' / 'p1_backfill' / 'reference'
INDEX_CODES = ('000300', '000905', '000852', '000016')


def _ts_code(raw: str, exchange: str | None = None) -> str:
    code = str(raw).zfill(6)
    if code.startswith(('6', '9')):
        suffix = 'SH'
    elif code.startswith(('0', '2', '3')):
        suffix = 'SZ'
    else:
        suffix = 'BJ'
    if exchange:
        suffix = str(exchange).upper()[:2]
    return f'{code}.{suffix}'


def _stock_codes(root: Path, limit: int) -> list[str]:
    index = pl.read_parquet(root / 'meta' / 'daily_index.parquet')
    codes = index.filter(
        (pl.col('data_type') == 'stock') & (pl.col('end') >= date(2026, 11, 1))
    )['ts_code'].to_list()
    return sorted(codes)[:limit]


def collect_industry(root: Path, limit: int, sleep: float) -> tuple[pl.DataFrame, dict]:
    import akshare as ak  # noqa: PLC0415

    codes = _stock_codes(root, limit)
    frames, failed = [], []
    for i, code in enumerate(codes, start=1):
        try:
            raw = ak.stock_industry_change_cninfo(
                symbol=code[:6], start_date='19900101',
                end_date=date.today().strftime('%Y%m%d'))
            if raw is None or len(raw) == 0:
                continue
            df = pl.from_pandas(raw).with_columns(pl.lit(code).alias('ts_code'))
            frames.append(df)
        except Exception as exc:  # noqa: BLE001
            failed.append(f'{code}: {type(exc).__name__}')
        if sleep:
            time.sleep(sleep)
        if i % 50 == 0:
            print(f'  industry {i}/{len(codes)}', flush=True)
    if not frames:
        return pl.DataFrame(), {'scanned': len(codes), 'failed': failed}
    merged = pl.concat(frames, how='vertical_relaxed')
    out = merged.select([
        pl.col('ts_code'),
        pl.col('变更日期').cast(pl.Date).alias('effective_date'),
        pl.col('行业编码').cast(pl.Utf8).alias('industry_code'),
        pl.col('行业大类').cast(pl.Utf8).alias('industry_name'),
        pl.col('分类标准').cast(pl.Utf8).alias('standard'),
        pl.lit('cninfo:stock_industry_change_cninfo').alias('source'),
    ]).filter(pl.col('effective_date').is_not_null()).sort(
        ['ts_code', 'standard', 'effective_date'])
    # 按 (代码, 标准) 把变更点转成 PIT 区间
    out = out.with_columns(
        pl.col('effective_date').shift(-1).over(
            ['ts_code', 'standard']).alias('_next')
    ).with_columns(
        pl.when(pl.col('_next').is_not_null())
          .then(pl.col('_next') - timedelta(days=1))
          .otherwise(pl.lit(date.today()))
          .alias('end_date')
    ).drop('_next')
    return out, {
        'scanned': len(codes), 'codes_with_events': out['ts_code'].n_unique(),
        'rows': out.height, 'earliest': str(out['effective_date'].min()),
        'latest': str(out['effective_date'].max()),
        'standards': out['standard'].unique().to_list()[:10], 'failed': failed,
    }


def collect_suspension(root: Path, limit: int, sleep: float) -> tuple[pl.DataFrame, dict]:
    import akshare as ak  # noqa: PLC0415

    cal = pl.read_parquet(root / 'meta' / 'suspension' / 'suspension_daily.parquet',
                          columns=['trade_date'])
    days = sorted({d for d in cal['trade_date'].to_list() if d}, reverse=True)[:limit]
    frames, failed = [], []
    for i, day in enumerate(days, start=1):
        try:
            raw = ak.news_trade_notify_suspend_baidu(date=day.strftime('%Y%m%d'))
            if raw is None or len(raw) == 0:
                continue
            df = pl.from_pandas(raw).with_columns(pl.lit(day).alias('trade_date'))
            frames.append(df)
        except Exception as exc:  # noqa: BLE001
            failed.append(f'{day}: {type(exc).__name__}')
        if sleep:
            time.sleep(sleep)
        if i % 20 == 0:
            print(f'  suspension {i}/{len(days)}', flush=True)
    if not frames:
        return pl.DataFrame(), {'scanned_days': len(days), 'failed': failed}
    merged = pl.concat(frames, how='vertical_relaxed')
    out = merged.select([
        pl.col('trade_date').cast(pl.Date),
        pl.col('股票代码').map_elements(
            lambda v: _ts_code(v), return_dtype=pl.Utf8).alias('ts_code'),
        pl.col('停牌时间').cast(pl.Utf8).alias('suspend_time'),
        pl.col('复牌时间').cast(pl.Utf8).alias('resume_time'),
        pl.col('停牌事项说明').cast(pl.Utf8).alias('reason'),
        pl.col('公告日期').cast(pl.Utf8).alias('announce_date'),
        pl.lit('baidu:trade_notify_suspend').alias('source'),
    ]).filter(pl.col('ts_code').is_not_null()).unique(
        subset=['trade_date', 'ts_code'], keep='first').sort(['trade_date', 'ts_code'])
    return out, {
        'scanned_days': len(days), 'rows': out.height,
        'day_min': str(out['trade_date'].min()), 'day_max': str(out['trade_date'].max()),
        'codes': out['ts_code'].n_unique(), 'failed': failed,
    }


def collect_membership(sleep: float) -> tuple[pl.DataFrame, dict]:
    import socket  # noqa: PLC0415

    import akshare as ak  # noqa: PLC0415

    # 中证接口偶发无响应且 akshare 未暴露 timeout；全局 socket 超时兜底，
    # 否则单个指数就能把整个补采脚本挂住（实测曾卡死 8 分钟）。
    socket.setdefaulttimeout(20)
    frames, failed = [], []
    for index_code in INDEX_CODES:
        try:
            raw = ak.index_stock_cons_csindex(symbol=index_code)
            if raw is None or len(raw) == 0:
                continue
            df = pl.from_pandas(raw)
            frames.append(df)
        except Exception as exc:  # noqa: BLE001
            failed.append(f'{index_code}: {type(exc).__name__}')
        if sleep:
            time.sleep(sleep)
    if not frames:
        return pl.DataFrame(), {'indices': list(INDEX_CODES), 'failed': failed}
    merged = pl.concat(frames, how='vertical_relaxed')
    out = merged.select([
        pl.col('日期').cast(pl.Date).alias('trade_date'),
        pl.col('指数代码').cast(pl.Utf8).alias('index_code'),
        pl.col('成分券代码').map_elements(
            lambda v: _ts_code(v), return_dtype=pl.Utf8).alias('ts_code'),
        pl.col('成分券名称').cast(pl.Utf8).alias('name'),
        pl.lit('csindex:index_stock_cons_csindex').alias('source'),
    ]).unique(subset=['trade_date', 'index_code', 'ts_code'], keep='first').sort(
        ['index_code', 'trade_date', 'ts_code'])
    return out, {
        'indices': out['index_code'].unique().to_list(),
        'rows': out.height, 'date_min': str(out['trade_date'].min()),
        'date_max': str(out['trade_date'].max()), 'failed': failed,
    }


NAMES = {
    'industry': 'industry_change_pit',
    'suspension': 'official_suspension',
    'membership': 'membership_snapshots',
}
PRODUCTION = {
    'industry': Path('meta/industry_pit/industry_change_pit.parquet'),
    'suspension': Path('meta/suspension/official_suspension.parquet'),
    'membership': Path('meta/index_membership/membership_snapshots.parquet'),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='P1 参考数据历史补采（可续跑）')
    parser.add_argument('--target', required=True,
                        choices=['industry', 'suspension', 'membership'])
    parser.add_argument('--store-root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--staging', type=Path, default=DEFAULT_STAGING)
    parser.add_argument('--limit', type=int, default=50,
                        help='industry: 代码数；suspension: 交易日数')
    parser.add_argument('--sleep', type=float, default=0.1)
    parser.add_argument('--commit', action='store_true',
                        help='写入生产 meta 目录（全量补齐并确认后才应使用）')
    args = parser.parse_args(argv)

    if args.target == 'industry':
        frame, stats = collect_industry(args.store_root, args.limit, args.sleep)
    elif args.target == 'suspension':
        frame, stats = collect_suspension(args.store_root, args.limit, args.sleep)
    else:
        frame, stats = collect_membership(args.sleep)

    args.staging.mkdir(parents=True, exist_ok=True)
    target = args.staging / f'{NAMES[args.target]}.parquet'
    if not frame.is_empty():
        frame.write_parquet(target, compression='zstd', statistics=True)
    if args.commit and not frame.is_empty():
        prod = args.store_root / PRODUCTION[args.target]
        prod.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(prod, compression='zstd', statistics=True)

    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'target': args.target, 'limit': args.limit,
        'staging_path': str(target), 'committed': args.commit,
        'complete': False,
        'note': '分批补采结果，历史未覆盖完整；不得据此宣称扩展契约已就绪',
        'stats': stats,
    }
    report_path = args.staging / f'{NAMES[args.target]}_report.json'
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                                      default=str), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
