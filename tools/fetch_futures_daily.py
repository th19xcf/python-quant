#!/usr/bin/env python3

r"""采集期货单合约日线到 staging（tushare `fut_daily`，单连接串行 + 节流）。

为什么按合约逐个取，而不是按交易日批量取
========================================
同一份数据两条路径的请求数差一个量级：中金所全历史约 4,040 个交易日，
按日批量要 4,040 次请求（≈88 分钟）；按合约只需 696 次（≈15 分钟）。
两者都返回「全部合约 × 当日」的同一份交易所原始数据，结果等价。

断点与失败留证
==============
- ``<staging>/daily/<exchange>/<ts_code>.parquet``：单合约落盘，存在且非空即跳过；
- ``<staging>/progress.csv``：逐合约一行（``ts_code, rows, first_date, last_date, status, fetched_at``）；
- ``<staging>/failed.csv``：空返回与异常逐条登记，重跑时只重试这些；
- 连续 3 次失败或命中限流关键字立即整体停压退出（退出码 3），不重试加压。

用法::

    .venv\\Scripts\\python.exe tools/fetch_futures_daily.py --market CFFEX
    .venv\\Scripts\\python.exe tools/fetch_futures_daily.py --market SHFE --market INE
    .venv\\Scripts\\python.exe tools/fetch_futures_daily.py --market CFFEX --limit 5
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import tushare as ts

ROOT = Path(__file__).resolve().parents[1]
STAGING = ROOT / '_staging' / 'multi_asset_tier2_20261110'
DAILY = STAGING / 'daily'
TOKEN_FILE = ROOT / '.secrets' / 'tushare_token.txt'

#: 纳入本次采集的品种（其余品种不落盘，避免把 3,600 个无关合约全拉一遍）
VARIETIES = {
    'CFFEX': ('IF', 'IH', 'IC', 'IM', 'T', 'TF', 'TS', 'TL'),
    'SHFE': ('AG',),
    'INE': ('SC',),
}

#: 命中任一关键字即判定为限流/封禁，立即停压
RATE_LIMIT_HINTS = ('每分钟', '每天', '超过访问', '频率', 'limit', 'too many', 'forbidden')

DEFAULT_THROTTLE = 1.3
MAX_CONSECUTIVE_FAILURES = 3


def _pro():
    return ts.pro_api(TOKEN_FILE.read_text(encoding='utf-8').strip())


def _append_csv(path: Path, row: dict[str, object], header: list[str]) -> None:
    new = not path.exists()
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if new:
            writer.writeheader()
        writer.writerow(row)


def load_contracts(pro, exchange: str, *, throttle: float) -> pd.DataFrame:
    """取该交易所全部普通合约，按品种白名单过滤。"""
    time.sleep(throttle)
    basic = pro.fut_basic(exchange=exchange, fut_type='1')
    if basic is None or basic.empty:
        raise RuntimeError(f'fut_basic({exchange}) 返回空，先排查链路再接数据')
    keep = basic[basic['fut_code'].isin(VARIETIES[exchange])].copy()
    keep = keep.sort_values(['fut_code', 'delist_date', 'ts_code']).reset_index(drop=True)
    STAGING.mkdir(parents=True, exist_ok=True)
    keep.to_csv(STAGING / f'contracts_{exchange.lower()}.csv', index=False, encoding='utf-8')
    return keep


def fetch_exchange(pro, exchange: str, *, throttle: float, limit: int | None) -> dict[str, int]:
    contracts = load_contracts(pro, exchange, throttle=throttle)
    out_dir = DAILY / exchange.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    progress = STAGING / f'progress_{exchange.lower()}.csv'
    failed = STAGING / f'failed_{exchange.lower()}.csv'
    prog_header = ['ts_code', 'rows', 'first_date', 'last_date', 'status', 'fetched_at']
    fail_header = ['ts_code', 'reason', 'detail', 'checked_at']

    stats = {'total': len(contracts), 'done': 0, 'skipped': 0, 'empty': 0, 'error': 0}
    consecutive_fail = 0
    todo = contracts['ts_code'].tolist()
    if limit is not None:
        todo = todo[:limit]

    for idx, code in enumerate(todo, 1):
        target = out_dir / f'{code}.parquet'
        if target.exists() and target.stat().st_size > 0:
            stats['skipped'] += 1
            continue
        time.sleep(throttle)
        now = datetime.now().isoformat(timespec='seconds')
        try:
            df = pro.fut_daily(ts_code=code)
        except Exception as exc:  # noqa: BLE001
            msg = f'{type(exc).__name__}: {exc}'
            stats['error'] += 1
            consecutive_fail += 1
            _append_csv(failed, {'ts_code': code, 'reason': 'exception', 'detail': msg,
                                 'checked_at': now}, fail_header)
            print(f'[ERR] {code} {msg[:120]}')
            if any(h in msg.lower() for h in RATE_LIMIT_HINTS):
                print('[STOP] 命中限流/封禁关键字，立即停压，不重试。')
                sys.exit(3)
            if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
                print(f'[STOP] 连续 {consecutive_fail} 次失败，停压退出。')
                sys.exit(3)
            continue
        consecutive_fail = 0
        if df is None or df.empty:
            stats['empty'] += 1
            _append_csv(failed, {'ts_code': code, 'reason': 'empty', 'detail': '',
                                 'checked_at': now}, fail_header)
            print(f'[EMPTY] {code}')
            continue
        df = df.sort_values('trade_date').reset_index(drop=True)
        df.to_parquet(target, index=False)
        stats['done'] += 1
        _append_csv(progress, {
            'ts_code': code, 'rows': len(df),
            'first_date': str(df['trade_date'].iloc[0]),
            'last_date': str(df['trade_date'].iloc[-1]),
            'status': 'ok', 'fetched_at': now,
        }, prog_header)
        if idx % 25 == 0 or idx == len(todo):
            print(f'[{exchange}] {idx}/{len(todo)} done={stats["done"]} '
                  f'skip={stats["skipped"]} empty={stats["empty"]} err={stats["error"]}')

    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='采集期货单合约日线到 staging')
    parser.add_argument('--market', action='append', choices=sorted(VARIETIES),
                        help='可重复；缺省为全部')
    parser.add_argument('--throttle', type=float, default=DEFAULT_THROTTLE,
                        help='每次请求前的休眠秒数（默认 1.3）')
    parser.add_argument('--limit', type=int, default=None, help='只取前 N 个合约（调试用）')
    args = parser.parse_args(argv)

    markets = args.market or sorted(VARIETIES)
    pro = _pro()
    summary: dict[str, dict[str, int]] = {}
    for market in markets:
        print(f'=== {market} ===')
        summary[market] = fetch_exchange(pro, market, throttle=args.throttle, limit=args.limit)
        print(f'{market}: {summary[market]}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
