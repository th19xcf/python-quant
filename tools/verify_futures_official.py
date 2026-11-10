#!/usr/bin/env python3

r"""用中金所官方日交易数据抽样对拍本地期货日线。

为什么抽样而不是全量
====================
官方接口按「交易所 × 单日」返回，全历史约 4,040 次请求（≈88 分钟）；
本表的数据源（tushare）本身就是交易所口径的转存，抽样 20 个分布在不同年份的交易日
即可覆盖「早期/近期、交割月/非交割月、单品种/全品种」四类情形。

对拍口径
========
逐合约比对 ``close / settle / oi / volume`` 四个字段，要求**完全相等**（同一交易所口径，
不存在四舍五入差异）；任一字段不等即记录差异并计入失败。

用法::

    .venv\\Scripts\\python.exe tools/verify_futures_official.py --days 20
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import date
from pathlib import Path

import akshare as ak
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
QUOTES = STORE / 'quotes' / 'futures' / 'cffex'
STAGING = ROOT / '_staging' / 'multi_asset_tier2_20261110'
THROTTLE = 1.3


def pick_sample_days(n: int, seed: int = 20261110) -> list[date]:
    cont_path = STORE / 'meta' / 'multi_asset' / 'futures_continuous.parquet'
    if cont_path.exists():
        days = (pl.read_parquet(cont_path).select('trade_date').unique()
                .sort('trade_date')['trade_date'].to_list())
    else:  # 尚未落盘时用本地单合约文件反推
        frames = [pl.read_parquet(p).select('trade_date') for p in QUOTES.glob('*.parquet')]
        if not frames:
            raise SystemExit('本地没有 cffex 日线，先跑采集与构建')
        days = sorted({str(d) for d in pl.concat(frames)['trade_date'].unique().to_list()})
        days = [date(int(d[:4]), int(d[4:6]), int(d[6:8])) for d in days]
    rng = random.Random(seed)  # noqa: S311 —— 只用于分层抽样选日期，不涉及安全
    # 分层：把全区间切成 n 段，每段随机取 1 天，保证覆盖不同年份
    step = max(1, len(days) // n)
    picked = []
    for i in range(0, len(days), step):
        chunk = days[i:i + step]
        if chunk:
            picked.append(rng.choice(chunk))
    return sorted(picked)[:n]


def main(argv: list[str] | None = None) -> int:
    global QUOTES
    parser = argparse.ArgumentParser(description='中金所官方日数据抽样对拍')
    parser.add_argument('--days', type=int, default=20)
    parser.add_argument('--dir', default=str(QUOTES),
                        help='单合约日线目录；落盘前可指向 staging 目录')
    args = parser.parse_args(argv)

    QUOTES = Path(args.dir)
    if not QUOTES.exists():
        raise SystemExit(f'目录不存在: {QUOTES}')

    days = pick_sample_days(args.days)
    rows: list[dict] = []
    summary: list[dict] = []
    for d in days:
        time.sleep(THROTTLE)
        try:
            off = ak.get_futures_daily(d.strftime('%Y%m%d'), d.strftime('%Y%m%d'), 'CFFEX')
        except Exception as exc:  # noqa: BLE001
            msg = f'{type(exc).__name__}: {exc}'
            print(f'[ERR] {d} {msg[:100]}')
            summary.append({'date': str(d), 'status': 'error', 'detail': msg})
            continue
        if off is None or off.empty:
            print(f'[EMPTY] {d}')
            summary.append({'date': str(d), 'status': 'empty'})
            continue
        local_frames = []
        for sym in off['symbol'].unique().tolist():
            path = QUOTES / f'{sym}.CFX.parquet'
            if path.exists():
                local_frames.append(pl.read_parquet(path))
        if not local_frames:
            summary.append({'date': str(d), 'status': 'no_local'})
            print(f'[NO-LOCAL] {d}')
            continue
        local = (pl.concat(local_frames, how='vertical_relaxed')
                 .with_columns(pl.col('trade_date').cast(pl.Utf8))
                 .filter(pl.col('trade_date') == d.strftime('%Y%m%d')))
        off = off.assign(symbol=off['symbol'] + '.CFX', trade_date=d.strftime('%Y%m%d'))
        off = off[['symbol', 'close', 'settle', 'open_interest', 'volume']]
        joined = local.join(pl.from_pandas(off), left_on='ts_code', right_on='symbol',
                            how='inner', suffix='_off')
        checks = {
            'close': (pl.col('close') - pl.col('close_off')).abs().max(),
            'settle': (pl.col('settle') - pl.col('settle_off')).abs().max(),
            'oi': (pl.col('oi') - pl.col('open_interest')).abs().max(),
            'volume': (pl.col('vol') - pl.col('volume')).abs().max(),
        }
        diffs = {k: float(joined.select(v.alias(k))[k][0] or 0) for k, v in checks.items()}
        ok = all(v < 1e-6 for v in diffs.values())
        summary.append({'date': str(d), 'status': 'ok' if ok else 'mismatch',
                        'contracts_matched': joined.height,
                        'contracts_official': int(off.shape[0]), 'max_abs_diff': diffs})
        print(f"{d} {len(diffs)} 项 {'一致' if ok else '差异'} "
              f"合约 {joined.height}/{off.shape[0]} {diffs}")
        if not ok:
            rows.append(joined.filter(
                (pl.col('close') != pl.col('close_off')) | (pl.col('settle') != pl.col('settle_off'))
                | (pl.col('oi') != pl.col('open_interest')) | (pl.col('vol') != pl.col('volume'))
            ).select(['ts_code', 'trade_date', 'close', 'close_off', 'settle', 'settle_off']))

    bad = [s for s in summary if s['status'] != 'ok']
    print(f'\n抽样 {len(summary)} 日，一致 {len(summary) - len(bad)} 日，异常 {len(bad)} 日')
    if rows:
        pl.concat(rows, how='vertical_relaxed').write_parquet(
            STAGING / 'cffex_official_mismatch.parquet')
    (STAGING / 'cffex_official_verify.json').write_text(
        json.dumps({'checked_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
                    'days': len(summary), 'summary': summary},
                   ensure_ascii=False, indent=2), encoding='utf-8')
    return 0 if not bad else 2


if __name__ == '__main__':
    raise SystemExit(main())
