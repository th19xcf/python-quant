#!/usr/bin/env python3

"""核对推荐驱动回测台账里的单票路径与本地行情是否一致。

直接从本地列存 parquet 重算「建仓价 / 期末收盘价 / 累计收益率（含成本）」，
与 ``replay_positions_*.csv`` 的对应行逐项比对：

- 价格误差 ≤ 0.01 元
- 收益率（毛收益与净收益）误差 ≤ 1e-12（同一 double 运算，应为 0）

用法::

    .venv\\Scripts\\python.exe scripts/verify_advice_backtest.py \
        --ledger reports/advice/replay_positions_short.csv \
        --ts-code 002275.SZ
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import polars as pl

from src.data.local_store.store import LocalQuoteStore
from src.utils.config import get_config

IMPACT_RATE = 0.001
COMMISSION_RATE = 0.0003
MIN_COMMISSION = 5.0
STAMP_DUTY_RATE = 0.001


def _load_raw(code: str) -> pl.DataFrame:
    root = get_config().data.local_store_path
    frame = LocalQuoteStore(root).read_file('stock', code)
    if frame is None or frame.is_empty():
        raise SystemExit(f'本地列存没有 {code} 的行情')
    frame = frame.with_columns(pl.col('date').cast(pl.Date))
    if 'hfq_factor' not in frame.columns:
        frame = frame.with_columns(pl.lit(1.0).alias('hfq_factor'))
    return frame.with_columns(pl.col('hfq_factor').cast(pl.Float64, strict=False)
                              .fill_null(1.0))


def _row(frame: pl.DataFrame, day: str) -> dict:
    rows = frame.filter(pl.col('date') == date.fromisoformat(day)).to_dicts()
    if not rows:
        raise SystemExit(f'本地行情缺 {day}')
    return rows[0]


def main() -> int:
    parser = argparse.ArgumentParser(description='核对回测单票路径')
    parser.add_argument('--ledger', required=True, help='replay_positions_*.csv')
    parser.add_argument('--ts-code', required=True, help='标的 ts_code')
    parser.add_argument('--entry-date', default=None, help='指定建仓日（同日多次建仓时用）')
    args = parser.parse_args()

    ledger = pl.read_csv(args.ledger)
    rows = ledger.filter(pl.col('ts_code') == args.ts_code)
    if args.entry_date:
        rows = rows.filter(pl.col('entry_date') == args.entry_date)
    if rows.is_empty():
        raise SystemExit(f'台账里没有 {args.ts_code} 的记录: {args.ledger}')
    row = rows.to_dicts()[0]

    raw = _load_raw(args.ts_code)
    entry = _row(raw, row['entry_date'])
    last = _row(raw, row['last_date'])
    prior = raw.filter(pl.col('date') < date.fromisoformat(row['entry_date'])
                       ).sort('date').tail(1).to_dicts()
    signal = prior[0] if prior else None

    # 成交口径：T+1 开盘价（后复权）+ 买入冲击 0.1%
    hfq_open = float(entry['open']) * float(entry['hfq_factor'])
    exec_entry = hfq_open * (1.0 + IMPACT_RATE)
    hfq_close = float(last['close']) * float(last['hfq_factor'])
    return_pct = (hfq_close / exec_entry - 1.0) * 100.0

    shares = int(row['shares'])
    notional_buy = shares * exec_entry
    buy_commission = max(MIN_COMMISSION, abs(notional_buy) * COMMISSION_RATE)
    basis = shares * exec_entry + buy_commission
    exit_price = hfq_close * (1.0 - IMPACT_RATE)
    notional_sell = shares * exit_price
    sell_cost = (max(MIN_COMMISSION, abs(notional_sell) * COMMISSION_RATE)
                 + notional_sell * STAMP_DUTY_RATE)
    net_return_pct = ((notional_sell - sell_cost) / basis - 1.0) * 100.0

    price_diff = abs(exec_entry - float(row['entry_price']))
    return_diff = abs(return_pct - float(row['return_pct']))
    net_diff = abs(net_return_pct - float(row['net_return_pct']))

    out = sys.stdout.write
    ref_price = (signal['raw_close'] if signal and 'raw_close' in signal
                 else signal['close'] if signal else None)
    ref_diff = (abs(float(ref_price) - float(row['entry_ref_price']))
                if ref_price is not None else 0.0)
    out(f'标的: {args.ts_code}  状态: {row["status"]}\n')
    out(f'建仓日: {row["entry_date"]}  信号日: '
        f'{signal["date"] if signal else "N/A"}  '
        f'推荐日收盘(raw): {float(ref_price):.4f}  '
        f'台账 entry_ref_price: {float(row["entry_ref_price"]):.4f}  '
        f'差 {ref_diff:.2e} 元\n')
    out(f'建仓价(后复权开盘×1.001): 台账 {float(row["entry_price"]):.10f} / '
        f'本地重算 {exec_entry:.10f}  差 {price_diff:.2e} 元\n')
    out(f'期末日: {row["last_date"]}  收盘(后复权): {hfq_close:.6f}\n')
    out(f'毛收益: 台账 {float(row["return_pct"]):.10f}% / '
        f'本地重算 {return_pct:.10f}%  差 {return_diff:.2e}\n')
    out(f'净收益(含双边佣金+印花税+冲击): 台账 {float(row["net_return_pct"]):.10f}% / '
        f'本地重算 {net_return_pct:.10f}%  差 {net_diff:.2e}\n')
    out(f'持有交易日数: {row["hold_days"]}\n')

    ok = price_diff <= 0.01 and return_diff <= 1e-12 and net_diff <= 1e-12
    out('PASS\n' if ok else 'FAIL\n')
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
