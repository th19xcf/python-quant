#!/usr/bin/env python3
"""样本外前向记录（月度）：只记录实际结果，不调参、不生成交易建议。

用法（可被外部定时任务直接调用）：
    python tools/oos_forward_monthly.py --mode auto            # 结算上月 + 记录本月信号
    python tools/oos_forward_monthly.py --month 2026-12 --mode signal
    python tools/oos_forward_monthly.py --mode settle --dry-run

输出：work/oos_forward/records.csv
字段：沿用 date/ts_code/weight/actual_return/benchmark_return/diff，
      追加 sleeve（etf_base / dividend），不改动既有六列的名称与语义。
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
RECORD = ROOT / 'work' / 'oos_forward' / 'records.csv'
BASE_CODES = ['510300.SH', '511010.SH', '518880.SH']
DIV_CODE = '510880.SH'
ROUND_TRIP_COST = 0.00165  # 16.5bp 往返（含冲击）
CASH_ANNUAL = 0.015


def _load_fund(code: str) -> pd.DataFrame:
    mkt = 'sh' if code.endswith('.SH') else 'sz'
    df = pd.read_parquet(STORE / f'quotes/fund/{mkt}/{code}.parquet')
    dc = [c for c in df.columns if 'date' in c.lower()][0]
    adj = pd.read_parquet(STORE / 'factors' / 'fund_adj_factor.parquet')
    adj = adj[adj.ts_code == code][['trade_date', 'qfq_factor']]
    adj['trade_date'] = pd.to_datetime(adj['trade_date'])
    df = df[[dc, 'open']].rename(columns={dc: 'trade_date'})
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.merge(adj, on='trade_date', how='left').sort_values('trade_date')
    df['qfq_factor'] = df['qfq_factor'].ffill()
    df['open_adj'] = df['open'] * df['qfq_factor']
    return df.set_index('trade_date')[['open_adj']].dropna()


def _last_trading_day(month: str) -> pd.Timestamp:
    cal = _load_fund('510300.SH').index
    sub = cal[cal.strftime('%Y-%m') == month]
    if sub.empty:
        raise SystemExit(f'无 {month} 交易日数据，无法记录/结算')
    return sub[-1]


def _next_trading_day(day: pd.Timestamp) -> pd.Timestamp:
    cal = _load_fund('510300.SH').index
    later = cal[cal > day]
    if later.empty:
        raise SystemExit(f'{day.date()} 之后无可成交日')
    return later[0]


def _dividend_position(decision: pd.Timestamp) -> float:
    iv = pd.read_parquet(STORE / 'meta/index_valuation/index_valuation.parquet')
    iv['trade_date'] = pd.to_datetime(iv['trade_date'])
    iv = iv[(iv.index_code == '000015.SH') & (iv.trade_date <= decision)].dropna(subset=['pb'])
    iv = iv.sort_values('trade_date')
    if len(iv) < 36:
        raise SystemExit('000015.SH PB 历史不足 36 期，拒绝外推')
    win = iv['pb'].tail(60)
    pct = (win.iloc[-1] >= win).mean()
    return 1.0 if pct <= 0.5 else 0.5


def _read_records() -> pd.DataFrame:
    cols = ['date', 'ts_code', 'weight', 'actual_return', 'benchmark_return', 'diff', 'sleeve']
    if not RECORD.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(RECORD, dtype=str, encoding='utf-8')
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    return df[cols]


def _append(df: pd.DataFrame, rows: list[dict], dry: bool) -> int:
    new = []
    for r in rows:
        mask = ((df['sleeve'] == r['sleeve']) & (df['date'] == r['date'])
                & (df['ts_code'] == r['ts_code']))
        if mask.any():
            continue
        new.append(r)
    if not new:
        return 0
    if not dry:
        RECORD.parent.mkdir(parents=True, exist_ok=True)
        out = pd.concat([df, pd.DataFrame(new)], ignore_index=True)
        out.to_csv(RECORD, index=False, encoding='utf-8')
    return len(new)


def write_signal(month: str, dry: bool) -> int:
    decision = _last_trading_day(month)
    rows = [dict(date=decision.date().isoformat(), ts_code=c, weight=f'{1/3:.6f}',
                 actual_return='', benchmark_return='', diff='', sleeve='etf_base')
            for c in BASE_CODES]
    pos = _dividend_position(decision)
    rows.append(dict(date=decision.date().isoformat(), ts_code=DIV_CODE,
                     weight=f'{pos:.6f}', actual_return='', benchmark_return='',
                     diff='', sleeve='dividend'))
    n = _append(_read_records(), rows, dry)
    print(f'[signal] {month} 决策日 {decision.date()} 新增 {n} 行'
          + ('（dry-run）' if dry else ''))
    return n


def settle(month: str, dry: bool) -> int:
    """结算 date 落在 month 的决策：以其次一交易日开盘买入，持有一个月。"""
    df = _read_records()
    pending = df[(df['date'].str.startswith(month)) & (df['actual_return'].fillna('') == '')]
    if pending.empty:
        print(f'[settle] {month} 无待回填行')
        return 0
    bench = _load_fund('510300.SH')['open_adj']
    filled = 0
    for _, row in pending.iterrows():
        decision = pd.Timestamp(row['date'])
        buy_day = _next_trading_day(decision)
        sell_day = _next_trading_day(_last_trading_day(
            (pd.Timestamp(buy_day) + pd.offsets.MonthEnd(1)).strftime('%Y-%m')))
        if sell_day > _load_fund('510300.SH').index[-1]:
            continue
        px = _load_fund(row['ts_code'])['open_adj']
        if buy_day not in px.index or sell_day not in px.index:
            continue
        r = px.loc[sell_day] / px.loc[buy_day] - 1 - ROUND_TRIP_COST
        b = bench.loc[sell_day] / bench.loc[buy_day] - 1
        w = float(row['weight'])
        if row['sleeve'] == 'dividend':
            r = w * r + (1 - w) * (CASH_ANNUAL / 12)
        df.loc[(df['sleeve'] == row['sleeve']) & (df['date'] == row['date'])
               & (df['ts_code'] == row['ts_code']),
               ['actual_return', 'benchmark_return', 'diff']] = [f'{r:.6f}', f'{b:.6f}', f'{r - b:.6f}']
        filled += 1
    if filled and not dry:
        df.to_csv(RECORD, index=False, encoding='utf-8')
    print(f'[settle] {month} 回填 {filled} 行' + ('（dry-run）' if dry else ''))
    return filled


def main() -> int:
    ap = argparse.ArgumentParser(description='月度样本外前向记录（只记录，不建议交易）')
    ap.add_argument('--month', default=date.today().strftime('%Y-%m'), help='YYYY-MM')
    ap.add_argument('--mode', choices=['auto', 'signal', 'settle'], default='auto')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    m = pd.Period(args.month, freq='M')
    prev = (m - 1).strftime('%Y-%m')
    if args.mode in ('auto', 'settle'):
        settle(prev, args.dry_run)
    if args.mode in ('auto', 'signal'):
        write_signal(args.month, args.dry_run)
    return 0


if __name__ == '__main__':
    sys.exit(main())
