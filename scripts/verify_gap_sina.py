"""对拍 236 条"整档跳空"候选：用新浪分红历史（独立第三源）判定是否真实漏记。"""
import sys
import time
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd
import polars as pl

ROOT = Path(r'e:\code\python\quant')
sys.path.insert(0, str(ROOT))
from scripts.verify_data_20261104 import to_int_days  # noqa: E402
OUT = ROOT / 'logs' / 'verification_20261104'
CACHE = OUT / 'sina_cache'
CACHE.mkdir(exist_ok=True)


def _date_int(text: str) -> int:
    return int(np.datetime64(text).astype('datetime64[D]').astype(int))


def sina_div(code6: str) -> pd.DataFrame | None:
    p = CACHE / f'{code6}.csv'
    if p.exists():
        try:
            return pd.read_csv(p, dtype={'除权除息日': str})
        except Exception:  # noqa: BLE001
            pass
    for _ in range(3):
        try:
            df = ak.stock_history_dividend_detail(symbol=code6, indicator='分红')
            df.to_csv(p, index=False, encoding='utf-8')
            time.sleep(0.15)
            return df
        except Exception:  # noqa: BLE001
            time.sleep(1.0)
    return None


def main() -> None:
    cand = pl.read_csv(OUT / 'gap_calibrated_candidates.csv')
    quotes_dir = ROOT / 'data' / 'local_store' / 'quotes' / 'stock'
    rows: list[dict] = []
    for code in sorted(cand['ts_code'].unique()):
        df = sina_div(code.split('.')[0])
        sub = cand.filter(pl.col('ts_code') == code)
        if df is None:
            for r in sub.to_dicts():
                rows.append({'ts_code': code, 'verdict': 'sina_unavailable',
                             'gap_date': r['date'], 'sina_date': None})
            continue
        df = df.copy()
        df['_d'] = pd.to_datetime(df['除权除息日'], errors='coerce')
        df = df[(df['进度'].astype(str) == '实施') & df['_d'].notna()]
        q = pl.read_parquet(
            quotes_dir / code.split('.')[-1].lower() / f'{code}.parquet',
            columns=['date', 'close'])
        dates = to_int_days(q['date'])
        closes = q['close'].to_numpy()
        for r in sub.to_dicts():
            pos = int(np.searchsorted(dates, _date_int(r['date'])))
            if pos <= 0 or pos >= len(dates):
                continue
            prev = float(closes[pos - 1])
            hit = df[(df['_d'] >= pd.Timestamp(r['date']) - pd.Timedelta(days=4)) &
                     (df['_d'] <= pd.Timestamp(r['date']) + pd.Timedelta(days=4))]
            if len(hit) == 0:
                rows.append({'ts_code': code, 'verdict': 'no_sina_event',
                             'gap_date': r['date'], 'sina_date': None,
                             'sina_share': None, 'sina_cash': None, 'residual': None})
                continue
            row = hit.iloc[0]
            share = (float(row.get('送股') or 0) + float(row.get('转增') or 0)) / 10.0
            cash = float(row.get('派息') or 0) / 10.0
            pred = (prev - cash) / (prev * (1 + share))
            resid = abs(closes[pos] / prev - pred) / pred
            rows.append({
                'ts_code': code,
                'verdict': 'sina_confirmed' if resid <= 0.03 else 'sina_event_mismatch',
                'gap_date': r['date'], 'sina_date': str(row['除权除息日'])[:10],
                'sina_share': round(share, 4), 'sina_cash': round(cash, 4),
                'residual': round(float(resid), 5),
            })
    out = pl.DataFrame(rows)
    out.write_csv(OUT / 'gap_sina_verdict.csv')
    print(out.group_by('verdict').agg(pl.len()).sort('verdict'))
    print(out.filter(pl.col('verdict').isin(['sina_confirmed', 'sina_event_mismatch']))
          .sort('gap_date').head(50))
    print('total gap rows', out.height, 'codes', out['ts_code'].n_unique())


if __name__ == '__main__':
    main()
