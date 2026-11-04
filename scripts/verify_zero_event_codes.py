"""统计"零事件标的"规模：本地无任何分红记录、但新浪有分红记录的标的数（抽样推断）。"""
import hashlib
import sys
import time
from pathlib import Path

import akshare as ak
import pandas as pd
import polars as pl

ROOT = Path(r'e:\code\python\quant')
sys.path.insert(0, str(ROOT))
from scripts.verify_data_20261104 import LOCAL, quote_files  # noqa: E402

OUT = ROOT / 'logs' / 'verification_20261104'
CACHE = OUT / 'sina_cache'
CACHE.mkdir(exist_ok=True)

div = pl.read_parquet(LOCAL / 'factors' / 'stock_dividend.parquet')
stock_codes = sorted(p.stem for p in quote_files('stock'))
ev_codes = set(div['ts_code'].unique().to_list())
zero_codes = [c for c in stock_codes if c not in ev_codes]
print('stock quotes', len(stock_codes), 'codes with events', len(ev_codes & set(stock_codes)),
      'zero-event codes', len(zero_codes))

# 13 条漏记事件所在标的的本地/新浪事件数
targets = ['600652.SH', '600485.SH', '600208.SH', '600812.SH', '000606.SZ',
           '600139.SH', '002411.SZ', '600175.SH', '000616.SZ', '002699.SZ',
           '002435.SZ', '600466.SH', '603729.SH']
for c in targets:
    print(c, 'local events', div.filter(pl.col('ts_code') == c).height)


def sina(code6: str, code: str) -> pl.DataFrame | None:
    p = CACHE / f'{code6}.csv'
    if p.exists():
        try:
            return pl.read_csv(p, infer_schema_length=0)
        except Exception:  # noqa: BLE001
            pass
    for _ in range(3):
        try:
            df = ak.stock_history_dividend_detail(symbol=code6, indicator='分红')
            df.to_csv(p, index=False, encoding='utf-8')
            time.sleep(0.15)
            return pl.read_csv(p, infer_schema_length=0)
        except Exception:  # noqa: BLE001
            time.sleep(1.0)
    return None


def rank(c: str) -> str:
    return hashlib.sha256(('zero-event|' + c).encode()).hexdigest()


sample = sorted(zero_codes, key=rank)[:30]
hits, total_ev = 0, 0
rows = []
for c in sample:
    df = sina(c.split('.')[0], c)
    n = 0
    if df is not None and '进度' in df.columns:
        d = df.filter(pl.col('进度').cast(pl.Utf8) == '实施')
        n = d.height
    hits += n > 0
    total_ev += n
    rows.append({'ts_code': c, 'sina_events': n})
print('sample 30 zero-event codes: with sina events', hits, 'missed events', total_ev)
print(pl.DataFrame(rows).sort('sina_events', descending=True).head(15))
