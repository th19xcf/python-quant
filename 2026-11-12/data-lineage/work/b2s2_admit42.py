# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""42 个「事件表缺登记、因子对」事件的准入复核。

三条判据（用户 2026-11-13 定）：
  C1  ref 落在当日 [low, high] 带内；
  C2  ref 与当日开盘价相对差 |ref/open - 1| <= 5%；
  C3  该事件能被独立价格证据支持：当日原始 bar 存在且非桩
      （open/low/high/close 均 > 0，volume > 0 且 amount > 0）。

只读，不写 data/。产出 CSV + JSON 供登记使用。
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

WORK = Path(__file__).parent
STORE = Path(r'e:\code\python\quant\data\local_store')
OUT_CSV = WORK / 'b2s2_admit42.csv'
OUT_JSON = WORK / 'b2s2_admit42.json'


def quote_frame(ts_code: str) -> pl.DataFrame:
    ex = 'sh' if ts_code.endswith('.SH') else 'sz'
    p = STORE / 'quotes' / 'fund' / ex / f'{ts_code}.parquet'
    if not p.exists():
        return pl.DataFrame()
    return pl.read_parquet(p).select(
        'date', 'open', 'high', 'low', 'close', 'volume', 'amount')


def main() -> None:
    ev = json.loads((WORK / 'b2s2_d4_evidence.json').read_text(encoding='utf-8'))
    rows = [r for r in ev['rows'] if r['verdict'] == 'akshare无对应事件']
    codes = sorted({r['ts_code'] for r in rows})
    quotes = {c: quote_frame(c) for c in codes}

    out = []
    for r in rows:
        c, d = r['ts_code'], r['date']
        ref = r['ref_factor']
        band_lo, band_hi = r['band_lo'], r['band_hi']
        q = quotes[c]
        bar = q.filter(pl.col('date') == pl.lit(date.fromisoformat(d))) if q.height else None
        bar_ok = bool(bar is not None and bar.height == 1)
        vals = bar.row(0, named=True) if bar_ok else {}
        vol = float(vals.get('volume') or 0.0)
        amt = float(vals.get('amount') or 0.0)
        prices_ok = all(float(vals.get(k) or 0) > 0 for k in ('open', 'high', 'low', 'close'))
        c1 = band_lo is not None and band_lo <= ref <= band_hi
        c2 = abs(ref / r['open'] - 1.0) <= 0.05
        c3 = bar_ok and prices_ok and vol > 0 and amt > 0
        out.append({
            'ts_code': c, 'date': d, 'step': r['step'], 'cprev': r['cprev'],
            'ref': round(ref, 6), 'open': r['open'], 'low': r['low'], 'high': r['high'],
            'band_lo': round(band_lo, 6), 'band_hi': round(band_hi, 6),
            'ref_vs_open_pct': round((ref / r['open'] - 1.0) * 100, 3),
            'single_price': r['single_price'],
            'volume': vol, 'amount': amt, 'bar_ok': bar_ok, 'prices_ok': prices_ok,
            'C1_in_band': c1, 'C2_open5pct': c2, 'C3_price_evidence': c3,
            'admit': c1 and c2 and c3,
        })

    df = pl.DataFrame(out).sort(['ts_code', 'date'])
    df.write_csv(OUT_CSV)

    admitted = [o for o in out if o['admit']]
    rejected = [o for o in out if not o['admit']]
    reasons = {}
    for o in rejected:
        rs = []
        if not o['C1_in_band']:
            rs.append('C1 越界')
        if not o['C2_open5pct']:
            rs.append(f"C2 |ref/open-1|={o['ref_vs_open_pct']:.3f}%")
        if not o['C3_price_evidence']:
            rs.append('C3 无可用原始 bar')
        reasons.setdefault('; '.join(x.split(' ')[0] for x in rs), []).append(
            {'ts_code': o['ts_code'], 'date': o['date'], 'detail': rs})

    payload = {
        'candidates': len(out),
        'admitted': len(admitted),
        'rejected': len(rejected),
        'admitted_by_code': {},
        'rejected_by_code': {},
        'reject_reasons': reasons,
        'rows': out,
    }
    from collections import Counter
    payload['admitted_by_code'] = dict(Counter(o['ts_code'] for o in admitted).most_common())
    payload['rejected_by_code'] = dict(Counter(o['ts_code'] for o in rejected).most_common())
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')

    print('candidates', len(out), 'admitted', len(admitted), 'rejected', len(rejected))
    print('admitted by code', payload['admitted_by_code'])
    print('rejected by code', payload['rejected_by_code'])
    for k, v in reasons.items():
        print('---', k)
        for e in v:
            print('   ', e['ts_code'], e['date'], '|', '; '.join(e['detail']))


if __name__ == '__main__':
    main()
