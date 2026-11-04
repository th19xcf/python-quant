#!/usr/bin/env python3
"""全市场分红事件跨源对拍 + 带校准的价格跳空完整性检测（只读）。"""
from __future__ import annotations

import bisect
import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_data_20261104 import (  # noqa: E402
    LOCAL, OUT, load_event_dict, quote_files, to_int_days, int_to_date, hrank,
)

CACHE = OUT / 'ak_fhps_cache'
CACHE.mkdir(parents=True, exist_ok=True)
PERIODS = ['20151231']
for y in range(2016, 2026):
    PERIODS += [f'{y}0630', f'{y}1231']
PERIODS += ['20260630', '20260930']


def fetch_period(p: str) -> pl.DataFrame:
    cache = CACHE / f'{p}.parquet'
    if cache.exists():
        return pl.read_parquet(cache)
    import akshare as ak
    import pandas as pd
    df = ak.stock_fhps_em(date=p)
    df = df[pd.to_datetime(df['除权除息日'], errors='coerce').notna()].copy()
    df['_d'] = pd.to_datetime(df['除权除息日'], errors='coerce').dt.date
    out = pl.DataFrame({
        'code': df['代码'].astype(str).str.zfill(6),
        'date': pl.Series([d.isoformat() if d is not None else None for d in df['_d']]),
        'cash': pl.Series([float(x) if pd.notna(x) else 0.0
                           for x in df['现金分红-现金分红比例']]),
        'share': pl.Series([float(x) if pd.notna(x) else 0.0
                            for x in df['送转股份-送转总比例']]),
    }).with_columns(pl.col('date').str.to_date('%Y-%m-%d', strict=False))
    out.write_parquet(cache)
    return out


def main() -> None:
    t0 = time.perf_counter()
    frames = []
    failed = []
    for p in PERIODS:
        try:
            frames.append(fetch_period(p))
        except Exception as exc:  # noqa: BLE001
            failed.append({'period': p, 'error': f'{type(exc).__name__}: {exc}'})
    ak = pl.concat(frames)
    ak = ak.filter(pl.col('date').is_not_null()).with_columns(
        pl.col('cash').fill_null(0.0), pl.col('share').fill_null(0.0))
    ak = ak.group_by(['code', 'date']).agg(
        pl.col('cash').sum(), pl.col('share').sum()).with_columns(
        (pl.col('cash') / 10.0).alias('cash'),
        (pl.col('share') / 10.0).alias('share'))
    ak = ak.filter((pl.col('cash') > 0) | (pl.col('share') > 0))
    ak = ak.filter(pl.col('date') <= date(2026, 11, 3))
    ak.write_csv(OUT / 'ak_fhps_all.csv')
    print('ak events', ak.height, 'codes', ak['code'].n_unique(),
          'fetch_seconds', round(time.perf_counter() - t0, 1), flush=True)

    ev, div = load_event_dict()
    local = div.with_columns([
        pl.col('cash_div').fill_null(0.0), pl.col('share_div').fill_null(0.0),
        pl.col('rights_issue_ratio').fill_null(0.0)]).filter(
        (pl.col('ex_date') >= date(2016, 1, 1)) &
        (pl.col('ex_date') <= date(2026, 11, 3)) &
        ((pl.col('cash_div') > 0) | (pl.col('share_div') > 0))
    )
    loc = local.with_columns(pl.col('ts_code').str.slice(0, 6).alias('code')).group_by(
        ['code', 'ex_date']).agg(pl.col('cash_div').sum(), pl.col('share_div').sum())
    loc = loc.rename({'ex_date': 'date', 'cash_div': 'cash', 'share_div': 'share'})

    # 精确日期对拍
    both = loc.join(ak, on=['code', 'date'], how='inner', suffix='_ak')
    only_ak = ak.join(loc, on=['code', 'date'], how='anti')
    only_local = loc.join(ak, on=['code', 'date'], how='anti')
    amt_ok = both.filter(
        (pl.col('cash') - pl.col('cash_ak')).abs() <= 1e-4).filter(
        (pl.col('share') - pl.col('share_ak')).abs() <= 1e-4)
    amt_diff_rows = both.filter(
        ((pl.col('cash') - pl.col('cash_ak')).abs() > 1e-4) |
        ((pl.col('share') - pl.col('share_ak')).abs() > 1e-4))

    # 临近日期（±5 天）视为日期映射差异而非漏记
    near = 0
    only_ak_rows = only_ak.to_dicts()
    local_index: dict[str, list] = {}
    for r in loc.select(['code', 'date']).to_dicts():
        local_index.setdefault(r['code'], []).append(r['date'])
    for r in only_ak_rows:
        ds = local_index.get(r['code'], [])
        if any(abs((d - r['date']).days) <= 5 for d in ds):
            near += 1

    only_ak_csv = OUT / 'cross_full_only_ak.csv'
    only_local_csv = OUT / 'cross_full_only_local.csv'
    only_ak.write_csv(only_ak_csv)
    only_local.write_csv(only_local_csv)

    # 价格跳空完整性：只认“整档送转比例 + 5日均价水平位移”
    candidates = []
    wide_rows = 0
    for p in quote_files('stock'):
        code = p.stem
        q = pl.read_parquet(p, columns=['date', 'open', 'close'])
        if q.height < 40:
            continue
        d = to_int_days(q['date'])
        c = q['close'].to_numpy().astype(float)
        o = q['open'].to_numpy().astype(float)
        ret = c[1:] / c[:-1] - 1
        over = o[1:] / c[:-1] - 1
        idx = np.nonzero((ret <= -0.07) & (over <= -0.07))[0] + 1
        wide_rows += len(idx)
        e = ev.get(code)
        for t in idx:
            prev_d, d_t = int(d[t - 1]), int(d[t])
            matched = False
            if e is not None:
                matched = bisect.bisect_right(e['date'], prev_d) < bisect.bisect_right(
                    e['date'], d_t)
            if matched:
                continue
            r = float(ret[t - 1])
            og = float(over[t - 1])
            # 开盘与收盘必须同时跳空且幅度接近，排除盘中杀跌
            if abs(og - r) > 0.01:
                continue
            implied = 1.0 / (1.0 + r) - 1.0
            if implied < 0.09:
                continue
            # 整档送转比例（0.05 的整数倍，误差 <= 0.005）
            step = implied / 0.05
            if abs(step - round(step)) > 0.1:
                continue
            # 5 日价格水平位移，排除普通暴跌
            if t < 6 or t + 5 >= len(c):
                continue
            pre = float(np.mean(c[t - 5:t]))
            post = float(np.mean(c[t + 1:t + 6]))
            exp_ratio = 1.0 / (1.0 + implied)
            if abs(post / pre - exp_ratio) / exp_ratio > 0.02:
                continue
            candidates.append({
                'ts_code': code, 'date': int_to_date(d_t),
                'ret': round(r, 5), 'implied_share': round(implied, 4),
                'pre_avg': round(pre, 3), 'post_avg': round(post, 3),
                'level_ratio': round(post / pre, 5),
                'expected_ratio': round(exp_ratio, 5),
            })

    # 候选与 AkShare 全市场事件对齐（±5 天）
    ak_index: dict[str, list] = {}
    for r in ak.select(['code', 'date']).to_dicts():
        ak_index.setdefault(r['code'], []).append(r['date'])
    confirmed = []
    for cand in candidates:
        code6 = cand['ts_code'].split('.')[0]
        cd = date.fromisoformat(cand['date'])
        hit = [x for x in ak_index.get(code6, []) if abs((x - cd).days) <= 5]
        local_hit = any(abs((x - cd).days) <= 5 for x in local_index.get(code6, []))
        cand['ak_hit'] = [str(x) for x in hit]
        cand['local_near_hit'] = local_hit
        if hit:
            confirmed.append(cand)
    if candidates:
        flat = [{**{k: v for k, v in c.items() if k != 'ak_hit'},
                 'ak_hit': ','.join(c['ak_hit'])} for c in candidates]
        pl.DataFrame(flat).write_csv(OUT / 'gap_calibrated_candidates.csv')

    result = {
        'ak_events_2016_2026': ak.height,
        'ak_codes': ak['code'].n_unique(),
        'periods_failed': failed,
        'local_events_in_range': loc.height,
        'matched_date': both.height,
        'amount_exact': amt_ok.height,
        'amount_diff': both.height - amt_ok.height,
        'only_ak': only_ak.height,
        'only_ak_near_local_date': near,
        'only_local': only_local.height,
        'years': {
            'ak': ak.group_by(pl.col('date').cast(pl.Utf8).str.slice(0, 4).alias('y'))
            .agg(pl.len()).sort('y').to_dicts(),
            'local': loc.group_by(pl.col('date').cast(pl.Utf8).str.slice(0, 4).alias('y'))
            .agg(pl.len()).sort('y').to_dicts(),
            'only_ak': only_ak.group_by(
                pl.col('date').cast(pl.Utf8).str.slice(0, 4).alias('y'))
            .agg(pl.len()).sort('y').to_dicts(),
            'only_local': only_local.group_by(
                pl.col('date').cast(pl.Utf8).str.slice(0, 4).alias('y'))
            .agg(pl.len()).sort('y').to_dicts(),
        },
        'amount_diff_samples': amt_diff_rows.head(10).to_dicts(),
        'only_local_top_codes': only_local.group_by('code').agg(
            pl.len().alias('n')).sort('n', descending=True).head(20).to_dicts(),
        'only_ak_top_codes': only_ak.group_by('code').agg(
            pl.len().alias('n')).sort('n', descending=True).head(20).to_dicts(),
        'only_ak_samples': only_ak.head(20).to_dicts(),
        'only_local_samples': only_local.head(20).to_dicts(),
        'gap_wide_rows': wide_rows,
        'gap_calibrated_candidates': len(candidates),
        'gap_candidates_confirmed_by_ak': len(confirmed),
        'confirmed_samples': confirmed[:30],
        'gap_candidates': candidates[:50],
        'seconds': round(time.perf_counter() - t0, 1),
    }
    with open(OUT / 'dividend_full.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps({k: v for k, v in result.items()
                      if k not in ('only_ak_samples', 'only_local_samples',
                                   'confirmed_samples', 'gap_candidates')},
                     ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
