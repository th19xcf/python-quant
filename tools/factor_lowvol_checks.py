#!/usr/bin/env python3
"""剔除式（剔 Q5）的两项检验：分段稳定性 + 市值混淆。

口径与 tools/factor_lowvol_variants.py 完全一致（同一因子值、同一两腿再平衡、
同一成本）。为让两段都在期初即已建仓，数据统一从 2009-08-01 读入（因子与
组合先在预热期运行），指标按区间切片计算。

三项检验：
1) 分段：2010-01-04~2016-12-30、2017-01-03~2026-11-13，剔除式 vs 同段基线；
2) 市值混淆（方案 A）：每月先在「自由流通市值」三分组内剔除各组波动率最高
   的 20%，合并等权，与直接剔 Q5 对比；
3) 剔除组 / 保留组的自由流通市值分布差异（中位数）。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_lowvol_checks.py
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2009-08-01'
SEG1 = (date(2010, 1, 4), date(2016, 12, 30))
SEG2 = (date(2017, 1, 3), date(2026, 11, 13))


def load_caps(data: Dict[str, pl.DataFrame],
              month_ends: List[date]) -> Dict[date, Dict[str, float]]:
    """各月末的自由流通市值（free_float_share 缺失回退 float_share，×原始收盘）。"""
    shares = pl.read_parquet(
        ROOT / 'data/local_store/meta/factors/stock_shares.parquet')
    shares = shares.sort(['ts_code', 'effective_date']).with_columns(
        pl.coalesce([pl.col('free_float_share'), pl.col('float_share')])
        .alias('shares'))
    by_code: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    codes_np = shares['ts_code'].to_numpy()
    ed_np = shares['effective_date'].to_numpy()
    sv_np = shares['shares'].to_numpy().astype('float64')
    starts = np.flatnonzero(np.r_[True, codes_np[1:] != codes_np[:-1]])
    ends = np.r_[starts[1:], len(codes_np)]
    for a, b in zip(starts, ends):
        by_code[str(codes_np[a])] = (ed_np[a:b], sv_np[a:b])
    me = np.array(month_ends, dtype='datetime64[D]')
    out: Dict[date, Dict[str, float]] = {d: {} for d in month_ends}
    for code, df in data.items():
        if code not in by_code:
            continue
        sdates, sval = by_code[code]
        pos = np.searchsorted(sdates, df['date'].to_numpy(), side='right') - 1
        raw = df['raw_close'].to_numpy().astype('float64')
        dates = df['date'].to_numpy()
        raw_me = np.searchsorted(dates, me)
        for j, day in enumerate(month_ends):
            k = raw_me[j]
            if k >= dates.size or dates[k] != me[j]:
                continue
            p = pos[k]
            if p < 0 or not np.isfinite(raw[k]) or raw[k] <= 0:
                continue
            sh = sval[p]
            if not np.isfinite(sh) or sh <= 0:
                continue
            out[day][code] = float(sh * raw[k])
    return out


def variant_size_neutral(panel, members, quoted, months, caps):
    path: Dict[date, List[str]] = {}
    for m in months:
        ranked = sorted(panel[m].items(), key=lambda kv: kv[1])
        cap = caps.get(m, {})
        graded = [(c, vol, cap[c]) for c, vol in ranked if c in cap]
        drop = set()
        if len(graded) >= 30:
            order = sorted(range(len(graded)), key=lambda i: graded[i][2])
            group = {graded[i][0]: min(2, rank * 3 // len(graded))
                     for rank, i in enumerate(order)}
            for g in range(3):
                members_g = sorted((x for x in graded if group[x[0]] == g),
                                   key=lambda x: x[1])
                cut = len(members_g) - int(len(members_g) * 0.8)
                drop |= {x[0] for x in members_g[cut:]}
        ranked_codes = {c for c, _ in ranked}
        path[m] = sorted((quoted[m] - drop) | (quoted[m] - ranked_codes))
    return path


def seg_metrics(result, a: date, b: date) -> Dict[str, object]:
    curve = [(d, e) for d, e in f.curve_of(result) if a <= d <= b]
    if len(curve) < 2:
        return {'missing': True}
    years = (curve[-1][0] - curve[0][0]).days / 365.25
    annual = (curve[-1][1] / curve[0][1]) ** (1.0 / years) - 1.0
    peak, mdd = curve[0][1], 0.0
    for _, eq in curve:
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1.0)
    trades = []
    for t in result['trades']:
        day = f.base.to_date(t['date'])
        if day is not None and a <= day <= b:
            trades.append(t)
    buy = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'buy')
    sell = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'sell')
    cost = sum(float(t.get('cost') or 0.0) for t in trades)
    avg_eq = sum(e for _, e in curve) / len(curve)
    notional = buy + sell
    gross = curve[-1][1] - curve[0][1] + cost
    return {
        'annual': round(annual, 6),
        'max_drawdown': round(mdd, 6),
        'turnover_two_way': round(notional / avg_eq / years, 4),
        'cost_share_of_gross': round(cost / gross, 4) if gross > 0 else None,
        'cost_bps': round(cost / notional * 1e4, 2) if notional else None,
        'final_equity': round(curve[-1][1], 2),
        'trades': len(trades),
    }


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    panel = f.compute_vol_panel(data, month_ends)
    members, short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, month_ends)
    months = [m for m in month_ends if m >= date(2009, 9, 1)]
    logger.info('预热后月末 {} 个（{}~{}）', len(months),
                months[0].isoformat(), months[-1].isoformat())

    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)

    caps = load_caps(data, months)
    logger.info('市值覆盖：末月中位 {} 只有市值', int(np.median([len(x) for x in caps.values()])))

    runs: Dict[str, Dict[str, object]] = {}
    paths = {
        'baseline': {m: sorted(quoted[m]) for m in months},
        'exclude': v.variant_exclude(panel, members, quoted, months),
        'size_neutral': variant_size_neutral(panel, members, quoted, months, caps),
    }
    for tag, path in paths.items():
        logger.info('{} 成分数中位 {}', tag, int(np.median([len(x) for x in path.values()])))
        sched = v.schedules_from_path(path, prev_day)
        result = f.run_portfolio(data, sched, dividends, st_status)
        runs[tag] = {
            'seg1': seg_metrics(result, *SEG1),
            'seg2': seg_metrics(result, *SEG2),
            'full': seg_metrics(result, SEG1[0], SEG2[1]),
        }
        print(tag.upper() + ' ' + json.dumps(runs[tag], ensure_ascii=False), flush=True)
        del result, sched, path
        import gc
        gc.collect()

    # 市值分布差异：Q5 与其余（有效分层月份）
    q5_med, rest_med, months_n = [], [], 0
    for m in months:
        if m not in members or m not in caps:
            continue
        cap = caps[m]
        drop = [cap[c] for c in members[m][4] if c in cap]
        keep = [cap[c] for c in (quoted[m] - set(members[m][4])) if c in cap]
        if len(drop) < 30 or len(keep) < 30:
            continue
        q5_med.append(np.median(drop) / 1e8)
        rest_med.append(np.median(keep) / 1e8)
        months_n += 1
    print('SIZE ' + json.dumps({
        'months': months_n,
        'q5_median_cap_yi_avg': round(float(np.mean(q5_med)), 2),
        'rest_median_cap_yi_avg': round(float(np.mean(rest_med)), 2),
        'q5_p25_p75_yi': [round(float(np.percentile(q5_med, 25)), 2),
                          round(float(np.percentile(q5_med, 75)), 2)],
        'rest_p25_p75_yi': [round(float(np.percentile(rest_med, 25)), 2),
                            round(float(np.percentile(rest_med, 75)), 2)],
    }, ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
