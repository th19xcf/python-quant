#!/usr/bin/env python3
"""决定性对照：月频 N=50 无缓冲 vs 周频 N=20 带缓冲（同 REV20、同 0.13% 成本、T+1 开盘）

可加拆分路径（固定顺序，便于复核）：
  A   周频 N=20 缓冲1.5N(30)
  A2  周频 N=20 无缓冲          -> 缓冲带效应 = A2 - A
  P   月频 N=20 无缓冲          -> 频率效应   = P  - A2
  B   月频 N=50 无缓冲          -> 集中度效应 = B  - P
每组都配 100 次同规则随机评分基线，用于给出策略分位与“相对随机缺口”的同样拆分。
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import polars as pl

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import main as M  # noqa: E402

OUT = HERE
N_REP = 100

CONFIGS = [
    ('A_weekly_n20_buf30', dict(rebal=5, n_hold=20, buffer_n=30, n_cand=30)),
    ('A2_weekly_n20_nobuf', dict(rebal=5, n_hold=20, buffer_n=20, n_cand=30)),
    ('P_monthly_n20_nobuf', dict(rebal=20, n_hold=20, buffer_n=20, n_cand=30)),
    ('B_monthly_n50_nobuf', dict(rebal=20, n_hold=50, buffer_n=50, n_cand=75)),
]
KEYS = ['ann', 'mdd', 'calmar', 'sharpe', 'turnover', 'trades', 'max_loss',
        'unsellable_pct', 'fill_rate', 'same_day_fill', 'avg_expo']


def run(L, cfg, score, seed=0):
    M.REBAL = cfg['rebal']
    m, _, _ = M.simulate(L, score=score, seed=seed, buffer_n=cfg['buffer_n'],
                         use_caps=True, n_hold=cfg['n_hold'], n_cand=cfg['n_cand'])
    return m


def main():
    t0 = time.time()
    lim = M.load_limit_panel()
    L = M.build_matrices(lim)
    del lim
    M.fill_quotes(L)
    M.fill_industry(L)
    M.fill_delist(L)
    print(f'load {time.time()-t0:.0f}s', flush=True)

    strat, rnd = {}, []
    for name, cfg in CONFIGS:
        t1 = time.time()
        strat[name] = run(L, cfg, 'rev')
        s = strat[name]
        print(f"{name:24s} ann={s['ann']*100:7.2f}% mdd={s['mdd']*100:7.2f}% "
              f"sharpe={s['sharpe']:6.3f} calmar={s['calmar']:6.3f} to={s['turnover']:5.2f} "
              f"trades={s['trades']:5d} maxloss={s['max_loss']*100:6.1f}% "
              f"fill={s['fill_rate']*100:6.2f}% {time.time()-t1:.1f}s", flush=True)
        for rep in range(N_REP):
            m = run(L, cfg, 'rand_iid', seed=5000 + rep)
            m['family'] = 'rand_iid'
            m['config'] = name
            m['rep'] = rep
            rnd.append(m)
        print(f'  rand {N_REP} reps {time.time()-t1:.0f}s', flush=True)

    rows = []
    for name, _ in CONFIGS:
        r = dict(strat[name]); r['config'] = name
        rows.append(r)
    pl.DataFrame(rows).write_csv(os.path.join(OUT, 'monthly50_metrics.csv'))
    rdf = pl.DataFrame(rnd)
    rdf.write_csv(os.path.join(OUT, 'monthly50_random_runs.csv'))

    # 策略分位 + 分布
    dist, summ = [], {}
    for name, _ in CONFIGS:
        sub = rdf.filter(pl.col('config') == name)
        summ[name] = {}
        for k in KEYS:
            v = sub[k].to_numpy().astype(float)
            v = v[np.isfinite(v)]
            sv = float(strat[name][k])
            if k in ('mdd', 'max_loss', 'unsellable_pct'):
                pct = float((v <= sv).mean())
            else:
                pct = float((v >= sv).mean())
            summ[name][k] = dict(strat=sv, mean=float(v.mean()),
                                 median=float(np.median(v)),
                                 p05=float(np.percentile(v, 5)),
                                 p95=float(np.percentile(v, 95)),
                                 p_one_sided=pct)
            dist.append(dict(config=name, metric=k, **summ[name][k]))
    pl.DataFrame(dist).write_csv(os.path.join(OUT, 'monthly50_random_summary.csv'))

    # 可加拆分（策略绝对值 / 相对随机缺口）
    def gap(name):
        return strat[name]['ann'] - summ[name]['ann']['mean']

    raw = [
        ('buffer_removal', strat['A2_weekly_n20_nobuf']['ann'] - strat['A_weekly_n20_buf30']['ann'],
         gap('A2_weekly_n20_nobuf') - gap('A_weekly_n20_buf30')),
        ('frequency(weekly->monthly)', strat['P_monthly_n20_nobuf']['ann'] - strat['A2_weekly_n20_nobuf']['ann'],
         gap('P_monthly_n20_nobuf') - gap('A2_weekly_n20_nobuf')),
        ('concentration(n20->n50)', strat['B_monthly_n50_nobuf']['ann'] - strat['P_monthly_n20_nobuf']['ann'],
         gap('B_monthly_n50_nobuf') - gap('P_monthly_n20_nobuf')),
    ]
    dec = pl.DataFrame([dict(component=c, effect_ann_pp=a * 100, effect_gap_pp=g * 100)
                        for c, a, g in raw])
    dec.write_csv(os.path.join(OUT, 'monthly50_decomposition.csv'))
    print(dec, flush=True)
    print('gap A=%.4f gap B=%.4f  total_change=%.4f' % (gap('A_weekly_n20_buf30'),
                                                       gap('B_monthly_n50_nobuf'),
                                                       strat['B_monthly_n50_nobuf']['ann'] - strat['A_weekly_n20_buf30']['ann']),
          flush=True)

    out = dict(strategy={k: {kk: float(vv) if isinstance(vv, (int, float, np.floating)) else vv
                             for kk, vv in v.items()} for k, v in strat.items()},
               distribution=summ, decomposition=dec.to_dicts(),
               configs={n: c for n, c in CONFIGS}, n_rep=N_REP,
               sample=['2010-01-04', '2026-11-20'], cost_roundtrip=0.0013,
               caps=dict(single=0.08, industry=0.30), exec='T+1 open')
    with open(os.path.join(OUT, 'monthly50_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=float)
    print(f'done {time.time()-t0:.0f}s', flush=True)


if __name__ == '__main__':
    main()
