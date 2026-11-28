#!/usr/bin/env python3
"""「不买名单」收窄版重跑（2026-11-28）。

只保留 R1（BIAS12 > +10%，去掉 5 日涨幅与 RSI14 两个触发条件）与
R3（20 日均换手池内最高 20%），去掉 R2、R4；并与三条版、四条版对照。

与 2026-11-27 版结果的差别（口径差异均在此写明）：
- 基准：项目规范 5.80%（年化），而不是可交易池等权 B；B 仅作并列参考；
- 成本：21.9bp（历史印花税口径，同原版）/ 35bp / 50bp 三档；
- DSR：N 计入前序试验（前序 6 + 本轮 4 = 10 为主口径），另给 N=4 / N=155 敏感性。

用法（项目根目录、venv）::

    .venv\\Scripts\\python.exe work/不买名单剔除_2026-11-26/收窄版重跑.py
"""
from __future__ import annotations

import importlib.util
import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location('excl_run', HERE / 'run.py')
run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run)  # type: ignore[union-attr]

BENCH_ANNUAL = 0.058
BENCH_WEEKLY = (1 + BENCH_ANNUAL) ** (1 / 52) - 1
COSTS = {'21.9bp(历史印花税)': None, '35bp': 0.0035, '50bp': 0.005}
# 前序 6 次（2026-11-26 预注册：主组 + 5 条单条组）+ 本轮 4 个变体（2/2c/3/4 条版）
N_PRIOR, N_ROUND = 6, 4
N_MAIN = N_PRIOR + N_ROUND
N_SOURCE_SWEEP = 105 + 40  # 上游：卖向/买向普查 65+40 变体、条件分层八特征×五档
N_UPPER = N_MAIN + N_SOURCE_SWEEP

VARIANTS = {
    'V2_收窄': ('r1b', 'r3'),   # 主组：BIAS12 单条件 + 高换手
    'V2c_R1复合': ('r1', 'r3'),  # 归因：原 R1 复合定义 + 高换手
    'V3_三条': ('r1', 'r2', 'r3'),
    'V4_四条': ('r1', 'r2', 'r3', 'r4'),
}


def net_with_cost(gross: pl.DataFrame, rt: float | None) -> pl.DataFrame:
    if rt is None:  # 历史印花税口径，与 run.py 一致
        rt_expr = (pl.when(pl.col('date') < run.STAMP_SWITCH).then(run.ROUND_TRIP_PRE)
                   .otherwise(run.ROUND_TRIP_POST))
    else:
        rt_expr = pl.lit(rt)
    return gross.with_columns((pl.col('gross') - pl.col('turn') * rt_expr).alias('net'))


def stat_block(net: np.ndarray, n_trials: int) -> dict:
    s = run.stats(net)
    ex = net - BENCH_WEEKLY
    from src.backtest.performance.benchmark_eval import deflated_sharpe_ratio
    return {'annual': s['annual'], 'mdd': s['mdd'], 'vol': s['vol'],
            'excess_vs_5.8': s['annual'] - BENCH_ANNUAL,
            't_vs_5.8': run.nw_t(ex),
            'dsr_vs_5.8': float(deflated_sharpe_ratio(ex, n_trials=n_trials, periods_per_year=52))}


def evaluate(gross_a: pl.DataFrame, base: pl.DataFrame, rt: float | None) -> dict:
    a = net_with_cost(gross_a, rt)
    b = net_with_cost(base, rt)
    j = a.select(['date', 'net', 'turn', 'n']).join(
        b.select(['date', pl.col('net').alias('bnet'), pl.col('n').alias('bn')]), on='date').sort('date')
    na, nb = j['net'].to_numpy(), j['bnet'].to_numpy()
    out = {'A': stat_block(na, N_MAIN), 'B_ew': stat_block(nb, N_MAIN),
           'excess_vs_B': float(run.stats(na)['annual'] - run.stats(nb)['annual']),
           'dsr_main_N%d' % N_MAIN: out_dsr(na, N_MAIN),
           'dsr_N4': out_dsr(na, N_ROUND), 'dsr_N155': out_dsr(na, N_UPPER),
           'turn_A_annual': float(j['turn'].mean() * 52),
           'kept_ratio_mean': float((j['n'] / j['bn']).mean())}
    out['segments_excess_vs_5.8'] = {}
    for name, s0, s1 in run.SEGS:
        k = j.filter((pl.col('date') >= s0) & (pl.col('date') <= s1))
        if k.height > 10:
            out['segments_excess_vs_5.8'][name] = float(
                run.stats(k['net'].to_numpy())['annual'] - BENCH_ANNUAL)
    return out


def out_dsr(net: np.ndarray, n_trials: int) -> float:
    from src.backtest.performance.benchmark_eval import deflated_sharpe_ratio
    return float(deflated_sharpe_ratio(net - BENCH_WEEKLY, n_trials=n_trials, periods_per_year=52))


def main() -> int:
    t0 = time.perf_counter()
    df, weeks, _ = run.load()
    df = run.features(df)
    sig = run.signal_frame(df, weeks)
    hold = run.holding_returns(df, weeks)
    bias = (df.filter(pl.col('date').is_in(weeks)).select(['date', run.G, 'bias12'])
            .unique(['date', run.G], keep='last'))
    sig = sig.join(bias, on=['date', run.G], how='left').with_columns(
        (pl.col('bias12') > 0.10).fill_null(False).alias('r1b'))
    base_gross = run.run_group(sig, hold, None)
    res = {'weeks': len(weeks),
           'bench_annual': BENCH_ANNUAL,
           'n_trials': {'main': N_MAIN, 'round': N_ROUND, 'upper': N_UPPER},
           'hit_rate': {k: float(sig[k].mean()) for k in ('r1b', 'r1', 'r2', 'r3', 'r4')},
           'variants': {}}
    for vname, rules in VARIANTS.items():
        g = run.run_group(sig, hold, rules)
        res['variants'][vname] = {'rules': list(rules), 'costs': {}}
        for cname, rt in COSTS.items():
            res['variants'][vname]['costs'][cname] = evaluate(g, base_gross, rt)
        m = res['variants'][vname]['costs']['21.9bp(历史印花税)']
        print(f'[{vname}] {"+".join(rules)}：年化 {m["A"]["annual"]:+.2%}，'
              f'超额(vs5.8%) {m["A"]["excess_vs_5.8"]:+.2%}，t {m["A"]["t_vs_5.8"]:.2f}，'
              f'DSR(N={N_MAIN}) {m["A"]["dsr_vs_5.8"]:.3f}', flush=True)
    res['elapsed_s'] = round(time.perf_counter() - t0, 1)
    (HERE / 'result_narrow.json').write_text(
        json.dumps(res, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print(f"[done] result_narrow.json ({res['elapsed_s']}s)", flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
