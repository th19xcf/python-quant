#!/usr/bin/env python3
"""beta 打底 + alpha 叠加：五条组合曲线的账户级对比（2026-11-21）。

复用骨架（不重写）
------------------
- 行情/引擎/成本/风控：``cross_section_score``（其内部复用
  ``baseline_pv_val_equal_weight`` + ``factor_lowvol_quantile`` +
  ``factor_lowvol_variants``）；
- 因子与打分：``cross_section_score.load_panel_factors`` /
  ``load_quality`` / ``load_momentum`` / ``month_scores`` / ``pick``，
  维度 = 估值 + 质量（去动量），top30。

五条曲线
--------
1. ``beta_ew``        全 A 等权、月末再平衡；
2. ``alpha_only``     估值+质量 top30 等权、满仓；
3. ``mix40_full``     alpha 40% + 全 A beta 60%，月度拉到目标权重；
4. ``overlay40``      同上，但 beta 篮子剔除 alpha 名（覆盖式，不重复计入）；
5a. ``overlay40_ma20``    全 A 等权指数在 20 日均线上方才满仓，否则 50%；
5b. ``overlay40_vol15``   波动率目标：目标年化 15%，20 日实测波动定仓，30%~100%。

空仓部分按年化 **1.5%**（假设，非实测）计息，逐日在事后叠加到引擎曲线上。

产物
----
``work/beta_alpha_2026-11-21/``：``compare.csv``、``equity_*.csv``、``run.log``

用法
----
    .venv\\Scripts\\python.exe tools/beta_alpha_combo.py
    .venv\\Scripts\\python.exe tools/beta_alpha_combo.py --skip-sweep
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from bisect import bisect_right
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import baseline_pv_val_equal_weight as base
import cross_section_score as cs
import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2013-06-01'
END = '2026-10-31'
SIGNAL_START = date(2015, 1, 1)
CASH_BUFFER = f.CASH_BUFFER           # 与既有基准一致：1% 现金缓冲
CASH_RATE = 0.015                     # 空仓收益假设（年化）
ALPHA_N = 30
ALPHA_W = 0.4
MA_WINDOW = 20
VOL_WINDOW = 20
VOL_TARGET = 0.15
POS_FLOOR, POS_CAP = 0.30, 1.00
WINDOWS = {
    'full': (date(2015, 1, 1), date(2026, 10, 31)),
    'seg1': (date(2015, 1, 1), date(2019, 12, 31)),
    'seg2': (date(2020, 1, 1), date(2023, 12, 31)),
    'seg3': (date(2024, 1, 1), date(2026, 10, 31)),
    'w2018': (date(2018, 1, 1), date(2026, 10, 31)),
}
OUT_DIR = ROOT / 'work' / 'beta_alpha_2026-11-21'
RUN_DIR = ROOT / 'data/local_store/backtest_runs/beta_alpha_20261121'


# ---------------------------------------------------------------------------
# 全 A 等权日频指数（只作择时信号，不作收益来源）
# ---------------------------------------------------------------------------

def build_ew_index(data: Dict[str, pl.DataFrame], calendar: List[date]
                   ) -> Tuple[np.ndarray, np.ndarray]:
    """日频等权指数：逐日取当日有行情的个股收益均值，逐日复利成点位。

    返回 ``(level, ret)``，与 ``calendar`` 等长；``ret[0] = 0``。
    """
    n = len(calendar)
    cals = np.array(calendar, dtype='datetime64[D]')
    acc = np.zeros(n)
    cnt = np.zeros(n)
    for df in data.values():
        dates = df['date'].to_numpy()
        close = df['close'].to_numpy().astype('float64')
        if close.size < 2:
            continue
        pos = np.searchsorted(cals, dates)
        ok = (pos[1:] < n) & (close[:-1] > 0) & (close[1:] > 0)
        if not ok.any():
            continue
        ret = np.zeros(close.size - 1)
        ret[ok] = close[1:][ok] / close[:-1][ok] - 1.0
        idx = pos[1:][ok]
        acc += np.bincount(idx, weights=ret[ok], minlength=n)
        cnt += np.bincount(idx, minlength=n)
    ret = acc / np.maximum(cnt, 1.0)
    level = np.cumprod(1.0 + ret)
    return level, ret


def month_positions(months: List[date], calendar: List[date],
                    level: np.ndarray, ret: np.ndarray) -> Dict[str, Dict[date, float]]:
    """月末 T 决定、T+1 起生效的仓位序列（只用 T 日及之前数据）。"""
    cals = np.array(calendar, dtype='datetime64[D]')
    pos_ma: Dict[date, float] = {}
    pos_vt: Dict[date, float] = {}
    for m in months:
        i = int(np.searchsorted(cals, np.datetime64(m, 'D')))
        if i >= level.size:
            i = level.size - 1
        if i < MA_WINDOW:
            continue
        ma = float(level[i - MA_WINDOW + 1:i + 1].mean())
        pos_ma[m] = 1.0 if level[i] > ma else 0.5
        w = ret[max(0, i - VOL_WINDOW + 1):i + 1]
        vol = float(np.std(w, ddof=1) * np.sqrt(252)) if w.size > 2 else 0.0
        pos_vt[m] = float(np.clip(VOL_TARGET / vol, POS_FLOOR, POS_CAP)) if vol > 0 else 1.0
    return {'ma20': pos_ma, 'vol15': pos_vt}


# ---------------------------------------------------------------------------
# 权重与调度表
# ---------------------------------------------------------------------------

def build_weights(months: List[date], univ: Dict[date, List[str]],
                  alpha_path: Dict[date, List[str]], alpha_w: float,
                  pos: Dict[date, float] | None = None,
                  beta_excl_alpha: bool = False) -> Dict[date, Dict[str, float]]:
    """月度目标权重：alpha 袖套 ``alpha_w``，其余给 beta 篮子，总和 = 0.99 × 仓位。"""
    out: Dict[date, Dict[str, float]] = {}
    for m in months:
        mult = (1.0 - CASH_BUFFER) * (pos.get(m, 1.0) if pos else 1.0)
        names = alpha_path.get(m) or []
        w: Dict[str, float] = {}
        aset = set(names)
        if alpha_w > 0 and names:
            aw = mult * alpha_w / len(names)
            for c in names:
                w[c] = aw
        bw = mult * (1.0 - alpha_w)
        pool = [c for c in univ[m] if not (beta_excl_alpha and c in aset)]
        if bw > 0 and pool:
            per = bw / len(pool)
            for c in pool:
                w[c] = w.get(c, 0.0) + per
        out[m] = w
    return out


def to_schedules(weights: Dict[date, Dict[str, float]],
                 prev_day: Dict[date, date]) -> Dict[str, Dict[date, Tuple[str, float]]]:
    """月频目标权重 → 引擎调度表（卖出日先减到目标权重，信号日开盘再补足）。"""
    sched: Dict[str, Dict[date, Tuple[str, float]]] = {}
    prev: set = set()
    for m in sorted(weights):
        cur = weights[m]
        sell_day = prev_day.get(m)
        for code, w in cur.items():
            sig = sched.setdefault(code, {})
            if sell_day is not None:
                sig[sell_day] = ('sell', w)
            sig[m] = ('buy', w)
        for code in prev - set(cur):
            if sell_day is not None:
                sched.setdefault(code, {})[sell_day] = ('sell', 0.0)
        prev = set(cur)
    return sched


def gross_by_day(weights: Dict[date, Dict[str, float]], months_sorted: List[date],
                 calendar: List[date]) -> Dict[date, float]:
    """每个交易日实际生效的目标总仓位（月内不变）。"""
    gross_m = {m: float(sum(w.values())) for m, w in weights.items()}
    out: Dict[date, float] = {}
    for d in calendar:
        j = bisect_right(months_sorted, d) - 1
        m = months_sorted[max(j, 0)]
        out[d] = gross_m.get(m, 0.0)
    return out


# ---------------------------------------------------------------------------
# 空仓计息 + 指标
# ---------------------------------------------------------------------------

def adjust_cash(curve: List[Tuple[date, float]], gross: Dict[date, float],
                rf: float = CASH_RATE) -> List[Tuple[date, float]]:
    """引擎现金不计息；把「1 - 总仓位」按年化 rf 逐日补回（假设，非实测）。"""
    if not curve:
        return curve
    out = [(curve[0][0], curve[0][1])]
    adj = curve[0][1]
    daily = rf / 252.0
    for i in range(1, len(curve)):
        d, eq = curve[i]
        r = eq / curve[i - 1][1] - 1.0
        r += (1.0 - gross.get(d, 0.0)) * daily
        adj = adj * (1.0 + r)
        out.append((d, adj))
    return out


def window_stats(curve: List[Tuple[date, float]], trades: List[Dict],
                 a: date, b: date, holdings: List[int],
                 gross: Dict[date, float]) -> Dict[str, Any]:
    st = cs.stats(curve, trades, a, b, holdings)
    rows = [(d, e) for d, e in curve if a <= d <= b]
    st['avg_position'] = round(float(np.mean([gross.get(d, 0.0) for d, _ in rows])), 4) if rows else None
    return st


def run_one(name: str, weights: Dict[date, Dict[str, float]], data,
            dividends, st_status, prev_day, months_sorted, calendar) -> Dict[str, Any]:
    sched = to_schedules(weights, prev_day)
    t0 = time.perf_counter()
    res = cs.run_backtest(data, sched, dividends, st_status)
    curve = f.curve_of(res)
    trades = res.get('trades') or []
    gross = gross_by_day(weights, months_sorted, calendar)
    curve_adj = adjust_cash(curve, gross)

    row: Dict[str, Any] = {'run': name}
    for tag, (a, b) in WINDOWS.items():
        hold = [len(weights[m]) for m in months_sorted if a <= m <= b]
        st = window_stats(curve_adj, trades, a, b, hold, gross)
        for k, val in st.items():
            row[f'{tag}_{k}'] = val
    row['elapsed_s'] = round(time.perf_counter() - t0, 1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': [d for d, _ in curve_adj],
                  'equity': [e for _, e in curve_adj]}).write_csv(OUT_DIR / f'equity_{name}.csv')
    if trades:
        pl.DataFrame(trades, infer_schema_length=None).write_parquet(
            RUN_DIR / f'{name}_trades.parquet')
    pl.DataFrame({'signal_date': sorted(weights),
                  'n_names': [len(weights[m]) for m in sorted(weights)],
                  'gross': [round(sum(weights[m].values()), 4) for m in sorted(weights)]}
                 ).write_csv(RUN_DIR / f'{name}_holdings.csv')
    logger.info('{} => {}', name, json.dumps(
        {k: row[f'full_{k}'] for k in ('annual', 'max_drawdown', 'sharpe',
                                       'avg_position')}, ensure_ascii=False))
    del res, curve, trades
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--skip-sweep', action='store_true', help='跳过混合比例扫描')
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level='INFO')
    logger.add(OUT_DIR / 'run.log', level='INFO', encoding='utf-8')
    t_start = time.perf_counter()

    data = base.load_universe(WARMUP_START, END)
    data, violations = base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if SIGNAL_START <= m <= date.fromisoformat(END)]
    logger.info('信号月 {} 个 {}~{}；标的 {} 只；契约剔除 {} 只',
                len(months), months[0], months[-1], len(data), len(violations))

    quoted = v.quoted_sets(data, months)
    mom, bars = cs.load_momentum(data, months)
    panel = cs.load_panel_factors(months)
    quality = cs.load_quality(months)
    st_sets = cs.load_st_sets(months)

    univ: Dict[date, List[str]] = {}
    for m in months:
        univ[m] = sorted(c for c in quoted[m]
                         if bars[m].get(c, 0) >= cs.MIN_BARS and c not in st_sets[m])
    scores = {m: cs.month_scores(m, univ[m], panel, quality, mom) for m in months}
    alpha_path = {m: cs.pick(scores[m], ['value', 'quality'], ALPHA_N) for m in months}
    logger.info('alpha 名单完成：{} 个月，平均 {} 只', len(alpha_path),
                round(np.mean([len(x) for x in alpha_path.values()]), 1))

    level, ret = build_ew_index(data, calendar)
    pos = month_positions(months, calendar, level, ret)
    mean_ma = float(np.mean([pos['ma20'][m] for m in months if m in pos['ma20']]))
    mean_vt = float(np.mean([pos['vol15'][m] for m in months if m in pos['vol15']]))
    logger.info('择时平均仓位：MA20 {:.3f}；vol15 {:.3f}', mean_ma, mean_vt)

    dividends = cs._load_local_dividends(list(data), None, None)
    st_status = cs._load_st_status(None, data, None, WARMUP_START, END, None, None)

    curves: List[Tuple[str, str, float, str, Dict[date, Dict[str, float]]]] = [
        ('beta_ew', '纯beta 全A等权', 0.0, '满仓', build_weights(months, univ, alpha_path, 0.0)),
        ('alpha_only', '纯alpha 估值+质量top30', 1.0, '满仓', build_weights(months, univ, alpha_path, 1.0)),
        ('mix40_full', '静态混合 40/60', ALPHA_W, '满仓',
         build_weights(months, univ, alpha_path, ALPHA_W)),
        ('overlay40', 'beta打底+alpha覆盖(剔除alpha名)', ALPHA_W, '满仓',
         build_weights(months, univ, alpha_path, ALPHA_W, beta_excl_alpha=True)),
        ('overlay40_ma20', '覆盖 + MA20开关', ALPHA_W, 'ma20',
         build_weights(months, univ, alpha_path, ALPHA_W, pos=pos['ma20'], beta_excl_alpha=True)),
        ('overlay40_vol15', '覆盖 + 波动率目标15%', ALPHA_W, 'vol15',
         build_weights(months, univ, alpha_path, ALPHA_W, pos=pos['vol15'], beta_excl_alpha=True)),
        (f'overlay40_fix{mean_ma:.2f}', '同平均仓位对照(MA20均值)', ALPHA_W, f'fix{mean_ma:.2f}',
         build_weights(months, univ, alpha_path, ALPHA_W,
                       pos={m: mean_ma for m in months}, beta_excl_alpha=True)),
        (f'overlay40_fix{mean_vt:.2f}', '同平均仓位对照(vol15均值)', ALPHA_W, f'fix{mean_vt:.2f}',
         build_weights(months, univ, alpha_path, ALPHA_W,
                       pos={m: mean_vt for m in months}, beta_excl_alpha=True)),
    ]
    if not args.skip_sweep:
        for w in (0.2, 0.3, 0.5, 0.6):
            curves.append((f'mix{int(w * 100)}_full', f'混合比例扫描 alpha {int(w * 100)}%',
                           w, '满仓', build_weights(months, univ, alpha_path, w)))

    rows: List[Dict[str, Any]] = []
    meta: Dict[str, Dict[str, Any]] = {}
    for name, desc, aw, rule, w in curves:
        r = run_one(name, w, data, dividends, st_status, prev_day, months, calendar)
        r['desc'] = desc
        r['alpha_weight'] = aw
        r['position_rule'] = rule
        rows.append(r)
        meta[name] = {'alpha_weight': aw, 'position_rule': rule}
        del w

    beta = next(r for r in rows if r['run'] == 'beta_ew')
    for r in rows:
        for tag in WINDOWS:
            r[f'{tag}_excess'] = (round(r[f'{tag}_annual'] - beta[f'{tag}_annual'], 6)
                                  if r.get(f'{tag}_annual') is not None
                                  and beta.get(f'{tag}_annual') is not None else None)
    cmp_df = pl.DataFrame(rows, infer_schema_length=None)
    front = ['run', 'desc', 'alpha_weight', 'position_rule']
    rest = [c for c in cmp_df.columns if c not in front]
    cmp_df = cmp_df.select(front + rest)
    cmp_df.write_csv(OUT_DIR / 'compare.csv')
    logger.info('总耗时 {:.1f}s', time.perf_counter() - t_start)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
