#!/usr/bin/env python3

"""用 880/881 白黑名单重算行业池 σ（K=5/K=10，2019+/2022+），并输出对照表。

口径完全沿用 ``docs/低噪声标的σ实测_2026-11-11.md`` §0.1/§3：

- 月收益：月末收盘对上一有效月末，单位 pp；880/881 无复权事件，直接用 ``close``
- 月末门控：标的该月最后一条行情日期必须等于 ``000300.SH`` 当月最后交易日
- 未完成月：仅剔除最后一个未完成月（当日为月中时）
- σ_abs：等权组合月收益的样本标准差（ddof=1）
- σ(Δ)：月初按过去 ``mom`` 个月收益排序取前 K 等权持有下月；同日从同一可投域随机抽 K 只
  （B 次，固定 seed）取均值；Δ = 规则 − 随机
- MDE：``(t_{1-α*/2,n-1} + t_{0.80,n-1}) × σ(Δ)/√n``，α*=0.0025，年化 ``(1+MDE/100)^12-1``

用法::

    python tools/index_pool_sigma.py
    # → reports/data_audit/index_pool_sigma_20261111.csv

先重算旧口径（880 段剔除任一月 |收益|>50pp、有效月数≥24）做标定，与上游报告
（542 只 / σ_abs 8.04 / σ(Δ) 6.16）对得上，再跑各白名单池。
"""

from __future__ import annotations

import argparse
import os

import numpy as np
import polars as pl
from scipy.stats import t as t_dist

K = 5
MOM = 3
B = 200
SEED = 7
ALPHA = 0.0025
POWER_T = 0.80
WINDOWS = {'2019+': '2019-01', '2022+': '2022-01'}
FULL_WINDOW = {'全历史': '1990-01', '2019+': '2019-01', '2022+': '2022-01'}


def calendar(qdir: str) -> dict[str, object]:
    bench = pl.read_parquet(os.path.join(qdir, '000300.SH.parquet')).sort('date')
    e = (bench.with_columns(pl.col('date').dt.strftime('%Y-%m').alias('ym'))
         .group_by('ym', maintain_order=True).agg(pl.col('date').max()).sort('ym'))
    # 未完成月：当日所在月的最后交易日尚未到，仅剔除最后一个月
    e = e.filter(pl.col('ym') != e['ym'].max())
    return dict(zip(e['ym'].to_list(), e['date'].to_list(), strict=False))


def month_series(path: str, cal: dict[str, object]) -> pl.DataFrame:
    q = pl.read_parquet(path).select(['date', 'close']).sort('date').drop_nulls('close')
    q = q.filter(pl.col('close') > 0)
    m = (q.with_columns(pl.col('date').dt.strftime('%Y-%m').alias('ym'))
         .group_by('ym', maintain_order=True)
         .agg(pl.col('date').last().alias('d'), pl.col('close').last().alias('c'))
         .sort('ym'))
    m = m.filter(pl.col('d') == pl.col('ym').replace_strict(
        cal, return_dtype=pl.Date, default=None))
    return m.select(['ym', (pl.col('c') / pl.col('c').shift(1) - 1).mul(100).alias('r')]
                    ).drop_nulls('r')


def build(qdir: str, cal: dict[str, object], codes: list[str],
          trunc: dict[str, str] | None = None) -> pl.DataFrame:
    frames = []
    for c in codes:
        p = os.path.join(qdir, f'{c}.parquet')
        if not os.path.isfile(p):
            continue
        df = month_series(p, cal)
        if trunc and trunc.get(c):
            df = df.filter(pl.col('ym') > str(trunc[c])[:7])
        if df.height:
            frames.append(df.with_columns(pl.lit(c).alias('code')))
    if not frames:
        return pl.DataFrame({'ym': [], 'r': [], 'code': []})
    return pl.concat(frames)


def ret_matrix(long: pl.DataFrame) -> tuple[list[str], list[str], np.ndarray]:
    wide = long.pivot(index='ym', on='code', values='r',
                      aggregate_function='first').sort('ym')
    yms = wide['ym'].to_list()
    codes = [c for c in wide.columns if c != 'ym']
    return yms, codes, wide.select(codes).to_numpy().astype(float)


def sigma_abs(yms: list[str], mat: np.ndarray, start: str) -> tuple[float, int]:
    rows = []
    for i, y in enumerate(yms):
        if y < start:
            continue
        v = mat[i][~np.isnan(mat[i])]
        if v.size >= 2:
            rows.append(float(v.mean()))
    a = np.array(rows)
    return float(a.std(ddof=1)), int(a.size)


def sigma_delta(yms: list[str], mat: np.ndarray, start: str, k: int = K) -> dict[str, float]:
    rng = np.random.default_rng(SEED)
    deltas, rots, rands = [], [], []
    idx = [i for i, y in enumerate(yms) if y >= start]
    for j, i in enumerate(idx):
        if j < MOM:
            continue
        cur = mat[i]
        avail = np.where(~np.isnan(cur))[0]
        if avail.size < k:
            continue
        past = mat[i - MOM:i, avail]
        ok = ~np.isnan(past).any(axis=0)
        cand = avail[ok]
        if cand.size < k:
            continue
        mom = np.prod(1 + past[:, ok] / 100, axis=0) - 1
        top = cand[np.argsort(-mom, kind='stable')[:k]]
        rot = float(cur[top].mean())
        draws = np.array([rng.choice(avail, k, replace=False) for _ in range(B)])
        rand = float(cur[draws].mean())
        deltas.append(rot - rand)
        rots.append(rot)
        rands.append(rand)
    d = np.array(deltas)
    n = d.size
    tcrit = float(t_dist.ppf(1 - ALPHA / 2, df=n - 1))
    tpow = float(t_dist.ppf(POWER_T, df=n - 1))
    mde = (tcrit + tpow) * d.std(ddof=1) / np.sqrt(n)
    return {'n': float(n), 'delta': float(d.mean()), 'sigma_delta': float(d.std(ddof=1)),
            'sigma_rot': float(np.array(rots).std(ddof=1)),
            'sigma_rand': float(np.array(rands).std(ddof=1)),
            'mde': float(mde), 'ann': float(((1 + mde / 100) ** 12 - 1) * 100)}


def evaluate(pname: str, ys: list[str], ms: np.ndarray, members: list[str],
             codes: list[str], extra: dict | None = None) -> dict:
    sm = ms[:, [codes.index(c) for c in members if c in codes]]
    rec: dict = {'pool': pname, 'members': len([c for c in members if c in codes])}
    rec.update(extra or {})
    for name, st in FULL_WINDOW.items():
        s, n = sigma_abs(ys, sm, st)
        rec[f'sigma_abs_{name}'] = round(s, 2)
        rec[f'n_abs_{name}'] = n
    for name, st in WINDOWS.items():
        for k in (5, 10):
            d = sigma_delta(ys, sm, st, k)
            rec[f'sd{k}_{name}'] = round(d['sigma_delta'], 2)
            rec[f'mde{k}_{name}'] = round(d['mde'], 2)
            rec[f'ann{k}_{name}'] = round(d['ann'], 1)
            rec[f'n{k}_{name}'] = int(d['n'])
            rec[f'delta{k}_{name}'] = round(d['delta'], 2)
            rec[f'rot{k}_{name}'] = round(d['sigma_rot'], 2)
            rec[f'rand{k}_{name}'] = round(d['sigma_rand'], 2)
    print(pname, rec, flush=True)
    return rec


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--store', default='data/local_store')
    ap.add_argument('--class-csv', default='reports/data_audit/'
                                          'index_880_881_pool_classification_20261111.csv')
    ap.add_argument('--out', default='reports/data_audit/index_pool_sigma_20261111.csv')
    args = ap.parse_args()

    qdir = os.path.join(args.store, 'quotes', 'index', 'sh')
    cal = calendar(qdir)
    cls = pl.read_csv(args.class_csv).select(
        ['ts_code', 'class', 'segment', 'jump_date', 'in_anomaly_50pp'])
    all_codes = sorted(cls['ts_code'].to_list())
    yms, codes, mat = ret_matrix(build(qdir, cal, all_codes))

    seg = {a: str(b) for a, b in
           zip(cls['ts_code'].to_list(), cls['segment'].to_list(), strict=False)}
    klass = dict(zip(cls['ts_code'].to_list(), cls['class'].to_list(), strict=False))
    jump = dict(zip(cls['ts_code'].to_list(), cls['jump_date'].to_list(), strict=False))
    anomaly = set(cls.filter(pl.col('in_anomaly_50pp'))['ts_code'].to_list())

    # 标定：旧口径 880 段，剔除 |月收益|>50pp，且有效月数>=24
    c880 = [c for c in codes if seg.get(c) == '880']
    bad, keep = [], []
    for c in c880:
        v = mat[:, codes.index(c)]
        v = v[~np.isnan(v)]
        if np.abs(v).max() > 50:
            bad.append(c)
        elif v.size >= 24:
            keep.append(c)
    print(f'[标定] 880 有行情 {len(c880)} 只；含|r|>50pp {len(bad)} 只；'
          f'清洗后>=24月 {len(keep)} 只', flush=True)

    p881 = [c for c in codes if seg.get(c) == '881']
    p880w = [c for c in codes if seg.get(c) == '880' and klass.get(c) == 'white']
    s_all, trunc_all = [], {}
    for c in codes:
        k = klass.get(c)
        if k == 'white':
            s_all.append(c)
        elif k == 'defect' and jump.get(c):
            s_all.append(c)
            trunc_all[c] = str(jump[c])
    p881_cut = [c for c in p881 if klass.get(c) == 'white'
                or (klass.get(c) == 'defect' and jump.get(c))]
    trunc881 = {c: str(jump[c]) for c in p881_cut if jump.get(c)}

    pools = {
        'OLD_880清洗': keep,
        '880_white': p880w,
        '881_全量': p881,
        '881_white': [c for c in p881 if klass.get(c) == 'white'],
        '合并_881全量+880white': p881 + p880w,
        '217内_white': [c for c in codes if klass.get(c) == 'white' and c in anomaly],
    }
    rows = [evaluate(n, yms, mat, m, codes) for n, m in pools.items()]

    y6, c6, m6 = ret_matrix(build(qdir, cal, p881_cut, trunc881))
    rows.append(evaluate('881_white+defect截断', y6, m6, p881_cut, c6,
                         {'truncated': len(trunc881)}))
    ya, ca, ma = ret_matrix(build(qdir, cal, s_all, trunc_all))
    rows.append(evaluate('全库_white+defect截断', ya, ma, s_all, ca,
                         {'truncated': len(trunc_all)}))
    rows.append(evaluate('全库_white+defect不截断', yms, mat, s_all, codes))

    out = pl.DataFrame(rows)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.write_csv(args.out)
    print('CSV:', args.out, flush=True)


if __name__ == '__main__':
    main()
