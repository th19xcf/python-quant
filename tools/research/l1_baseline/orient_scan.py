#!/usr/bin/env python
"""扫描 run_l1.evaluate_one 的 orient=-1 翻转缺陷影响面（只读）。

缺陷定义（2026-11-11 修复前的 run_l1.evaluate_one）：
    orient=-1 时把 q5_excess_bp 乘 -1，但 turnover_q5 / turnover_mean 不翻。
    q5_excess 的正确取向值应是「原始 Q1 组相对全池的超额」；
    单取反 q5_excess 得到的是 (total - raw_Q5)，与 (raw_Q1 - total) 不等。
    Q5-Q1 与 mono 的取反在数学上正确（小者取反即镜像组），不受影响。

输出：
    orient_defect_scan_h20.csv   49 因子逐行
    orient_scan_summary.json     计数与极值
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[3]
# 中间缓存（可重建）：pre_h20 / labels_oo_h20
STAGE = ROOT / '_staging' / 'l1_equal_weight_20261111'
# 证据产物：扫描表与汇总
OUT = ROOT / 'reports' / 'l1_baseline'
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_l1  # noqa: E402

N_GROUPS = 5
MIN_NAMES = 100
FWD = 'fwd_20_oo'


def group_detail(df: pl.DataFrame, col: str) -> dict:
    """与 run_l1.group_stats 完全同式，另返回每组均值与每组换手。"""
    w = df.select(['date', 'ts_code', col, FWD]).filter(
        pl.col(col).is_not_null() & pl.col(FWD).is_not_null())
    w = w.with_columns(
        pl.col(col).rank().over('date').alias('_r'),
        pl.len().over('date').alias('_n'),
    ).filter(pl.col('_n') >= MIN_NAMES).with_columns(
        ((pl.col('_r').cast(pl.UInt32) * N_GROUPS) // (pl.col('_n') + 1) + 1)
        .cast(pl.Int8).alias('_g'))
    per = w.group_by(['date', '_g']).agg(
        pl.col(FWD).mean().alias('ret'),
        pl.col('ts_code').alias('members')).sort(['date', '_g'])
    means: dict[int, float] = {}
    turns: dict[int, float] = {}
    for g in range(1, N_GROUPS + 1):
        sub = per.filter(pl.col('_g') == g)
        means[g] = float(sub['ret'].mean())
        sets = [set(x) for x in sub['members'].to_list()]
        ch = [len(prev - cur) / max(len(prev), 1)
              for prev, cur in zip(sets[:-1], sets[1:], strict=False)]
        turns[g] = float(np.mean(ch)) if ch else 0.0
    total = float(w[FWD].mean())
    return {'means': means, 'turns': turns, 'total': total,
            'n_periods': per['date'].n_unique()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--work-dir', default=None,
                    help='中间缓存目录（默认 _staging/l1_equal_weight_20261111）')
    ap.add_argument('--out-dir', default=None,
                    help='证据产物目录（默认 reports/l1_baseline）')
    args = ap.parse_args()
    global STAGE, OUT
    if args.work_dir:
        STAGE = Path(args.work_dir).resolve()
    if args.out_dir:
        OUT = Path(args.out_dir).resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    pre = pl.read_parquet(STAGE / 'pre_h20.parquet',
                          columns=['date', 'ts_code', *run_l1.FACTORS])
    lab = pl.read_parquet(STAGE / 'labels_oo_h20.parquet')
    df = pre.join(lab, on=['date', 'ts_code'], how='left')

    ic = pl.read_csv(OUT / 'single_factor_h20_oo.csv')
    icmap = {r['factor']: r for r in ic.iter_rows(named=True)}

    rows = []
    for f in run_l1.FACTORS:
        if f not in df.columns:
            continue
        d = group_detail(df, f)
        m, t, total = d['means'], d['turns'], d['total']
        raw_q5_exc = (m[5] - total) * 1e4
        raw_q1_exc = (m[1] - total) * 1e4
        raw_spread = (m[5] - m[1]) * 1e4
        rank_ic = float(icmap[f]['rank_ic'])
        orient = 1.0 if rank_ic > 0 else -1.0
        # evaluate_one 的实际输出
        code_exc = raw_q5_exc * orient
        code_turn = t[5]
        code_spread = raw_spread * orient
        # 正确的取向输出：镜像组 = 原始 Q1
        true_exc = raw_q1_exc if orient < 0 else raw_q5_exc
        true_turn = t[1] if orient < 0 else t[5]
        true_spread = -raw_spread if orient < 0 else raw_spread
        rows.append({
            'factor': f,
            'rank_ic': rank_ic,
            'icir': float(icmap[f]['icir']),
            'abs_icir': float(icmap[f]['abs_icir']),
            'orientation': orient,
            'raw_q5_excess_bp': raw_q5_exc,
            'raw_q1_excess_bp': raw_q1_exc,
            'code_flipped_excess_bp': code_exc,
            'true_oriented_excess_bp': true_exc,
            'excess_err_bp': code_exc - true_exc,
            'raw_q5_turnover': t[5],
            'raw_q1_turnover': t[1],
            'code_turnover': code_turn,
            'true_oriented_turnover': true_turn,
            'turnover_err_pp': (code_turn - true_turn) * 100,
            'raw_spread_bp': raw_spread,
            'code_spread_bp': code_spread,
            'true_oriented_spread_bp': true_spread,
            'spread_err_bp': code_spread - true_spread,
            'n_periods': d['n_periods'],
        })

    scan = pl.DataFrame(rows).sort(['orientation', 'abs_icir'], descending=[False, True])
    scan.write_csv(OUT / 'orient_defect_scan_h20.csv')

    neg = scan.filter(pl.col('orientation') < 0)
    pos = scan.filter(pl.col('orientation') > 0)
    top20 = neg.head(20).select(
        ['factor', 'abs_icir', 'code_flipped_excess_bp', 'true_oriented_excess_bp',
         'excess_err_bp', 'code_turnover', 'true_oriented_turnover', 'turnover_err_pp'])
    summary = {
        'panel': 'pre_h20.parquet + labels_oo_h20.parquet（143 期，open->open）',
        'n_factors': scan.height,
        'n_negative_orientation': neg.height,
        'n_positive_orientation': pos.height,
        'negative_share': neg.height / scan.height if scan.height else None,
        'positive_side_max_abs_err_bp': float(pos['excess_err_bp'].abs().max()) if pos.height else 0.0,
        'negative_side_excess_err_bp': {
            'min': float(neg['excess_err_bp'].min()),
            'max': float(neg['excess_err_bp'].max()),
            'mean_abs': float(neg['excess_err_bp'].abs().mean()),
            'max_abs': float(neg['excess_err_bp'].abs().max()),
            'n_abs_gt_1bp': int((neg['excess_err_bp'].abs() > 1).sum()),
            'n_abs_gt_5bp': int((neg['excess_err_bp'].abs() > 5).sum()),
        },
        'negative_side_turnover_err_pp': {
            'min': float(neg['turnover_err_pp'].min()),
            'max': float(neg['turnover_err_pp'].max()),
            'mean_abs': float(neg['turnover_err_pp'].abs().mean()),
            'n_gt_1pp': int((neg['turnover_err_pp'].abs() > 1).sum()),
        },
        'spread_err_bp_max_abs': float(scan['spread_err_bp'].abs().max()),
        'top20_negative_by_abs_icir': top20.to_dicts(),
    }
    (OUT / 'orient_scan_summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in summary.items()
                      if k != 'top20_negative_by_abs_icir'},
                     ensure_ascii=False, indent=2))
    print(top20)


if __name__ == '__main__':
    main()
