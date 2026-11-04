#!/usr/bin/env python3
"""聚焦分析：复权因子快照精度 / 行情内因子列是否过期（只读）。"""
from __future__ import annotations

import bisect
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_data_20261104 import (  # noqa: E402
    LOCAL, OUT, load_event_dict, load_factor_dict, naive_factors, quote_files,
    to_int_days, hrank,
)

ev, div = load_event_dict()
fac = load_factor_dict()

codes = sorted(p.stem for p in quote_files('stock'))
stats = {
    'quote_all_one_but_snapshot_not': [],
    'snapshot_all_one_but_quote_not': [],
    'quote_vs_snapshot': [],
    'naive_vs_snapshot_qfq': [],
    'naive_vs_snapshot_hfq': [],
}
qfq_round = hfq_round = 0
max_qfq_r = max_hfq_r = 0.0
snap_missing_tail = 0
snap_missing_head = 0

for code in codes:
    p = LOCAL / 'quotes' / 'stock' / code.split('.')[-1].lower() / f'{code}.parquet'
    q = pl.read_parquet(p, columns=['date', 'close', 'qfq_factor', 'hfq_factor'])
    d = to_int_days(q['date']); c = q['close'].to_numpy()
    sq = q['qfq_factor'].to_numpy(); sh = q['hfq_factor'].to_numpy()
    fd, fq, fh = fac[code]
    # 对齐快照到行情日期
    idx = np.searchsorted(fd, d)
    inb = idx < len(fd)
    idx_c = np.clip(idx, 0, len(fd) - 1)
    aligned = inb & (fd[idx_c] == d)
    if fd[0] > d[0]:
        snap_missing_head += 1
    if fd[-1] < d[-1]:
        snap_missing_tail += 1

    qn, hn = naive_factors(d, c, ev.get(code, {
        'date': np.array([], np.int64), 'cash': np.array([]), 'share': np.array([]),
        'rights': np.array([]), 'rprice': np.array([])}))

    # 快照 vs naive（仅对齐日期）
    rq = np.abs(qn[aligned] - fq[idx_c][aligned]) / np.maximum(np.abs(fq[idx_c][aligned]), 1e-300)
    rh = np.abs(hn[aligned] - fh[idx_c][aligned]) / np.maximum(np.abs(fh[idx_c][aligned]), 1e-300)
    if len(rq):
        stats['naive_vs_snapshot_qfq'].append((code, float(np.max(rq)), len(rq)))
        stats['naive_vs_snapshot_hfq'].append((code, float(np.max(rh)), len(rh)))
        max_qfq_r = max(max_qfq_r, float(np.max(rq)))
        max_hfq_r = max(max_hfq_r, float(np.max(rh)))

    # 快照 vs 行情内因子列（仅对齐日期）
    if aligned.sum():
        rq2 = np.abs(fq[idx_c][aligned] - sq[aligned]) / np.maximum(np.abs(sq[aligned]), 1e-300)
        stats['quote_vs_snapshot'].append((code, float(np.max(rq2))))
        if (np.all(sq == 1.0) and not np.all(fq[idx_c][aligned] == 1.0)):
            stats['quote_all_one_but_snapshot_not'].append(code)
        if (np.all(fq[idx_c][aligned] == 1.0) and not np.all(sq == 1.0)):
            stats['snapshot_all_one_but_quote_not'].append(code)

    # 6 位小数舍入检查
    a = fq[np.isfinite(fq)]
    b = fh[np.isfinite(fh)]
    if len(a):
        qfq_round = max(qfq_round, float(np.max(np.abs(a * 1e6 - np.round(a * 1e6))))
                        if len(a) else 0)
    if len(b):
        hfq_round = max(hfq_round,
                        float(np.max(np.abs(b * 1e6 - np.round(b * 1e6)))))

# 可逆性样本中快照 vs naive 的极值
samples = json.load(open(OUT / 'reversibility_samples.json', encoding='utf-8'))
sample_codes = {c for v in samples.values() for c in v}
rows = []
for code in sorted(sample_codes):
    p = LOCAL / 'quotes' / 'stock' / code.split('.')[-1].lower() / f'{code}.parquet'
    if not p.exists():
        continue
    q = pl.read_parquet(p, columns=['date', 'close'])
    d = to_int_days(q['date']); c = q['close'].to_numpy()
    fd, fq, fh = fac[code]
    idx = np.searchsorted(fd, d); inb = idx < len(fd)
    idx_c = np.clip(idx, 0, len(fd) - 1); aligned = inb & (fd[idx_c] == d)
    qn, hn = naive_factors(d, c, ev.get(code, {
        'date': np.array([], np.int64), 'cash': np.array([]), 'share': np.array([]),
        'rights': np.array([]), 'rprice': np.array([])}))
    rq = float(np.max(np.abs(qn[aligned] - fq[idx_c][aligned]) /
                      np.maximum(np.abs(fq[idx_c][aligned]), 1e-300))) if aligned.sum() else None
    rh = float(np.max(np.abs(hn[aligned] - fh[idx_c][aligned]) /
                      np.maximum(np.abs(fh[idx_c][aligned]), 1e-300))) if aligned.sum() else None
    rows.append({'ts_code': code, 'aligned_rows': int(aligned.sum()),
                 'max_rel_qfq': rq, 'max_rel_hfq': rh})

out = {
    'codes': len(codes),
    'snapshot_missing_head': snap_missing_head,
    'snapshot_missing_tail': snap_missing_tail,
    'quote_all_one_but_snapshot_not': stats['quote_all_one_but_snapshot_not'],
    'count_quote_all_one_but_snapshot_not': len(stats['quote_all_one_but_snapshot_not']),
    'count_snapshot_all_one_but_quote_not': len(stats['snapshot_all_one_but_quote_not']),
    'quote_vs_snapshot_mismatch_gt_1e-12': len(
        [1 for _, v in stats['quote_vs_snapshot'] if v > 1e-12]),
    'quote_vs_snapshot_gt_1e-6': len(
        [1 for _, v in stats['quote_vs_snapshot'] if v > 1e-6]),
    'max_rel_quote_vs_snapshot': max((v for _, v in stats['quote_vs_snapshot']), default=None),
    'max_rel_naive_vs_snapshot_qfq': max_qfq_r,
    'max_rel_naive_vs_snapshot_hfq': max_hfq_r,
    'qfq_round_residual': qfq_round,
    'hfq_round_residual': hfq_round,
    'naive_vs_snapshot_fail_qfq': len([1 for _, v, _ in stats['naive_vs_snapshot_qfq']
                                       if v > 1e-12]),
    'naive_vs_snapshot_fail_hfq': len([1 for _, v, _ in stats['naive_vs_snapshot_hfq']
                                       if v > 1e-12]),
    'worst_naive_snapshot_qfq': sorted(
        stats['naive_vs_snapshot_qfq'], key=lambda x: -x[1])[:15],
    'worst_naive_snapshot_hfq': sorted(
        stats['naive_vs_snapshot_hfq'], key=lambda x: -x[1])[:15],
    'worst_quote_snapshot': sorted(stats['quote_vs_snapshot'], key=lambda x: -x[1])[:15],
    'sample_rows': rows,
}
with open(OUT / 'factor_detail.json', 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, indent=2, default=str)
print(json.dumps({k: v for k, v in out.items() if k not in (
                  'worst_naive_snapshot_qfq', 'worst_naive_snapshot_hfq',
                  'worst_quote_snapshot', 'sample_rows')},
                 ensure_ascii=False, indent=2))
print('worst qfq:', out['worst_naive_snapshot_qfq'][:5])
print('worst hfq:', out['worst_naive_snapshot_hfq'][:5])
print('worst quote:', out['worst_quote_snapshot'][:5])
