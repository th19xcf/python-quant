"""把 eval_results.json / 单因子表格式化成 markdown 片段，供文档直接粘贴。"""
import json
from pathlib import Path

import polars as pl

OUT = Path(__file__).resolve().parents[3] / 'reports' / 'l1_baseline'
r = json.loads((OUT / 'eval_results.json').read_text(encoding='utf-8'))
sel = r['selected']
s_oo = pl.read_csv(OUT / 'single_factor_h20_oo.csv')
s_c2c = pl.read_csv(OUT / 'single_factor_h20.csv')
oo = {x['factor']: x for x in s_oo.iter_rows(named=True)}
cc = {x['factor']: x for x in s_c2c.iter_rows(named=True)}
pool = r['h20']['pool_ic_oo']
rows = sorted(pool.values(), key=lambda x: -abs(x['icir']))

lines = []
lines.append('### 入池 26 因子（h=20，主口径 open->open）')
lines.append('')
lines.append('| # | 因子 | \\|RankIC\\| | \\|ICIR\\| | ICIR年化 | 方向 | \\|RankIC\\| (c2c) | \\|ICIR\\| (c2c) |')
lines.append('|---|---|---|---|---|---|---|---|')
for i, x in enumerate(rows, 1):
    f = x['factor']
    d = '正' if x['rank_ic'] > 0 else '负'
    lines.append(
        f"| {i} | {f} | {abs(x['rank_ic']):.4f} | {abs(x['icir']):.4f} | "
        f"{x['icir_annual']:+.2f} | {d} | {abs(cc[f]['rank_ic']):.4f} | "
        f"{abs(cc[f]['icir']):.4f} |")
n = len(rows)
print('\n'.join(lines))
print()
print(f'入池 {n}：均值 |RankIC| {sum(abs(x["rank_ic"]) for x in rows) / n:.4f}，'
      f'中位 {sorted(abs(x["rank_ic"]) for x in rows)[n // 2]:.4f}；'
      f'均值 |ICIR| {sum(abs(x["icir"]) for x in rows) / n:.4f}；'
      f'正向 {sum(1 for x in rows if x["rank_ic"] > 0)} / 负向 '
      f'{sum(1 for x in rows if x["rank_ic"] < 0)}')

print()
print('### 分域表')
print()
print('| 域 | \\|RankIC\\| | \\|ICIR\\| | ICIR年化 | 有效截面数 | 平均每期只数 | 最少只数 | <20 只被剔除期数 |')
print('|---|---|---|---|---|---|---|')
dom = r['h20']['composite_oo']['domains']
for name, d in dom.items():
    exc = d['n_periods_excluded_lt20']
    lines2 = (f"| {name} | {d['abs_rank_ic']:.4f} | {d['abs_icir']:.4f} | "
              f"{d['icir_annual']:.2f} | {d['n_periods_ic']} | "
              f"{d['mean_names_per_period']:.0f} | {d['min_names_per_period']} | "
              f"{exc} |")
    print(lines2)
print()
print('n_periods_total', {k: v['n_periods_total'] for k, v in dom.items()})

print()
print('### 合成 vs 单因子最优 vs 三档')
v = r['vs_single_best_h20']
for k, val in v.items():
    print(k, '=', val)
print()
print('tier dist', r['tier_distribution_h20']['counts'], 'n=', r['tier_distribution_h20']['n'])
tt = {x['factor']: x['tier'] for x in r['tier_distribution_h20']['detail']}
print('pool tiers', {f: tt[f] for f in sel if f in tt})
print('A 档因子', [f for f, t in tt.items() if t == 'A'])
print()
print('### label compare table (first 5)')
for row in r['label_compare_h20']['table'][:5]:
    print(row)
