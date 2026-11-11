"""口径对账：turnover_20 低换手组（取向 Q5）的超额 / 换手，及换手—超额曲线。

修正后（方案 A：取向在分组前对齐）这里应复现文档 §5 / §10.3 的
Q5 超额 64.9 bp/月、单边换手 32.6%/月，并证明它不等于旧公式的 90.5 bp。
"""
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import turnover_curve as tc  # noqa: E402

import run_l1  # noqa: E402

df = pl.read_parquet(tc.PRE, columns=['date', 'ts_code', 'turnover_20'])
lab = pl.read_parquet(tc.LAB)
df = df.join(lab, on=['date', 'ts_code'], how='left')
df = df.filter(pl.col('turnover_20').is_not_null())

g = run_l1.group_stats(df, 'turnover_20', 'fwd_20_oo')
print('raw  group1(低换手)  超额 =', round(g['q1_excess_bp'], 4),
      ' 换手 =', round(g['turnover_q1'], 6))
print('raw  group5(高换手)  超额 =', round(g['q5_excess_bp'], 4),
      ' 换手 =', round(g['turnover_q5'], 6))
print('raw  group_means_bp =',
      {k: round(v * 1e4, 2) for k, v in g['group_means'].items()})

ev = run_l1.evaluate_one(df, 'turnover_20', 'fwd_20_oo', 20, orient=-1.0)
print('取向 Q5（=原始 Q1）超额 =', round(ev['q5_excess_bp'], 4),
      ' 换手 =', round(ev['turnover_q5_monthly'], 6),
      ' 只数 =', round(ev['holdings_q5'], 1))
print('旧公式（raw Q5 超额取反） =', round(-g['q5_excess_bp'], 4),
      ' 换手（未翻） =', round(g['turnover_q5'], 6))

# 真复现：对文档数字做硬断言（并列边界容差 0.1 bp / 5e-4，远小于缺陷量级）
assert abs(ev['q5_excess_bp'] - g['q1_excess_bp']) < 0.1
assert abs(ev['turnover_q5_monthly'] - g['turnover_q1']) < 5e-4
assert abs(ev['q5_excess_bp'] - 64.9069) < 0.02, ev['q5_excess_bp']
assert abs(ev['turnover_q5_monthly'] - 0.32606) < 1e-3, ev['turnover_q5_monthly']
assert abs((-g['q5_excess_bp']) - 90.5369) < 0.01, -g['q5_excess_bp']
print('PASS: 取向 Q5 超额 64.9 bp / 换手 32.6% 复现；旧公式 90.5 bp / 36.7% 已证伪')

c = pl.read_csv(tc.OUT / 'turnover_curve.csv')
print(c.select(['method', 'param', 'turnover_pct', 'holdings', 'q5_excess_gross_bp',
                'q5_excess_net_bp', 'q5_minus_q1_gross_bp']).sort(['method', 'turnover_pct']))
print('max gross excess across curve =', c['q5_excess_gross_bp'].max())
