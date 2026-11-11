"""49 因子修前（事后翻转）vs 修后（分组前取向）逐字段对照。"""
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
# 中间缓存（可重建）
STAGE = ROOT / '_staging' / 'l1_equal_weight_20261111'
# 证据产物
OUT = ROOT / 'reports' / 'l1_baseline'
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_l1  # noqa: E402

FWD = 'fwd_20_oo'
pre = pl.read_parquet(STAGE / 'pre_h20.parquet',
                      columns=['date', 'ts_code', *run_l1.FACTORS])
lab = pl.read_parquet(STAGE / 'labels_oo_h20.parquet')
df = pre.join(lab, on=['date', 'ts_code'], how='left')
ic = pl.read_csv(OUT / 'single_factor_h20_oo.csv')
sign = {r['factor']: (1.0 if r['rank_ic'] > 0 else -1.0)
        for r in ic.iter_rows(named=True)}

rows = []
for f in run_l1.FACTORS:
    orient = sign[f]
    g_raw = run_l1.group_stats(df, f, FWD)
    g_ori = run_l1.group_stats(
        df.with_columns((pl.col(f) * orient).alias('__o__')), '__o__', FWD)
    # 修前：raw 分组后事后翻转（换手不翻）
    old_gm = {f'Q{i}': g_raw['group_means'][f'Q{6 - i}'] * 1e4
              for i in range(1, 6)} if orient < 0 else \
             {k: v * 1e4 for k, v in g_raw['group_means'].items()}
    old = {
        'q5_excess': g_raw['q5_excess_bp'] * orient,
        'turnover_q5': g_raw['turnover_q5'],
        'spread': g_raw['q5_minus_q1_bp'] * orient,
        'mono': g_raw['mono'] * orient,
        'group_means': old_gm,
    }
    new = {
        'q5_excess': g_ori['q5_excess_bp'],
        'turnover_q5': g_ori['turnover_q5'],
        'spread': g_ori['q5_minus_q1_bp'],
        'mono': g_ori['mono'],
        'group_means': {k: v * 1e4 for k, v in g_ori['group_means'].items()},
    }
    gm_diff = max(abs(old['group_means'][k] - new['group_means'][k]) for k in old['group_means'])
    rows.append({
        'factor': f, 'orient': orient,
        'old_q5_excess_bp': old['q5_excess'], 'new_q5_excess_bp': new['q5_excess'],
        'd_excess_bp': new['q5_excess'] - old['q5_excess'],
        'old_turnover_q5': old['turnover_q5'], 'new_turnover_q5': new['turnover_q5'],
        'd_turnover_pp': (new['turnover_q5'] - old['turnover_q5']) * 100,
        'old_spread_bp': old['spread'], 'new_spread_bp': new['spread'],
        'd_spread_bp': new['spread'] - old['spread'],
        'old_mono': old['mono'], 'new_mono': new['mono'],
        'd_mono': new['mono'] - old['mono'],
        'max_group_mean_diff_bp': gm_diff,
        'new_holdings_q5': g_ori['holdings_q5'],
    })

scan = pl.DataFrame(rows)
scan.write_csv(OUT / 'old_vs_new_49.csv')

pos = scan.filter(pl.col('orient') > 0)
neg = scan.filter(pl.col('orient') < 0)
pos_bitwise = all(
    abs(r['d_excess_bp']) == 0 and abs(r['d_turnover_pp']) == 0
    and abs(r['d_spread_bp']) == 0 and abs(r['d_mono']) == 0
    and r['max_group_mean_diff_bp'] == 0
    for r in pos.iter_rows(named=True))
summary = {
    'n_factors': scan.height,
    'n_positive': pos.height, 'positive_all_bitwise_identical': bool(pos_bitwise),
    'n_negative': neg.height,
    'negative_excess_changed_all': bool((neg['d_excess_bp'].abs() > 0).all()),
    'negative_turnover_changed_all': bool((neg['d_turnover_pp'].abs() > 0).all()),
    'negative_max_abs_d_excess_bp': float(neg['d_excess_bp'].abs().max()),
    'negative_max_abs_d_turnover_pp': float(neg['d_turnover_pp'].abs().max()),
    'tie_noise_max_d_spread_bp': float(scan['d_spread_bp'].abs().max()),
    'tie_noise_max_d_mono': float(scan['d_mono'].abs().max()),
    'tie_noise_max_group_mean_diff_bp': float(scan['max_group_mean_diff_bp'].max()),
    'top10_abs_d_excess': neg.sort(pl.col('d_excess_bp').abs(), descending=True)
    .head(10).select(['factor', 'd_excess_bp', 'd_turnover_pp']).to_dicts(),
}
(OUT / 'old_vs_new_49_summary.json').write_text(
    json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(summary, ensure_ascii=False, indent=2))
