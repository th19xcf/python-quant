#!/usr/bin/env python
"""把 residual_ic.csv 转成分档、覆盖率汇总与 Bonferroni 预算表。"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import t as t_dist

ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / 'reports' / 'factor_candidates_20261111'
OUT = ROOT / '_staging' / 'factor_candidates_20261111'
OUT.mkdir(parents=True, exist_ok=True)

# 候选定义与族（与 eval_factor_candidates.py 保持一致）
DEFS = {
    'q_roe_ttm': ('质量', '归母净利TTM / 期末归母净资产', '2015（财务 PIT，2010 起有报表）'),
    'q_roa_ttm': ('质量', '归母净利TTM / 期末总资产', '2015（财务 PIT）'),
    'q_gross_margin': ('质量', '毛利TTM / 营收TTM', '2015（财务 PIT；金融业毛利缺失）'),
    'q_asset_turnover': ('质量', '营收TTM / 期末总资产', '2015（财务 PIT）'),
    'q_accruals': ('质量', '(归母净利TTM - 经营现金流TTM) / 总资产', '2015（财务 PIT）'),
    'q_ocf_to_profit': ('质量', '经营现金流TTM / 归母净利TTM（净利<=0 记空）', '2015（财务 PIT）'),
    'g_revenue_yoy': ('成长', '本期累计营收 / 上年同期累计 - 1', '2015（财务 PIT）'),
    'g_profit_yoy': ('成长', '(归母净利 - 上年同期) / |上年同期|', '2015（财务 PIT）'),
    'g_sue': ('成长', '(单季净利 - 去年同季) / std(过去8季季节差分)', '2015（需 6 个季度以上）'),
    'v_ep': ('价值', '归母净利TTM / 当日总市值', '2015（total_mv 全历史）'),
    'v_cfp': ('价值', '经营现金流TTM / 当日总市值', '2015（total_mv 全历史）'),
    'v_sp': ('价值', '营收TTM / 当日总市值', '2015（total_mv 全历史）'),
    'v_ivol_60': ('波动/彩票', 'CAPM 残差收益（60日滚动 beta）的 60 日标准差', '2014（需 60 日预热）'),
    'v_max_5d_21': ('波动/彩票', '过去 21 日窗口内 5 日收益的最大值', '2013 下半年（需 26 日预热）'),
    'v_downside_vol_60': ('波动/彩票', '60 日内负收益半方差开方', '2014（需 60 日预热）'),
    'v_skew_60': ('波动/彩票', '60 日收益偏度', '2014（需 60 日预热）'),
    'l_amihud_20': ('流动性', 'mean(|日收益| / 成交额)，20 日', '2013 下半年（需 20 日预热）'),
    'm_mom_12_1': ('动量/反转', 'close[t-21] / close[t-252] - 1（跳过近一月）', '2014（需 252 日预热）'),
    'm_resid_mom_12_1': ('动量/反转', '120日滚动 beta 的 CAPM 残差收益，过去 252 日剔除近 21 日累加',
                         '2014 下半年（需 120+252 日预热）'),
    'm_dist_52w_high': ('动量/反转', 'close / 252日最高价 - 1', '2014（需 252 日预热）'),
    'm_reversal_5': ('动量/反转', 'close / close[t-5] - 1', '2013 下半年（需 5 日预热）'),
}
T_LOOKUP = {5: 230, 20: 58, 60: 19}


def main() -> None:
    ic = pl.read_csv(REPORTS / 'residual_ic.csv')
    cov = pl.read_csv(REPORTS / 'factor_coverage_by_year.csv')

    # 覆盖率汇总
    cov_rows = []
    for f in sorted(ic['candidate'].unique().to_list()):
        g = cov.filter(pl.col('factor') == f).sort('year')
        if g.is_empty():
            continue
        total = g['rows'].sum()
        nn = g['nonnull'].sum()
        full = g.filter(pl.col('coverage') >= 0.5)
        cov_rows.append({
            'factor': f, 'coverage_all': round(nn / total, 4),
            'first_year_ge_50pct': int(full['year'].min()) if not full.is_empty() else None,
            'min_year_coverage': round(float(g['coverage'].min()), 4),
            'pit_start_note': DEFS[f][2],
        })
    cov_df = pl.DataFrame(cov_rows)
    cov_df.write_csv(REPORTS / 'coverage_summary.csv')

    # 宽表：每个候选一行，h=5/20/60 三列
    wide = ic.pivot(on='horizon', index='candidate',
                    values=['resid_ic_oos', 'resid_t_oos', 'raw_ic_oos',
                            'delta_r2_oos_pct', 'coverage_oos', 'resid_ic_std_oos'])
    wide = wide.join(cov_df.rename({'factor': 'candidate'}), on='candidate', how='left')
    maxrho = ic.group_by('candidate').agg(pl.col('max_abs_rho').max())
    wide = wide.join(maxrho, on='candidate', how='left')

    def gv(row, col, h):
        k = f'{col}_{h}'
        return row.get(k) if k in row else None

    rank_rows = []
    for r in wide.to_dicts():
        f = r['candidate']
        ics_ = {h: gv(r, 'resid_ic_oos', h) for h in (5, 20, 60)}
        ts_ = {h: gv(r, 'resid_t_oos', h) for h in (5, 20, 60)}
        signs = [np.sign(ics_[h]) for h in (5, 20, 60) if ics_[h] is not None
                 and np.isfinite(ics_[h])]
        consistent = len(signs) >= 2 and all(s == signs[0] for s in signs)
        t20 = ts_[20] if ts_[20] is not None else np.nan
        rho = r.get('max_abs_rho') or 0.0
        cov_oos = gv(r, 'coverage_oos', 20) or 0.0
        d20 = gv(r, 'delta_r2_oos_pct', 20) or np.nan
        n_pass = sum(1 for h in (5, 20, 60)
                     if ts_[h] is not None and np.isfinite(ts_[h])
                     and abs(ts_[h]) >= 3.1)

        if rho >= 0.95:
            tier = '不纳入'
            why = f'与现有因子最大 |rho|={rho:.2f}，属于重复'
        elif (n_pass >= 2 and cov_oos >= 0.60 and consistent and rho < 0.90
              and np.isfinite(d20) and d20 >= 0.02):
            tier = '值得纳入'
            why = '三个期限中至少两个残差 t>=3.1、三期限同号、增量解释力达标'
        elif ((np.isfinite(t20) and abs(t20) >= 1.8) or n_pass >= 1):
            tier = '观察'
            why = ('与现有因子高相关（0.90~0.95），残差信息仍在但重复度高'
                   if rho >= 0.90 else
                   '有一定增量但未过 Bonferroni 门槛或期限/覆盖不稳')
        else:
            tier = '不纳入'
            why = 'OOS 残差 IC 不显著'
        rank_rows.append({
            'candidate': f, 'family': DEFS[f][0], 'definition': DEFS[f][1],
            'direction': '?', 'tier': tier, 'rationale': why,
            'resid_ic_5': ics_[5], 'resid_t_5': ts_[5],
            'resid_ic_20': ics_[20], 'resid_t_20': ts_[20],
            'resid_ic_60': ics_[60], 'resid_t_60': ts_[60],
            'raw_ic_oos_20': gv(r, 'raw_ic_oos', 20),
            'delta_r2_oos_pct_20': d20,
            'coverage_oos_20': cov_oos,
            'max_abs_rho': rho,
            'pit_start_note': DEFS[f][2],
            'sign_consistent': consistent,
        })
    rank = pl.DataFrame(rank_rows).sort(
        pl.col('resid_t_20').abs(), descending=True, nulls_last=True)
    rank.write_csv(REPORTS / 'ranking.csv')

    pl.DataFrame([{
        'candidate': f, 'family': DEFS[f][0], 'definition': DEFS[f][1],
        'direction': '?', 'pit_start': DEFS[f][2],
        'computable': True,
    } for f in DEFS]).write_csv(REPORTS / 'candidate_definition.csv')

    # Bonferroni 预算
    budget = []
    for h in (5, 20, 60):
        sd = float(np.nanmedian(ic.filter(pl.col('horizon') == h)['resid_ic_std_oos']
                                .to_numpy()))
        T = T_LOOKUP[h]
        for N in (20, 30, 40):
            alpha = 0.05 / N
            tcrit = float(t_dist.ppf(1 - alpha / 2, df=T - 1))
            mde_ic = tcrit * sd / math.sqrt(T)
            mde_ret = tcrit * 0.0352 / math.sqrt(19)  # 既有 60 日 σ=3.52%, n=19 口径
            budget.append({
                'n_tests': N, 'horizon': h, 'alpha_star': alpha,
                't_critical': round(tcrit, 3),
                'ic_sd_used': round(sd, 5),
                'n_oos_periods': T,
                'mde_ic': round(mde_ic, 5),
                'mde_return_per_period_pct': round(mde_ret * 100, 3),
            })
    bdf = pl.DataFrame(budget)
    bdf.write_csv(REPORTS / 'bonferroni_budget.csv')

    summary = {
        'tier_counts': rank.group_by('tier').len().to_dict(as_series=False),
        'tiers': {t: rank.filter(pl.col('tier') == t)['candidate'].to_list()
                  for t in ('值得纳入', '观察', '不纳入')},
        'n_candidates': rank.height,
        'budget': budget,
    }
    (OUT / 'analysis_summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
