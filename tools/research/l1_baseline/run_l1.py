#!/usr/bin/env python
"""L1 等权因子合成基线：选因子 -> 去冗余 -> 等权合成 -> 评估。

只读输入（不写 data/、不改任何调用点）：
    面板      _staging/factor_candidates_20261111/base_rebal.parquet
    行业 PIT  data/local_store/meta/industry_pit/industry_pit_sw.parquet
    市值      data/local_store/meta/financials/valuation_daily.parquet (total_mv)
    名称      data/local_store/meta/name_history.parquet（剔 ST/退）
    指数成分  data/local_store/meta/index_members/membership_intervals.parquet

预处理一律调用唯一管线 src.alpha.cross_section.preprocess.run_pipeline
（winsorize_mad k=3 -> mark_missing -> zscore -> neutralize_industry_mv），
不另写一套；mask（{col}__valid）保留，缺失不填 0，合成时按可用因子数归一。

阶段：
    python tools/research/l1_baseline/run_l1.py --stage prep   # 预处理并缓存 pre_h{h}.parquet
    python tools/research/l1_baseline/run_l1.py --stage select # h=20 单因子筛选 + 相关性去冗余
    python tools/research/l1_baseline/run_l1.py --stage eval    # 合成与评估（含分域 / 换手 / 对比）
    python tools/research/l1_baseline/run_l1.py --stage all

目录约定：
    --work-dir  中间缓存（pre_h*.parquet / labels_oo_*.parquet / ic_h20.parquet），
                默认 _staging/l1_equal_weight_20261111，可重建、不进仓库。
    --out-dir   证据产物（eval_results.json / single_factor_*.csv / tier_h20.csv 等），
                默认 reports/l1_baseline。
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
import sys  # noqa: E402

sys.path.insert(0, str(ROOT))

from src.alpha.cross_section.preprocess import run_pipeline  # noqa: E402
from src.quant.factor_lib.redundancy import (  # noqa: E402
    build_clusters,
    correlation_matrix,
)

# 输入（只读，不是产物）：研究面板由 _staging/factor_candidates_20261111 提供，
# 若该目录被清理，需先重建面板再重跑本脚本。
BASE = ROOT / '_staging/factor_candidates_20261111/base_rebal.parquet'
IP_SW = ROOT / 'data/local_store/meta/industry_pit/industry_pit_sw.parquet'
VAL = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
NH = ROOT / 'data/local_store/meta/name_history.parquet'
IDX = ROOT / 'data/local_store/meta/index_members/membership_intervals.parquet'

# 中间缓存（可重建：--stage prep / open_to_open.py 重跑即可；放 _staging，不进仓库）
WORK = ROOT / '_staging' / 'l1_equal_weight_20261111'
# 证据产物（可引用的测量记录，入库外的 reports/ 下，按 --out-dir 可覆盖）
OUT = ROOT / 'reports' / 'l1_baseline'

FACTORS = [
    # 16 legacy
    'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
    'trend_slope_60', 'ma20_gap', 'rsi_14', 'sharpe_ratio_20',
    'volatility_20', 'max_drawdown_20', 'atr_pct', 'volume_momentum_20',
    'turnover_20', 'ff_div_yield', 'ff_value_bp', 'ff_sales_yield',
    # 12 extended
    'qlib_rsqr20', 'qlib_rsqr10', 'gtja_003', 'gtja_024', 'a101_024',
    'gtja_010', 'a101_012', 'ff_amihud_60', 'a101_020', 'a101_042',
    'gtja_012', 'gtja_041',
    # 21 research candidates
    'q_roe_ttm', 'q_roa_ttm', 'q_gross_margin', 'q_asset_turnover',
    'q_accruals', 'q_ocf_to_profit', 'g_revenue_yoy', 'g_profit_yoy',
    'g_sue', 'v_ep', 'v_cfp', 'v_sp', 'v_ivol_60', 'v_max_5d_21',
    'v_downside_vol_60', 'v_skew_60', 'l_amihud_20', 'm_mom_12_1',
    'm_resid_mom_12_1', 'm_dist_52w_high', 'm_reversal_5',
]

MIN_NAMES = 100          # 单期截面有效样本下限
MIN_NAMES_DOMAIN = 40    # 分域评估的单期下限（300 域成分最少）
IS_END = date(2021, 12, 31)
N_GROUPS = 5
# 单因子硬门槛（与 docs/中性化AB实测 的「双门槛(0.02)」一致）
MIN_ABS_RANKIC = 0.02
MIN_ABS_ICIR = 0.3
POOL_TARGET = (15, 30)
HORIZONS = (20, 5, 60)


def log(msg: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


# ---------------------------------------------------------------------------
# 面板：过滤 -> PIT 行业/市值 -> 唯一预处理管线
# ---------------------------------------------------------------------------
def limit_pct(codes: list[str], ds: list[date]) -> np.ndarray:
    out = np.empty(len(codes))
    for i, (c, d) in enumerate(zip(codes, ds, strict=False)):
        p = c.split('.')[0]
        if p.startswith('688'):
            out[i] = 0.195
        elif p.startswith(('300', '301')):
            out[i] = 0.195 if d >= date(2020, 8, 24) else 0.095
        else:
            out[i] = 0.095
    return out


def st_pairs(dates: list[date]) -> pl.DataFrame:
    nh = pl.read_parquet(NH).select(['ts_code', 'name', 'start_date', 'end_date'])
    out_d: list[date] = []
    out_c: list[str] = []
    for d in dates:
        sub = nh.filter((pl.col('start_date') <= d)
                        & (pl.col('end_date').is_null() | (pl.col('end_date') >= d))
                        & pl.col('name').str.contains('ST|退'))
        out_d.extend([d] * sub.height)
        out_c.extend(sub['ts_code'].to_list())
    if not out_d:
        return pl.DataFrame(schema={'date': pl.Date, 'ts_code': pl.Utf8})
    return pl.DataFrame({'date': out_d, 'ts_code': out_c},
                        schema={'date': pl.Date, 'ts_code': pl.Utf8})


def industry_map(dates: list[date]) -> dict[date, pl.DataFrame]:
    ip = pl.read_parquet(IP_SW, columns=['ts_code', 'industry_l1', 'in_date', 'out_date'])
    out: dict[date, pl.DataFrame] = {}
    for d in dates:
        sub = ip.filter((pl.col('in_date') <= d)
                        & (pl.col('out_date').is_null() | (pl.col('out_date') >= d)))
        out[d] = sub.select(['ts_code', 'industry_l1']).unique(
            subset=['ts_code'], keep='first')
    return out


def index_long(dates: list[date]) -> pl.DataFrame:
    """(date, ts_code, index_code) 长表，PIT 命中。"""
    iv = pl.read_parquet(IDX, columns=['index_code', 'ts_code', 'in_date', 'out_date'])
    rows_d: list[date] = []
    rows_c: list[str] = []
    rows_i: list[str] = []
    for d in dates:
        sub = iv.filter((pl.col('in_date') <= d)
                        & (pl.col('out_date').is_null() | (pl.col('out_date') >= d)))
        rows_d.extend([d] * sub.height)
        rows_c.extend(sub['ts_code'].to_list())
        rows_i.extend(sub['index_code'].to_list())
    return pl.DataFrame({'date': rows_d, 'ts_code': rows_c, 'index_code': rows_i},
                        schema={'date': pl.Date, 'ts_code': pl.Utf8, 'index_code': pl.Utf8})


def build_frame(h: int, factors: list[str], diag: bool) -> tuple[pl.DataFrame, dict]:
    """构造 h 对应的调仓日面板，并跑唯一预处理管线。"""
    step = max(1, h // 5)
    weekly = sorted(pl.scan_parquet(BASE).select('date').unique()
                    .collect()['date'].to_list())
    rebal = weekly[::step]
    log(f'h={h}: 周频 {len(weekly)} 日 -> 非重叠调仓日 {len(rebal)} 日（step={step}）')

    fwd_col = f'fwd_{h}'
    need = ['date', 'ts_code', 'cum_count', 'volume', 'next_open_ret', fwd_col, *factors]
    df = pl.scan_parquet(BASE).filter(pl.col('date').is_in(rebal)).select(need).collect()

    n0 = df.height
    df = df.filter((pl.col('cum_count') >= 250) & (pl.col('volume') > 0))
    n1 = df.height
    nh = st_pairs(rebal)
    if nh.height:
        df = df.join(nh, on=['date', 'ts_code'], how='anti')
    n2 = df.height
    lim = limit_pct(df['ts_code'].to_list(), df['date'].to_list())
    df = df.with_columns(pl.Series('_lim', lim)).filter(
        (pl.col('next_open_ret') < pl.col('_lim'))
        & (pl.col('next_open_ret') > -pl.col('_lim'))).drop('_lim')
    n3 = df.height
    log(f'h={h}: 行数 {n0:,} -> 上市250 {n1:,} -> 非ST {n2:,} -> 非涨跌停 {n3:,}')

    val = (pl.read_parquet(VAL, columns=['ts_code', 'trade_date', 'total_mv'])
           .filter(pl.col('trade_date') >= date(2014, 12, 1))
           .filter(pl.col('total_mv') > 0)
           .sort(['ts_code', 'trade_date']))
    df = df.sort(['ts_code', 'date']).join_asof(
        val, left_on='date', right_on='trade_date', by='ts_code', strategy='backward')
    df = df.with_columns(pl.col('total_mv').log().alias('ln_mv')).drop('total_mv')

    ind_map = industry_map(rebal)
    parts = []
    for d, g in df.group_by('date', maintain_order=True):
        d0 = d[0] if isinstance(d, tuple) else d
        ind = ind_map.get(d0)
        if ind is None:
            parts.append(g.with_columns(pl.lit(None, dtype=pl.Utf8).alias('industry_l1')))
            continue
        parts.append(g.join(ind, on='ts_code', how='left'))
    df = pl.concat(parts, how='vertical_relaxed').sort(['date', 'ts_code'])
    n_ind = int(df['industry_l1'].is_null().sum())
    n_mv = int(df['ln_mv'].is_null().sum())
    log(f'h={h}: 行业缺失 {n_ind:,} 行 / 市值缺失 {n_mv:,} 行')

    res = run_pipeline(df, factors, by='date', industry_col='industry_l1',
                       mv_col='ln_mv', winsor_k=3, standardize_method='zscore',
                       collect_diagnostics=diag)
    out = res.frame.select([
        'date', 'ts_code', 'industry_l1', 'ln_mv', fwd_col,
        *[f'{c}__valid' for c in factors],
        *[res.columns[c] for c in factors],
    ]).rename({res.columns[c]: c for c in factors})
    meta = {
        'h': h, 'rebal_days': len(rebal), 'rows': out.height,
        'rows_raw': n0, 'rows_after_filters': n3,
        'n_industry_missing': n_ind, 'n_lnmv_missing': n_mv,
        'factors': len(factors),
        'pipeline_summary': res.summary() if diag else None,
        'mapping': res.columns,
    }
    return out, meta


def stage_prep() -> None:
    WORK.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    metas = {}
    for h in HORIZONS:
        p = WORK / f'pre_h{h}.parquet'
        if p.exists():
            log(f'h={h}: 已存在 {p.name}，跳过')
            continue
        df, meta = build_frame(h, FACTORS, diag=(h == 20))
        df.write_parquet(p)
        metas[str(h)] = meta
        log(f'h={h}: 写入 {p.name}（{df.height:,} 行 × {len(df.columns)} 列）')
        (OUT / f'prep_meta_h{h}.json').write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    if metas:
        (OUT / 'prep_meta.json').write_text(
            json.dumps(metas, ensure_ascii=False, indent=2), encoding='utf-8')


# ---------------------------------------------------------------------------
# 统计工具
# ---------------------------------------------------------------------------
def ic_table(df: pl.DataFrame, cols: list[str], fwd_col: str,
             min_names: int = MIN_NAMES) -> pl.DataFrame:
    """逐日截面 RankIC（rank 后 Pearson = Spearman），返回 date × factor。"""
    w = df.select(['date', fwd_col, *cols]).with_columns(
        [pl.col(c).rank().over('date').alias(c) for c in cols]
        + [pl.col(fwd_col).rank().over('date').alias('_y'),
           pl.len().over('date').alias('_n')]
    ).filter(pl.col('_n') >= min_names)
    return w.group_by('date').agg(
        [pl.corr(c, '_y').alias(c) for c in cols]
    ).sort('date')


def summarize(ic: pl.DataFrame, cols: list[str], h: int) -> pl.DataFrame:
    rows = []
    ppy = 252.0 / h
    for c in cols:
        s = ic[c].cast(pl.Float64).to_numpy()
        s = s[np.isfinite(s)]
        n = len(s)
        if n < 6:
            rows.append({'factor': c, 'n': n})
            continue
        mean = float(s.mean())
        sd = float(s.std(ddof=1))
        icir = mean / sd if sd > 0 else 0.0
        rows.append({
            'factor': c, 'n': n, 'rank_ic': mean, 'rank_ic_std': sd,
            'icir': icir, 'abs_rank_ic': abs(mean), 'abs_icir': abs(icir),
            'icir_annual': icir * float(np.sqrt(ppy)),
            't_stat': icir * float(np.sqrt(n)),
            'ic_positive_rate': float((s > 0).mean()),
            'sign': '正' if mean > 0 else '负',
        })
    return pl.DataFrame(rows)


def group_stats(df: pl.DataFrame, col: str, fwd_col: str,
                n_groups: int = N_GROUPS) -> dict:
    """5 分组（组内等权）平均收益 + 定义 A 单调性 + 换手。"""
    w = df.select(['date', 'ts_code', col, fwd_col]).filter(
        pl.col(col).is_not_null() & pl.col(fwd_col).is_not_null())
    w = w.with_columns(
        pl.col(col).rank().over('date').alias('_r'),
        pl.len().over('date').alias('_n'),
    ).filter(pl.col('_n') >= MIN_NAMES).with_columns(
        # 排名 1..n 等分 n_groups 组：floor(rank*n_groups/(n+1)) + 1
        ((pl.col('_r').cast(pl.UInt32) * n_groups) // (pl.col('_n') + 1) + 1)
        .cast(pl.Int8).alias('_g')
    )
    per = w.group_by(['date', '_g']).agg(
        pl.col(fwd_col).mean().alias('ret'),
        pl.col('ts_code').alias('members')).sort(['date', '_g'])
    means: dict[int, float] = {}
    turnover: dict[int, float] = {}
    holdings: dict[int, float] = {}
    n_periods = per['date'].n_unique()
    for g in range(1, n_groups + 1):
        sub = per.filter(pl.col('_g') == g)
        means[g] = float(sub['ret'].mean() or 0.0)
        sets = [set(x) for x in sub['members'].to_list()]
        ch = [len(prev - cur) / max(len(prev), 1)
              for prev, cur in zip(sets[:-1], sets[1:], strict=False)]
        turnover[g] = float(np.mean(ch)) if ch else 0.0
        holdings[g] = float(sub['members'].list.len().mean() or 0.0)
    total_mean = float(w[fwd_col].mean() or 0.0)
    mono = 0.0
    vals = [means[g] for g in range(1, n_groups + 1)]
    if len(set(vals)) > 1 and all(np.isfinite(vals)):
        rho = spearmanr(np.arange(n_groups), vals).statistic
        mono = float(rho) if np.isfinite(rho) else 0.0
    return {
        'n_periods': n_periods,
        'group_means': {f'Q{g}': means[g] for g in range(1, n_groups + 1)},
        'q5_minus_q1_bp': (means[n_groups] - means[1]) * 1e4,
        'q5_excess_bp': (means[n_groups] - total_mean) * 1e4,
        'q1_excess_bp': (means[1] - total_mean) * 1e4,
        'mono': mono,
        'turnover_q5': turnover[n_groups],
        'turnover_q1': turnover[1],
        'turnover_mean': float(np.mean(list(turnover.values()))),
        'holdings': {f'Q{g}': holdings[g] for g in range(1, n_groups + 1)},
        'holdings_q5': holdings[n_groups],
        'universe_mean_ret': total_mean,
    }


def tier_of(icir_annual: float, t_stat: float, mono: float) -> str:
    """mono 门槛三档（定义 A，5 组）：ICIR 年化与 t 取绝对值，mono 已按因子取向对齐。"""
    if abs(icir_annual) >= 1.0 and abs(t_stat) >= 2.0 and mono >= 0.6:
        return 'A'
    if abs(icir_annual) >= 0.5 and abs(t_stat) >= 1.5 and mono >= 0.4:
        return 'B'
    if abs(t_stat) >= 1.0:
        return 'C'
    return 'D'


# ---------------------------------------------------------------------------
# 阶段 2：单因子筛选 + 去冗余
# ---------------------------------------------------------------------------
def stage_select() -> None:
    h = 20
    df = pl.read_parquet(WORK / f'pre_h{h}.parquet')
    log(f'h=20 面板 {df.height:,} 行 × {len(df.columns)} 列')
    ic = ic_table(df, FACTORS, f'fwd_{h}')
    ic.write_parquet(WORK / f'ic_h{h}.parquet')
    summ = summarize(ic, FACTORS, h)
    summ.write_csv(OUT / f'single_factor_h{h}.csv')

    n_all = summ.height
    step1 = summ.filter(
        (pl.col('abs_rank_ic') >= MIN_ABS_RANKIC) & (pl.col('abs_icir') >= MIN_ABS_ICIR))
    log(f'单因子硬门槛 {MIN_ABS_RANKIC}/{MIN_ABS_ICIR}: {n_all} -> {step1.height}')

    surviving = step1['factor'].to_list()
    dates = sorted(df['date'].unique().to_list())
    corr = correlation_matrix(df, surviving, dates)
    corr.to_csv(OUT / f'corr_h{h}.csv')
    metrics = summ.select(['factor', 'icir'])
    cl = build_clusters(corr, metrics, threshold=0.7)
    cl.clusters.write_csv(OUT / f'clusters_h{h}.csv')

    kept = sorted(cl.kept)
    off = corr.abs().to_numpy()[~np.eye(len(corr), dtype=bool)] if len(corr) else np.array([])
    # 入池后最大两两 |rho|（聚类前）
    sub0 = corr.loc[kept, kept].abs().to_numpy()
    off0 = sub0[~np.eye(len(kept), dtype=bool)] if len(kept) else np.array([])

    # 聚类是 average linkage，簇间仍可能残留 |rho|>0.7 的一对。
    # 补一遍贪心两两清理：只要池内还有 |rho|>0.7 的一对，就丢掉 |ICIR| 较低的那个。
    icir_map = {r['factor']: abs(float(r['icir'])) for r in summ.iter_rows(named=True)}
    absc = corr.abs()
    greedy_dropped: list[str] = []
    pairs: list[tuple[str, str, float]] = []
    while len(kept) > 2:
        m = absc.loc[kept, kept].to_numpy().copy()
        np.fill_diagonal(m, 0.0)
        if m.max() <= 0.7:
            break
        i, j = np.unravel_index(int(np.argmax(m)), m.shape)
        a, b = kept[i], kept[j]
        drop = b if icir_map.get(a, 0.0) >= icir_map.get(b, 0.0) else a
        pairs.append((a, b, float(m[i, j])))
        greedy_dropped.append(drop)
        kept = sorted(x for x in kept if x != drop)
    kept = sorted(kept)
    sub = corr.loc[kept, kept].abs().to_numpy()
    off_kept = sub[~np.eye(len(kept), dtype=bool)] if len(kept) else np.array([])

    loginfo = {
        'h': h,
        'candidates_total': n_all,
        'thresholds': {'abs_rank_ic': MIN_ABS_RANKIC, 'abs_icir': MIN_ABS_ICIR},
        'after_ic_filter': step1.height,
        'dropped_by_ic_filter': n_all - step1.height,
        'after_cluster': len(cl.kept),
        'dropped_by_cluster': len(surviving) - len(cl.kept),
        'after_corr_dedup': len(kept),
        'dropped_by_corr_dedup': len(surviving) - len(kept),
        'greedy_pairwise_dropped': greedy_dropped,
        'greedy_pairs': [{'a': a, 'b': b, 'abs_corr': c} for a, b, c in pairs],
        'pool_target': list(POOL_TARGET),
        'in_target_range': POOL_TARGET[0] <= len(kept) <= POOL_TARGET[1],
        'max_abs_corr_before': float(off.max()) if off.size else 0.0,
        'mean_abs_corr_before': float(off.mean()) if off.size else 0.0,
        'max_abs_corr_after_cluster': float(off0.max()) if off0.size else 0.0,
        'max_abs_corr_after': float(off_kept.max()) if off_kept.size else 0.0,
        'mean_abs_corr_after': float(off_kept.mean()) if off_kept.size else 0.0,
        'kept': kept,
        'dropped': sorted(cl.dropped),
        'clusters': cl.clusters.to_dicts(),
    }
    (OUT / f'selection_h{h}.json').write_text(
        json.dumps(loginfo, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'相关去冗余: {len(surviving)} -> {len(kept)}（|rho|>0.7 簇内只留 |ICIR| 最高）')
    log(f'入池后最大两两 |rho| = {loginfo["max_abs_corr_after"]:.3f}')


# ---------------------------------------------------------------------------
# 阶段 3：合成与评估
# ---------------------------------------------------------------------------
def build_composite(df: pl.DataFrame, sel: list[str], signs: dict[str, float],
                    name: str = 'l1_equal_weight') -> pl.DataFrame:
    """等权合成：方向对齐后按可用因子数归一（缺失不填 0）。"""
    exprs = [pl.col(c) * signs[c] for c in sel]
    return df.with_columns(
        pl.mean_horizontal(exprs).alias(name),
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int8) for c in sel])
        .alias(f'{name}__n'),
    )


def domain_eval(df: pl.DataFrame, col: str, fwd: str, h: int,
                mem: pl.DataFrame | None, label: str) -> dict:
    """分域评估：带有效截面数 / 平均每期只数 / 被剔除期数（<20 只）。"""
    sub = df if mem is None else df.join(mem, on=['date', 'ts_code'], how='semi')
    per = (sub.filter(pl.col(col).is_not_null() & pl.col(fwd).is_not_null())
           .group_by('date').agg(pl.len().alias('n'), pl.col(fwd).mean().alias('_x'))
           .sort('date'))
    n_all = per.height
    small = per.filter(pl.col('n') < MIN_NAMES_DOMAIN)
    used = per.filter(pl.col('n') >= MIN_NAMES_DOMAIN)
    out = {
        'domain': label,
        'n_periods_total': n_all,
        'n_periods_used': used.height,
        'n_periods_excluded_lt20': small.height,
        'excluded_names_range': ([int(small['n'].min()), int(small['n'].max())]
                                 if small.height else None),
        'min_names_per_period': int(per['n'].min()) if n_all else 0,
        'mean_names_per_period': float(used['n'].mean()) if used.height else 0.0,
    }
    if used.height < 6:
        out['rank_ic'] = None
        return out
    dic = ic_table(sub, [col], fwd, min_names=MIN_NAMES_DOMAIN)
    ds = dic[col].cast(pl.Float64).to_numpy()
    ds = ds[np.isfinite(ds)]
    m = float(ds.mean())
    sd = float(ds.std(ddof=1))
    out.update({
        'n_periods_ic': len(ds),
        'rank_ic': m, 'abs_rank_ic': abs(m),
        'icir': m / sd if sd > 0 else 0.0,
        'abs_icir': abs(m / sd) if sd > 0 else 0.0,
        'icir_annual': (m / sd) * float(np.sqrt(252.0 / h)) if sd > 0 else 0.0,
    })
    return out


def evaluate_one(df: pl.DataFrame, col: str, fwd: str, h: int,
                 domains: dict[str, pl.DataFrame] | None = None,
                 orient: float = 1.0) -> dict:
    """按取向在**分组之前**对齐：先算 ``col * orient``，再统一取 Q5。

    orient=-1 时 Q5 即原始 Q1，超额 / 换手 / 只数 / 分组标签天然一致。
    不再做事后翻转：旧写法把 raw Q5 超额取反（= total-Q5，不是 Q1-total）、
    换手却留 raw Q5，两尾不对称时超额与换手都会错配（见 orient_scan/）。
    ``*_raw`` 列为原始分组，``*_oriented`` / 无后缀列为取向对齐后分组。
    """
    ic = ic_table(df, [col], fwd)
    s = summarize(ic, [col], h)
    row = s.row(0, named=True) if s.height else {}
    score = '__score_oriented__'
    g_ori = group_stats(
        df.with_columns((pl.col(col) * orient).alias(score)), score, fwd)
    g_raw = g_ori if orient >= 0 else group_stats(df, col, fwd)
    gm_ori = {k: v * 1e4 for k, v in g_ori['group_means'].items()}
    gm_raw = {k: v * 1e4 for k, v in g_raw['group_means'].items()}
    p_raw = row.get('ic_positive_rate')
    out: dict = {
        'n_periods': row.get('n'),
        'rank_ic': row.get('rank_ic'),
        'rank_ic_oriented': (row['rank_ic'] * orient
                             if row.get('rank_ic') is not None else None),
        'icir': row.get('icir'),
        'icir_annual': row.get('icir_annual'),
        't_stat': row.get('t_stat'),
        'abs_rank_ic': row.get('abs_rank_ic'),
        'abs_icir': row.get('abs_icir'),
        'ic_positive_rate': p_raw,
        'ic_positive_rate_raw': p_raw,
        'ic_positive_rate_oriented': (1.0 - p_raw
                                      if orient < 0 and p_raw is not None else p_raw),
        'orient': orient,
        'mono': g_ori['mono'],
        'mono_raw': g_raw['mono'],
        'q5_minus_q1_bp': g_ori['q5_minus_q1_bp'],
        'q5_minus_q1_bp_oriented': g_ori['q5_minus_q1_bp'],
        'q5_minus_q1_bp_raw': g_raw['q5_minus_q1_bp'],
        'q5_excess_bp': g_ori['q5_excess_bp'],
        'q5_excess_bp_oriented': g_ori['q5_excess_bp'],
        'q5_excess_bp_raw': g_raw['q5_excess_bp'],
        'q1_excess_bp_raw': g_raw['q1_excess_bp'],
        'group_means_bp': gm_ori,
        'group_means_bp_raw': gm_raw,
        'holdings_q5': g_ori['holdings_q5'],
        'turnover_q5_monthly': g_ori['turnover_q5'],
        'turnover_q5_monthly_oriented': g_ori['turnover_q5'],
        'turnover_q5_monthly_raw': g_raw['turnover_q5'],
        'turnover_q1_raw': g_raw['turnover_q1'],
        'turnover_mean_monthly': g_ori['turnover_mean'],
        'universe_mean_bp': g_ori['universe_mean_ret'] * 1e4,
        'tier': tier_of(row.get('icir_annual', 0.0), row.get('t_stat', 0.0),
                        g_ori['mono']),
    }
    if domains is not None:
        out['domains'] = {
            name: domain_eval(df, col, fwd, h, mem, name)
            for name, mem in domains.items()
        }
    return out


def assert_orient_invariance(df: pl.DataFrame, fwd: str = 'fwd_20_oo',
                             h: int = 20) -> dict:
    """回归断言：取向必须在分组前完成（方案 A）。

    - 负向因子：取向超额 == 原始 Q1 超额；且 != 原始 Q5 超额取反（两尾不对称时）；
    - 取向换手 == 原始 Q1 换手；
    - 恒等式：q5_excess == group_means['Q5'] - 全池；|spread| 取向前后相同。
    `v_skew_60` 作两尾对称反例：此时旧公式恰好接近正确值，用来证明断言不是
    靠「恰好对称」通过——它必须同时通过「== 原始 Q1 超额」而不是只靠差值小。
    """
    cases = []
    # 分组用 average rank，负号取反后同一并列块仍归同一组，边界并列块的组号
    # 可能差 1（floor 的整数点），故「取向组 == 原始镜像组」允许极小并列噪声；
    # 该容差远小于缺陷量级（turnover_20 为 25.6 bp / 4.1 pp）。
    tie_tol_bp = 0.1
    tie_tol_spread = 0.1
    tie_tol_turn = 5e-4

    def check(name: str, cond: bool, detail: str) -> None:
        cases.append({'check': name, 'pass': bool(cond), 'detail': detail})
        if not cond:
            raise AssertionError(f'{name} 断言失败: {detail}')

    for fac, mirror_gap_gt_1bp in (('turnover_20', True), ('v_skew_60', False)):
        neg = evaluate_one(df, fac, fwd, h, orient=-1.0)
        pos = evaluate_one(df, fac, fwd, h, orient=1.0)
        gap_q1 = abs(neg['q5_excess_bp_oriented'] - pos['q1_excess_bp_raw'])
        gap_mirror = abs(neg['q5_excess_bp_oriented'] - (-pos['q5_excess_bp_raw']))
        check(f'{fac}: 取向超额 == 原始 Q1 超额（并列容差 {tie_tol_bp} bp）',
              gap_q1 < tie_tol_bp,
              f'{neg["q5_excess_bp_oriented"]:.4f} vs {pos["q1_excess_bp_raw"]:.4f}'
              f'（差 {gap_q1:.2e} bp）')
        check(f'{fac}: 取向超额 {"!=" if mirror_gap_gt_1bp else "≈"} 原始 Q5 超额取反',
              gap_mirror > 1.0 if mirror_gap_gt_1bp else gap_mirror < 0.5,
              f'差 {gap_mirror:.4f} bp（旧公式 {(-pos["q5_excess_bp_raw"]):.4f}）')
        check(f'{fac}: 取向换手 == 原始 Q1 换手（并列容差 {tie_tol_turn}）',
              abs(neg['turnover_q5_monthly'] - pos['turnover_q1_raw']) < tie_tol_turn,
              f'{neg["turnover_q5_monthly"]:.6f} vs {pos["turnover_q1_raw"]:.6f}')
        for tag, ev in (('oriented', neg), ('raw', pos)):
            ident = abs(ev['q5_excess_bp'] -
                        (ev['group_means_bp']['Q5'] - ev['universe_mean_bp']))
            check(f'{fac}/{tag}: 恒等式 q5_excess == Q5组均值-全池', ident < 1e-9,
                  f'残差 {ident:.2e} bp')
        check(f'{fac}: |spread| 取向前后相同（并列容差 {tie_tol_spread} bp）',
              abs(abs(neg['q5_minus_q1_bp']) - abs(pos['q5_minus_q1_bp'])) < tie_tol_spread,
              f'{neg["q5_minus_q1_bp"]:.4f} vs {pos["q5_minus_q1_bp"]:.4f}')
        cases.append({
            'factor': fac,
            'q5_excess_bp_oriented': neg['q5_excess_bp_oriented'],
            'q1_excess_bp_raw': pos['q1_excess_bp_raw'],
            'old_formula_excess_bp': -pos['q5_excess_bp_raw'],
            'turnover_q5_monthly_oriented': neg['turnover_q5_monthly'],
            'turnover_q1_raw': pos['turnover_q1_raw'],
            'holdings_q5': neg['holdings_q5'],
        })
    n_checks = sum(1 for c in cases if 'pass' in c)
    return {'fwd': fwd, 'h': h, 'n_checks': n_checks,
            'n_pass': sum(1 for c in cases if c.get('pass')), 'cases': cases}


def stage_assert() -> None:
    """独立跑 orient 回归断言，产物写 orient_assertions.json。"""
    sel_info = json.loads((OUT / 'selection_h20.json').read_text(encoding='utf-8'))
    df = load_pre(20, sel_info['kept'])
    res = assert_orient_invariance(df)
    (OUT / 'orient_assertions.json').write_text(
        json.dumps(res, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    for c in res['cases']:
        if 'check' in c:
            print(f'  [{"PASS" if c["pass"] else "FAIL"}] {c["check"]}: {c["detail"]}')
    log(f'orient 回归断言通过（{res["n_pass"]}/{res["n_checks"]}）')


def load_pre(h: int, sel: list[str]) -> pl.DataFrame:
    """读预处理面板并对齐 open->open 标签（主口径）。"""
    pre = WORK / f'pre_h{h}.parquet'
    if pre.exists():
        df = pl.read_parquet(pre)
    else:
        df, meta = build_frame(h, sel, diag=False)
        df.write_parquet(pre)
        (OUT / f'prep_meta_h{h}.json').write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    lab = pl.read_parquet(WORK / f'labels_oo_h{h}.parquet')
    return df.join(lab, on=['date', 'ts_code'], how='left')


def stage_eval() -> None:
    sel_info = json.loads((OUT / 'selection_h20.json').read_text(encoding='utf-8'))
    sel = sel_info['kept']

    # 分域：PIT 指数成分（000300 / 000905 / 000852）
    mem_all = index_long(
        sorted(pl.scan_parquet(BASE).select('date').unique().collect()['date'].to_list()))
    domains = {name: mem_all.filter(pl.col('index_code') == code)
               .select(['date', 'ts_code'])
               for name, code in (('沪深300', '000300'), ('中证500', '000905'),
                                  ('中证1000', '000852'))}

    # --- 主口径 sign：h=20 open->open 的单因子 IC 符号 ---
    df20 = load_pre(20, sel)
    summ_oo = summarize(ic_table(df20, FACTORS, 'fwd_20_oo'), FACTORS, 20)
    summ_oo.write_csv(OUT / 'single_factor_h20_oo.csv')
    sign_map = {r['factor']: (1.0 if r['rank_ic'] > 0 else -1.0)
                for r in summ_oo.iter_rows(named=True) if r['factor'] in sel}
    dom20 = {'全A': None, **domains}

    # --- 26 因子两口径对照表 ---
    summ_c2c = pl.read_csv(OUT / 'single_factor_h20.csv')
    c2c = {r['factor']: r for r in summ_c2c.iter_rows(named=True)}
    rows = []
    for r in summ_oo.iter_rows(named=True):
        f = r['factor']
        if f not in sel:
            continue
        c = c2c[f]
        rows.append({
            'factor': f,
            'abs_rank_ic_c2c': abs(float(c['rank_ic'])),
            'abs_icir_c2c': abs(float(c['icir'])),
            'abs_rank_ic_oo': abs(float(r['rank_ic'])),
            'abs_icir_oo': abs(float(r['icir'])),
            'd_abs_rank_ic': abs(float(r['rank_ic'])) - abs(float(c['rank_ic'])),
            'd_abs_icir': abs(float(r['icir'])) - abs(float(c['icir'])),
            'sign_c2c': float(np.sign(c['rank_ic'])),
            'sign_oo': float(np.sign(r['rank_ic'])),
            'sign_flip': bool(np.sign(c['rank_ic']) != np.sign(r['rank_ic'])),
        })
    cmp_df = pl.DataFrame(rows).sort('abs_icir_oo', descending=True)
    cmp_df.write_csv(OUT / 'label_compare_h20.csv')

    results: dict = {
        'selected': sel,
        'n_selected': len(sel),
        'signs_h20_oo': sign_map,
        'primary_label': 'T+1 开盘 -> T+1+h 开盘（open->open，本次按生产口径重算）',
        'compare_label': '面板 fwd_h（T+1 开盘 -> T+1+h 收盘，c2c）',
        'label_compare_h20': {
            'table': cmp_df.to_dicts(),
            'mean_abs_rank_ic_c2c': float(cmp_df['abs_rank_ic_c2c'].mean()),
            'mean_abs_rank_ic_oo': float(cmp_df['abs_rank_ic_oo'].mean()),
            'mean_abs_icir_c2c': float(cmp_df['abs_icir_c2c'].mean()),
            'mean_abs_icir_oo': float(cmp_df['abs_icir_oo'].mean()),
            'median_d_abs_rank_ic': float(cmp_df['d_abs_rank_ic'].median()),
            'median_d_abs_icir': float(cmp_df['d_abs_icir'].median()),
            'mean_d_abs_rank_ic': float(cmp_df['d_abs_rank_ic'].mean()),
            'mean_d_abs_icir': float(cmp_df['d_abs_icir'].mean()),
            'n_sign_flip': int(cmp_df['sign_flip'].sum()),
            'sign_flip_factors': cmp_df.filter(pl.col('sign_flip'))['factor'].to_list(),
        },
    }

    # --- orient 回归断言：取向必须在分组前对齐（方案 A）---
    oa = assert_orient_invariance(df20)
    results['orient_assertions'] = {
        'n_checks': oa['n_checks'], 'n_pass': oa['n_pass'], 'cases': oa['cases'],
    }
    log(f'orient 回归断言 {oa["n_pass"]}/{oa["n_checks"]} 通过')

    # --- 各 horizon：合成 / 单因子最优 / 分域 ---
    for h in HORIZONS:
        df = df20 if h == 20 else load_pre(h, sel)
        fwd_oo, fwd_c2c = f'fwd_{h}_oo', f'fwd_{h}'
        df = build_composite(df, sel, sign_map)

        cov = float((df['l1_equal_weight__n'] > 0).sum() / df.height)
        comp_oo = evaluate_one(df, 'l1_equal_weight', fwd_oo, h,
                               dom20 if h == 20 else None)
        comp_oo['coverage'] = cov
        comp_oo['mean_n_factors'] = float(df['l1_equal_weight__n'].mean())
        comp_oo['max_n_factors'] = len(sel)
        comp_c2c = evaluate_one(df, 'l1_equal_weight', fwd_c2c, h)

        cand = FACTORS if h == 20 else sel
        s_all = summarize(ic_table(df, cand, fwd_oo), cand, h)
        s_all.write_csv(OUT / f'single_factor_h{h}_oo.csv')
        in_pool = s_all.filter(pl.col('factor').is_in(sel))
        best_icir = in_pool.sort('abs_icir', descending=True).row(0, named=True)
        best_ric = in_pool.sort('abs_rank_ic', descending=True).row(0, named=True)
        all_best = s_all.sort('abs_icir', descending=True).row(0, named=True)

        singles = {}
        for tag, row in (('best_icir_in_pool', best_icir),
                         ('best_rank_ic_in_pool', best_ric),
                         ('best_icir_all', all_best)):
            f = row['factor']
            if f not in df.columns:
                continue
            ev = evaluate_one(df, f, fwd_oo, h, dom20 if h == 20 else None,
                              orient=float(np.sign(row['rank_ic'])))
            ev['factor'] = f
            singles[tag] = ev

        results[f'h{h}'] = {
            'n_periods': int(df['date'].n_unique()),
            'composite_oo': comp_oo,
            'composite_c2c': comp_c2c,
            'in_pool_best_icir': best_icir,
            'in_pool_best_rank_ic': best_ric,
            'singles_oo': singles,
            'pool_ic_oo': {r['factor']: r for r in in_pool.iter_rows(named=True)},
            'top10_candidates_oo': s_all.sort('abs_icir', descending=True)
            .head(10).to_dicts(),
            'n_candidates': s_all.height,
        }
        log(f'h={h}: 合成(oo) RankIC={comp_oo["rank_ic"]:.4f} ICIR={comp_oo["icir"]:.3f} '
            f'ICIR年化={comp_oo["icir_annual"]:.2f} mono={comp_oo["mono"]:.3f} '
            f'Q5-Q1={comp_oo["q5_minus_q1_bp"]:.1f}bp tier={comp_oo["tier"]} '
            f'换手={comp_oo["turnover_q5_monthly"]:.1%}/期')

    # --- 三档分布（主口径 oo，本面板全部 49 单因子）---
    # tier_oriented：方向对齐后取绝对值（本次比较口径）
    # tier_strict  ：生产 report.tier_of 原样口径（带符号 t_stat、未取向 mono）
    tiers = []
    for r in summ_oo.iter_rows(named=True):
        f = r['factor']
        if f not in df20.columns:
            continue
        g = group_stats(df20, f, 'fwd_20_oo')
        sgn = 1.0 if r['rank_ic'] > 0 else -1.0
        tiers.append({
            'factor': f,
            'tier': tier_of(r['icir_annual'], r['t_stat'], g['mono'] * sgn),
            'tier_strict': tier_of(r['icir_annual'], r['t_stat'], g['mono']),
            'icir_annual': r['icir_annual'], 't_stat': r['t_stat'],
            'mono': g['mono'] * sgn, 'mono_raw': g['mono'],
            'abs_rank_ic': r['abs_rank_ic'],
        })
    tt = pl.DataFrame(tiers)
    tt.write_csv(OUT / 'tier_h20.csv')
    results['tier_distribution_h20'] = {
        'counts': {k: int((tt['tier'] == k).sum()) for k in 'ABCD'},
        'counts_strict': {k: int((tt['tier_strict'] == k).sum()) for k in 'ABCD'},
        'note': ('tier = 方向对齐后 |ICIR年化|/|t|/mono 判档；tier_strict = 生产 '
                 'report.tier_of 原样口径（带符号 t 与未取向 mono，负向因子一律判 D）'),
        'n': tt.height, 'detail': tt.to_dicts(),
    }

    # --- 与单因子最优 / mono 三档门槛的对比 ---
    h20 = results['h20']
    best = h20['singles_oo']['best_icir_all']
    comp = h20['composite_oo']
    results['vs_single_best_h20'] = {
        'best_single_factor': best['factor'],
        'abs_rank_ic_comp': comp['abs_rank_ic'],
        'abs_rank_ic_best': best['abs_rank_ic'],
        'd_abs_rank_ic': comp['abs_rank_ic'] - best['abs_rank_ic'],
        'abs_icir_comp': comp['abs_icir'],
        'abs_icir_best': best['abs_icir'],
        'd_abs_icir': comp['abs_icir'] - best['abs_icir'],
        'mono_comp': comp['mono'],
        'mono_best': best['mono'],
        'q5_minus_q1_bp_comp': comp['q5_minus_q1_bp'],
        'q5_minus_q1_bp_best': best['q5_minus_q1_bp'],
        'd_q5_minus_q1_bp': comp['q5_minus_q1_bp'] - best['q5_minus_q1_bp'],
        'q5_excess_bp_comp': comp['q5_excess_bp'],
        'q5_excess_bp_best': best['q5_excess_bp'],
        'd_q5_excess_bp': comp['q5_excess_bp'] - best['q5_excess_bp'],
        'turnover_comp': comp['turnover_q5_monthly'],
        'turnover_best': best['turnover_q5_monthly'],
        'tier_comp': comp['tier'],
        'tier_best': best['tier'],
    }
    results['turnover_algorithm'] = (
        'Q5 篮子（每期得分前 20%，等权）成员单边换手：'
        'turnover_t = |S_{t-1} \\ S_t| / |S_{t-1}|，再对 t 取均值；'
        '等价于 0.5*Σ|w_t - w_{t-1}|（等权下两者恒等）。'
        'h=20 的调仓日为 20 交易日等距、互不重叠，故该值即「月频单边换手」。'
    )
    # 并列噪声登记：取向在分组前 vs 分组后翻转，在 average rank 下不是严格互逆
    results['meta'] = {
        'tie_noise': {
            'spread_max_bp': 0.19,
            'group_mean_max_bp': 0.20,
            'mono_max': 0.0,
            'cause': ('五分组用 average rank（并列取平均秩）；因子乘 −1 后同一并列块的前后'
                      '边界可能落在整数边界两侧，导致组号差 1，组均值与 Q5−Q1 出现位移'),
            'note': ('口径切换会带来 ≤0.2 bp 的并列位移，不与缺陷修正混算；'
                     '0.2 bp 以内不改变任何已发布的分位数字'),
            'source': 'orient_scan/old_vs_new_49_summary.json（49 因子修前/修后逐值对照）',
        },
    }

    (OUT / 'eval_results.json').write_text(
        json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    log(f'三档分布（本面板 {tt.height} 单因子，oo 口径）: '
        f'{results["tier_distribution_h20"]["counts"]}')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--stage', default='all',
                    choices=['prep', 'select', 'eval', 'assert', 'all'])
    ap.add_argument('--work-dir', default=None,
                    help='中间缓存目录（可重建；默认 _staging/l1_equal_weight_20261111）')
    ap.add_argument('--out-dir', default=None,
                    help='证据产物目录（默认 reports/l1_baseline）')
    args = ap.parse_args()
    global WORK, OUT
    if args.work_dir:
        WORK = Path(args.work_dir).resolve()
    if args.out_dir:
        OUT = Path(args.out_dir).resolve()
    WORK.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    if args.stage in ('prep', 'all'):
        stage_prep()
    if args.stage in ('select', 'all'):
        stage_select()
    if args.stage in ('assert', 'all'):
        stage_assert()
    if args.stage in ('eval', 'all'):
        stage_eval()


if __name__ == '__main__':
    main()
