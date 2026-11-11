#!/usr/bin/env python
"""L2 分域合成：域内重选因子 -> 等权 / ICIR 加权 -> 300/500/1000 三域对照。

只读输入（不写 data/、不改任何现有调用点、不碰 src/advice/*）：
    预处理面板  _staging/l1_equal_weight_20261111/pre_h20.parquet（49 因子残差 + __valid mask）
    oo 标签     _staging/l1_equal_weight_20261111/labels_oo_h20.parquet
    指数成分    data/local_store/meta/index_members/membership_intervals.parquet（PIT 区间）
    全池入池表  reports/l1_baseline/selection_h20.json（26 因子，只读引用）

评估 / 分组 / 分档函数一律复用 tools/research/l1_baseline/run_l1.py
（ic_table / summarize / group_stats / tier_of / MIN_NAMES_DOMAIN），
保证与 L1 基线同口径，不另写第二套实现。

本脚本只做一件事的对照：**把选因子的尺子从全池换成域内**，因子值与其他一切不变
（残差化仍用全样本截面，不按域重算），因此差异只来自「选谁 / 给多少权重」。

阶段：
    --stage probe   各域每期只数 + 每因子有效截面数（判断 300 域样本量是否够域内筛选）
    --stage select  全A / 300 / 500 / 1000 的域内重选（full 与 is 两个窗口）
    --stage eval    三方案 × 四域 × full/IS/OOS
    --stage all

产物全部写 _staging/domain_composite_20261111/（可重建，不入库）。
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

# 复用 L1 基线的评估实现（按路径加载：tools/ 不是包，但脚本无副作用可安全导入）
_L1_PATH = ROOT / 'tools/research/l1_baseline/run_l1.py'
_spec = importlib.util.spec_from_file_location('l1_baseline_run', _L1_PATH)
l1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(l1)

PRE = ROOT / '_staging/l1_equal_weight_20261111/pre_h20.parquet'
LAB = ROOT / '_staging/l1_equal_weight_20261111/labels_oo_h20.parquet'
IDX = ROOT / 'data/local_store/meta/index_members/membership_intervals.parquet'
POOL_JSON = ROOT / 'reports/l1_baseline/selection_h20.json'
BASE_JSON = ROOT / 'reports/l1_baseline/eval_results.json'

OUT = ROOT / '_staging/domain_composite_20261111'

FACTORS = l1.FACTORS
H = 20
FWD = 'fwd_20_oo'
IS_END = date(2021, 12, 31)
# 门槛与全池那套**完全一致**，不为 300 域放宽
MIN_ABS_RANKIC = l1.MIN_ABS_RANKIC
MIN_ABS_ICIR = l1.MIN_ABS_ICIR
CORR_THRESHOLD = 0.7
MIN_NAMES_DOMAIN = l1.MIN_NAMES_DOMAIN  # 40

FULL_A = '全A'
DOMAINS = {'沪深300': '000300', '中证500': '000905', '中证1000': '000852'}
ALL_DOMAINS = [FULL_A, *DOMAINS]


def log(msg: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


# ---------------------------------------------------------------------------
# 面板与域成分
# ---------------------------------------------------------------------------
def load_panel() -> pl.DataFrame:
    pre = pl.read_parquet(PRE)
    lab = pl.read_parquet(LAB)
    df = pre.join(lab, on=['date', 'ts_code'], how='left')
    log(f'面板 {df.height:,} 行 × {len(df.columns)} 列，'
        f'{df["date"].n_unique()} 期，标签非空 {df[FWD].is_not_null().sum():,}')
    return df


def domain_members(dates: list[date]) -> dict[str, pl.DataFrame]:
    """(date, ts_code) 长表，PIT 命中；全A 不建成员表（None）。"""
    iv = pl.read_parquet(IDX, columns=['index_code', 'ts_code', 'in_date', 'out_date'])
    out: dict[str, pl.DataFrame] = {}
    for name, code in DOMAINS.items():
        rows: list[date] = []
        codes: list[str] = []
        for d in dates:
            sub = iv.filter((pl.col('index_code') == code)
                            & (pl.col('in_date') <= d)
                            & (pl.col('out_date').is_null() | (pl.col('out_date') >= d)))
            rows.extend([d] * sub.height)
            codes.extend(sub['ts_code'].to_list())
        out[name] = pl.DataFrame({'date': rows, 'ts_code': codes},
                                 schema={'date': pl.Date, 'ts_code': pl.Utf8})
    return out


def restrict(df: pl.DataFrame, mem: pl.DataFrame | None) -> pl.DataFrame:
    if mem is None:
        return df
    return df.join(mem, on=['date', 'ts_code'], how='semi')


# ---------------------------------------------------------------------------
# 样本量体检
# ---------------------------------------------------------------------------
def stage_probe(df: pl.DataFrame, mems: dict[str, pl.DataFrame]) -> dict:
    dates = sorted(df['date'].unique().to_list())
    res: dict = {'periods_total': len(dates), 'domains': {}}
    for name in ALL_DOMAINS:
        sub = restrict(df, mems.get(name))
        base = sub.filter(pl.col(FWD).is_not_null())
        per = base.group_by('date').agg(pl.len().alias('n')).sort('date')
        cov = base.group_by('date').agg(
            [pl.col(c).is_not_null().sum().alias(c) for c in FACTORS])
        fac = {}
        for c in FACTORS:
            arr = cov[c].to_numpy()
            fac[c] = {
                'min': int(arr.min()), 'p10': float(np.percentile(arr, 10)),
                'median': float(np.median(arr)),
                'min_share_of_names': float(arr.min() / per['n'].min()),
            }
        res['domains'][name] = {
            'rows': base.height,
            'n_periods': per.height,
            'names_min': int(per['n'].min()),
            'names_median': float(per['n'].median()),
            'names_max': int(per['n'].max()),
            'q5_names_median': float(per['n'].median()) / 5.0,
            'factor_coverage': fac,
            'factors_below_100_min': [c for c in FACTORS if fac[c]['min'] < 100],
            'factors_below_50_min': [c for c in FACTORS if fac[c]['min'] < 50],
        }
        log(f'{name}: {per.height} 期，每期 {int(per["n"].min())}~{int(per["n"].max())} 只'
            f'（中位 {per["n"].median():.0f}）；因子有效数最低 '
            f'{min(v["min"] for v in fac.values())}')
    (OUT / 'probe_sample_size.json').write_text(
        json.dumps(res, ensure_ascii=False, indent=2), encoding='utf-8')
    return res


# ---------------------------------------------------------------------------
# 域内重选（门槛 / 去冗余 / 贪心 与全池完全一致）
# ---------------------------------------------------------------------------
def select_in_domain(sub: pl.DataFrame, dates: list[date], tag: str) -> dict:
    d = sub.filter(pl.col('date').is_in(dates))
    ic = l1.ic_table(d, FACTORS, FWD, min_names=MIN_NAMES_DOMAIN)
    summ = l1.summarize(ic, FACTORS, H)
    n_all = summ.height
    step1 = summ.filter((pl.col('abs_rank_ic') >= MIN_ABS_RANKIC)
                        & (pl.col('abs_icir') >= MIN_ABS_ICIR))
    surviving = step1['factor'].to_list()

    if len(surviving) >= 2:
        corr = l1.correlation_matrix(d, surviving, dates)
        cl = l1.build_clusters(corr, summ.select(['factor', 'icir']),
                               threshold=CORR_THRESHOLD)
        kept = sorted(cl.kept)
        dropped_cluster = sorted(cl.dropped)
    else:
        corr = None
        kept = sorted(surviving)
        dropped_cluster = []

    icir_map = {r['factor']: abs(float(r['icir'])) for r in summ.iter_rows(named=True)}
    greedy_dropped: list[str] = []
    pairs: list[tuple[str, str, float]] = []
    if corr is not None:
        absc = corr.abs()
        while len(kept) > 2:
            m = absc.loc[kept, kept].to_numpy().copy()
            np.fill_diagonal(m, 0.0)
            if m.max() <= CORR_THRESHOLD:
                break
            i, j = np.unravel_index(int(np.argmax(m)), m.shape)
            a, b = kept[i], kept[j]
            drop = b if icir_map.get(a, 0.0) >= icir_map.get(b, 0.0) else a
            pairs.append((a, b, float(m[i, j])))
            greedy_dropped.append(drop)
            kept = sorted(x for x in kept if x != drop)

    metrics = {r['factor']: r for r in summ.iter_rows(named=True)}
    signs = {f: (1.0 if metrics[f]['rank_ic'] > 0 else -1.0) for f in kept}
    log(f'  [{tag}] 49 -> 硬门槛 {step1.height} -> 聚类 {len(kept) + len(greedy_dropped)} '
        f'-> 贪心 {len(kept)}')
    return {
        'tag': tag,
        'n_periods': int(d['date'].n_unique()),
        'candidates_total': n_all,
        'thresholds': {'abs_rank_ic': MIN_ABS_RANKIC, 'abs_icir': MIN_ABS_ICIR,
                       'corr': CORR_THRESHOLD, 'min_names': MIN_NAMES_DOMAIN},
        'after_ic_filter': step1.height,
        'dropped_by_ic_filter': sorted(set(FACTORS) - set(surviving)),
        'after_cluster': len(kept) + len(greedy_dropped),
        'dropped_by_cluster': dropped_cluster,
        'greedy_pairs': [{'a': a, 'b': b, 'abs_corr': c} for a, b, c in pairs],
        'greedy_dropped': greedy_dropped,
        'kept': kept,
        'n_kept': len(kept),
        'signs': signs,
        'icir': {f: float(metrics[f]['icir']) for f in kept},
        'abs_icir': {f: float(metrics[f]['abs_icir']) for f in kept},
        'abs_rank_ic': {f: float(metrics[f]['abs_rank_ic']) for f in kept},
        'rank_ic': {f: float(metrics[f]['rank_ic']) for f in kept},
        'single_factor_metrics': [
            {k: (float(v) if isinstance(v, (int, float, np.floating)) else v)
             for k, v in r.items()} for r in summ.iter_rows(named=True)
        ],
    }


def stage_select(df: pl.DataFrame, mems: dict[str, pl.DataFrame]) -> dict:
    dates = sorted(df['date'].unique().to_list())
    is_dates = [d for d in dates if d <= IS_END]
    oos_dates = [d for d in dates if d > IS_END]
    log(f'窗口：full={len(dates)} 期，IS={len(is_dates)} 期（≤{IS_END}），'
        f'OOS={len(oos_dates)} 期')

    pool = json.loads(POOL_JSON.read_text(encoding='utf-8'))['kept']
    out: dict = {
        'windows': {'full': len(dates), 'is': len(is_dates), 'oos': len(oos_dates)},
        'pool26': pool,
        'domains': {},
    }
    for name in ALL_DOMAINS:
        sub = restrict(df, mems.get(name)).filter(pl.col(FWD).is_not_null())
        out['domains'][name] = {}
        for win, ds in (('full', dates), ('is', is_dates)):
            sel = select_in_domain(sub, ds, f'{name}/{win}')
            # 记录该域该窗口的单因子 IC 序列摘要（供文档引用）
            out['domains'][name][win] = sel
        # 全A 全样本应复现 26 因子池（口径自检）
        if name == FULL_A:
            rep = sorted(out['domains'][name]['full']['kept'])
            out['full_a_reproduces_pool26'] = {
                'identical': rep == sorted(pool),
                'only_in_recomputed': sorted(set(rep) - set(pool)),
                'only_in_published': sorted(set(pool) - set(rep)),
            }
            log(f'  自检：全A 全样本重选 == 已发布 26 因子池：'
                f'{out["full_a_reproduces_pool26"]["identical"]}')

    # 重合度矩阵
    overlap: dict = {}
    for win in ('full', 'is'):
        sets = {d: set(out['domains'][d][win]['kept']) for d in ALL_DOMAINS}
        pair = {}
        for i, a in enumerate(ALL_DOMAINS):
            for b in ALL_DOMAINS[i + 1:]:
                inter = sets[a] & sets[b]
                union = sets[a] | sets[b]
                pair[f'{a}|{b}'] = {
                    'n_inter': len(inter), 'n_union': len(union),
                    'jaccard': len(inter) / len(union) if union else 0.0,
                    'inter': sorted(inter),
                }
        overlap[win] = {'pairwise': pair,
                        'sizes': {d: len(sets[d]) for d in ALL_DOMAINS}}
        # 与全池 26 的重合
        overlap[win]['vs_pool26'] = {
            d: {'n_inter': len(sets[d] & set(pool)),
                'share_of_domain': len(sets[d] & set(pool)) / max(len(sets[d]), 1),
                'share_of_pool': len(sets[d] & set(pool)) / len(pool),
                'inter': sorted(sets[d] & set(pool))}
            for d in ALL_DOMAINS
        }
    out['overlap'] = overlap
    (OUT / 'selection_domains.json').write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    return out


# ---------------------------------------------------------------------------
# 合成
# ---------------------------------------------------------------------------
def make_eq(df: pl.DataFrame, sel: list[str], signs: dict[str, float],
            name: str) -> pl.DataFrame:
    exprs = [pl.col(c) * signs[c] for c in sel]
    return df.with_columns(pl.mean_horizontal(exprs).alias(name))


def make_icir_w(df: pl.DataFrame, sel: list[str], signs: dict[str, float],
                abs_icir: dict[str, float], name: str) -> pl.DataFrame:
    raw = {c: abs(float(abs_icir[c])) for c in sel}
    tot = sum(raw.values())
    w = {c: raw[c] / tot for c in sel}
    num = pl.sum_horizontal([pl.col(c) * signs[c] * w[c] for c in sel])
    den = pl.sum_horizontal(
        [pl.when(pl.col(c).is_not_null()).then(pl.lit(w[c])).otherwise(pl.lit(0.0))
         for c in sel])
    return df.with_columns(
        pl.when(den > 0).then(num / den).otherwise(None).alias(name),
        den.alias(f'{name}__wsum'))


# ---------------------------------------------------------------------------
# 评估
# ---------------------------------------------------------------------------
def spread_stats(ddf: pl.DataFrame, col: str, fwd: str,
                 n_groups: int = 5) -> dict:
    """逐期 Q5−Q1 与 Q5 超额序列，给均值 / t / 95% CI（点估计不够，必须带显著性）。"""
    w = ddf.select(['date', 'ts_code', col, fwd]).filter(
        pl.col(col).is_not_null() & pl.col(fwd).is_not_null())
    w = w.with_columns(
        pl.col(col).rank().over('date').alias('_r'),
        pl.len().over('date').alias('_n'),
    ).filter(pl.col('_n') >= MIN_NAMES_DOMAIN).with_columns(
        ((pl.col('_r').cast(pl.UInt32) * n_groups) // (pl.col('_n') + 1) + 1)
        .cast(pl.Int8).alias('_g'))
    per = w.group_by(['date', '_g']).agg(
        pl.col(fwd).mean().alias('ret')).sort(['date', '_g'])
    uni = w.group_by('date').agg(pl.col(fwd).mean().alias('_u')).sort('date')
    q5 = per.filter(pl.col('_g') == n_groups).select(['date', 'ret'])
    q1 = per.filter(pl.col('_g') == 1).select(['date', 'ret'])
    j = q5.join(q1, on='date', suffix='_q1').join(uni, on='date', how='left')
    sp = (j['ret'] - j['ret_q1']).to_numpy()
    ex = (j['ret'] - j['_u']).to_numpy()
    n = len(sp)

    def _t(x: np.ndarray) -> tuple[float, float, float]:
        if len(x) < 3:
            return float('nan'), float('nan'), float('nan')
        m = float(x.mean())
        se = float(x.std(ddof=1) / np.sqrt(len(x)))
        return m * 1e4, (m / se if se > 0 else float('nan')), 1.96 * se * 1e4

    sp_m, sp_t, sp_ci = _t(sp)
    ex_m, ex_t, ex_ci = _t(ex)
    return {'n_periods': n, 'spread_bp': sp_m, 'spread_t': sp_t,
            'spread_ci95_bp': sp_ci, 'excess_bp': ex_m, 'excess_t': ex_t,
            'excess_ci95_bp': ex_ci}


def eval_score(ddf: pl.DataFrame, col: str) -> dict:
    ic = l1.ic_table(ddf, [col], FWD, min_names=MIN_NAMES_DOMAIN)
    s = l1.summarize(ic, [col], H)
    if not s.height or s['n'][0] < 6:
        return {'n_periods': 0}
    r = s.row(0, named=True)
    g = l1.group_stats(ddf, col, FWD)
    sp = spread_stats(ddf, col, FWD)
    return {
        'n_periods': int(r['n']),
        'rank_ic': float(r['rank_ic']),
        'abs_rank_ic': float(r['abs_rank_ic']),
        'icir': float(r['icir']),
        'abs_icir': float(r['abs_icir']),
        'icir_annual': float(r['icir_annual']),
        'ic_t': float(r['t_stat']),
        'ic_positive_rate': float(r['ic_positive_rate']),
        'mono': float(g['mono']),
        'q5_minus_q1_bp': float(g['q5_minus_q1_bp']),
        'q5_excess_bp': float(g['q5_excess_bp']),
        'spread_t': sp['spread_t'],
        'spread_ci95_bp': sp['spread_ci95_bp'],
        'excess_t': sp['excess_t'],
        'excess_ci95_bp': sp['excess_ci95_bp'],
        'turnover_q5': float(g['turnover_q5']),
        'turnover_mean': float(g['turnover_mean']),
        'holdings_q5': float(g['holdings_q5']),
        'universe_mean_bp': float(g['universe_mean_ret']) * 1e4,
        'group_means_bp': {k: v * 1e4 for k, v in g['group_means'].items()},
        'tier': l1.tier_of(r['icir_annual'], r['t_stat'], g['mono']),
    }


def stage_eval(df: pl.DataFrame, mems: dict[str, pl.DataFrame],
               sel: dict) -> dict:
    dates = sorted(df['date'].unique().to_list())
    is_dates = {d for d in dates if d <= IS_END}
    oos_dates = {d for d in dates if d > IS_END}
    pool = sel['pool26']
    base = json.loads(BASE_JSON.read_text(encoding='utf-8'))
    pool_signs = {f: float(base['signs_h20_oo'][f]) for f in pool}

    results: dict = {'schemes': {}, 'by_domain': {}}
    for name in ALL_DOMAINS:
        ddf = restrict(df, mems.get(name)).filter(pl.col(FWD).is_not_null())
        d_selections = sel['domains'][name]

        work = ddf
        schemes: dict[str, str] = {}
        # A：全池 26 等权（方向取已发布的全池 oo 符号），尺子不变
        # 注意：A 的因子集与符号都来自全样本，OOS 列不是干净样本外
        work = make_eq(work, pool, pool_signs, 'A_pool26_ew')
        schemes['A_pool26_ew'] = '全池26等权（符号=全池全样本）'

        for win in ('full', 'is'):
            s = d_selections[win]
            kept, signs = s['kept'], s['signs']
            if len(kept) >= 1:
                work = make_eq(work, kept, signs, f'B_{win}_ew')
                work = make_icir_w(work, kept, signs, s['abs_icir'], f'C_{win}_icir')
                schemes[f'B_{win}_ew'] = f'域内重选等权（窗口={win}，{len(kept)} 因子）'
                schemes[f'C_{win}_icir'] = f'域内 ICIR 加权（窗口={win}，{len(kept)} 因子）'

        out: dict = {}
        for scheme in schemes:
            # 列名与方案键同名（A_pool26_ew / B_{win}_ew / C_{win}_icir）
            rec: dict = {}
            for win_label, mask in (('full', None), ('is', is_dates), ('oos', oos_dates)):
                sub = work if mask is None else work.filter(pl.col('date').is_in(mask))
                rec[win_label] = eval_score(sub, scheme)
            rec['selection_in_sample'] = win_label_flag(scheme)
            out[scheme] = rec
        results['by_domain'][name] = {
            'domain': name,
            'n_periods_full': len(dates),
            'n_periods_is': len(is_dates),
            'n_periods_oos': len(oos_dates),
            'rows': ddf.height,
            'schemes': out,
            'scheme_desc': schemes,
        }
        log(f'{name}: ' + ', '.join(
            f'{k} full ICIR={v["full"].get("icir", float("nan")):.3f}/'
            f'OOS {v["oos"].get("icir", float("nan")):.3f}' for k, v in out.items()))

    results['pool_signs'] = pool_signs
    results['pool26'] = pool
    results['meta'] = {
        'label': 'open->open（T+1 开盘 -> T+1+h 开盘）',
        'h': H, 'is_end': str(IS_END),
        'thresholds': {'abs_rank_ic': MIN_ABS_RANKIC, 'abs_icir': MIN_ABS_ICIR,
                       'corr': CORR_THRESHOLD, 'min_names': MIN_NAMES_DOMAIN},
        'note': ('A / B_full / C_full 的因子集与符号都在全样本上确定，'
                 'OOS 列存在样本内选择污染，不能当样本外证据；'
                 'B_is / C_is 是唯一干净的样本外口径。'),
    }
    (OUT / 'eval_schemes.json').write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    return results


def win_label_flag(scheme: str) -> bool:
    return scheme.startswith(('A_', 'B_full', 'C_full'))


# ---------------------------------------------------------------------------
# 逐因子分域 IC 表（不做任何选择，用于判断「域内到底有没有信号」）
# ---------------------------------------------------------------------------
def stage_factor_table(df: pl.DataFrame, mems: dict[str, pl.DataFrame]) -> dict:
    dates = sorted(df['date'].unique().to_list())
    is_dates = {d for d in dates if d <= IS_END}
    oos_dates = {d for d in dates if d > IS_END}
    out: dict = {'windows': {'full': len(dates), 'is': len(is_dates),
                             'oos': len(oos_dates)}, 'domains': {}}
    for name in ALL_DOMAINS:
        ddf = restrict(df, mems.get(name)).filter(pl.col(FWD).is_not_null())
        rec: dict = {}
        for win, mask in (('full', None), ('is', is_dates), ('oos', oos_dates)):
            sub = ddf if mask is None else ddf.filter(pl.col('date').is_in(mask))
            ic = l1.ic_table(sub, FACTORS, FWD, min_names=MIN_NAMES_DOMAIN)
            summ = l1.summarize(ic, FACTORS, H)
            rec[win] = {r['factor']: {
                'rank_ic': float(r['rank_ic']), 'abs_rank_ic': float(r['abs_rank_ic']),
                'icir': float(r['icir']), 'abs_icir': float(r['abs_icir']),
                't_stat': float(r['t_stat']), 'n': int(r['n']),
            } for r in summ.iter_rows(named=True)}
        # 通过同一硬门槛的因子数（该窗口内独立数，不含去冗余）
        rec['n_pass'] = {
            w: sum(1 for f in FACTORS
                   if rec[w][f]['abs_rank_ic'] >= MIN_ABS_RANKIC
                   and rec[w][f]['abs_icir'] >= MIN_ABS_ICIR)
            for w in ('full', 'is', 'oos')
        }
        rec['max_abs_icir'] = {
            w: max(rec[w][f]['abs_icir'] for f in FACTORS) for w in ('full', 'is', 'oos')}
        # 因子效力的时段持续性：IS 与 OOS 的 |ICIR| 秩相关（近似 0 = 轮动，正相关 = 可持续）
        a_abs = np.array([rec['is'][f]['abs_icir'] for f in FACTORS])
        b_abs = np.array([rec['oos'][f]['abs_icir'] for f in FACTORS])
        a_sig = np.array([rec['is'][f]['icir'] for f in FACTORS])
        b_sig = np.array([rec['oos'][f]['icir'] for f in FACTORS])
        rec['is_oos_persistence'] = {
            'spearman_abs_icir': float(spearmanr(a_abs, b_abs).statistic),
            'spearman_signed_icir': float(spearmanr(a_sig, b_sig).statistic),
            'sign_agree_share': float(np.mean(np.sign(a_sig) == np.sign(b_sig))),
            'n_factors': len(FACTORS),
        }
        out['domains'][name] = rec
        log(f'{name} 逐因子通过门槛数 full/is/oos = '
            f'{rec["n_pass"]["full"]}/{rec["n_pass"]["is"]}/{rec["n_pass"]["oos"]}，'
            f'最大 |ICIR| = {rec["max_abs_icir"]["full"]:.3f}/'
            f'{rec["max_abs_icir"]["is"]:.3f}/{rec["max_abs_icir"]["oos"]:.3f}，'
            f'IS~OOS |ICIR| 秩相关 = {rec["is_oos_persistence"]["spearman_abs_icir"]:.3f}')
    (OUT / 'factor_table_by_domain.json').write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    return out


# ---------------------------------------------------------------------------
# 选因子稳定性（子窗口重选）
# ---------------------------------------------------------------------------
SUB_WINDOWS = [
    ('2015-2017', date(2015, 1, 1), date(2017, 12, 31)),
    ('2018-2020', date(2018, 1, 1), date(2020, 12, 31)),
    ('2021-2023', date(2021, 1, 1), date(2023, 12, 31)),
    ('2024-2026', date(2024, 1, 1), date(2026, 12, 31)),
]


def stage_stability(df: pl.DataFrame, mems: dict[str, pl.DataFrame]) -> dict:
    """把样本切成 4 段等长区间分别重选，看子集是否稳定。

    每段约 36 期，ICIR 标准误约 1/sqrt(36)=0.167，与门槛 0.3 同量级，
    因此「同一域在不同子窗口选出完全不同的因子」本身就是样本噪声的直接证据。
    """
    out: dict = {'sub_windows': [{'name': n, 'start': str(a), 'end': str(b)}
                                 for n, a, b in SUB_WINDOWS], 'domains': {}}
    for name in ALL_DOMAINS:
        sub = restrict(df, mems.get(name)).filter(pl.col(FWD).is_not_null())
        rec = {}
        freq: dict[str, int] = {}
        for wname, a, b in SUB_WINDOWS:
            ds = [d for d in sorted(sub['date'].unique().to_list()) if a <= d <= b]
            if len(ds) < 12:
                rec[wname] = {'n_periods': len(ds), 'skipped': True}
                continue
            s = select_in_domain(sub, ds, f'{name}/{wname}')
            rec[wname] = {k: s[k] for k in
                          ('n_periods', 'after_ic_filter', 'n_kept', 'kept',
                           'abs_icir', 'abs_rank_ic')}
            for f in s['kept']:
                freq[f] = freq.get(f, 0) + 1
        out['domains'][name] = {
            'by_window': rec,
            'selection_frequency': dict(sorted(freq.items(),
                                               key=lambda kv: -kv[1])),
            'distinct_factors_selected': len(freq),
        }
        log(f'{name} 子窗口重选：' + ' / '.join(
            f'{w}={rec[w].get("n_kept", "NA")}' for w, _, _ in SUB_WINDOWS))
    (OUT / 'selection_stability.json').write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--stage', default='all',
                    choices=['probe', 'select', 'eval', 'stability', 'factor', 'all'])
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    df = load_panel()
    dates = sorted(df['date'].unique().to_list())
    mems = domain_members(dates)

    if args.stage in ('probe', 'all'):
        stage_probe(df, mems)
    if args.stage in ('factor', 'all'):
        stage_factor_table(df, mems)
    if args.stage in ('select', 'eval', 'stability', 'all'):
        sel = stage_select(df, mems)
        if args.stage in ('eval', 'all'):
            stage_eval(df, mems, sel)
        if args.stage in ('stability', 'all'):
            stage_stability(df, mems)


if __name__ == '__main__':
    main()
