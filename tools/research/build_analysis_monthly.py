#!/usr/bin/env python3
"""阶段 2/3：ff_* 因子 + 行业/市值中性化 + IC/ICIR/五分组（归档自
E:\\www\\tmp\\factor_eval_20261105\\evaluate.py）。

输入（由 ``prepare_monthly_samples.py`` 产出，默认在 ``reports/research_tmp``）：
- ``pv_monthly.parquet``
- ``eligible_monthly.parquet``

输出：
- ``<output-dir>/analysis_monthly.parquet``（默认落到
  ``data/local_store/meta/research_inputs/``）
- 评估辅助文件 ``metrics.json`` / ``factor_metrics.csv`` /
  ``factor_metrics.json`` / ``factor_corr.csv`` 写入工作目录

历史行业口径：默认取 ``<store>/meta/industry`` 下最新的
``industry_YYYYMMDD.parquet``；要复现 2026-11-05 的首版产物需显式传
``--industry-file .../industry_20261105.parquet``。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.advice.factor_panel import SCORE_COLUMNS  # noqa: E402
from src.advice.fundamental_factors import (  # noqa: E402
    FUNDAMENTAL_COLUMNS,
    compute_fundamental_factors,
)

DATA_DIR = REPO_ROOT / 'data' / 'local_store'
DEFAULT_WORK_DIR = REPO_ROOT / 'reports' / 'research_tmp'
DEFAULT_OUTPUT_DIR = DATA_DIR / 'meta' / 'research_inputs'

#: 每个价量因子需要的最少历史行数（不足则视为缺失，不用 0 填充值充数）
MIN_HISTORY = {
    'momentum_1m': 21, 'momentum_3m': 61, 'momentum_6m': 121, 'momentum_12m': 241,
    'rsi_14': 16, 'macd_hist': 36, 'kdj_k': 16, 'kdj_d': 16,
    'volatility_20': 21, 'volatility_60': 61, 'atr_14': 16,
    'volume_momentum_20': 21, 'volume_price_correlation': 22,
    'sharpe_ratio_20': 21, 'max_drawdown_20': 21, 'downside_risk_20': 21,
    'ma20_gap': 20, 'ma60_gap': 60, 'trend_slope_60': 60,
    'atr_pct': 16, 'ret_20': 21, 'amount_ma20': 20,
    'liquidity_log': 20,
}

IS_START, IS_END = '2015-01-01', '2022-12-31'
OOS_START, OOS_END = '2023-01-01', '2026-10-31'
MIN_CS = 100

FF_COLUMNS = [
    'ff_size', 'ff_value_bp', 'ff_value_ep',
    'ff_sales_yield', 'ff_div_yield', 'ff_investment',
]

logger.remove()
logger.add(sys.stderr, level='WARNING')


def _require(path: Path, hint: str) -> Path:
    if not path.exists():
        raise SystemExit(f'缺少输入: {path}\n{hint}')
    return path


def latest_industry_file(store: Path) -> Path:
    cands = sorted(
        p for p in (store / 'meta' / 'industry').glob('industry_*.parquet')
        if re.fullmatch(r'industry_\d{8}\.parquet', p.name))
    if not cands:
        raise SystemExit(
            f'未找到行业快照: {store / "meta" / "industry"}/industry_YYYYMMDD.parquet')
    return cands[-1]


def winsorize(x: np.ndarray, lo: float = 0.01, hi: float = 0.99) -> np.ndarray:
    a, b = np.nanquantile(x, [lo, hi])
    return np.clip(x, a, b)


def spearman(x: np.ndarray, y: np.ndarray) -> float:
    rx = pd.Series(x).rank(method='average').to_numpy()
    ry = pd.Series(y).rank(method='average').to_numpy()
    if rx.std() == 0 or ry.std() == 0:
        return np.nan
    return float(np.corrcoef(rx, ry)[0, 1])


def residualize(y: np.ndarray, design: np.ndarray) -> np.ndarray:
    beta, *_ = np.linalg.lstsq(design, y, rcond=None)
    return y - design @ beta


def annualize(monthly: list[float]) -> float:
    if not monthly:
        return np.nan
    prod = float(np.prod([1.0 + r for r in monthly]))
    return prod ** (12.0 / len(monthly)) - 1.0


def period_metrics(df: pd.DataFrame, factors: list[str],
                   neutral: bool) -> tuple[dict, dict]:
    """返回 (factor->metrics, factor->quintile 年化) 。"""
    ics: dict[str, list[float]] = {f: [] for f in factors}
    qret: dict[str, dict[int, list[float]]] = {
        f: {k: [] for k in range(1, 6)} for f in factors}
    qls: dict[str, list[float]] = {f: [] for f in factors}
    uni: list[float] = []
    used_months = 0

    for _, sub in df.groupby('date', sort=True):
        n = len(sub)
        if n < MIN_CS:
            continue
        ret = sub['fwd_ret'].to_numpy(dtype=float)
        if not np.isfinite(ret).any():
            continue
        uni.append(float(np.nanmean(ret)))
        base = sub[['ln_mv', 'ind_code']].copy()
        ind_dummies = pd.get_dummies(base['ind_code'], drop_first=True, dtype=float)
        design = None
        if neutral:
            mv = base['ln_mv'].to_numpy(dtype=float)
            if not np.isfinite(mv).any():
                mv = np.zeros(n)
            else:
                mv = np.where(np.isfinite(mv), mv, np.nanmedian(mv))
                sd = mv.std()
                mv = (mv - mv.mean()) / (sd if sd > 0 else 1.0)
            design = np.column_stack([
                np.ones(n), mv, ind_dummies.to_numpy(dtype=float)])
        any_factor = False
        for f in factors:
            x = sub[f].to_numpy(dtype=float)
            ok = np.isfinite(x) & np.isfinite(ret)
            if ok.sum() < MIN_CS:
                continue
            xs = winsorize(x[ok])
            if neutral:
                z = (xs - xs.mean()) / (xs.std() or 1.0)
                dd = design[ok]
                res = residualize(z, dd)
            else:
                res = xs
            ic = spearman(res, ret[ok])
            if np.isfinite(ic):
                ics[f].append(ic)
                any_factor = True
            order = np.argsort(res)
            r_sorted = ret[ok][order]
            k = len(order)
            bounds = np.linspace(0, k, 6).astype(int)
            for g in range(5):
                seg = r_sorted[bounds[g]:bounds[g + 1]]
                if seg.size:
                    qret[f][g + 1].append(float(np.nanmean(seg)))
            q1 = float(np.nanmean(r_sorted[bounds[0]:bounds[1]]))
            q5 = float(np.nanmean(r_sorted[bounds[4]:bounds[5]]))
            qls[f].append(q5 - q1)
        if any_factor:
            used_months += 1

    out: dict[str, dict] = {}
    qann: dict[str, dict] = {}
    for f in factors:
        vals = np.array(ics[f], dtype=float)
        if vals.size == 0:
            out[f] = {'n_months': 0}
            qann[f] = {}
            continue
        mean = float(vals.mean())
        std = float(vals.std(ddof=1)) if vals.size > 1 else np.nan
        icir = mean / std if std and np.isfinite(std) and std > 0 else np.nan
        tstat = icir * np.sqrt(vals.size) if np.isfinite(icir) else np.nan
        qs = {g: annualize(qret[f][g]) for g in range(1, 6)}
        ls_m = qls[f]
        qann[f] = {
            'Q1': qs[1], 'Q2': qs[2], 'Q3': qs[3], 'Q4': qs[4], 'Q5': qs[5],
            'long_short': annualize(ls_m) if ls_m else np.nan,
            'n_months_ls': len(ls_m),
        }
        out[f] = {
            'ic_mean': mean, 'ic_std': std, 'icir': icir, 't': tstat,
            'n_months': int(vals.size),
            'ic_pos_ratio': float((vals > 0).mean()),
        }
    uni_ann = annualize(uni)
    return out, {'quintiles': qann, 'universe_ann': uni_ann,
                 'n_months': used_months}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='构建 analysis_monthly 面板（阶段 2/3）')
    p.add_argument('--store', type=Path, default=DATA_DIR, help='本地数据根目录')
    p.add_argument('--work-dir', type=Path, default=DEFAULT_WORK_DIR,
                   help='中间件目录（读 pv_monthly/eligible_monthly，写评估文件）')
    p.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR,
                   help='analysis_monthly.parquet 输出目录')
    p.add_argument('--industry-file', type=Path, default=None,
                   help='行业快照 parquet；默认取 industry_YYYYMMDD 中最新一份')
    p.add_argument('--output-name', default='analysis_monthly.parquet')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    store = Path(args.store).resolve()
    work_dir = Path(args.work_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    pv_path = _require(work_dir / 'pv_monthly.parquet',
                       '请先运行 tools/research/prepare_monthly_samples.py')
    elig_path = _require(work_dir / 'eligible_monthly.parquet',
                         '请先运行 tools/research/prepare_monthly_samples.py')
    ind_path = (Path(args.industry_file).resolve()
                if args.industry_file else latest_industry_file(store))
    if not ind_path.exists():
        raise SystemExit(f'缺少行业快照: {ind_path}')

    pv = pl.read_parquet(pv_path)
    elig = pl.read_parquet(elig_path)
    shares = pl.read_parquet(
        store / 'meta/factors/stock_shares.parquet',
        columns=['ts_code', 'effective_date', 'float_share'])
    shares = shares.filter(
        pl.col('float_share').is_not_null() & (pl.col('float_share') > 0)
    ).unique(subset=['ts_code', 'effective_date'], keep='last') \
     .sort(['ts_code', 'effective_date'])
    ind = pl.read_parquet(ind_path, columns=['code', 'industry_l1']).rename(
        {'code': 'ts_code', 'industry_l1': 'industry'})

    df = (pv.join(elig, on=['ts_code', 'date'], how='inner')
          .join(ind, on='ts_code', how='left')
          .sort(['ts_code', 'date']))
    df = df.with_columns(pl.col('industry').fill_null('unknown').alias('ind_code'))

    df = df.join_asof(
        shares, left_on='date', right_on='effective_date', by='ts_code',
        strategy='backward')
    df = df.with_columns(
        (pl.col('raw_close') * pl.col('float_share').cast(pl.Float64))
        .log().alias('ln_mv'))

    exprs = []
    for f, need in MIN_HISTORY.items():
        if f in df.columns:
            exprs.append(
                pl.when(pl.col('hist_seq') >= need).then(pl.col(f))
                .otherwise(None).alias(f))
    df = df.with_columns(exprs)

    panel = df.select(['ts_code', 'date', 'volume'])
    fund = compute_fundamental_factors(panel)
    df = df.hstack([fund[c] for c in FUNDAMENTAL_COLUMNS
                    if c not in df.columns])

    factors = [c for c in SCORE_COLUMNS if c in df.columns
               and c != 'history_rows'] + FF_COLUMNS

    cover_all = {f: float(df[f].is_not_null().mean()) for f in factors}

    out_path = output_dir / args.output_name
    out_cols = ['ts_code', 'date', 'fwd_ret', 'fwd_ret_21c', 'ln_mv',
                'ind_code', *factors]
    df.select(out_cols).write_parquet(out_path)
    print(f'analysis_monthly: {df.height:,} 行 × {len(out_cols)} 列 '
          f'→ {out_path}', flush=True)

    pdf = df.to_pandas()
    pdf['date'] = pd.to_datetime(pdf['date'])
    results = {}
    for label, lo, hi in [('IS', IS_START, IS_END), ('OOS', OOS_START, OOS_END)]:
        seg = pdf[(pdf['date'] >= lo) & (pdf['date'] <= hi) &
                  pdf['fwd_ret'].notna()].copy()
        cov = {f: float(seg[f].notna().mean()) for f in factors}
        raw_m, raw_q = period_metrics(seg, factors, neutral=False)
        neu_m, neu_q = period_metrics(seg, factors, neutral=True)
        results[label] = {'cov': cov, 'raw': raw_m, 'neutral': neu_m,
                          'raw_q': raw_q, 'neutral_q': neu_q,
                          'rows': int(len(seg)),
                          'n_dates': int(seg['date'].nunique())}
        print(f'[{label}] rows={len(seg):,} dates={seg["date"].nunique()} '
              f'universe_ann={neu_q["universe_ann"]:.2%}', flush=True)

    payload = {'coverage_all': cover_all,
               'min_history': MIN_HISTORY,
               'results': results}
    with open(work_dir / 'metrics.json', 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=float)

    rows = []
    for f in factors:
        isa, oos = results['IS'], results['OOS']
        nm, rm = isa['neutral'].get(f, {}), isa['raw'].get(f, {})
        onm = oos['neutral'].get(f, {})
        iq = results['IS']['neutral_q']['quintiles'].get(f, {})
        rows.append({
            'factor': f,
            'IS_IC': nm.get('ic_mean', np.nan),
            'IS_ICIR': nm.get('icir', np.nan),
            'IS_t': nm.get('t', np.nan),
            'IS_ICIR_raw': rm.get('icir', np.nan),
            'IS_IC_raw': rm.get('ic_mean', np.nan),
            'dICIR': (nm.get('icir', np.nan) - rm.get('icir', np.nan)
                      if np.isfinite(nm.get('icir', np.nan))
                      and np.isfinite(rm.get('icir', np.nan)) else np.nan),
            'OOS_IC': onm.get('ic_mean', np.nan),
            'OOS_ICIR': onm.get('icir', np.nan),
            'OOS_t': onm.get('t', np.nan),
            'LS_ann': iq.get('long_short', np.nan),
            'Q1': iq.get('Q1', np.nan), 'Q2': iq.get('Q2', np.nan),
            'Q3': iq.get('Q3', np.nan), 'Q4': iq.get('Q4', np.nan),
            'Q5': iq.get('Q5', np.nan),
            'coverage_all': cover_all[f],
            'coverage_IS': isa['cov'][f],
            'coverage_OOS': oos['cov'][f],
            'n_months_IS': nm.get('n_months', 0),
        })
    table = pd.DataFrame(rows).sort_values('IS_ICIR', ascending=False, na_position='last')
    table.to_csv(work_dir / 'factor_metrics.csv', index=False, encoding='utf-8-sig')
    with open(work_dir / 'factor_metrics.json', 'w', encoding='utf-8') as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2, default=float)

    is_dates = sorted(pdf[(pdf['date'] >= IS_START) & (pdf['date'] <= IS_END)
                          & pdf['fwd_ret'].notna()]['date'].unique())[::3]
    mats = [pdf[pdf['date'] == d][factors].rank().corr(min_periods=100)
            for d in is_dates]
    avg_corr = sum(mats) / len(mats)
    avg_corr.to_csv(work_dir / 'factor_corr.csv', encoding='utf-8-sig')
    pairs = []
    for i, a in enumerate(factors):
        for b in factors[i + 1:]:
            v = avg_corr.loc[a, b]
            if np.isfinite(v):
                pairs.append((abs(v), v, a, b))
    pairs.sort(reverse=True)
    print('平均秩相关最高的 12 对:', flush=True)
    for av, v, a, b in pairs[:12]:
        print(f'  {a:>26s} ~ {b:<26s} {v:+.3f}', flush=True)

    pd.set_option('display.width', 200)
    pd.set_option('display.max_rows', 60)
    print(table[['factor', 'IS_IC', 'IS_ICIR', 'IS_t', 'IS_ICIR_raw',
                 'dICIR', 'OOS_ICIR', 'LS_ann', 'coverage_all']]
          .to_string(index=False, float_format=lambda v: f'{v:.4f}'), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
