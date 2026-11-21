#!/usr/bin/env python3
"""「低估值+质量」月频组合 alpha 真实性核查（2026-11-21）。

三项检查
--------
1. **补退市股**：用与基准对账（``benchmark_reconcile.py``）同一补样池
   （本地 259 只已退市中有行情的全补回）重跑 全市场等权 / 仅EP / 仅质量 /
   EP+质量(top30)，与旧池结果并排。
2. **PIT 真伪**：核查 ``income`` / ``balance`` 是否存在同一报告期的多版本快照；
   再做 30 / 90 天财务信息延迟的稳健性重跑（EP/BP 按价格比回推到当期，
   ROE 沿用延迟日值，质量两件套按 ann_date+lag 取 asof）。
3. **行业+市值中性化**：申万一级 + 对数流通市值 5 分组，在「同行业且同市值组」
   单元格内重排 EP / 质量分（单元格 <5 只时退化为行业内、再退化为市值组内），
   与未中性化版本并排。

口径与 ``cross_section_score.py`` 完全一致（月频、T+1 开盘、0.13% 往返、
剔 ST 与上市不足 60 日、后复权、全 A）；脚本复用其 ``month_scores`` /
``pick`` / ``run_backtest`` / ``stats``，不重写打分与引擎。

产物：``work/alpha真实性核查_2026-11-21/``
    compare_final.csv  neutralized.csv  pit_delay.csv
    alpha真实性核查_2026-11-21.md  run.log
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import baseline_pv_val_equal_weight as base
import cross_section_score as css
import factor_lowvol_quantile as f
import factor_lowvol_variants as v
from src.core.usecases.run_backtest import _load_local_dividends, _load_st_status

OUT_DIR = ROOT / 'work' / 'alpha真实性核查_2026-11-21'
RUN_DIR = ROOT / 'data/local_store/backtest_runs/alpha_auth_20261121'
STORE = ROOT / 'data/local_store'
VAL = STORE / 'meta/financials/valuation_daily.parquet'
MONTHS_CACHE: List[date] = []


# ---------------------------------------------------------------------------
# 数据装载
# ---------------------------------------------------------------------------

def load_universe_with_delisted() -> Tuple[Dict[str, pl.DataFrame], Dict[str, Any]]:
    """补样池：绕过 DataFetcher 的退市剔除，其余管线与基准完全一致。"""
    import src.data.managers.data_fetcher as dfmod
    orig = dfmod._delisted_codes

    sb = pl.read_parquet(STORE / 'meta/stock_basic.parquet',
                         columns=['ts_code', 'status'])
    delisted = set(sb.filter(pl.col('status').is_not_null() & (pl.col('status') != 'L')
                             )['ts_code'].to_list())
    codes_all = {Path(p).stem for p in
                 glob.glob(str(STORE / 'quotes/stock/**/*.parquet'), recursive=True)}
    miss = sorted(c for c in delisted if c not in codes_all)
    dfmod._delisted_codes = lambda codes: []          # 关键：不剔退市
    try:
        data = base.load_universe(css.WARMUP_START, css.END)
    finally:
        dfmod._delisted_codes = orig
    data, violations = base.drop_contract_violations(data)
    diag = {
        'delisted_in_basic': len(delisted),
        'delisted_with_local_quote': sum(c in codes_all for c in delisted),
        'delisted_without_local_quote': len(miss),
        'delisted_without_quote_sample': miss[:10],
        'delisted_loaded': sum(c in data for c in delisted),
        'delisted_dropped_by_contract': sum(v['code'] in delisted for v in violations),
        'violations_total': len(violations),
        'codes_loaded': len(data),
    }
    return data, diag


def load_panel_lag(months: List[date], lag_days: int,
                   data: Dict[str, pl.DataFrame]
                   ) -> Dict[date, Dict[str, Tuple[float, float, float]]]:
    """信号日 m 使用 m-lag_days 时点可得的财报因子，价格回推到 m。

    ep = EPS/P, bp = BPS/P，EPS/BPS 在 m0（财报时点）已固定，
    故 ep_m = ep_m0 * (P_m0 / P_m)。roe_ttm 与价格无关，直接沿用 m0 值。
    """
    lo = (np.datetime64(months[0]) - np.timedelta64(lag_days + 400, 'D')).astype('datetime64[D]')
    df = pl.scan_parquet(css.PANEL).filter(
        (pl.col('signal_date') >= pl.lit(date.fromisoformat(str(lo))))
        & (pl.col('signal_date') <= pl.lit(months[-1]))
    ).select(['ts_code', 'signal_date', 'ep', 'bp', 'roe_ttm']).collect()
    df = df.sort(['ts_code', 'signal_date'])
    out: Dict[date, Dict[str, Tuple[float, float, float]]] = {d: {} for d in months}

    by_code: Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
    codes = df['ts_code'].to_numpy()
    days = np.array(df['signal_date'].to_list(), dtype='datetime64[D]')
    ep = df['ep'].to_numpy().astype('float64')
    bp = df['bp'].to_numpy().astype('float64')
    roe = df['roe_ttm'].to_numpy().astype('float64')
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    for a, b in zip(starts, ends):
        by_code[str(codes[a])] = (days[a:b], ep[a:b], bp[a:b], roe[a:b])

    px: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for c in by_code:
        d = data.get(c)
        if d is None:
            continue
        px[c] = (d['date'].to_numpy(), d['close'].to_numpy().astype('float64'))

    lag = np.timedelta64(lag_days, 'D')
    for m in months:
        target = np.datetime64(m) - lag
        for code, (d_arr, e_arr, b_arr, r_arr) in by_code.items():
            j = int(np.searchsorted(d_arr, target, side='right')) - 1
            if j < 0:
                continue
            if lag_days == 0:
                hit = int(np.searchsorted(d_arr, np.datetime64(m)))
                if hit >= d_arr.size or d_arr[hit] != np.datetime64(m):
                    continue
                j = hit
            p = px.get(code)
            ratio = 1.0
            if p is not None:
                dpx, cpx = p
                i0 = int(np.searchsorted(dpx, d_arr[j], side='right')) - 1
                i1 = int(np.searchsorted(dpx, np.datetime64(m), side='right')) - 1
                # 必须有 m 当日行情，否则该票当日不可交易/不可入选
                if (i1 < 0 or i1 >= dpx.size or dpx[i1] != np.datetime64(m)
                        or i0 < 0 or cpx[i1] <= 0 or cpx[i0] <= 0):
                    continue
                ratio = cpx[i0] / cpx[i1]
            out[m][code] = tuple(
                None if x is None or not np.isfinite(x) else float(x)
                for x in (e_arr[j] * ratio, b_arr[j] * ratio, r_arr[j]))
    return out


def load_quality_lag(months: List[date], lag_days: int
                     ) -> Dict[date, Dict[str, Tuple[float, float]]]:
    """与 ``cross_section_score.load_quality`` 同逻辑，ann_date 统一后移 lag_days。"""
    fin = css.FIN
    bal = pl.read_parquet(fin / 'balance.parquet',
                          columns=['ts_code', 'ann_date', 'debt_ratio', 'total_assets'])
    bal = bal.filter(pl.col('total_assets') > 0)
    if lag_days:
        bal = bal.with_columns((pl.col('ann_date') + pl.duration(days=lag_days)).alias('ann_date'))
    debt = css._asof_by_code(bal, 'ann_date', 'debt_ratio')

    inc = pl.read_parquet(fin / 'income.parquet',
                          columns=['ts_code', 'end_date', 'ann_date', 'revenue', 'gross_profit'])
    inc = inc.filter((pl.col('revenue') > 0) & pl.col('gross_profit').is_not_null())
    inc = (inc.with_columns((pl.col('gross_profit') / pl.col('revenue')).alias('gm'))
           .sort(['ts_code', 'end_date'])
           .with_columns(pl.col('gm').rolling_std(window_size=8, min_samples=6)
                         .over('ts_code').alias('gm_std')))
    if lag_days:
        inc = inc.with_columns((pl.col('ann_date') + pl.duration(days=lag_days)).alias('ann_date'))
    gmstd = css._asof_by_code(inc, 'ann_date', 'gm_std')

    me = np.array(months, dtype='datetime64[D]')
    empty = (np.array([], dtype='datetime64[D]'), np.array([], dtype='float64'))
    out: Dict[date, Dict[str, Tuple[float, float]]] = {d: {} for d in months}
    for code in set(debt) | set(gmstd):
        d_days, d_val = debt.get(code, empty)
        g_days, g_val = gmstd.get(code, empty)
        pd_ = np.searchsorted(d_days, me, side='right') - 1
        pg = np.searchsorted(g_days, me, side='right') - 1
        for j, day in enumerate(months):
            if pd_[j] >= 0 or pg[j] >= 0:
                out[day][code] = (float(d_val[pd_[j]]) if pd_[j] >= 0 else None,
                                  float(g_val[pg[j]]) if pg[j] >= 0 else None)
    return out


def ep_only_signals(months: List[date], univ: Dict[date, List[str]],
                    panel: Dict[date, Dict[str, Tuple[float, float, float]]]
                    ) -> Dict[date, Dict[str, Dict[str, float]]]:
    """纯 EP 单因子（不含 BP），用于「仅 EP」这一组。"""
    out = {}
    for m in months:
        p = panel.get(m, {})
        ep = {c: p[c][0] for c in univ[m]
              if c in p and p[c][0] is not None and p[c][0] > 0}
        out[m] = {c: {'ep': s} for c, s in css.pct_rank(ep).items()}
    return out


def load_industry(months: List[date]) -> Dict[date, Dict[str, str]]:
    df = pl.scan_parquet(css.PANEL).filter(
        pl.col('signal_date').is_in(months)
    ).select(['ts_code', 'signal_date', 'industry_l1']).collect()
    out: Dict[date, Dict[str, str]] = {d: {} for d in months}
    for code, day, ind in zip(df['ts_code'].to_list(), df['signal_date'].to_list(),
                              df['industry_l1'].to_list()):
        if ind:
            out[day][code] = ind
    return out


def load_circ_mv(months: List[date]) -> Dict[date, Dict[str, float]]:
    """信号日收盘流通市值（valuation_daily 当日已有行情，故基本不缺）。"""
    df = pl.scan_parquet(VAL).filter(pl.col('trade_date').is_in(months)).select(
        ['ts_code', 'trade_date', 'circ_mv', 'total_mv']).collect()
    out: Dict[date, Dict[str, float]] = {d: {} for d in months}
    for code, day, cmv, tmv in zip(df['ts_code'].to_list(), df['trade_date'].to_list(),
                                   df['circ_mv'].to_list(), df['total_mv'].to_list()):
        mv = cmv if cmv and cmv > 0 else tmv
        if mv and mv > 0:
            out[day][code] = float(mv)
    return out


# ---------------------------------------------------------------------------
# 中性化打分
# ---------------------------------------------------------------------------

def neutralize(day: date, scores: Dict[str, Dict[str, float]], dims: List[str],
               ind: Dict[str, str], mv: Dict[str, float], nsize: int = 5
               ) -> Dict[str, Dict[str, float]]:
    """在「同行业 + 同市值组」单元格内重排各维度分数（单调变换不改变单元内次序）。"""
    codes = [c for c in scores if c in mv]
    if len(codes) < nsize * 3:
        return scores
    logmv = np.log(np.array([mv[c] for c in codes]))
    qs = np.quantile(logmv, np.linspace(0, 1, nsize + 1)[1:-1])
    grp = {c: int(np.searchsorted(qs, np.log(mv[c]))) for c in codes}

    out: Dict[str, Dict[str, float]] = {c: dict(scores[c]) for c in codes}
    for dim in dims:
        assigned: set = set()
        for level in ('cell', 'ind', 'all'):
            buckets: Dict[Any, Dict[str, float]] = {}
            for c in codes:
                if c in assigned:
                    continue
                s = scores[c].get(dim)
                if s is None:
                    continue
                i = ind.get(c)
                key = ((i, grp[c]) if level == 'cell' else i) if level != 'all' else 'all'
                buckets.setdefault(key, {})[c] = s
            for key, bucket in buckets.items():
                if level != 'all' and len(bucket) < 5:
                    continue      # 单元太小，留待下一级口径
                for c, r in css.pct_rank(bucket).items():
                    if c in out:
                        out[c][dim] = r
                    assigned.add(c)
    return out


# ---------------------------------------------------------------------------
# 回测 / 指标
# ---------------------------------------------------------------------------

def evaluate(name: str, path: Dict[date, List[str]], data, dividends, st_status,
             prev_day, bench_curve: List[Tuple[date, float]] | None = None) -> Dict[str, Any]:
    cached = _load_cache().get(name)
    if cached is not None:
        logger.info('{} 命中缓存，跳过回测', name)
        return cached
    sched = v.schedules_from_path(path, prev_day)
    res = css.run_backtest(data, sched, dividends, st_status)
    curve = f.curve_of(res)
    trades = res.get('trades') or []
    row: Dict[str, Any] = {'run': name}
    for tag, (a, b) in css.SEGS.items():
        hold = [len(vv) for m, vv in path.items() if a <= m <= b]
        for k, val in css.stats(curve, trades, a, b, hold).items():
            row[f'{tag}_{k}' if tag != 'full' else k] = val
    row['yearly'] = yearly_returns(curve)
    row['underwater'] = longest_underwater_months(curve)
    if bench_curve:
        row['excess_monthly'], row['excess_hit'] = monthly_excess(curve, bench_curve)
    out = RUN_DIR / name
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': [d for d, _ in curve],
                  'equity': [e for _, e in curve]}).write_parquet(out / 'equity.parquet')
    if trades:
        pl.DataFrame(trades, infer_schema_length=None).write_parquet(out / 'trades.parquet')
    logger.info('{} => {}', name, json.dumps(
        {k: row.get(k) for k in ('annual', 'max_drawdown', 'sharpe', 'seg1_annual',
                                 'seg2_annual', 'seg3_annual', 'excess_monthly')},
        ensure_ascii=False))
    _save_cache(name, row)
    del res, curve, trades
    return row


def _rows_to_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """嵌套字段（yearly / underwater）压平成 JSON 字符串后落 CSV。"""
    flat = []
    for r in rows:
        d = dict(r)
        for k in ('yearly', 'underwater'):
            if isinstance(d.get(k), dict):
                d[k] = json.dumps(d[k], ensure_ascii=False)
        flat.append(d)
    pl.DataFrame(flat, infer_schema_length=None).write_csv(path)


CACHE_PATH = OUT_DIR / 'runs_cache.json'


def _load_cache() -> Dict[str, Any]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def _save_cache(name: str, row: Dict[str, Any]) -> None:
    cache = _load_cache()
    cache[name] = row
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, default=str), encoding='utf-8')


def _month_end_equity(curve: List[Tuple[date, float]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for d, e in curve:
        out[f'{d.year}-{d.month:02d}'] = e
    return out


def yearly_returns(curve: List[Tuple[date, float]]) -> Dict[str, float]:
    me = _month_end_equity(curve)
    out: Dict[str, float] = {}
    for y in sorted({int(k[:4]) for k in me}):
        ks = sorted(k for k in me if k.startswith(str(y)))
        base_eq = None
        prev_year = [k for k in me if int(k[:4]) == y - 1]
        if prev_year:
            base_eq = me[max(prev_year)]
        elif ks:
            base_eq = me[ks[0]]
        if base_eq and base_eq > 0:
            out[str(y)] = round(me[ks[-1]] / base_eq - 1.0, 4)
    return out


def longest_underwater_months(curve: List[Tuple[date, float]]) -> Dict[str, Any]:
    """最长水下持续期（按月末净值计，单位：月）。"""
    me = _month_end_equity(curve)
    ks = sorted(me)
    peak, peak_k, worst, cur, start = -1e18, None, 0, 0, None
    worst_span = None
    for k in ks:
        eq = me[k]
        if eq >= peak:
            if cur > worst:
                worst, worst_span = cur, (start, peak_k, k)
            peak, peak_k, cur, start = eq, k, 0, None
        else:
            if cur == 0:
                start = peak_k
            cur += 1
    if cur > worst:
        worst, worst_span = cur, (start, peak_k, None)
    return {'months': worst, 'peak': worst_span[1] if worst_span else None,
            'end': worst_span[2] if worst_span else None}


def monthly_excess(curve: List[Tuple[date, float]],
                   bench: List[Tuple[date, float]]) -> Tuple[float, float]:
    a, b = _month_end_equity(curve), _month_end_equity(bench)
    ks = sorted(set(a) & set(b))
    ex = []
    for i in range(1, len(ks)):
        ra = a[ks[i]] / a[ks[i - 1]] - 1
        rb = b[ks[i]] / b[ks[i - 1]] - 1
        ex.append(ra - rb)
    if not ex:
        return None, None
    return round(float(np.mean(ex) * 12), 4), round(float(np.mean([x > 0 for x in ex])), 4)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--skip-neu', action='store_true')
    ap.add_argument('--skip-pit', action='store_true')
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level='INFO')
    logger.add(OUT_DIR / 'run.log', level='INFO', encoding='utf-8')

    t0 = time.perf_counter()
    data, diag = load_universe_with_delisted()
    logger.info('补样池诊断：{}', json.dumps(diag, ensure_ascii=False))

    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if css.SIGNAL_START <= m <= date.fromisoformat(css.END)]
    logger.info('信号月 {} 个 {}~{}', len(months), months[0], months[-1])

    quoted = v.quoted_sets(data, months)
    mom, bars = css.load_momentum(data, months)
    panel = css.load_panel_factors(months)
    quality = css.load_quality(months)
    st_sets = css.load_st_sets(months)

    univ = {m: [c for c in quoted[m]
                if bars[m].get(c, 0) >= css.MIN_BARS and c not in st_sets[m]]
            for m in months}
    signals = {m: css.month_scores(m, univ[m], panel, quality, mom) for m in months}
    logger.info('补样池打分完成，耗时 {:.1f}s', time.perf_counter() - t0)

    dividends = _load_local_dividends(list(data), None, None)
    st_status = _load_st_status(None, data, None, css.WARMUP_START, css.END, None, None)

    rows: List[Dict[str, Any]] = []
    bench = {m: sorted(univ[m]) for m in months}
    brow = evaluate('bench_sup_ew', bench, data, dividends, st_status, prev_day)
    brow.update({'group': '全市场等权', 'pool': '补样', 'dims': ''})
    rows.append(brow)
    bench_curve = [(d, e) for d, e in _curve_of_run('bench_sup_ew')]

    sig_ep = ep_only_signals(months, univ, panel)
    specs = [
        ('value_n30_sup', '仅估值(EP+BP)', ['value'], 'value', signals),
        ('ep_only_n30_sup', '仅EP(纯EP单因子)', ['ep'], 'ep', sig_ep),
        ('quality_n30_sup', '仅质量', ['quality'], 'quality', signals),
        ('combo_vq_n30_sup', 'EP+质量', ['value', 'quality'], 'value+quality', signals),
    ]
    for name, label, dims, dimkey, sigs in specs:
        path = css.build_paths(months, sigs, dims, 30)
        r = evaluate(name, path, data, dividends, st_status, prev_day, bench_curve)
        r.update({'group': label, 'pool': '补样', 'dims': dimkey})
        rows.append(r)

    # ---------------- 检查二：PIT 延迟 ----------------
    pit_rows = []
    if not args.skip_pit:
        for lag in (0, 30, 90):
            p_lag = load_panel_lag(months, lag, data)
            q_lag = load_quality_lag(months, lag)
            sig_lag = {m: css.month_scores(m, univ[m], p_lag, q_lag, mom) for m in months}
            path = css.build_paths(months, sig_lag, ['value', 'quality'], 30)
            r = evaluate(f'combo_vq_n30_pit{lag}', path, data, dividends,
                         st_status, prev_day, bench_curve)
            r.update({'lag_days': lag, 'group': 'EP+质量'})
            pit_rows.append(r)
            logger.info('PIT lag={} 完成 {:.0f}s', lag, time.perf_counter() - t0)
    if pit_rows:
        _rows_to_csv(pit_rows, OUT_DIR / 'pit_delay.csv')

    # ---------------- 检查三：行业 + 市值中性化 ----------------
    neu_rows = []
    if not args.skip_neu:
        ind = load_industry(months)
        mv = load_circ_mv(months)
        sig_neu = {m: neutralize(m, signals[m], ['value', 'quality'],
                                 ind[m], mv[m]) for m in months}
        for name, label, dims in (
                ('neu_value_n30', '仅EP(中性化)', ['value']),
                ('neu_quality_n30', '仅质量(中性化)', ['quality']),
                ('neu_combo_vq_n30', 'EP+质量(中性化)', ['value', 'quality'])):
            path = css.build_paths(months, sig_neu, dims, 30)
            r = evaluate(name, path, data, dividends, st_status, prev_day, bench_curve)
            r.update({'group': label, 'variant': 'neutralized'})
            neu_rows.append(r)
            logger.info('中性化 {} 完成 {:.0f}s', name, time.perf_counter() - t0)
    if neu_rows:
        # 叠加最严口径：财务延迟 90 天 + 行业/市值中性化
        p_lag = load_panel_lag(months, 90, data)
        q_lag = load_quality_lag(months, 90)
        sig_strict = {m: css.month_scores(m, univ[m], p_lag, q_lag, mom) for m in months}
        neu_strict = {m: neutralize(m, sig_strict[m], ['value', 'quality'],
                                    ind[m], mv[m]) for m in months}
        path = css.build_paths(months, neu_strict, ['value', 'quality'], 30)
        r = evaluate('strict_combo_n30', path, data, dividends, st_status,
                     prev_day, bench_curve)
        r.update({'group': 'EP+质量(中性化+PIT90)', 'variant': 'neutralized'})
        neu_rows.append(r)
        _rows_to_csv(neu_rows, OUT_DIR / 'neutralized.csv')

    # ---------------- 对照表 ----------------
    old = pl.read_csv(css.OUT_DIR / 'compare.csv', infer_schema_length=None)
    old_map = {r['run']: r for r in old.to_dicts()}
    att = pl.read_csv(css.OUT_DIR / 'attribution.csv', infer_schema_length=None)
    old_map.update({r['run']: r for r in att.to_dicts()})
    keep = ['annual', 'max_drawdown', 'vol', 'sharpe', 'calmar', 'win_rate_monthly',
            'turnover_2way', 'avg_holdings', 'seg1_annual', 'seg2_annual', 'seg3_annual']
    cmp_rows = []
    for r in rows:
        d = {'group': r['group'], 'pool': r['pool'], 'run': r['run']}
        d.update({k: r.get(k) for k in keep})
        d['yearly'] = json.dumps(r.get('yearly') or {}, ensure_ascii=False)
        d['excess_vs_bench_monthly'] = r.get('excess_monthly')
        d['excess_hit'] = r.get('excess_hit')
        d['underwater_months'] = (r.get('underwater') or {}).get('months')
        cmp_rows.append(d)
    old_pairs = [('benchmark_all_a_ew', '全市场等权'), ('value_monthly_n30', '仅EP'),
                 ('quality_monthly_n30', '仅质量'), ('combo_no_momentum_monthly_n30', 'EP+质量')]
    for run, label in old_pairs:
        o = old_map.get(run)
        if o is None:
            continue
        d = {'group': label, 'pool': '旧池', 'run': run}
        d.update({k: o.get(k) for k in keep})
        d['yearly'] = ''
        d['excess_vs_bench_monthly'] = None
        d['excess_hit'] = None
        d['underwater_months'] = None
        cmp_rows.append(d)
    pl.DataFrame(cmp_rows, infer_schema_length=None).write_csv(OUT_DIR / 'compare_final.csv')
    logger.info('总耗时 {:.1f}s', time.perf_counter() - t0)
    print(pl.DataFrame(cmp_rows, infer_schema_length=None).select(
        ['group', 'pool', 'annual', 'max_drawdown', 'sharpe',
         'seg1_annual', 'seg2_annual', 'seg3_annual']))
    return 0


def _curve_of_run(name: str) -> List[Tuple[date, float]]:
    d = pl.read_parquet(RUN_DIR / name / 'equity.parquet')
    return list(zip(d['date'].to_list(), d['equity'].to_list()))


if __name__ == '__main__':
    raise SystemExit(main())
