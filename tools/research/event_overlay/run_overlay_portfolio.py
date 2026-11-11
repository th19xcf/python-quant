#!/usr/bin/env python
"""ev_equity_incentive 事件 overlay 的合成链路增量实测。

只读 _staging/event_factor_increment_20261111/panel_ev.parquet（26 因子残差 +
S0 + 事件脉冲 + fwd_20_oo 标签），输出只写 _staging/event_overlay_20261111/。
不改 src/、不改 tools/research/l1_baseline/、不写 data/。

机制
----
score = S0 + λ·σ_t(S0)·1{ev_equity_incentive_t20 > 0}
按 score 取 top-N 等权持有 20 个交易日（非重叠调仓），λ 只在 IS(≤2021-12-31)
内选优，OOS(≥2022-01-01) 只用于验收。

组合口径
--------
- 调仓日：panel_ev 的 144 个非重叠日期（h=20 等距），末期 2026-10-15 无标签剔除。
- 收益：fwd_20_oo（T+1 开盘 -> T+21 开盘）。
- 成本：src.backtest.costs.cost_model.CostModel，按 trade_date 取历史印花税/过户费，
  按 exp(ln_mv)/1e8（亿元）取分档滑点；买入侧按新权重增量计费、卖出侧按减量计费。
- 换手：单边 = 0.5·Σ|w_t − w_{t−1}|（等权下等于 |S_{t−1}\\S_t| / |S_{t−1}|）。
- 年化：period=20 交易日，ppy = 252/20 = 12.6，几何复利。

用法
----
    .venv\\Scripts\\python.exe tools\\research\\event_overlay\\run_overlay_portfolio.py
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.backtest.costs.cost_model import CostModel  # noqa: E402

PANEL = ROOT / '_staging/event_factor_increment_20261111/panel_ev.parquet'
SEL = ROOT / 'reports/l1_baseline/selection_h20.json'
EVAL = ROOT / 'reports/l1_baseline/eval_results.json'
OUT = ROOT / '_staging/event_overlay_20261111'

FWD = 'fwd_20_oo'
IS_END = date(2021, 12, 31)
PPY = 252.0 / 20.0          # 12.6 期/年
N_PRIMARY = 100
N_LIST = (50, 100, 200)
#: λ 以「S0 当期截面标准差」为单位，0.10 表示触发股加 0.1σ
LAMBDA_GRID = (0.05, 0.10, 0.15, 0.20, 0.30, 0.50, 0.75, 1.00, 1.50)
#: 组合内权重倾斜（零换手对照），w_i ∝ 1 + λ·触发
TILT_GRID = (0.5, 1.0, 2.0)
MIN_NAMES = 100


def log(m: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {m}', flush=True)


# ---------------------------------------------------------------------------
# 数据
# ---------------------------------------------------------------------------
def load_panel() -> tuple[pl.DataFrame, list[str], dict]:
    sel = json.loads(SEL.read_text(encoding='utf-8'))['kept']
    signs = json.loads(EVAL.read_text(encoding='utf-8'))['signs_h20_oo']
    df = pl.read_parquet(PANEL)
    rec = pl.mean_horizontal([pl.col(c) * signs[c] for c in sel]).alias('_s0_rec')
    chk = df.select(['S0', rec, 'S0__n', pl.sum_horizontal(
        [pl.col(c).is_not_null().cast(pl.Int8) for c in sel]).alias('_n_rec')])
    diff = (chk['S0'] - chk['_s0_rec']).abs().max()
    nmis = int((chk['S0__n'] != chk['_n_rec']).sum())
    assert diff is not None and diff < 1e-12, f'S0 与 26 因子合成不一致: {diff}'
    assert nmis == 0, f'可用因子数不一致: {nmis} 行'

    df = df.select(['date', 'ts_code', 'industry_l1', 'ln_mv', FWD, 'S0',
                    'ev_equity_incentive_t20', 'ev_equity_incentive_t60'])
    df = df.filter(pl.col('S0').is_not_null())
    df = df.with_columns(
        pl.col('ln_mv').exp().truediv(1e8).alias('mv_yi'),
        (pl.col('ev_equity_incentive_t20') > 0).cast(pl.Int8).alias('trig20'),
        (pl.col('ev_equity_incentive_t60') > 0).cast(pl.Int8).alias('trig60'),
    )
    meta = {
        's0_reconstruct_max_abs_diff': float(diff),
        's0_factor_count_mismatch_rows': nmis,
        's0_of_26_factors': sel,
        'signs_h20_oo': signs,
        'is_end': str(IS_END), 'ppy': PPY,
    }
    return df, sel, meta


def attach_rates(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """费率只取决于 (交易日, 市值档)，按 144 期 × 3 档调用 CostModel，再按行贴回。

    与逐行调用等价（滑点只分档、佣金/印花税/过户费只随日期变），但避免 52 万次
    Python 调用。档位边界与 CostModel.STOCK_SLIPPAGE_TIERS_YI 一致（500/100 亿）。
    """
    cm = CostModel()
    dates = df['date'].unique().sort().to_list()
    tiers = [(500.0, 15.0), (100.0, 20.0), (0.0, 30.0)]  # (下限亿元, 代表市值)
    bmap: dict[date, list[float]] = {}
    smap: dict[date, list[float]] = {}
    for d in dates:
        bmap[d], smap[d] = [], []
        for _, mv_rep in tiers:
            bmap[d].append(cm.get_buy_cost_rate(d, '000001.SZ', total_mv_yi=mv_rep))
            smap[d].append(cm.get_sell_cost_rate(d, '000001.SZ', total_mv_yi=mv_rep))
    mv = df['mv_yi'].to_numpy()
    tier = np.where(mv >= 500.0, 0, np.where(mv >= 100.0, 1, 2))
    buy = np.array([bmap[d][t] for d, t in zip(df['date'].to_list(), tier, strict=True)])
    sell = np.array([smap[d][t] for d, t in zip(df['date'].to_list(), tier, strict=True)])
    out = df.with_columns(pl.Series('rate_buy', buy), pl.Series('rate_sell', sell))
    meta = {
        'source': 'src.backtest.costs.cost_model.CostModel',
        'buy_rate_mean_bp': float(buy.mean() * 1e4),
        'sell_rate_mean_bp': float(sell.mean() * 1e4),
        'round_trip_mean_bp': float((buy.mean() + sell.mean()) * 1e4),
        'mv_unit': 'exp(ln_mv)/1e8 = 亿元；滑点档 500/100 亿（15/20/30bp 单边）',
    }
    return out, meta


# ---------------------------------------------------------------------------
# 组合构建
# ---------------------------------------------------------------------------
def basket_weights(df: pl.DataFrame, score: str, n: int,
                   tilt: float | None = None,
                   trig_col: str = 'trig20') -> dict[date, tuple[dict, set]]:
    """逐期取 top-N，返回 {date: ({code: weight}, 触发集合)}。"""
    d = df.select(['date', 'ts_code', score, trig_col]).filter(
        pl.col(score).is_not_null())
    d = d.sort(['date', score, 'ts_code'], descending=[False, True, False])
    d = d.group_by('date', maintain_order=True).head(n)
    out: dict[date, tuple[dict, set]] = {}
    for dd, g in d.group_by('date', maintain_order=True):
        d0 = dd[0] if isinstance(dd, tuple) else dd
        codes = g['ts_code'].to_list()
        trig = g[trig_col].to_numpy().astype(bool)
        if tilt is None:
            w = np.full(len(codes), 1.0 / len(codes))
        else:
            raw = 1.0 + tilt * trig.astype(float)
            w = raw / raw.sum()
        out[d0] = (dict(zip(codes, w.tolist(), strict=True)),
                   {c for c, t in zip(codes, trig, strict=True) if t})
    return out


def period_series(df: pl.DataFrame, baskets: dict[date, tuple[dict, set]],
                  label: str) -> pl.DataFrame:
    """按调仓序列算每期 gross/cost/net/turnover，及全池等权基准。"""
    fwd = dict(zip(df['ts_code'] + '|' + df['date'].cast(pl.Utf8),
                   df[FWD].to_list(), strict=True))
    rate_b = dict(zip(df['ts_code'] + '|' + df['date'].cast(pl.Utf8),
                      df['rate_buy'].to_list(), strict=True))
    rate_s = dict(zip(df['ts_code'] + '|' + df['date'].cast(pl.Utf8),
                      df['rate_sell'].to_list(), strict=True))
    uni = {r['date']: r['m'] for r in df.filter(pl.col(FWD).is_not_null())
           .group_by('date').agg(pl.col(FWD).mean().alias('m')).iter_rows(named=True)}
    # 卖出侧兜底费率：持仓股在当期已从面板消失（退市/长期停牌）时没有当日行，
    # 按当日最高档（30bp 滑点）计费，不静默按 0 成本卖出。
    fb_sell = {r['date']: r['mx'] for r in df.group_by('date').agg(
        pl.col('rate_sell').max().alias('mx')).iter_rows(named=True)}
    last_sell: dict[str, float] = {}

    dates = sorted(d for d, (w, _) in baskets.items() if w)
    prev: dict[str, float] = {}
    rows = []
    for d in dates:
        w, trig = baskets[d]
        dkey = d.isoformat()
        rets, wsum, tc = 0.0, 0.0, 0.0
        ud = uni.get(d)
        for c, wt in w.items():
            r = fwd.get(f'{c}|{dkey}')
            if r is not None and np.isfinite(r):
                rets += wt * r
                wsum += wt
                if c in trig and ud is not None:
                    tc += wt * (r - ud)
        if wsum < 0.5:
            prev = w
            continue
        gross = rets / wsum
        trig_contrib = tc / wsum
        buy = sell = turn = 0.0
        for c in set(prev) | set(w):
            old, new = prev.get(c, 0.0), w.get(c, 0.0)
            dw = new - old
            turn += abs(dw)
            if dw > 0:
                buy += dw * rate_b[f'{c}|{dkey}']
            elif dw < 0:
                rs = rate_s.get(f'{c}|{dkey}', last_sell.get(c, fb_sell[d]))
                sell += (-dw) * rs
        for c in w:
            last_sell[c] = rate_s[f'{c}|{dkey}']
        cost = buy + sell
        rows.append({
            'variant': label, 'date': d, 'n_hold': len(w),
            'n_trig_in_basket': len(trig), 'coverage': wsum,
            'trig_share': (sum(w[c] for c in w if c in trig)),
            'trig_contrib': trig_contrib,
            'gross': gross, 'cost': cost, 'net': gross - cost,
            'turnover_1side': turn / 2.0,
            'universe': uni.get(d),
        })
        prev = w
    return pl.DataFrame(rows)


def metrics(ser: pl.DataFrame, scope: str, is_end: date = IS_END) -> dict:
    s = ser.sort('date')
    if scope == 'IS':
        s = s.filter(pl.col('date') <= is_end)
    elif scope == 'OOS':
        s = s.filter(pl.col('date') > is_end)
    r = s['net'].to_numpy()
    g = s['gross'].to_numpy()
    n = len(r)
    if n < 2:
        return {'scope': scope, 'n_periods': n}

    def ann(x: np.ndarray) -> float:
        return float(np.prod(1.0 + x) ** (PPY / len(x)) - 1.0)

    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    mdd = float((eq / peak - 1.0).min())
    u = s['universe'].fill_null(strategy='forward').fill_null(float(g.mean())).to_numpy()
    tcg = s['trig_contrib'].to_numpy()
    tc_ann = float(tcg.mean() * 1e4 * PPY)
    tc_se = (float(tcg.std(ddof=1) / np.sqrt(n) * 1e4 * PPY)
             if n > 2 and tcg.std(ddof=1) > 0 else None)
    return {
        'scope': scope, 'n_periods': n,
        'ann_net': ann(r), 'ann_gross': ann(g),
        'mean_net_bp': float(r.mean() * 1e4), 'mean_gross_bp': float(g.mean() * 1e4),
        'cost_bp_per_rebal': float(s['cost'].mean() * 1e4),
        'cost_ann_bp': float(s['cost'].mean() * 1e4 * PPY),
        'turnover_1side': float(s['turnover_1side'].mean()),
        'trig_share_mean': float(s['trig_share'].mean()),
        'trig_contrib_ann_bp': tc_ann,
        'trig_contrib_se_ann_bp': tc_se,
        'trig_contrib_t': (tc_ann / tc_se if tc_se else None),
        'sharpe': float(r.mean() / r.std(ddof=1) * np.sqrt(PPY))
        if r.std(ddof=1) > 0 else None,
        'mdd': mdd,
        'excess_vs_universe_ann': ann(r) - ann(u),
        'mean_excess_bp': float((g - u).mean() * 1e4),
        'n_trig_mean': float(s['n_trig_in_basket'].mean()),
    }


def diff_stats(a: pl.DataFrame, b: pl.DataFrame, scope: str,
               is_end: date = IS_END) -> dict:
    """a=overlay 与 b=base 的逐期净收益差（配对）。"""
    j = (a.select(['date', pl.col('net').alias('a')])
         .join(b.select(['date', pl.col('net').alias('b')]), on='date', how='inner')
         .sort('date'))
    if scope == 'IS':
        j = j.filter(pl.col('date') <= is_end)
    elif scope == 'OOS':
        j = j.filter(pl.col('date') > is_end)
    d = (j['a'] - j['b']).to_numpy()
    n = len(d)
    if n < 3:
        return {'scope': scope, 'n': n}
    mu, sd = float(d.mean()), float(d.std(ddof=1))
    se = sd / np.sqrt(n)
    return {
        'scope': scope, 'n': n,
        'mean_bp_per_rebal': mu * 1e4,
        'ann_bp': mu * 1e4 * PPY,
        'se_ann_bp': se * 1e4 * PPY,
        't': mu / se if se > 0 else None,
        'win_rate': float((d > 0).mean()),
        'ann_increment_vs_1se': (mu * PPY) / (se * PPY) if se > 0 else None,
    }


# ---------------------------------------------------------------------------
# overlay 单独成组：触发股 vs 同日×同行业×同市值 5 分位对照组
# ---------------------------------------------------------------------------
def standalone(df: pl.DataFrame, trig_col: str) -> dict:
    d = df.select(['date', 'ts_code', 'industry_l1', 'ln_mv', FWD, trig_col])
    d = d.filter(pl.col(FWD).is_not_null())
    d = d.with_columns(
        ((pl.col('ln_mv').rank().over('date') * 5) // (pl.len().over('date') + 1))
        .clip(0, 4).alias('sb'))
    base = d.group_by(['date', 'industry_l1', 'sb']).agg(
        pl.len().alias('cnt'), pl.col(FWD).sum().alias('sf'),
        pl.col(FWD).is_not_null().sum().alias('nf'))
    trig = d.filter(pl.col(trig_col) == 1)
    agg = trig.group_by(['date', 'industry_l1', 'sb']).agg(
        pl.len().alias('tn'), pl.col(FWD).sum().alias('tsf'),
        pl.col(FWD).is_not_null().sum().alias('tnf'))
    m = base.join(agg, on=['date', 'industry_l1', 'sb'], how='left').with_columns(
        pl.col('tn').fill_null(0), pl.col('tsf').fill_null(0.0),
        pl.col('tnf').fill_null(0))
    m = m.with_columns((pl.col('nf') - pl.col('tnf')).alias('cn'),
                       (pl.col('sf') - pl.col('tsf')).alias('cs'))
    m = m.with_columns((pl.col('cs') / pl.col('cn')).alias('ctrl'))
    good = m.filter(pl.col('cn') >= 10).select(['date', 'industry_l1', 'sb', 'ctrl'])
    t = trig.join(good, on=['date', 'industry_l1', 'sb'], how='inner')
    t = t.with_columns((pl.col(FWD) - pl.col('ctrl')).alias('abn'))
    per = t.group_by('date').agg(pl.len().alias('n_trig'),
                                 pl.col('abn').mean().alias('abn'),
                                 pl.col(FWD).mean().alias('raw')).sort('date')

    def st(scope: str) -> dict:
        s = per
        if scope == 'IS':
            s = s.filter(pl.col('date') <= IS_END)
        elif scope == 'OOS':
            s = s.filter(pl.col('date') > IS_END)
        x = s['abn'].to_numpy()
        n = len(x)
        mu, sd = float(x.mean()), float(x.std(ddof=1))
        return {'scope': scope, 'n_dates': n, 'n_triggers': int(s['n_trig'].sum()),
                'mean_abn_bp': mu * 1e4,
                't': mu / (sd / np.sqrt(n)) if sd > 0 else None,
                'win_rate': float((x > 0).mean()),
                'mean_raw_bp': float(s['raw'].mean() * 1e4)}

    return {k: st(k) for k in ('full', 'IS', 'OOS')}


# ---------------------------------------------------------------------------
def main() -> None:
    t0 = time.time()
    OUT.mkdir(parents=True, exist_ok=True)
    df, sel, meta = load_panel()
    df, cost_meta = attach_rates(df)
    meta['cost_model'] = cost_meta
    log(f'面板 {df.height:,} 行 / {df["date"].n_unique()} 期；费率均值 '
        f'买 {cost_meta["buy_rate_mean_bp"]:.2f}bp / '
        f'卖 {cost_meta["sell_rate_mean_bp"]:.2f}bp / '
        f'往返 {cost_meta["round_trip_mean_bp"]:.2f}bp')

    # 触发覆盖
    cov = []
    for tc in ('trig20', 'trig60'):
        s = df.group_by('date').agg(
            pl.len().alias('n'), pl.col(tc).sum().alias('k')).with_columns(
            (pl.col('k') / pl.col('n')).alias('share'))
        cov.append({'trigger': tc, 'mean_share': float(s['share'].mean()),
                    'p5': float(s['share'].quantile(0.05)),
                    'p95': float(s['share'].quantile(0.95)),
                    'mean_names': float(s['k'].mean())})
    log(f'触发占比: {[(c["trigger"], round(c["mean_share"], 4)) for c in cov]}')

    # 当期截面 σ(S0)
    df = df.with_columns(pl.col('S0').std(ddof=1).over('date').alias('s0_sd'))

    results: dict = {'meta': meta, 'coverage': cov}
    series: dict[str, pl.DataFrame] = {}

    # ---- 基线：top-N 按 S0 ----
    for n in N_LIST:
        b = basket_weights(df, 'S0', n)
        series[f'base_N{n}'] = period_series(df, b, f'base_N{n}')

    # ---- 机制 A：score 加分（IS 定 λ）----
    grid_rows = []
    for n in N_LIST:
        for lam in LAMBDA_GRID:
            col = f'score_lam{int(lam * 100)}'
            d2 = df.with_columns((pl.col('S0') + pl.col('s0_sd') * lam
                                  * pl.col('trig20')).alias(col))
            b = basket_weights(d2, col, n)
            ser = period_series(d2, b, f'A_N{n}_lam{lam}')
            series[f'A_N{n}_lam{lam}'] = ser
            mi = metrics(ser, 'IS')
            grid_rows.append({'N': n, 'lambda': lam, **{
                k: mi[k] for k in ('ann_net', 'cost_bp_per_rebal',
                                   'turnover_1side', 'sharpe', 'mdd',
                                   'trig_share_mean', 'n_trig_mean')}})
    grid = pl.DataFrame(grid_rows)
    grid.write_csv(OUT / 'lambda_grid_IS.csv')
    # IS 选优（净年化最大，同值取更小 λ）
    pick = (grid.filter(pl.col('N') == N_PRIMARY)
            .sort(['ann_net', 'lambda'], descending=[True, False]).row(0, named=True))
    lam_star = float(pick['lambda'])
    results['lambda_selection'] = {
        'criterion': 'IS 窗口组合净年化收益最大（同值取更小 λ）',
        'N': N_PRIMARY, 'lambda_star': lam_star,
        'at_grid_boundary': lam_star in (LAMBDA_GRID[0], LAMBDA_GRID[-1]),
        'is_ann_net_at_star': pick['ann_net'],
        'grid': grid.filter(pl.col('N') == N_PRIMARY).to_dicts(),
    }
    log(f'IS 选定 λ* = {lam_star}（IS 净年化 {pick["ann_net"]:.4%}）')

    # ---- 主对照表 ----
    main_rows = []
    for n in N_LIST:
        base_ser = series[f'base_N{n}']
        for lam in (lam_star, *[x for x in LAMBDA_GRID if x != lam_star]):
            overl = series[f'A_N{n}_lam{lam}']
            for scope in ('FULL', 'IS', 'OOS'):
                bm = metrics(base_ser, scope)
                om = metrics(overl, scope)
                ds = diff_stats(overl, base_ser, scope)
                main_rows.append({
                    'N': n, 'variant': 'A_score_overlay', 'lambda': lam,
                    'lambda_is_chosen': lam == lam_star, 'scope': scope,
                    'base_ann_net': bm.get('ann_net'),
                    'over_ann_net': om.get('ann_net'),
                    'd_ann_net': (om['ann_net'] - bm['ann_net'])
                    if om.get('ann_net') is not None else None,
                    'base_ann_gross': bm.get('ann_gross'),
                    'over_ann_gross': om.get('ann_gross'),
                    'base_cost_bp': bm.get('cost_bp_per_rebal'),
                    'over_cost_bp': om.get('cost_bp_per_rebal'),
                    'base_turnover': bm.get('turnover_1side'),
                    'over_turnover': om.get('turnover_1side'),
                    'base_sharpe': bm.get('sharpe'), 'over_sharpe': om.get('sharpe'),
                    'base_mdd': bm.get('mdd'), 'over_mdd': om.get('mdd'),
                    'd_ann_bp': ds.get('ann_bp'), 'se_ann_bp': ds.get('se_ann_bp'),
                    't_diff': ds.get('t'), 'win_rate': ds.get('win_rate'),
                    'n_trig_in_basket': om.get('n_trig_mean'),
                    'base_trig_share': bm.get('trig_share_mean'),
                    'over_trig_share': om.get('trig_share_mean'),
                    'base_trig_contrib_ann_bp': bm.get('trig_contrib_ann_bp'),
                    'over_trig_contrib_ann_bp': om.get('trig_contrib_ann_bp'),
                    'trig_contrib_se_ann_bp': om.get('trig_contrib_se_ann_bp'),
                })
    main = pl.DataFrame(main_rows)
    main.write_csv(OUT / 'main_compare.csv')

    # ---- 机制 B：零换手对照（组合内权重倾斜，选股不变）----
    tilt_rows = []
    for lam in (0.0, *TILT_GRID):
        b = basket_weights(df, 'S0', N_PRIMARY, tilt=(None if lam == 0 else lam))
        ser = period_series(df, b, f'B_tilt{lam}')
        series[f'B_tilt{lam}'] = ser
        for scope in ('IS', 'OOS'):
            m = metrics(ser, scope)
            ds = diff_stats(ser, series[f'base_N{N_PRIMARY}'], scope)
            tilt_rows.append({'variant': 'B_weight_tilt', 'lambda_tilt': lam,
                              'scope': scope,
                              'ann_net': m.get('ann_net'),
                              'cost_bp_per_rebal': m.get('cost_bp_per_rebal'),
                              'turnover_1side': m.get('turnover_1side'),
                              'sharpe': m.get('sharpe'), 'mdd': m.get('mdd'),
                              'd_ann_bp': ds.get('ann_bp'),
                              'se_ann_bp': ds.get('se_ann_bp'),
                              't_diff': ds.get('t')})
    tilt = pl.DataFrame(tilt_rows)
    tilt.write_csv(OUT / 'tilt_zero_turnover.csv')

    # ---- 触发粒度与 τ 稳健性（主 N 上）----
    rob_rows = []
    for tc in ('trig20', 'trig60'):
        col = f'score_{tc}_star'
        d2 = df.with_columns((pl.col('S0') + pl.col('s0_sd') * lam_star
                              * pl.col(tc)).alias(col))
        b = basket_weights(d2, col, N_PRIMARY, trig_col=tc)
        ser = period_series(d2, b, f'A_{tc}_star')
        series[f'A_{tc}_star'] = ser
        for scope in ('IS', 'OOS'):
            m = metrics(ser, scope)
            ds = diff_stats(ser, series[f'base_N{N_PRIMARY}'], scope)
            rob_rows.append({'trigger': tc, 'scope': scope,
                             'ann_net': m.get('ann_net'),
                             'd_ann_bp': ds.get('ann_bp'),
                             'se_ann_bp': ds.get('se_ann_bp'),
                             't_diff': ds.get('t'),
                             'turnover_1side': m.get('turnover_1side'),
                             'cost_bp_per_rebal': m.get('cost_bp_per_rebal'),
                             'n_trig_in_basket': m.get('n_trig_mean')})
    rob = pl.DataFrame(rob_rows)
    rob.write_csv(OUT / 'trigger_robustness.csv')

    # ---- overlay 单独成组 ----
    so = {'t20': standalone(df, 'trig20'), 't60': standalone(df, 'trig60')}
    (OUT / 'standalone_overlay.json').write_text(
        json.dumps(so, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    # ---- 逐期序列 ----
    keep = ['base_N100', f'A_N100_lam{lam_star}', *[k for k in series
                                                    if k.startswith('B_tilt')]]
    pl.concat([series[k] for k in keep if k in series], how='vertical_relaxed'
              ).write_csv(OUT / 'period_series.csv')

    results['tilt'] = tilt.to_dicts()
    results['robustness'] = rob.to_dicts()
    results['standalone'] = so
    (OUT / 'overlay_results.json').write_text(
        json.dumps(results, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')

    # ---- 控制台摘要 ----
    print(grid.filter(pl.col('N') == N_PRIMARY).sort('lambda').to_pandas()
          .to_string(index=False))
    print(main.filter(pl.col('N') == N_PRIMARY)
          .select(['lambda', 'lambda_is_chosen', 'scope', 'base_ann_net',
                   'over_ann_net', 'd_ann_net', 'base_cost_bp', 'over_cost_bp',
                   'base_turnover', 'over_turnover', 'base_sharpe', 'over_sharpe',
                   'base_mdd', 'over_mdd', 'd_ann_bp', 'se_ann_bp', 't_diff'])
          .sort(['scope', 'lambda']).to_pandas().to_string(index=False))
    print(tilt.to_pandas().to_string(index=False))
    print(rob.to_pandas().to_string(index=False))
    print(json.dumps(so, ensure_ascii=False, indent=2, default=str))
    log(f'完成 {time.time() - t0:.0f}s')


if __name__ == '__main__':
    main()
