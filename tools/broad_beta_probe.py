#!/usr/bin/env python3
"""宽基/红利 ETF 抓 beta 的可测性验证（2026-11-21）。

三件事
------
1. 四条宽基 beta（沪深300 / 中证500 / 中证1000 / 红利）用 **ETF 真实行情**
   算年化、最大回撤、Sharpe，与已有的「等权全 A beta」「纯 alpha」并排；
2. 用指数 PE/PB 的 **当时可得** 历史分位调仓，比较择时后与买入持有；
3. 指数底仓 + 低估值质量 alpha 叠加，扫底仓比例，看 Sharpe / 回撤。

口径
----
- ETF 收益用本地 ``quotes/fund`` 的日线 + ``hfq_factor``（含分红再投）；
- 成本沿用项目惯例 0.13% 往返：买入 1.5bp，卖出 11.5bp（ETF 实际无印花税，此处偏保守）；
- 信号在月末 T 收盘确认，T+1 开盘成交（ETF 袖套）；alpha 袖套按净值收盘成交；
- 停牌 / 零成交日不交易，顺延到下一个有行情的交易日；
- 未投入部分按年化 1.5% 计息（与 ``beta_alpha_combo.py`` 同假设，非实测）。

产物（``work/宽基beta_2026-11-21/``）
------------------------------------
``coverage_etf.csv``            四只 ETF 覆盖、缺口、零成交、ADV
``beta_metrics.csv``            四条 beta + 参考基准的指标（全窗口 / 共同窗口）
``equity_beta_<code>.csv``      各 beta 买入持有净值
``index_valuation_monthly.csv`` 指数 PE/PB 月频序列与分位（滞后一期权重的 PIT 口径）
``timing_metrics.csv``          择时网格（PE/PB × 分位口径 × 阈值规则 × 指数）
``equity_timing_*.csv``         择时净值
``combo_metrics.csv``           底仓比例叠加网格
``equity_combo_*.csv``          叠加净值
``summary.json``                关键结论数字

用法
----
    .venv\\Scripts\\python.exe tools/broad_beta_probe.py
"""
from __future__ import annotations

import bisect
import json
import math
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'work' / '宽基beta_2026-11-21'
FUND_DIR = ROOT / 'data' / 'local_store' / 'quotes' / 'fund'
VAL_PATH = ROOT / 'data' / 'local_store' / 'meta' / 'financials' / 'valuation_daily.parquet'
WEIGHT_PATH = ROOT / 'data' / 'local_store' / 'meta' / 'index_members' / 'index_weight_monthly.parquet'
POOL_PATH = ROOT / 'data' / 'local_store' / 'meta' / 'multi_asset' / 'etf_pool.parquet'
ALPHA_EQUITY = ROOT / 'work' / 'beta_alpha_2026-11-21' / 'equity_alpha_only.csv'
BETA_EW_EQUITY = ROOT / 'work' / 'beta_alpha_2026-11-21' / 'equity_beta_ew.csv'

MAX_INVEST = 0.99          # 与既有基准一致：1% 现金缓冲
CASH_RATE = 0.015          # 空仓收益假设（年化）
BUY_COST = 0.00015         # 1.5bp
SELL_COST = 0.00115        # 11.5bp（佣金 1.5 + 印花税 10，沿用股票口径）
SWITCH_COST = 0.00065      # 袖套之间调仓按单边 6.5bp
FULL = (date(2015, 1, 1), date(2026, 10, 31))
COMMON = (date(2017, 1, 1), date(2026, 10, 31))   # 四只 ETF 都有行情的共同窗口

ETFS = [
    ('510300.SH', '沪深300ETF华泰柏瑞', '沪深300', '000300.SH'),
    ('510500.SH', '中证500ETF南方', '中证500', '000905.SH'),
    ('512100.SH', '中证1000ETF南方', '中证1000', '000852.SH'),
    ('510880.SH', '红利ETF华泰柏瑞', '红利(上证红利)', '000015.SH'),
]


# ---------------------------------------------------------------------------
# 指标
# ---------------------------------------------------------------------------

def stats(dates: List[date], eq: np.ndarray, a: date, b: date,
          base: Optional[float] = None) -> Dict:
    idx = [i for i, d in enumerate(dates) if a <= d <= b]
    if len(idx) < 5:
        return {'annual': None, 'max_drawdown': None, 'sharpe': None,
                'vol': None, 'calmar': None, 'win_rate_monthly': None,
                'years': None}
    e = eq[idx]
    d = [dates[i] for i in idx]
    # 曲线上第一个点是建仓当日收盘；用 base=1.0 把建仓当日的涨跌和成本算回来
    if base is not None:
        e = np.concatenate(([base], e))
    years = (d[-1] - d[0]).days / 365.25
    annual = (e[-1] / e[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    peak, mdd = e[0], 0.0
    for v in e:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    rets = e[1:] / e[:-1] - 1.0
    vol = float(np.std(rets, ddof=1) * math.sqrt(252)) if rets.size > 2 else 0.0
    monthly: Dict[Tuple[int, int], float] = {}
    for dd, v in zip(d, e):
        monthly[(dd.year, dd.month)] = v
    ms = [monthly[k] for k in sorted(monthly)]
    mrets = [ms[i] / ms[i - 1] - 1.0 for i in range(1, len(ms))]
    win = float(np.mean([r > 0 for r in mrets])) if mrets else None
    return {
        'annual': round(annual, 6), 'max_drawdown': round(mdd, 6),
        'vol': round(vol, 6), 'sharpe': round(annual / vol, 4) if vol > 0 else None,
        'calmar': round(annual / abs(mdd), 4) if mdd < 0 else None,
        'win_rate_monthly': round(win, 4) if win is not None else None,
        'years': round(years, 2),
    }


# ---------------------------------------------------------------------------
# ETF 行情
# ---------------------------------------------------------------------------

def load_etf(code: str) -> pl.DataFrame:
    f = FUND_DIR / ('sh' if code.endswith('.SH') else 'sz') / f'{code}.parquet'
    df = pl.read_parquet(f, columns=['date', 'open', 'close', 'volume', 'hfq_factor'])
    df = df.with_columns([
        (pl.col('open') * pl.col('hfq_factor')).alias('adj_open'),
        (pl.col('close') * pl.col('hfq_factor')).alias('adj_close'),
    ]).sort('date')
    return df


def coverage() -> pl.DataFrame:
    pool = pl.read_parquet(POOL_PATH).filter(
        pl.col('ts_code').is_in([c for c, *_ in ETFS]))
    rows = []
    for code, name, label, idx_code in ETFS:
        df = load_etf(code)
        p = pool.filter(pl.col('ts_code') == code).row(0, named=True)
        z = df.filter(pl.col('volume') == 0)['date'].to_list()
        rows.append({
            'ts_code': code, 'name': name, 'beta_label': label, 'track_index': idx_code,
            'list_date': str(p['list_date']), 'first_date': str(df['date'][0]),
            'last_date': str(df['date'][-1]), 'rows': df.height,
            'missing_days': p['missing_days'], 'max_gap_days': p['max_gap_days'],
            'zero_volume_days': len(z),
            'zero_volume_dates': ','.join(str(x) for x in z[:10]),
            'adv_20d_yi': round((p['adv_20d'] or 0) / 1e8, 2),
            'gap_kind': p['gap_kind'],
            'in_full_window': df['date'][0] <= FULL[0],
            'in_common_window': df['date'][0] <= COMMON[0],
        })
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# 单标的 / 双袖套日频模拟（T+1 开盘成交）
# ---------------------------------------------------------------------------

def _daily_dates(df: pl.DataFrame) -> List[date]:
    return df['date'].to_list()


def sim_single(df: pl.DataFrame, targets: Dict[date, float],
               start: date, end: date) -> Tuple[List[date], np.ndarray, Dict]:
    """单 ETF 模拟：``targets`` 为信号日 → 目标仓位（占总权益比例）。

    信号日 T 收盘确认，T+1 开盘调仓；停牌 / 零成交日顺延。
    """
    dates = [d for d in _daily_dates(df) if d <= end]
    opens = dict(zip(df['date'].to_list(), df['adj_open'].to_list()))
    closes = dict(zip(df['date'].to_list(), df['adj_close'].to_list()))
    vols = dict(zip(df['date'].to_list(), df['volume'].to_list()))
    dset = set(dates)

    sig_days = sorted(targets)
    exec_queue: Dict[date, float] = {}
    for s in sig_days:
        j = bisect.bisect_right(dates, s)
        if j < len(dates):
            exec_queue[dates[j]] = targets[s]   # 若当天停牌会顺延

    start_i = bisect.bisect_left(dates, start)
    if start_i >= len(dates):
        return [], np.array([]), {}
    # 初始建仓用中性满仓，避免把区间内第一个月末的信号提前用到区间起点
    units = 0.0
    cash = 1.0
    pending: Optional[float] = MAX_INVEST
    eq: List[float] = []
    out_dates: List[date] = []
    traded = 0
    cost_sum = 0.0
    rf_daily = CASH_RATE / 252.0
    exec_days = sorted(exec_queue)

    for d in dates[start_i:]:
        if d not in dset:
            continue
        cash *= (1.0 + rf_daily)
        # 调仓：当天有计划则更新目标；停牌 / 零成交时保留 pending，顺延到下一交易日
        if d in exec_queue:
            pending = exec_queue[d]
        if pending is not None and d in closes and vols.get(d, 0) > 0:
            target = pending
            px = opens[d]
            total = cash + units * px
            tgt_val = target * total
            cur_val = units * px
            delta = tgt_val - cur_val
            if abs(delta) > 1e-12:
                if delta > 0:
                    c = delta * BUY_COST
                    units += (delta - c) / px
                    cash -= delta
                else:
                    c = -delta * SELL_COST
                    units += delta / px
                    cash += -delta - c
                cost_sum += c
                traded += 1
            pending = None
        eq.append(cash + units * closes[d])
        out_dates.append(d)
    return out_dates, np.array(eq), {'trades': traded, 'cost_pct': round(cost_sum * 100, 4)}


def month_ends(calendar: List[date], a: date, b: date) -> List[date]:
    """区间内每个自然月的最后一个交易日。"""
    out: Dict[Tuple[int, int], date] = {}
    for d in calendar:
        if a <= d <= b:
            out[(d.year, d.month)] = d
    return [out[k] for k in sorted(out)]


# ---------------------------------------------------------------------------
# 指数 PE/PB（PIT：t 日用 t 日估值 + t 之前最近一期成分权重）
# ---------------------------------------------------------------------------

def _harmonic(df: pl.DataFrame) -> Tuple[Optional[float], Optional[float], float]:
    w = df['weight'].to_numpy()
    pe = df['pe'].to_numpy()
    pb = df['pb'].to_numpy()
    mask_pe = np.isfinite(pe) & (pe > 0)
    mask_pb = np.isfinite(pb) & (pb > 0)
    pe_v = float(w[mask_pe].sum() / (w[mask_pe] / pe[mask_pe]).sum()) if mask_pe.any() else None
    pb_v = float(w[mask_pb].sum() / (w[mask_pb] / pb[mask_pb]).sum()) if mask_pb.any() else None
    cov = float(w[mask_pe].sum() / w.sum()) if w.sum() > 0 else 0.0
    return pe_v, pb_v, cov


def build_index_valuation(a: date, b: date) -> pl.DataFrame:
    w = pl.read_parquet(WEIGHT_PATH)
    w = w.with_columns(pl.col('trade_date').str.strptime(pl.Date, '%Y%m%d').alias('d'))
    w = w.filter(pl.col('index_code').is_in([x[3] for x in ETFS if x[3].startswith('000')]))
    codes = w['ts_code'].unique().to_list()
    val = pl.scan_parquet(VAL_PATH).filter(
        pl.col('ts_code').is_in(codes) & (pl.col('trade_date') >= date(2004, 12, 1))
    ).select(['ts_code', 'trade_date', 'pe', 'pb']).collect()

    # 估值日取全 A 交易日（用 510300 的日历），只在月末计算
    cal = _daily_dates(load_etf('510300.SH'))
    # 估值历史从 2005 年起，便于 t 日有足够长的分位窗口
    hist_start = date(2005, 1, 1)
    sig_days = month_ends(cal, hist_start, b)

    rows = []
    for idx_code in sorted(set(w['index_code'].to_list())):
        wi = w.filter(pl.col('index_code') == idx_code).sort('d')
        w_dates = wi['d'].unique().sort().to_list()
        for t in sig_days:
            k = bisect.bisect_left(w_dates, t) - 1    # 严格早于 t 的最近一期权重
            if k < 0:
                continue
            snap = w_dates[k]
            ww = wi.filter(pl.col('d') == snap).select(['ts_code', 'weight'])
            vv = val.filter(pl.col('trade_date') == t).select(['ts_code', 'pe', 'pb'])
            if vv.is_empty():
                continue
            j = ww.join(vv, on='ts_code', how='inner')
            if j.is_empty():
                continue
            pe_v, pb_v, cov = _harmonic(j)
            rows.append({'index_code': idx_code, 'signal_date': t, 'weight_date': snap,
                         'pe': pe_v, 'pb': pb_v, 'pe_weight_cov': round(cov, 4)})
    out = pl.DataFrame(rows, infer_schema_length=None, schema_overrides={
        'pe': pl.Float64, 'pb': pl.Float64, 'pe_weight_cov': pl.Float64}).sort(
        ['index_code', 'signal_date'])

    # 分位：expanding（只用 ≤t 的月度值）与 rolling 60 个月
    res = []
    for idx_code in out['index_code'].unique().to_list():
        s = out.filter(pl.col('index_code') == idx_code).sort('signal_date')
        pe = s['pe'].to_list()
        pb = s['pb'].to_list()
        for i in range(s.height):
            r = {'index_code': idx_code, 'signal_date': s['signal_date'][i],
                 'pe': pe[i], 'pb': pb[i],
                 'pe_weight_cov': s['pe_weight_cov'][i]}
            for col, vals in (('pe', pe), ('pb', pb)):
                v = vals[i]
                if v is None:
                    r[f'{col}_pct_exp'] = None
                    r[f'{col}_pct_60'] = None
                    continue
                hist = [x for x in vals[:i + 1] if x is not None]
                r[f'{col}_pct_exp'] = (round(sum(1 for x in hist if x <= v) / len(hist), 4)
                                       if len(hist) >= 24 else None)
                win = [x for x in vals[max(0, i - 59):i + 1] if x is not None]
                r[f'{col}_pct_60'] = (round(sum(1 for x in win if x <= v) / len(win), 4)
                                      if len(win) >= 36 else None)
            res.append(r)
    return pl.DataFrame(res, infer_schema_length=None, schema_overrides={
        'pe': pl.Float64, 'pb': pl.Float64, 'pe_weight_cov': pl.Float64,
        'pe_pct_exp': pl.Float64, 'pe_pct_60': pl.Float64,
        'pb_pct_exp': pl.Float64, 'pb_pct_60': pl.Float64})


# ---------------------------------------------------------------------------
# 择时规则
# ---------------------------------------------------------------------------

RULES = {
    'bin30': lambda p: 1.0 if p <= 0.30 else 0.5,
    'tier30_70': lambda p: 1.0 if p <= 0.30 else (0.7 if p <= 0.70 else 0.5),
    'bin50': lambda p: 1.0 if p <= 0.50 else 0.5,
    'tier20_60': lambda p: 1.0 if p <= 0.20 else (0.7 if p <= 0.60 else 0.5),
}


def timing_targets(val_df: pl.DataFrame, etf_cal: List[date], a: date, b: date,
                   metric: str, mode: str, rule: str) -> Tuple[Dict[date, float], float]:
    col = f'{metric}_pct_{mode}'
    sig = month_ends(etf_cal, a, b)
    s = {d: p for d, p in zip(val_df['signal_date'].to_list(),
                              val_df[col].to_list())}
    fn = RULES[rule]
    targets: Dict[date, float] = {}
    pos = []
    for d in sig:
        p = s.get(d)
        w = MAX_INVEST if p is None else MAX_INVEST * fn(p)
        targets[d] = w
        pos.append(w)
    return targets, (float(np.mean(pos)) if pos else 0.0)


# ---------------------------------------------------------------------------
# 双袖套：ETF 底仓 + alpha
# ---------------------------------------------------------------------------

def load_alpha() -> Tuple[List[date], np.ndarray]:
    df = pl.read_csv(ALPHA_EQUITY)
    dates = [date.fromisoformat(x) for x in df['date'].to_list()]
    nav = df['equity'].to_numpy().astype('float64')
    nav = nav / nav[0]
    return dates, nav


def sim_combo(etf: pl.DataFrame, alpha_dates: List[date], alpha_nav: np.ndarray,
              ratio: float, a: date, b: date) -> Tuple[List[date], np.ndarray, Dict]:
    """ETF 底仓比例 ``ratio``，alpha 袖套 1-ratio；月末 T 信号，ETF T+1 开盘、alpha T+1 收盘调仓。"""
    etf_dates = [d for d in _daily_dates(etf) if a <= d <= b]
    alpha_map = {d: v for d, v in zip(alpha_dates, alpha_nav) if a <= d <= b}
    cal = sorted(set(etf_dates) | set(alpha_map))
    if len(cal) < 10:
        return [], np.array([]), {}
    opens = dict(zip(etf['date'].to_list(), etf['adj_open'].to_list()))
    closes = dict(zip(etf['date'].to_list(), etf['adj_close'].to_list()))
    vols = dict(zip(etf['date'].to_list(), etf['volume'].to_list()))
    a_nav = dict(alpha_map)

    sig = month_ends(cal, a, b)
    exec_map: Dict[date, int] = {}
    for s in sig:
        j = bisect.bisect_right(cal, s)
        if j < len(cal):
            exec_map[cal[j]] = 1

    # 初始建仓：首个有行情的交易日按目标权重一次建好；零成交则顺延
    w_b = max(0.0, min(1.0, MAX_INVEST * ratio))
    w_a = MAX_INVEST - w_b
    beta_units = 0.0
    last_nav = a_nav.get(cal[0])
    if last_nav is None:
        first_a = min([d for d in a_nav if d >= cal[0]], default=None)
        last_nav = a_nav[first_a] if first_a else 1.0
    alpha_units = 0.0
    cash = 1.0
    pending = True
    rf_daily = CASH_RATE / 252.0

    out_d: List[date] = []
    eq: List[float] = []
    cost_sum = 0.0
    rebals = 0
    last_px = opens.get(cal[0], closes.get(cal[0], 1.0))
    for d in cal:
        cash *= (1.0 + rf_daily)
        if d in a_nav:
            last_nav = a_nav[d]
        if d in opens:
            last_px = opens[d]
        if d in exec_map:
            pending = True
        if pending and d in opens and d in closes and vols.get(d, 0) > 0:
            px = opens[d]
            total = cash + beta_units * px + alpha_units * last_nav
            tb = MAX_INVEST * ratio * total
            ta = MAX_INVEST * (1.0 - ratio) * total
            db = tb - beta_units * px
            if abs(db) > 1e-12:
                if db > 0:
                    c = db * BUY_COST
                    beta_units += (db - c) / px
                    cash -= db
                else:
                    c = -db * SELL_COST
                    beta_units += db / px
                    cash += -db - c
                cost_sum += c
            da = ta - alpha_units * last_nav
            if abs(da) > 1e-12:
                c = abs(da) * SWITCH_COST
                alpha_units += (da - np.sign(da) * c) / last_nav
                cash -= da
                cost_sum += c
            rebals += 1
            pending = False
        eq.append(cash + beta_units * closes.get(d, last_px) + alpha_units * last_nav)
        out_d.append(d)
    return out_d, np.array(eq), {'rebals': rebals, 'cost_pct': round(cost_sum * 100, 4)}


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    cov = coverage()
    cov.write_csv(OUT / 'coverage_etf.csv')
    print('== 覆盖 ==')
    print(cov.select(['beta_label', 'ts_code', 'first_date', 'last_date', 'rows',
                      'missing_days', 'zero_volume_days', 'adv_20d_yi']))

    # 1) 四条 beta 买入持有
    beta_rows = []
    etf_data: Dict[str, pl.DataFrame] = {}
    for code, name, label, idx_code in ETFS:
        df = load_etf(code)
        etf_data[code] = df
        cal = _daily_dates(df)
        first = max(FULL[0], df['date'][0])
        targets = {month_ends(cal, first, FULL[1])[0]: MAX_INVEST}
        # 买入持有：只在首个交易日建仓一次
        ds, eq, info = sim_single(df, targets, first, FULL[1])
        pl.DataFrame({'date': [str(x) for x in ds], 'equity': eq}).write_csv(
            OUT / f'equity_beta_{code}.csv')
        for tag, (a, b) in (('full', (first, FULL[1])), ('common', COMMON)):
            st = stats(ds, eq, a, b, base=1.0 if a <= ds[0] else None)
            beta_rows.append({'run': f'beta_{label}', 'code': code, 'window': tag,
                              'start': str(a), 'end': str(b), **st,
                              'trades': info.get('trades'),
                              'cost_pct': info.get('cost_pct')})
    # 参考：等权全 A beta、纯 alpha（复用已有净值，按同一 stats 重算）
    for name, path in (('beta_ew_allA', BETA_EW_EQUITY), ('alpha_only', ALPHA_EQUITY)):
        df = pl.read_csv(path)
        ds = [date.fromisoformat(x) for x in df['date'].to_list()]
        eq = df['equity'].to_numpy().astype('float64')
        for tag, (a, b) in (('full', FULL), ('common', COMMON)):
            beta_rows.append({'run': name, 'code': '', 'window': tag,
                              'start': str(a), 'end': str(b), **stats(ds, eq, a, b),
                              'trades': None})
    # 共同窗口下，alpha 与全 A beta 也需与 1000 的起始对齐，故用 COMMON
    bm = pl.DataFrame(beta_rows)
    bm.write_csv(OUT / 'beta_metrics.csv')
    print('== beta 指标 ==')
    print(bm.select(['run', 'window', 'annual', 'max_drawdown', 'sharpe',
                     'vol', 'years']))

    # 2) 指数估值分位择时
    val = build_index_valuation(FULL[0], FULL[1])
    val = val.with_columns([pl.col(c).cast(pl.Float64)
                            for c in val.columns if c.endswith(('_exp', '_60'))])
    val.write_csv(OUT / 'index_valuation_monthly.csv')
    # 估值覆盖缺口（PE/PB 分年非空率），避免把「未接线」当成「无数据」
    vcov = (val.with_columns(pl.col('signal_date').dt.year().alias('year'))
            .group_by(['index_code', 'year']).agg(
                pl.len().alias('months'),
                pl.col('pe').is_not_null().sum().alias('pe_nonnull'),
                pl.col('pb').is_not_null().sum().alias('pb_nonnull'),
                pl.col('pe_weight_cov').mean().round(4).alias('pe_cov_mean')))
    vcov.write_csv(OUT / 'valuation_coverage_by_year.csv')
    timing_rows = []
    for code, name, label, idx_code in ETFS:
        if idx_code not in set(val['index_code'].to_list()):
            continue                      # 红利指数无 PIT 成分权重，如实缺测
        vdf = val.filter(pl.col('index_code') == idx_code)
        df = etf_data[code]
        cal = _daily_dates(df)
        first = max(FULL[0], df['date'][0])
        for metric in ('pe', 'pb'):
            for mode in ('exp', '60'):
                for rule in RULES:
                    variant = f'{metric}_{mode}_{rule}'
                    tg, avg_pos = timing_targets(vdf, cal, first, FULL[1], metric, mode, rule)
                    ds, eq, info = sim_single(df, tg, first, FULL[1])
                    pl.DataFrame({'date': [str(x) for x in ds], 'equity': eq}).write_csv(
                        OUT / f'equity_timing_{code}_{metric}_{mode}_{rule}.csv')
                    for tag, (a, b) in (('full', (first, FULL[1])), ('common', COMMON)):
                        timing_rows.append({
                            'beta_label': label, 'code': code, 'index_code': idx_code,
                            'variant': variant, 'metric': metric, 'pct_mode': mode,
                            'rule': rule, 'window': tag,
                            **stats(ds, eq, a, b, base=1.0 if a <= ds[0] else None),
                            'avg_position': round(avg_pos, 4),
                            'trades': info.get('trades'),
                            'cost_pct': info.get('cost_pct')})
                    # 对照：恒定仓位 = 该规则实际平均仓位（同样的月频调仓机制）
                    avg_r = round(avg_pos, 3)
                    c_rule = f'const@{avg_r:.3f}'
                    tg_c = {d: avg_r for d in month_ends(cal, first, FULL[1])}
                    ds_c, eq_c, _ = sim_single(df, tg_c, first, FULL[1])
                    pl.DataFrame({'date': [str(x) for x in ds_c], 'equity': eq_c}).write_csv(
                        OUT / f'equity_control_{code}_{metric}_{mode}_{rule}.csv')
                    for tag, (a, b) in (('full', (first, FULL[1])), ('common', COMMON)):
                        timing_rows.append({
                            'beta_label': label, 'code': code, 'index_code': idx_code,
                            'variant': variant, 'metric': metric, 'pct_mode': mode,
                            'rule': c_rule, 'window': tag,
                            **stats(ds_c, eq_c, a, b, base=1.0 if a <= ds_c[0] else None),
                            'avg_position': round(avg_pos, 4), 'trades': None})
        # 买入持有参考（不再平衡、只在起点建仓）
        tg_b = {month_ends(cal, first, FULL[1])[0]: MAX_INVEST}
        ds_b, eq_b, info_b = sim_single(df, tg_b, first, FULL[1])
        for tag, (a, b) in (('full', (first, FULL[1])), ('common', COMMON)):
            timing_rows.append({
                'beta_label': label, 'code': code, 'index_code': idx_code,
                'variant': 'buyhold', 'metric': '', 'pct_mode': '', 'rule': 'buyhold',
                'window': tag, **stats(ds_b, eq_b, a, b, base=1.0 if a <= ds_b[0] else None),
                'avg_position': MAX_INVEST, 'trades': 1})
    tm = pl.DataFrame(timing_rows)
    tm.write_csv(OUT / 'timing_metrics.csv')
    # 最优规则 vs 同平均仓位恒定对照
    is_rule = ~pl.col('rule').is_in(['buyhold']) & ~pl.col('rule').str.starts_with('const@')
    best = tm.filter(is_rule).sort('sharpe', descending=True).group_by(
        ['beta_label', 'window']).head(1)
    cmp_rows = []
    for r in best.iter_rows(named=True):
        c = tm.filter((pl.col('beta_label') == r['beta_label'])
                      & (pl.col('window') == r['window'])
                      & (pl.col('variant') == r['variant'])
                      & pl.col('rule').str.starts_with('const@'))
        bh = tm.filter((pl.col('beta_label') == r['beta_label'])
                       & (pl.col('window') == r['window']) & (pl.col('rule') == 'buyhold'))
        cmp_rows.append({
            'beta_label': r['beta_label'], 'window': r['window'],
            'metric': r['metric'], 'pct_mode': r['pct_mode'], 'rule': r['rule'],
            'annual': r['annual'], 'mdd': r['max_drawdown'], 'sharpe': r['sharpe'],
            'avg_pos': r['avg_position'],
            'ctrl_sharpe': c['sharpe'][0] if c.height else None,
            'ctrl_mdd': c['max_drawdown'][0] if c.height else None,
            'bh_sharpe': bh['sharpe'][0] if bh.height else None,
            'bh_mdd': bh['max_drawdown'][0] if bh.height else None,
        })
    print('== 择时：最优规则 vs 同仓位恒定对照 vs 买入持有 ==')
    print(pl.DataFrame(cmp_rows))

    # 稳健性：16 个变体的分布，不只看最好的那一个
    rob = []
    for (lb, tag) in [(x, y) for x in tm['beta_label'].unique().to_list()
                      for y in ('full', 'common')]:
        g = tm.filter((pl.col('beta_label') == lb) & (pl.col('window') == tag))
        rules = g.filter(is_rule)
        bh = g.filter(pl.col('rule') == 'buyhold')
        pairs = []
        for r in rules.iter_rows(named=True):
            c = g.filter((pl.col('variant') == r['variant'])
                         & pl.col('rule').str.starts_with('const@'))
            pairs.append((r['sharpe'], c['sharpe'][0] if c.height else None))
        rob.append({
            'beta_label': lb, 'window': tag, 'n_variants': rules.height,
            'sharpe_median': round(float(np.median([p[0] for p in pairs])), 4),
            'sharpe_min': round(float(np.min([p[0] for p in pairs])), 4),
            'sharpe_max': round(float(np.max([p[0] for p in pairs])), 4),
            'share_beat_buyhold': round(float(np.mean(
                [p[0] > bh['sharpe'][0] for p in pairs])), 2),
            'share_beat_control': round(float(np.mean(
                [p[1] is not None and p[0] > p[1] for p in pairs])), 2),
            'buyhold_sharpe': bh['sharpe'][0],
            'buyhold_mdd': bh['max_drawdown'][0],
        })
    rob_df = pl.DataFrame(rob)
    rob_df.write_csv(OUT / 'timing_robustness.csv')
    print('== 择时稳健性（16 个变体） ==')
    print(rob_df)

    # 3) 底仓 + alpha 叠加
    adates, anav = load_alpha()
    combo_rows = []
    for code, name, label, idx_code in ETFS:
        df = etf_data[code]
        first = max(FULL[0], df['date'][0])
        for ratio in (0.2, 0.3, 0.4, 0.5, 0.6):
            ds, eq, info = sim_combo(df, adates, anav, ratio, first, FULL[1])
            pl.DataFrame({'date': [str(x) for x in ds], 'equity': eq}).write_csv(
                OUT / f'equity_combo_{code}_{int(ratio * 100)}.csv')
            for tag, (a, b) in (('full', (first, FULL[1])), ('common', COMMON)):
                combo_rows.append({'beta_label': label, 'code': code,
                                   'base_ratio': ratio, 'window': tag,
                                   **stats(ds, eq, a, b, base=1.0 if a <= ds[0] else None),
                                   'rebals': info.get('rebals'),
                                   'cost_pct': info.get('cost_pct')})
    cm = pl.DataFrame(combo_rows)
    cm.write_csv(OUT / 'combo_metrics.csv')
    print('== 叠加（按 Sharpe 排序） ==')
    print(cm.sort('sharpe', descending=True).head(12)
          .select(['beta_label', 'base_ratio', 'window', 'annual', 'max_drawdown',
                   'sharpe', 'vol']))

    # 日收益相关性（解释叠加效果）：红利/300/500/1000 + alpha
    a_df = pl.DataFrame({'date': [str(x) for x in adates], 'alpha': anav})
    rets = a_df.with_columns((pl.col('alpha') / pl.col('alpha').shift(1) - 1).alias('alpha'))
    for code, *_ in ETFS:
        df = etf_data[code]
        r = pl.DataFrame({'date': [str(x) for x in df['date'].to_list()],
                          code: (df['adj_close'] / df['adj_close'].shift(1) - 1).to_list()})
        rets = rets.join(r, on='date', how='inner')
    rets = rets.filter(pl.col('date').str.slice(0, 4).cast(pl.Int32) >= FULL[0].year)
    cols = ['alpha'] + [c for c, *_ in ETFS]
    corr = rets.select(cols).drop_nulls().corr()
    pl.DataFrame({'row': cols, **{c: corr[c].to_list() for c in cols}}).write_csv(
        OUT / 'corr_daily.csv')

    # summary
    def pick(df, **kw):
        x = df
        for k, v in kw.items():
            x = x.filter(pl.col(k) == v)
        return x.row(0, named=True) if x.height else None

    summary = {
        'coverage': cov.to_dicts(),
        'beta_full': bm.filter(pl.col('window') == 'full').to_dicts(),
        'timing_best_vs_control': cmp_rows,
        'timing_robustness': rob_df.to_dicts(),
        'combo_best': cm.filter(pl.col('window') == 'full')
                        .sort('sharpe', descending=True).head(5).to_dicts(),
        'elapsed_s': round(time.perf_counter() - t0, 1),
    }
    (OUT / 'summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print('elapsed', summary['elapsed_s'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
