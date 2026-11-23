#!/usr/bin/env python3
"""横截面打分排序组合（估值 / 质量 / 动量），月频与双月频调仓。

复用既有骨架
------------
- 行情与回测引擎：``baseline_pv_val_equal_weight`` + ``factor_lowvol_quantile``
  + ``factor_lowvol_variants``（hfq 出信号、raw 价成交、T+1 开盘撮合、
  停牌/涨跌停不可成交、分红入池）；
- 因子：正式因子面板 ``data/local_store/factors/factor_panel.parquet``
  （ep / bp / roe_ttm，均按 ann_date PIT），余额表 debt_ratio，利润表
  毛利率序列（近 8 个报告期标准差）。

口径
----
- 全 A、2015-01 起；剔 ST（``meta/st/st_intervals``）与上市不足 60 个交易日
  （按本地 bar 数计）；后复权信号 / 原始价成交；
- T 日（月末）收盘打分，T+1 开盘成交；成本固定 **0.13% 往返**
  （佣金 1.5bp/边 + 卖出印花税 10bp，滑点/冲击/过户费置 0）；
- 维度内先做横截面分位打分（0~1），维度间等权；
- 持仓数 20/30/50，频率 monthly / bimonthly。

产物
----
``work/横截面打分_2026-11-21/compare.csv`` / ``attribution.csv`` / ``run.log``
``data/local_store/backtest_runs/cross_score_20261121/<run>/equity.parquet``
``.../trades.parquet``

用法
----
    .venv\\Scripts\\python.exe tools/cross_section_score.py --limit-months 12
    .venv\\Scripts\\python.exe tools/cross_section_score.py
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
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
import factor_lowvol_quantile as f
import factor_lowvol_variants as v
from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.input_contract import BAR_CONTRACT
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.core.usecases.run_backtest import _load_local_dividends, _load_st_status
from src.risk import NoSuspendedTrade, PriceLimitTradable, RiskEngine

WARMUP_START = '2013-06-01'
END = '2026-10-31'
SIGNAL_START = date(2015, 1, 1)
SEGS = {
    'full': (date(2015, 1, 1), date(2026, 10, 31)),
    'seg1': (date(2015, 1, 1), date(2019, 12, 31)),
    'seg2': (date(2020, 1, 1), date(2023, 12, 31)),
    'seg3': (date(2024, 1, 1), date(2026, 10, 31)),
}
MIN_BARS = 60
MIN_MOM_BARS = 252
MIN_MOM_SKIP = 21
OUT_DIR = ROOT / 'work' / '横截面打分_2026-11-21'
RUN_DIR = ROOT / 'data/local_store/backtest_runs/cross_score_20261121'
PANEL = ROOT / 'data/local_store/factors/factor_panel.parquet'
FIN = ROOT / 'data/local_store/meta/financials'


class RoundTrip13bp(CostModel):
    """往返成本（默认 16.5bp 含冲击）：佣金 1.5bp/边 + 卖出印花税 5bp，
    余额按双边滑点计（2026-11-23 修正：原实现 slippage_rate 硬返回 0，
    导致 bps 参数失效、实际恒为 13bp）。"""

    def __init__(self, bps: float = 16.5) -> None:
        self.round_trip_bps = bps
        self.slippage_side = max(0.0, bps / 1e4 - 2 * 0.00015 - 0.0005) / 2
        super().__init__({
            'commission_rate': 0.00015, 'min_commission': 5.0,
            'stamp_tax_rate': 0.0005, 'transfer_fee_rate': 0.0,
            'slippage': self.slippage_side, 'impact_coef': 0.0,
        })

    def slippage_rate(self, ts_code: str = '', adv20: float | None = None,
                      total_mv_yi: float | None = None,
                      category: str | None = None) -> float:
        return self.slippage_side

    def _stamp_tax_rate(self, trade_date: Any, side: str, ts_code: str = '') -> float:
        return 0.0 if side != 'sell' else 0.0005

    def _transfer_fee_rate(self, trade_date: Any, ts_code: str = '') -> float:
        return 0.0


# ---------------------------------------------------------------------------
# 数据装载
# ---------------------------------------------------------------------------

def load_momentum(data: Dict[str, pl.DataFrame], months: List[date]
                  ) -> Tuple[Dict[date, Dict[str, float]],
                             Dict[date, Dict[str, int]]]:
    """月末动量 12-1 与可用 bar 数（只用 T 日及之前数据）。"""
    me = np.array(months, dtype='datetime64[D]')
    mom: Dict[date, Dict[str, float]] = {d: {} for d in months}
    bars: Dict[date, Dict[str, int]] = {d: {} for d in months}
    for code, df in data.items():
        dates = df['date'].to_numpy()
        close = df['close'].to_numpy().astype('float64')
        n = close.size
        pos = np.searchsorted(dates, me)
        hit = np.nonzero((pos < n) & (dates[np.minimum(pos, n - 1)] == me))[0]
        for j in hit:
            k = int(pos[j])
            bars[months[j]][code] = k + 1
            if k >= MIN_MOM_BARS:
                p0, p1 = close[k - MIN_MOM_BARS], close[k - MIN_MOM_SKIP]
                if p0 > 0 and p1 > 0 and np.isfinite(p0) and np.isfinite(p1):
                    mom[months[j]][code] = float(p1 / p0 - 1.0)
    return mom, bars


def load_panel_factors(months: List[date]) -> Dict[date, Dict[str, Tuple[float, float, float]]]:
    """因子面板按信号日精确取（该日无记录即视为当日不可用，不做填补）。"""
    df = pl.scan_parquet(PANEL).filter(pl.col('signal_date').is_in(months)).select(
        ['ts_code', 'signal_date', 'ep', 'bp', 'roe_ttm']).collect()
    out: Dict[date, Dict[str, Tuple[float, float, float]]] = {d: {} for d in months}
    codes = df['ts_code'].to_list()
    days = df['signal_date'].to_list()
    ep = df['ep'].to_list()
    bp = df['bp'].to_list()
    roe = df['roe_ttm'].to_list()
    for i in range(len(codes)):
        out[days[i]][codes[i]] = (ep[i], bp[i], roe[i])
    return out


def _asof_by_code(df: pl.DataFrame, day_col: str, value_col: str
                  ) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    df = df.drop_nulls([day_col, value_col]).sort(['ts_code', day_col])
    codes = df['ts_code'].to_numpy()
    days = df[day_col].to_numpy()
    vals = df[value_col].to_numpy().astype('float64')
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    return {str(codes[a]): (days[a:b], vals[a:b]) for a, b in zip(starts, ends)}


def load_quality(months: List[date]) -> Dict[date, Dict[str, Tuple[float, float]]]:
    """质量两件套（ROE 由面板给）：资产负债率、毛利率近 8 期标准差，均按 ann_date asof。"""
    bal = pl.read_parquet(FIN / 'balance.parquet',
                          columns=['ts_code', 'ann_date', 'debt_ratio', 'total_assets'])
    bal = bal.filter(pl.col('total_assets') > 0)
    debt = _asof_by_code(bal, 'ann_date', 'debt_ratio')

    inc = pl.read_parquet(FIN / 'income.parquet',
                          columns=['ts_code', 'end_date', 'ann_date', 'revenue', 'gross_profit'])
    inc = inc.filter((pl.col('revenue') > 0) & pl.col('gross_profit').is_not_null())
    inc = (inc.with_columns((pl.col('gross_profit') / pl.col('revenue')).alias('gm'))
           .sort(['ts_code', 'end_date'])
           .with_columns(pl.col('gm').rolling_std(window_size=8, min_samples=6)
                         .over('ts_code').alias('gm_std')))
    gmstd = _asof_by_code(inc, 'ann_date', 'gm_std')

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


def load_st_sets(months: List[date]) -> Dict[date, set]:
    path = ROOT / 'data/local_store/meta/st/st_intervals.parquet'
    df = pl.read_parquet(path, columns=['ts_code', 'start_date', 'end_date'])
    rows = df.rows()
    out: Dict[date, set] = {}
    for m in months:
        bad = set()
        for code, s, e in rows:
            if s is not None and s <= m and (e is None or e >= m):
                bad.add(code)
        out[m] = bad
    return out


# ---------------------------------------------------------------------------
# 打分
# ---------------------------------------------------------------------------

def pct_rank(vals: Dict[str, float]) -> Dict[str, float]:
    n = len(vals)
    if n < 2:
        return {c: 0.5 for c in vals}
    order = sorted(vals.items(), key=lambda kv: kv[1])
    return {code: i / (n - 1) for i, (code, _) in enumerate(order)}


def month_scores(day: date, universe: List[str], panel: Dict, quality: Dict,
                 mom: Dict) -> Dict[str, Dict[str, float]]:
    """返回 {code: {'value':..,'quality':..,'momentum':..}}，缺失维度不出现在字典里。"""
    p = panel.get(day, {})
    q = quality.get(day, {})
    mo = mom.get(day, {})

    ep = {c: p[c][0] for c in universe if c in p and p[c][0] is not None and p[c][0] > 0}
    bp = {c: p[c][1] for c in universe if c in p and p[c][1] is not None and p[c][1] > 0}
    roe = {c: p[c][2] for c in universe if c in p and p[c][2] is not None}
    debt = {c: q[c][0] for c in universe if c in q and q[c][0] is not None}
    gmstd = {c: q[c][1] for c in universe if c in q and q[c][1] is not None}
    mm = {c: mo[c] for c in universe if c in mo}

    r_ep, r_bp = pct_rank(ep), pct_rank(bp)
    r_roe = pct_rank(roe)
    r_debt = pct_rank({c: -v for c, v in debt.items()})
    r_gm = pct_rank({c: -v for c, v in gmstd.items()})

    value: Dict[str, float] = {}
    for c in universe:
        parts = [r for r in (r_ep.get(c), r_bp.get(c)) if r is not None]
        if parts:
            value[c] = sum(parts) / len(parts)
    quality: Dict[str, float] = {}
    for c in universe:
        parts = [r for r in (r_roe.get(c), r_debt.get(c), r_gm.get(c)) if r is not None]
        if len(parts) >= 2:
            quality[c] = sum(parts) / len(parts)
    momentum = {c: r for c, r in pct_rank(mm).items()}

    keys = {c: {} for c in universe}
    for c, s in value.items():
        keys[c]['value'] = s
    for c, s in quality.items():
        keys[c]['quality'] = s
    for c, s in momentum.items():
        keys[c]['momentum'] = s
    return {c: d for c, d in keys.items() if d}


def pick(path_scores: Dict[str, Dict[str, float]], dims: List[str],
         top_n: int) -> List[str]:
    cand = []
    for code, sc in path_scores.items():
        parts = [sc.get(d) for d in dims]
        if any(x is None for x in parts):
            continue
        cand.append((sum(parts) / len(parts), code))
    cand.sort(key=lambda x: (-x[0], x[1]))
    return sorted(c for _, c in cand[:top_n])


def build_paths(months: List[date], signals: Dict[date, Dict[str, Dict[str, float]]],
                dims: List[str], top_n: int, screen=None) -> Dict[date, List[str]]:
    """打分 → （可选）建仓前筛除 → 取前 N。

    ``screen(day, codes) -> set[str]`` 在评分之后、``pick`` 之前调用，
    返回要剔除的代码；缺省 None 时与未接入筛选逐值一致。
    """
    path = {}
    for m in months:
        scores = signals[m] if m in signals else {}
        if screen is not None and scores:
            dropped = screen(m, list(scores))
            if dropped:
                scores = {c: v for c, v in scores.items() if c not in dropped}
        names = pick(scores, dims, top_n)
        path[m] = names
    return path


# ---------------------------------------------------------------------------
# 回测与指标
# ---------------------------------------------------------------------------

def run_backtest(data, schedules, dividends, st_status) -> Dict[str, Any]:
    used = {code: data[code] for code in schedules if code in data}
    strategies = {code: base.MonthlyTargetWeightStrategy(sig)
                  for code, sig in schedules.items() if code in used and sig}
    used = {code: df for code, df in used.items() if code in strategies}
    engine = PortfolioBacktestEngine(used)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies(strategies)
    engine.set_cost_model(RoundTrip13bp())
    return engine.run_backtest(initial_capital=base.INITIAL_CAPITAL,
                               lot_size=100, industry_map={},
                               st_status=st_status, dividends=dividends,
                               pooled_cash=True)


def stats(curve: List[Tuple[date, float]], trades: List[Dict], a: date, b: date,
          holdings: List[int]) -> Dict[str, Any]:
    rows = [(d, e) for d, e in curve if a <= d <= b]
    if len(rows) < 5:
        return {'annual': None}
    years = (rows[-1][0] - rows[0][0]).days / 365.25
    eq0, eq1 = rows[0][1], rows[-1][1]
    annual = (eq1 / eq0) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    peak, mdd = rows[0][1], 0.0
    rets = []
    for i, (_, eq) in enumerate(rows):
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1.0)
        if i:
            rets.append(eq / rows[i - 1][1] - 1.0)
    vol = float(np.std(rets, ddof=1) * np.sqrt(252)) if len(rets) > 2 else 0.0
    monthly = {}
    for d, eq in rows:
        monthly[(d.year, d.month)] = eq
    ms = [monthly[k] for k in sorted(monthly)]
    mrets = [ms[i] / ms[i - 1] - 1.0 for i in range(1, len(ms))]
    win = float(np.mean([r > 0 for r in mrets])) if mrets else None
    sel = [t for t in trades if (t.get('date') and a <= base.to_date(t['date']) <= b)]
    buy = sum(t['price'] * t['shares'] for t in sel if t.get('signal') == 'buy')
    sell = sum(t['price'] * t['shares'] for t in sel if t.get('signal') == 'sell')
    cost = sum(float(t.get('cost') or 0.0) for t in sel)
    avg_eq = sum(e for _, e in rows) / len(rows)
    notional = buy + sell
    return {
        'annual': round(annual, 6), 'max_drawdown': round(mdd, 6),
        'vol': round(vol, 6),
        'sharpe': round(annual / vol, 4) if vol > 0 else None,
        'calmar': round(annual / abs(mdd), 4) if mdd < 0 else None,
        'win_rate_monthly': round(win, 4) if win is not None else None,
        'turnover_2way': round(notional / avg_eq / years, 4) if years > 0 else None,
        'cost_bps_of_turnover': round(cost / notional * 1e4, 2) if notional else None,
        'avg_holdings': round(float(np.mean(holdings)), 1) if holdings else None,
        'trades': len(sel), 'final_equity': round(eq1, 2),
    }


def evaluate(name: str, path: Dict[date, List[str]], data, dividends, st_status,
             prev_day) -> Dict[str, Any]:
    sched = v.schedules_from_path(path, prev_day)
    res = run_backtest(data, sched, dividends, st_status)
    curve = f.curve_of(res)
    trades = res.get('trades') or []
    row: Dict[str, Any] = {'run': name}
    for tag, (a, b) in SEGS.items():
        hold = [len(vv) for m, vv in path.items() if a <= m <= b]
        st = stats(curve, trades, a, b, hold)
        for k, val in st.items():
            row[f'{tag}_{k}' if tag != 'full' else k] = val
    # 落盘净值与交易
    out = RUN_DIR / name
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': [d for d, _ in curve],
                  'equity': [e for _, e in curve]}).write_parquet(out / 'equity.parquet')
    if trades:
        pl.DataFrame(trades, infer_schema_length=None).write_parquet(out / 'trades.parquet')
    pl.DataFrame({'signal_date': sorted(path),
                  'n': [len(path[m]) for m in sorted(path)]}).write_csv(out / 'holdings.csv')
    logger.info('{} => {}', name, json.dumps(
        {k: row[k] for k in ('annual', 'max_drawdown', 'sharpe', 'calmar', 'seg1_annual',
                             'seg2_annual', 'seg3_annual')}, ensure_ascii=False))
    del res, curve, trades
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit-months', type=int, default=0,
                    help='只跑前 N 个信号月（冒烟用，不写正式产物）')
    ap.add_argument('--skip-benchmark', action='store_true')
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level='INFO')
    logger.add(OUT_DIR / 'run.log', level='INFO', encoding='utf-8')

    t0 = time.perf_counter()
    data = base.load_universe(WARMUP_START, END)
    data, violations = base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if SIGNAL_START <= m <= date.fromisoformat(END)]
    if args.limit_months:
        months = months[:args.limit_months]
    logger.info('信号月 {} 个 {}~{}；标的 {} 只；引擎契约剔除 {} 只',
                len(months), months[0], months[-1], len(data), len(violations))

    quoted = v.quoted_sets(data, months)
    mom, bars = load_momentum(data, months)
    panel = load_panel_factors(months)
    quality = load_quality(months)
    st_sets = load_st_sets(months)

    # 可用性统计（写进报告）
    cov = []
    for m in months:
        univ = {c for c in quoted[m] if bars[m].get(c, 0) >= MIN_BARS and c not in st_sets[m]}
        n = len(univ)
        cnt = {
            'ep': sum(1 for c in univ if c in panel[m] and panel[m][c][0] not in (None, 0)
                      and panel[m][c][0] > 0),
            'bp': sum(1 for c in univ if c in panel[m] and panel[m][c][1] not in (None, 0)
                      and panel[m][c][1] > 0),
            'roe': sum(1 for c in univ if c in panel[m] and panel[m][c][2] is not None),
            'debt': sum(1 for c in univ if c in quality[m] and quality[m][c][0] is not None),
            'gmstd': sum(1 for c in univ if c in quality[m] and quality[m][c][1] is not None),
            'mom12': sum(1 for c in univ if c in mom[m]),
        }
        cov.append({'month': m.isoformat(), 'universe': n,
                    **{k: round(x / n, 4) if n else None for k, x in cnt.items()}})
    cov_df = pl.DataFrame(cov)
    cov_df.write_csv(OUT_DIR / 'coverage.csv')
    yearly = cov_df.with_columns(pl.col('month').str.slice(0, 4).alias('year')).group_by('year').agg(
        pl.col('universe').mean().round(0).alias('universe'),
        *[pl.col(c).mean().round(3).alias(c) for c in
          ('ep', 'bp', 'roe', 'debt', 'gmstd', 'mom12')]).sort('year')
    logger.info('因子覆盖率（年均）:\n{}', yearly)

    signals = {m: month_scores(m, [c for c in quoted[m]
                                   if bars[m].get(c, 0) >= MIN_BARS and c not in st_sets[m]],
                               panel, quality, mom) for m in months}
    logger.info('打分完成：{} 个月', len(signals))

    dividends = _load_local_dividends(list(data), None, None)
    st_status = _load_st_status(None, data, None, WARMUP_START, END, None, None)

    rows: List[Dict[str, Any]] = []
    if not args.skip_benchmark:
        bench = {m: sorted(c for c in quoted[m]
                           if bars[m].get(c, 0) >= MIN_BARS and c not in st_sets[m])
                 for m in months}
        rows.append(evaluate('benchmark_all_a_ew', bench, data, dividends,
                             st_status, prev_day))

    schemes = {'value': ['value'], 'quality': ['quality'],
               'momentum': ['momentum'], 'combo': ['value', 'quality', 'momentum']}
    freqs = {'monthly': months, 'bimonthly': months[::2]}
    for scheme, dims in schemes.items():
        for freq, ml in freqs.items():
            for n in (20, 30, 50):
                name = f'{scheme}_{freq}_n{n}'
                path = build_paths(ml, signals, dims, n)
                rows.append(evaluate(name, path, data, dividends, st_status, prev_day))

    cmp_df = pl.DataFrame(rows, infer_schema_length=None)
    cmp_df.write_csv(OUT_DIR / 'compare.csv')

    # 归因：combo 逐维度删除（n=30 月频）
    att: List[Dict[str, Any]] = []
    base_row = next(r for r in rows if r['run'] == 'combo_monthly_n30')
    for drop, dims in (('value', ['quality', 'momentum']),
                       ('quality', ['value', 'momentum']),
                       ('momentum', ['value', 'quality'])):
        name = f'combo_no_{drop}_monthly_n30'
        path = build_paths(months, signals, dims, 30)
        r = evaluate(name, path, data, dividends, st_status, prev_day)
        r['removed'] = drop
        r['marginal_annual'] = round((base_row['annual'] or 0) - (r['annual'] or 0), 6)
        r['marginal_sharpe'] = round((base_row['sharpe'] or 0) - (r['sharpe'] or 0), 4)
        att.append(r)
    pl.DataFrame(att, infer_schema_length=None).write_csv(OUT_DIR / 'attribution.csv')

    logger.info('总耗时 {:.1f}s', time.perf_counter() - t0)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
