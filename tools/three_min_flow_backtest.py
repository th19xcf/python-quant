#!/usr/bin/env python
"""三分钟选股法 A/B 对照回测：日线近似「资金流」是否有增量。

A 组（基准）：成交额 + 资产负债率 + 净利润同比（EPS 代理）+ 换手率
             → 当日涨幅前 10 行业 → PB < 行业均值×1.2 → 按 BP 取 topN
B 组：A 组候选池（BP 前 60）内，额外要求 OBV20 上行且量比 LB ≥ 1。

口径：
- 信号日 = 月度研究面板 ``analysis_monthly`` 的月末交易日（2015-01~2026-10）；
  t 日收盘出信号，t+1 开盘成交（与 position_management_sim 一致）；
- 止盈 +12%（10%~15% 区间中值）、止损 -5%，收盘触发、次日开盘执行；
- 费用唯一来源 ``src.backtest.costs.cost_model.CostModel``；
- 指标复用 ``src.tech_analysis`` 的 OBV / 量比实现，不新增指标；
- 绩效指标复用 ``src.advice.factor_eval._curve_stats``。

用法::

    .venv\\Scripts\\python.exe tools/three_min_flow_backtest.py --verify-indicator
    .venv\\Scripts\\python.exe tools/three_min_flow_backtest.py            # dry-run
    .venv\\Scripts\\python.exe tools/three_min_flow_backtest.py --apply    # 落运行记录
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.factor_eval import _curve_stats
from src.backtest.costs.cost_model import CostModel
from src.risk.execution_rules import ExecutionRules
from src.tech_analysis.indicator_calculator import calculate_obv_polars
from src.tech_analysis.indicators.volume import calculate_lb

STORE = ROOT / 'data' / 'local_store'
MONTHLY = STORE / 'meta/research_inputs/analysis_monthly.parquet'
NEW_FACTORS = STORE / 'meta/research_inputs/new_factors.parquet'
FACTOR_PANEL = STORE / 'factors/factor_panel.parquet'
VALUATION = STORE / 'meta/financials/valuation_daily.parquet'
INCOME = STORE / 'meta/financials/income.parquet'
BALANCE = STORE / 'meta/financials/balance.parquet'
CAL_INDEX = STORE / 'quotes/index/sh/000001.SH.parquet'
QUOTES_STOCK = STORE / 'quotes/stock'
DEFAULT_OUT = STORE / 'backtest_runs'
INITIAL = 1_000_000.0


def log(msg: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


# ---------------------------------------------------------------- 数据装载
def load_monthly(start: date, end: date) -> pl.DataFrame:
    m = pl.read_parquet(MONTHLY, columns=['ts_code', 'date', 'amount_ma20'])
    n = pl.read_parquet(NEW_FACTORS, columns=['ts_code', 'date', 'turnover_20'])
    df = m.join(n, on=['ts_code', 'date'], how='inner').filter(
        (pl.col('date') >= start) & (pl.col('date') <= end)).sort(['date', 'ts_code'])
    log(f'月度面板：{df.height} 行 / {df["date"].n_unique()} 个月')
    return df


def load_calendar(start: date, end: date) -> list[date]:
    cal = pl.read_parquet(CAL_INDEX, columns=['date'])['date'].to_list()
    return sorted(d for d in cal if start <= d <= end)


def load_industry_returns(signal_dates: list[date],
                          prev_map: dict[date, date]) -> pl.DataFrame:
    """行业内个股等权当日涨幅（估值表 close 派生，|ret|>35% 视为除权跳变剔除）。"""
    need = sorted(set(signal_dates) | set(prev_map.values()))
    val = pl.scan_parquet(VALUATION).filter(
        pl.col('trade_date').is_in(need)).select(
        'ts_code', 'trade_date', 'close').collect()
    ind = pl.scan_parquet(FACTOR_PANEL).filter(
        pl.col('signal_date').is_in(need)).select(
        pl.col('signal_date').alias('trade_date'), 'ts_code', 'bp', 'industry_l1').collect()
    pairs = pl.DataFrame({'trade_date': signal_dates,
                          'prev_date': [prev_map[d] for d in signal_dates]})
    cur = val.join(pairs, on='trade_date')
    prv = val.rename({'trade_date': 'prev_date', 'close': 'prev_close'}).select(
        'ts_code', 'prev_date', 'prev_close')
    joined = cur.join(prv, on=['ts_code', 'prev_date'], how='inner').with_columns(
        (pl.col('close') / pl.col('prev_close') - 1.0).alias('ret')).filter(
        pl.col('ret').is_finite() & (pl.col('ret').abs() < 0.35))
    joined = joined.join(
        ind.select('ts_code', 'trade_date', 'industry_l1'),
        on=['ts_code', 'trade_date'], how='inner').drop_nulls('industry_l1')
    out = joined.group_by(['trade_date', 'industry_l1']).agg(
        pl.col('ret').mean().alias('ind_ret'), pl.len().alias('n')).filter(
        pl.col('n') >= 5)
    log(f'行业收益：{out.height} 行 / {out["trade_date"].n_unique()} 个月')
    return out


def _asof_table(path: Path, value_col: str) -> pl.DataFrame:
    df = pl.read_parquet(path, columns=['ts_code', 'ann_date', 'end_date', value_col])
    df = df.drop_nulls(['ann_date', value_col]).sort(
        ['ts_code', 'ann_date', 'end_date']).unique(
        subset=['ts_code', 'ann_date'], keep='last')
    return df.select('ts_code', 'ann_date', value_col).sort(['ts_code', 'ann_date'])


def build_pool(monthly: pl.DataFrame, signal_dates: list[date],
               ind_returns: pl.DataFrame, top_n: int, eval_depth: int,
               amount_floor: float, turn_lo: float, turn_hi: float,
               yoy_floor: float, debt_cap: float) -> tuple[pl.DataFrame, pl.DataFrame]:
    top10 = ind_returns.sort(['trade_date', 'ind_ret'],
                             descending=[False, True]).group_by(
        'trade_date', maintain_order=True).head(10).select(
        'trade_date', 'industry_l1')
    base = monthly.filter(
        (pl.col('amount_ma20') >= amount_floor)
        & (pl.col('turnover_20') >= turn_lo)
        & (pl.col('turnover_20') <= turn_hi))
    labels = pl.scan_parquet(FACTOR_PANEL).filter(
        pl.col('signal_date').is_in(signal_dates)).select(
        pl.col('signal_date').alias('date'), 'ts_code', 'bp', 'industry_l1').collect()
    base = base.join(labels, on=['ts_code', 'date'], how='inner')
    pool = base.join(
        ind_returns.select(pl.col('trade_date').alias('date'), 'industry_l1'),
        on=['date', 'industry_l1'], how='inner')
    pool = pool.join(
        top10.select(pl.col('trade_date').alias('date'), 'industry_l1'),
        on=['date', 'industry_l1'], how='inner')
    inc = _asof_table(INCOME, 'net_profit_yoy')
    bal = _asof_table(BALANCE, 'debt_ratio')
    pool = pool.sort(['ts_code', 'date'])
    pool = pool.join_asof(inc, left_on='date', right_on='ann_date', by='ts_code',
                          strategy='backward')
    pool = pool.join_asof(bal, left_on='date', right_on='ann_date', by='ts_code',
                          strategy='backward')
    pool = pool.filter((pl.col('net_profit_yoy') > yoy_floor)
                       & (pl.col('debt_ratio') < debt_cap)
                       & (pl.col('bp') > 0) & pl.col('bp').is_finite())
    pool = pool.with_columns((1.0 / pl.col('bp')).alias('pb'))
    mean_pb = pool.group_by(['date', 'industry_l1']).agg(
        pl.col('pb').mean().alias('ind_mean_pb'), pl.len().alias('ind_n'))
    pool = pool.join(mean_pb, on=['date', 'industry_l1'], how='inner').filter(
        (pl.col('ind_n') < 5) | (pl.col('pb') < pl.col('ind_mean_pb') * 1.2))
    ranked = pool.sort(['date', 'bp'], descending=[False, True]).group_by(
        'date', maintain_order=True).head(eval_depth)
    log(f'候选池：{pool.height} 行，评估池（每期 BP 前 {eval_depth}）{ranked.height} 行')
    return ranked, top10


# ---------------------------------------------------------------- 指标与行情
def stock_quote_map() -> dict[str, Path]:
    out: dict[str, Path] = {}
    for market in ('sh', 'sz', 'bj'):
        d = QUOTES_STOCK / market
        if not d.exists():
            continue
        for p in d.glob('*.parquet'):
            out[p.stem] = p
    return out


def load_quotes(codes: list[str], paths: dict[str, Path],
                since: date) -> dict[str, pl.DataFrame]:
    frames: dict[str, pl.DataFrame] = {}
    t0 = time.time()
    for i, code in enumerate(sorted(codes), 1):
        p = paths.get(code)
        if p is None:
            continue
        try:
            df = pl.read_parquet(p, columns=[
                'date', 'open', 'high', 'low', 'close', 'pre_close', 'volume'])
        except Exception:  # noqa: BLE001
            continue
        df = df.filter(pl.col('date') >= since).sort('date')
        if df.height >= 30:
            frames[code] = df
        if i % 200 == 0:
            log(f'  行情装载 {i}/{len(codes)}（{time.time() - t0:.0f}s）')
    log(f'行情装载完成：{len(frames)} 只（{time.time() - t0:.0f}s）')
    return frames


def add_flow_indicators(frames: dict[str, pl.DataFrame],
                        signal_dates: set[date]) -> dict[tuple[str, date], tuple[float, float]]:
    """复用指标库：OBV 20 日变化 + 量比。返回 (code, date) → (obv20, lb)。"""
    out: dict[tuple[str, date], tuple[float, float]] = {}
    for code, df in frames.items():
        try:
            with_obv = calculate_obv_polars(df)
            with_all = calculate_lb(with_obv.lazy()).collect().with_columns(
                (pl.col('obv') - pl.col('obv').shift(20)).alias('obv20'))
        except Exception as exc:  # noqa: BLE001
            log(f'  指标失败 {code}: {exc}')
            continue
        for row in with_all.filter(pl.col('date').is_in(signal_dates)).iter_rows(
                named=True):
            obv20 = row.get('obv20')
            lb = row.get('lb')
            if obv20 is None or lb is None or not math.isfinite(float(obv20)):
                continue
            out[(code, row['date'])] = (float(obv20), float(lb))
    return out


# ---------------------------------------------------------------- 组合模拟
def simulate(picks: dict[date, list[str]], frames: dict[str, pl.DataFrame],
             calendar: list[date], rebalance_days: dict[date, date],
             top_n: int, take_profit: float, stop_loss: float) -> dict:
    rules = ExecutionRules()
    cost_model = CostModel()
    rows_by_date: dict[str, dict[date, dict]] = {}
    for code, df in frames.items():
        rows_by_date[code] = {r['date']: r for r in df.select(
            'date', 'open', 'close', 'pre_close', 'volume').iter_rows(named=True)}
    cal = [d for d in calendar if d >= min(rebalance_days.values())]

    cash = INITIAL
    positions: dict[str, dict] = {}
    pending: dict[str, str] = {}
    trades: list[dict] = []
    curve: list[dict] = []
    pending_sell_cost = 0.0
    total_buy = 0.0
    total_cost = 0.0
    last_close: dict[str, float] = {}

    def can_trade(code: str, day: date, side: str) -> tuple[bool, dict | None]:
        row = rows_by_date.get(code, {}).get(day)
        if row is None or not row['open'] or row['volume'] <= 0:
            return False, row
        prev_close = row['pre_close']
        if not prev_close:
            return False, row
        limit = rules.resolve_limit_pct(code, is_st=None, trade_date=day)
        move = row['open'] / prev_close - 1.0
        if side == 'buy':
            return abs(move) < limit - 0.004, row
        return move > -limit + 0.004, row

    def equity_now(day: date) -> float:
        eq = cash
        for code, pos in positions.items():
            px = last_close.get(code)
            if px is None:
                row = rows_by_date.get(code, {}).get(day)
                px = row['close'] if row and row['close'] else pos['entry_price']
            eq += pos['shares'] * px
        return eq

    def sell(code: str, day: date, reason: str) -> bool:
        nonlocal cash, total_cost
        ok, row = can_trade(code, day, 'sell')
        if not ok:
            return False
        pos = positions[code]
        price = float(row['open'])
        proceeds = pos['shares'] * price
        fee = cost_model.calculate_sell_cost(
            pos['shares'], price, trade_date=day, ts_code=code)
        cash += proceeds - fee
        total_cost += fee
        pnl = proceeds - fee - pos['cost_basis']
        trades.append({
            'ts_code': code, 'entry_date': pos['entry_date'], 'exit_date': day,
            'exit_reason': reason, 'entry_price': pos['entry_price'],
            'exit_price': price, 'shares': pos['shares'],
            'ret': pnl / max(pos['cost_basis'], 1e-9), 'pnl': pnl,
            'held_days': pos['held_days'],
        })
        del positions[code]
        return True

    for day in cal:
        for code, reason in list(pending.items()):
            if code not in positions or sell(code, day, reason):
                pending.pop(code, None)
        target = None
        if day in rebalance_days.values():
            sig = next(s for s, d in rebalance_days.items() if d == day)
            target = picks.get(sig, [])
        if target is not None:
            for code in list(positions):
                if code not in target and not sell(code, day, 'rebalance'):
                    pending[code] = 'rebalance'
            eq_open = equity_now(day)
            slots = max(1, len(target))
            for code in target:
                if code in positions:
                    continue
                ok, row = can_trade(code, day, 'buy')
                if not ok:
                    continue
                price = float(row['open'])
                notional = min(cash * 0.98, eq_open / slots)
                shares = int(notional / price / 100) * 100
                if shares <= 0:
                    continue
                fee = cost_model.calculate_buy_cost(
                    shares, price, trade_date=day, ts_code=code)
                cost_basis = shares * price + fee
                if cost_basis > cash:
                    continue
                cash -= cost_basis
                total_buy += shares * price
                total_cost += fee
                positions[code] = {
                    'shares': shares, 'entry_price': price, 'entry_date': day,
                    'cost_basis': cost_basis, 'held_days': 0,
                }
                last_close[code] = price
        for code, pos in positions.items():
            row = rows_by_date.get(code, {}).get(day)
            if row and row['close']:
                last_close[code] = float(row['close'])
                pos['held_days'] += 1
                ret = row['close'] / pos['entry_price'] - 1.0
                if ret >= take_profit:
                    pending[code] = 'take_profit'
                elif ret <= -stop_loss:
                    pending[code] = 'stop_loss'
        curve.append({'date': day, 'equity': equity_now(day),
                      'cash': cash, 'positions': len(positions)})

    curve_df = pl.DataFrame(curve)
    stats = _curve_stats(curve_df, INITIAL, len(curve))
    wins = sum(1 for t in trades if t['pnl'] > 0)
    years = max(len(curve) / 252.0, 1e-9)
    avg_eq = float(curve_df['equity'].mean()) if curve_df.height else INITIAL
    stats.update({
        'trades': len(trades),
        'trade_win_rate': wins / len(trades) if trades else 0.0,
        'avg_positions': float(curve_df['positions'].mean()) if curve_df.height else 0.0,
        'avg_held_days': float(np.mean([t['held_days'] for t in trades])) if trades else 0.0,
        'annual_turnover': total_buy / avg_eq / years if avg_eq > 0 else 0.0,
        'total_buy_notional': total_buy,
        'total_cost': total_cost,
        'exits': {k: sum(1 for t in trades if t['exit_reason'] == k)
                  for k in ('take_profit', 'stop_loss', 'rebalance')},
    })
    return {'curve': curve_df, 'trades': trades, 'stats': stats}


# ---------------------------------------------------------------- 指标对拍
def verify_indicator() -> None:
    q = pl.read_parquet(QUOTES_STOCK / 'sh' / '600519.SH.parquet').sort(
        'date').tail(60)
    lib = calculate_lb(calculate_obv_polars(q).lazy()).collect()
    man_obv = (np.sign(q['close'].diff().fill_null(0)) * q['volume']).cum_sum()
    man_lb = q['volume'] / q['volume'].shift(1).rolling_mean(5, min_samples=1)
    obv_diff = abs(float(lib['obv'][-1]) - float(man_obv[-1]))
    lb_diff = float((lib['lb'] - man_lb).abs().max())
    print(json.dumps({'code': '600519.SH', 'rows': q.height,
                      'obv_lib': float(lib['obv'][-1]),
                      'obv_manual': float(man_obv[-1]),
                      'obv_abs_diff': obv_diff,
                      'lb_max_abs_diff': lb_diff}, ensure_ascii=False, indent=2))


# ---------------------------------------------------------------- 主流程
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='三分钟选股法 A/B 回测（默认 dry-run）')
    ap.add_argument('--apply', action='store_true', help='实跑并写运行记录')
    ap.add_argument('--verify-indicator', action='store_true', help='只做指标对拍')
    ap.add_argument('--top-n', type=int, default=20)
    ap.add_argument('--eval-depth', type=int, default=60)
    ap.add_argument('--take-profit', type=float, default=0.12)
    ap.add_argument('--stop-loss', type=float, default=0.05)
    ap.add_argument('--start', type=date.fromisoformat, default=date(2015, 1, 1))
    ap.add_argument('--end', type=date.fromisoformat, default=date(2026, 10, 31))
    ap.add_argument('--out', type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)
    if args.verify_indicator:
        verify_indicator()
        return 0

    t0 = time.time()
    monthly = load_monthly(args.start, args.end)
    signal_dates = monthly['date'].unique().sort().to_list()
    cal_all = load_calendar(date(2015, 1, 1), date(2026, 11, 20))
    prev_of = {d: cal_all[i - 1] for i, d in enumerate(cal_all)
               if i > 0 and d in set(signal_dates)}
    rebalance_days = {d: cal_all[cal_all.index(d) + 1] for d in signal_dates
                      if d in cal_all and cal_all.index(d) + 1 < len(cal_all)}
    signal_dates = [d for d in signal_dates if d in rebalance_days]
    ind_ret = load_industry_returns(signal_dates, prev_of)
    ranked, _ = build_pool(
        monthly, signal_dates, ind_ret, args.top_n, args.eval_depth,
        amount_floor=5e7, turn_lo=0.03, turn_hi=0.15, yoy_floor=8.0, debt_cap=60.0)

    codes = ranked['ts_code'].unique().to_list()
    paths = stock_quote_map()
    frames = load_quotes(codes, paths, date(2014, 12, 1))
    flow = add_flow_indicators(frames, set(signal_dates))

    def pick_rows(use_flow: bool) -> dict[date, list[str]]:
        rows = ranked.to_dicts()
        out: dict[date, list[str]] = {}
        for r in rows:
            key = (r['ts_code'], r['date'])
            if use_flow:
                f = flow.get(key)
                if f is None or f[0] <= 0 or f[1] < 1.0:
                    continue
            out.setdefault(r['date'], []).append(r['ts_code'])
        for d in out:
            out[d] = out[d][:args.top_n]
        return out

    picks_a, picks_b = pick_rows(False), pick_rows(True)
    log(f'信号：A {sum(len(v) for v in picks_a.values())} 个持仓机会 / '
        f'B {sum(len(v) for v in picks_b.values())} 个')

    if not args.apply:
        print(json.dumps({'mode': 'dry-run', 'months': len(signal_dates),
                          'a_picks': sum(len(v) for v in picks_a.values()),
                          'b_picks': sum(len(v) for v in picks_b.values()),
                          'top_n': args.top_n, 'take_profit': args.take_profit,
                          'stop_loss': args.stop_loss}, ensure_ascii=False, indent=2))
        return 0

    keep = {c for v in picks_a.values() for c in v} | {
        c for v in picks_b.values() for c in v}
    sim_frames = {c: frames[c] for c in keep if c in frames}
    res_a = simulate(picks_a, sim_frames, cal_all, rebalance_days,
                     args.top_n, args.take_profit, args.stop_loss)
    res_b = simulate(picks_b, sim_frames, cal_all, rebalance_days,
                     args.top_n, args.take_profit, args.stop_loss)
    delta = {k: res_b['stats'][k] - res_a['stats'][k]
             for k in ('annualized', 'max_drawdown', 'sharpe', 'calmar',
                       'trade_win_rate', 'annual_turnover')}
    summary = {
        'generated_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'period': [str(signal_dates[0]), str(signal_dates[-1])],
        'months': len(signal_dates),
        'config': {'top_n': args.top_n, 'eval_depth': args.eval_depth,
                   'take_profit': args.take_profit, 'stop_loss': args.stop_loss,
                   'amount_floor': 5e7, 'turnover_range': [0.03, 0.15],
                   'net_profit_yoy_floor': 8.0, 'debt_ratio_cap': 60.0,
                   'industry_top': 10, 'pb_cap_x_industry_mean': 1.2,
                   'flow_filter': 'OBV20>0 且 量比>=1'},
        'A': res_a['stats'], 'B': res_b['stats'], 'B_minus_A': delta,
        'elapsed_s': round(time.time() - t0, 1),
    }
    run_dir = args.out / f'three_min_ab_{time.strftime("%Y%m%d")}'
    run_dir.mkdir(parents=True, exist_ok=True)
    for name, res in (('A', res_a), ('B', res_b)):
        d = run_dir / name
        d.mkdir(exist_ok=True)
        res['curve'].write_csv(d / 'equity.csv')
        pl.DataFrame(res['trades']).write_csv(d / 'trades.csv')
        (d / 'metrics.json').write_text(json.dumps(
            res['stats'], ensure_ascii=False, indent=2), encoding='utf-8')
    (run_dir / 'summary.json').write_text(json.dumps(
        summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    log(f'完成：{run_dir}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
