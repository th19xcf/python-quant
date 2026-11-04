#!/usr/bin/env python3
"""最小可验证实验：ETF 12-1 动量月频轮动（只读本地日线）。

口径
----
* 标的池：MySQL ``fund_basic`` 中 ``fund_type='ETF'`` 且有本地行情的代码，
  剔除 ``placeholder_quote_codes.csv`` 登记的 96 只占位桩；
* 时点选池：每个月末，用「过去 60 个交易日日均成交额」排序取前 30 只，
  且要求至少 252 个交易日历史（避免用未来流动性选池）；
* 信号：12-1 动量 = 复权收盘价[t-21] / 复权收盘价[t-252] - 1；
* 组合：动量前 5 名等权，月末收盘出信号、下一交易日**开盘**成交；
* 成本：完整往返 0.154%，即单边 0.077%，按调仓换手率计
  （全仓换股 turnover=2 → 成本 2×0.077%=0.154%）；
* 基准：同一时点合格池等权、同频调仓、同成本。

复权：``close × qfq_factor``（基金因子快照 ``factors/fund_adj_factor.parquet``）。

运行::

    .venv\\Scripts\\python.exe tools/etf_momentum_backtest.py

产物：``reports/etf_audit/momentum_nav.csv``、
``reports/etf_audit/momentum_result.json``。不改生产代码、不写库、不提交 git。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

STORE = PROJECT_ROOT / 'data' / 'local_store'
QUOTES = STORE / 'quotes' / 'fund'
FACTORS = STORE / 'factors'
ANOMALIES = STORE / 'meta' / 'anomalies'
OUT_DIR = PROJECT_ROOT / 'reports' / 'etf_audit'
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_POOL = 30          # 时点流动性前 30
HOLD_K = 5             # 持仓只数
COST_ROUND_TRIP = 0.00154
COST_SIDE = COST_ROUND_TRIP / 2.0
MIN_HISTORY = 252
LOOKBACK_LONG = 252
LOOKBACK_SKIP = 21
JUMP_THRESHOLD = 0.35   # 份额折算/拆分识别阈值（ETF 单日真实波动不会到 35%）
NON_EQUITY_KEYWORDS = ('债', '黄金', '白银', '豆粕', '原油', '商品', '货币',
                       '日利', '添益', '现金', '存单', '短融', '政金', '信用')


def is_equity_etf(name: str | None) -> bool:
    name = name or ''
    return not any(k in name for k in NON_EQUITY_KEYWORDS)


def load_etf_codes(equity_only: bool = True) -> tuple[list[str], dict]:
    from sqlalchemy import create_engine, text

    from src.utils.config import get_config

    d = get_config().database
    engine = create_engine(
        f'mysql+pymysql://{d.username}:{d.password}@{d.host}:{d.port}/{d.database}',
        connect_args={'connect_timeout': 10})
    with engine.connect() as s:
        rows = s.execute(text(
            "select ts_code, name, list_date from fund_basic where fund_type='ETF'")).fetchall()
    meta = pl.read_parquet(STORE / 'meta' / 'daily_index.parquet').filter(
        pl.col('data_type') == 'fund')
    local = set(meta['ts_code'].to_list())
    ph = set(pl.read_csv(ANOMALIES / 'placeholder_quote_codes.csv')['ts_code'].to_list())
    names = {r[0]: r[1] for r in rows}
    codes = sorted(r[0] for r in rows
                   if r[0] in local and r[0] not in ph
                   and (not equity_only or is_equity_etf(names.get(r[0]))))
    return codes, names


def load_panel(codes: list[str]) -> pd.DataFrame:
    """返回长表 date/ts_code/adj_open/adj_close/amount。"""
    factors = pl.read_parquet(FACTORS / 'fund_adj_factor.parquet').select(
        ['ts_code', 'trade_date', 'qfq_factor'])
    factors = factors.filter(pl.col('ts_code').is_in(codes))
    factor_by_code = {(k[0],): v for k, v in
                      factors.partition_by('ts_code', as_dict=True).items()}
    frames = []
    for code in codes:
        market = 'sh' if code.endswith('SH') else 'sz'
        path = QUOTES / market / f'{code}.parquet'
        if not path.exists():
            continue
        q = pl.read_parquet(path).select(
            ['date', 'ts_code', 'open', 'close', 'amount', 'volume'])
        q = q.with_columns(pl.col('date').cast(pl.Date).alias('trade_date'))
        f = factor_by_code.get((code,))
        if f is not None:
            f = f.select(['trade_date', 'qfq_factor'])
            q = q.join(f, on='trade_date', how='left')
        else:
            q = q.with_columns(pl.lit(1.0).alias('qfq_factor'))
        # 因子快照与行情同源同区间；极少数缺口按前向/1.0 兜底
        q = q.sort('trade_date').with_columns(
            pl.col('qfq_factor').forward_fill().fill_null(1.0))
        q = q.with_columns([
            (pl.col('close') * pl.col('qfq_factor')).alias('adj_close'),
            (pl.col('open') * pl.col('qfq_factor')).alias('adj_open'),
        ])
        # 份额折算修复：ETF 拆分日常出现 -40%~-60% 的原始价跳变，事件表覆盖不全、
        # 个别因子方向也有误。这里仅对「复权后单日 |收益| > 35%」的极值按拆分处理：
        # 当日经济收益记为 0，并用 clean 复权价重标定 open，避免污染动量。
        q = q.with_columns(
            (pl.col('adj_close') / pl.col('adj_close').shift(1) - 1.0).alias('raw_ret'))
        q = q.with_columns(
            pl.when(pl.col('raw_ret').abs() > JUMP_THRESHOLD)
            .then(0.0).otherwise(pl.col('raw_ret')).alias('ret_clean'))
        base = q['adj_close'][0]
        q = q.with_columns(
            ((pl.col('ret_clean').fill_null(0.0) + 1.0).cum_prod() * base)
            .alias('adj_close_clean'))
        q = q.with_columns(
            (pl.col('adj_close_clean') / pl.col('close')).alias('scale'))
        q = q.with_columns(
            (pl.col('open') * pl.col('scale')).alias('adj_open_clean'))
        repaired = q.filter(pl.col('raw_ret').abs() > JUMP_THRESHOLD).height
        if repaired:
            print(f'[backtest] 拆分跳变修复 {code}: {repaired} 日')
        q = q.select(['trade_date', 'ts_code', 'adj_open_clean', 'adj_close_clean',
                      'amount', 'volume', 'close', 'open', 'raw_ret']).rename({
            'adj_open_clean': 'adj_open', 'adj_close_clean': 'adj_close'})
        frames.append(q)
    df = pl.concat(frames, how='vertical_relaxed').to_pandas()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    return float((nav / peak - 1.0).min())


def perf(nav: pd.Series) -> dict:
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    total = nav.iloc[-1] / nav.iloc[0]
    ann = total ** (1 / years) - 1 if years > 0 else np.nan
    ret = nav.pct_change().dropna()
    vol = ret.std() * np.sqrt(252)
    sharpe = (ret.mean() * 252) / vol if vol else np.nan
    mdd = max_drawdown(nav)
    return {
        'start': str(nav.index[0].date()),
        'end': str(nav.index[-1].date()),
        'years': round(years, 2),
        'total_return': round(float(total - 1), 4),
        'annual_return': round(float(ann), 4),
        'volatility': round(float(vol), 4),
        'sharpe': round(float(sharpe), 3),
        'max_drawdown': round(mdd, 4),
        'calmar': round(float(ann / abs(mdd)), 3) if mdd else np.nan,
    }


def build_month_ends(calendar: pd.DatetimeIndex) -> list[pd.Timestamp]:
    s = pd.Series(calendar, index=calendar)
    return list(s.groupby([s.index.year, s.index.month]).last().values)


def run(pivot_close, pivot_open, pivot_amt, calendar, month_ends, k: int,
        equal_weight_benchmark: bool) -> tuple[pd.Series, list[dict]]:
    nav = pd.Series(index=calendar, dtype=float)
    nav.iloc[0] = 1.0
    weights: dict[str, float] = {}
    trades = []
    rebalance_set = set(month_ends)

    def close_to_close(prev_d, cur_d, w):
        r = 0.0
        for code, wt in w.items():
            p0 = pivot_close.at[prev_d, code] if code in pivot_close.columns else np.nan
            p1 = pivot_close.at[cur_d, code] if code in pivot_close.columns else np.nan
            r += wt * (p1 / p0 - 1.0) if (pd.notna(p0) and pd.notna(p1) and p0 > 0) else 0.0
        return r

    def overnight(prev_d, cur_d, w):
        r = 0.0
        for code, wt in w.items():
            p0 = pivot_close.at[prev_d, code] if code in pivot_close.columns else np.nan
            o1 = pivot_open.at[cur_d, code] if code in pivot_open.columns else np.nan
            r += wt * (o1 / p0 - 1.0) if (pd.notna(p0) and pd.notna(o1) and p0 > 0) else 0.0
        return r

    def intraday(cur_d, w):
        r = 0.0
        for code, wt in w.items():
            o1 = pivot_open.at[cur_d, code] if code in pivot_open.columns else np.nan
            c1 = pivot_close.at[cur_d, code] if code in pivot_close.columns else np.nan
            r += wt * (c1 / o1 - 1.0) if (pd.notna(o1) and pd.notna(c1) and o1 > 0) else 0.0
        return r

    for i in range(1, len(calendar)):
        d = calendar[i]
        prev = calendar[i - 1]
        if prev in rebalance_set:
            # 用 prev 收盘信号，在 d 开盘调仓：旧仓吃隔夜，新仓吃日内
            amt_hist = pivot_amt.loc[:prev].tail(60)
            avg_amt = amt_hist.mean()
            close_hist = pivot_close.loc[:prev]
            if len(close_hist) <= LOOKBACK_LONG:
                nav.iloc[i] = nav.iloc[i - 1] * (1.0 + close_to_close(prev, d, weights))
                continue
            valid = close_hist.columns[
                close_hist.notna().sum() >= MIN_HISTORY + LOOKBACK_SKIP]
            pool = avg_amt[valid].dropna().sort_values(ascending=False).head(TOP_POOL)
            pool = pool[pool > 0]
            if equal_weight_benchmark:
                target = {c: 1.0 / len(pool) for c in pool.index} if len(pool) else {}
                picked = list(pool.index)
            else:
                mom = (close_hist.iloc[-LOOKBACK_SKIP - 1] /
                       close_hist.iloc[-LOOKBACK_LONG - 1] - 1.0)
                mom = mom[mom.index.isin(pool.index)].dropna()
                picked = list(mom.sort_values(ascending=False).head(k).index)
                target = {c: 1.0 / len(picked) for c in picked} if picked else {}
            turnover = sum(abs(target.get(c, 0.0) - weights.get(c, 0.0))
                           for c in set(target) | set(weights))
            cost = turnover * COST_SIDE
            nav.iloc[i] = nav.iloc[i - 1] * (1.0 + overnight(prev, d, weights))
            nav.iloc[i] *= (1.0 - cost) * (1.0 + intraday(d, target))
            trades.append({
                'rebalance': str(prev.date()),
                'execute': str(d.date()),
                'pool_size': int(len(pool)),
                'picked': picked,
                'turnover': round(turnover, 4),
                'cost': round(cost, 6),
            })
            weights = target
        else:
            nav.iloc[i] = nav.iloc[i - 1] * (1.0 + close_to_close(prev, d, weights))
    return nav, trades


def main(universe: str = 'equity') -> None:
    equity_only = universe == 'equity'
    codes, names = load_etf_codes(equity_only=equity_only)
    print(f'[backtest] 标的池={universe}，ETF 代码数: {len(codes)}')
    panel = load_panel(codes)
    print(f'[backtest] 面板行数: {panel.height if hasattr(panel, "height") else len(panel)}')

    # 复权收益合理性检查
    rets = panel.groupby('ts_code')['adj_close'].pct_change()
    extreme = int((rets.abs() > 0.20).sum())
    print(f'[backtest] 复权后单日 |收益|>20% 行数: {extreme}')

    pivot_close = panel.pivot(index='trade_date', columns='ts_code', values='adj_close')
    pivot_open = panel.pivot(index='trade_date', columns='ts_code', values='adj_open')
    pivot_amt = panel.pivot(index='trade_date', columns='ts_code', values='amount')
    calendar = pivot_close.index[pivot_close.notna().sum(axis=1) > 0]
    month_ends = build_month_ends(calendar)

    nav_m, trades_m = run(pivot_close, pivot_open, pivot_amt, calendar, month_ends,
                          HOLD_K, False)
    nav_b, trades_b = run(pivot_close, pivot_open, pivot_amt, calendar, month_ends,
                          HOLD_K, True)
    # 起点：首个有合格池的调仓次日
    first = min(t['execute'] for t in trades_m)
    nav_m = nav_m[nav_m.index >= pd.Timestamp(first)].dropna()
    nav_b = nav_b[nav_b.index >= pd.Timestamp(first)].dropna()

    # 可投资子样本：合格池 >= 20 只（更接近真实轮动场景）
    pool20 = [t for t in trades_m if t['pool_size'] >= 20]
    sub_start = min(t['execute'] for t in pool20) if pool20 else first
    nav_m20 = nav_m[nav_m.index >= pd.Timestamp(sub_start)]
    nav_b20 = nav_b[nav_b.index >= pd.Timestamp(sub_start)]
    # 年度合格池规模
    pool_by_year = pd.Series(
        [t['pool_size'] for t in trades_m],
        index=pd.to_datetime([t['rebalance'] for t in trades_m])).groupby(
            lambda d: d.year).mean().round(1)

    res = {
        'universe': universe,
        'universe_codes': len(codes),
        'panel_rows': int(len(panel)),
        'extreme_adj_days': extreme,
        'repaired_split_days': int((panel['raw_ret'].abs() > JUMP_THRESHOLD).sum()),
        'params': {
            'top_pool_liquidity': TOP_POOL, 'hold_k': HOLD_K,
            'cost_round_trip': COST_ROUND_TRIP,
            'momentum': 'close_adj[t-21]/close_adj[t-252]-1',
            'execution': '月末收盘出信号，下一交易日开盘成交',
            'jump_repair': f'复权后单日 |收益|>{JUMP_THRESHOLD:.0%} 视为份额折算，收益记 0',
        },
        'momentum_k5_full': perf(nav_m),
        'equal_weight_full': perf(nav_b),
        'subsample_start_pool20': str(sub_start),
        'momentum_k5_pool20': perf(nav_m20),
        'equal_weight_pool20': perf(nav_b20),
        'rebalances': len(trades_m),
        'avg_turnover': round(float(np.mean([t['turnover'] for t in trades_m])), 3),
        'avg_cost_per_rebalance': round(
            float(np.mean([t['cost'] for t in trades_m])), 5),
        'pool_size_by_year': {int(k): float(v) for k, v in pool_by_year.items()},
    }
    # 年度收益
    def yearly(nav):
        return {int(y): round(float(g.iloc[-1] / g.iloc[0] - 1), 4)
                for y, g in nav.groupby(nav.index.year)}
    res['yearly_momentum'] = yearly(nav_m)
    res['yearly_benchmark'] = yearly(nav_b)

    # 月度胜率（相对等权）
    mm = nav_m.resample('ME').last().pct_change().dropna()
    mb = nav_b.resample('ME').last().pct_change().dropna()
    common = mm.index.intersection(mb.index)
    res['monthly_win_rate_vs_benchmark'] = round(
        float((mm[common] > mb[common]).mean()), 3)
    res['monthly_excess_mean'] = round(float((mm[common] - mb[common]).mean()), 4)

    # 最近一次调仓明细
    res['last_rebalance'] = trades_m[-1] if trades_m else None
    res['last_rebalance_names'] = {
        c: names.get(c, '') for c in (trades_m[-1]['picked'] if trades_m else [])}

    pd.DataFrame({'momentum_k5': nav_m, 'equal_weight': nav_b}).to_csv(
        OUT_DIR / f'momentum_nav_{universe}.csv', encoding='utf-8')
    (OUT_DIR / f'momentum_result_{universe}.json').write_text(
        json.dumps(res, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--universe', choices=('equity', 'all'), default='equity',
                        help='equity=宽基/行业/跨境等权益 ETF（剔除债券/黄金/商品/货币）')
    args = parser.parse_args()
    main(args.universe)
