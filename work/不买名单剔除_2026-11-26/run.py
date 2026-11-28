#!/usr/bin/env python3
"""「不买名单」剔除检验（预注册：同目录 预注册.md，2026-11-26 22:32 冻结）。

用法（项目根目录、venv）::

    python work/不买名单剔除_2026-11-26/run.py

只跑一次：结果写入 result.json / run.log；result.json 已存在时拒绝重跑。

实现口径（预注册第 3 节已写明，此处补充）：
- 周末 = 每个自然周最后一个交易日（统一日历 = 000300.SH 交易日）。
- RSI14 用 Wilder 平滑（alpha = 1/14）；BIAS12 = 收盘 / 12 日均价 − 1。
- 12-1 动量 = close[t−21] / close[t−252] − 1（后复权）。
- 换手率：stock_turnover.turn（百分数）的 20 日均值。
- 行业：factor_panel.industry_l1（PIT，按信号日向前填充）。
"""
from __future__ import annotations

import json
import math
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA = ROOT / 'data' / 'local_store'
OUT = Path(__file__).resolve().parent
G = 'ts_code'

LOAD_START = date(2013, 6, 1)
START, END = date(2015, 1, 1), date(2026, 10, 31)
SEGS = (('2015-2019', date(2015, 1, 1), date(2019, 12, 31)),
        ('2020-2023', date(2020, 1, 1), date(2023, 12, 31)),
        ('2024-2026', date(2024, 1, 1), date(2026, 10, 31)))
MIN_LISTED, MAX_SUSP20, MIN_AMT20 = 60, 5, 5e7
ROUND_TRIP_POST, ROUND_TRIP_PRE = 21.9e-4, 26.9e-4
STAMP_SWITCH = date(2023, 8, 28)
N_TRIALS, NW_LAG = 6, 4
GROUPS = {'A': ('r1', 'r2', 'r3', 'r4'), 'A1': ('r1',), 'A2': ('r2',), 'A3': ('r3',),
          'A4': ('r4',), 'A5': ('r5',), 'A_plus_R5': ('r1', 'r2', 'r3', 'r4', 'r5')}
LOG_FH = None


def log(*a: object) -> None:
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    if LOG_FH:
        LOG_FH.write(s + '\n')
        LOG_FH.flush()


def nw_t(x: np.ndarray, lag: int = NW_LAG) -> float:
    x = np.asarray(x, float)
    x = x[np.isfinite(x)]
    n = x.size
    if n < 10:
        return float('nan')
    e = x - x.mean()
    var = float(e @ e) / n
    for k in range(1, lag + 1):
        var += 2.0 * (1.0 - k / (lag + 1.0)) * float(e[k:] @ e[:-k]) / n
    se = math.sqrt(max(var, 0.0) / n)
    return float(x.mean() / se) if se > 0 else float('nan')


def load() -> tuple[pl.DataFrame, list[date], list[date]]:
    t0 = time.perf_counter()
    cal = (pl.read_parquet(DATA / 'quotes' / 'index' / 'sh' / '000300.SH.parquet', columns=['date'])
           .with_columns(pl.col('date').cast(pl.Date)).filter(pl.col('date') >= LOAD_START)
           .unique().sort('date').with_columns(pl.int_range(pl.len()).cast(pl.Int32).alias('cidx')))
    q = (pl.scan_parquet(str(DATA / 'quotes' / 'stock' / '*' / '*.parquet'))
         .select(['date', G, 'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount',
                  'hfq_factor'])
         .with_columns(pl.col('date').cast(pl.Date))
         .filter((pl.col('date') >= LOAD_START) & ~pl.col(G).str.ends_with('.BJ')
                 & ~pl.col(G).str.slice(0, 1).is_in(['2', '9']))
         .collect())
    df = q.join(cal, on='date', how='inner').sort([G, 'date'])
    lim = (pl.scan_parquet(str(DATA / 'meta' / 'limit' / 'daily' / '*.parquet'))
           .select(['trade_date', G, 'st_status', 'listing_days', 'one_word_up', 'one_word_down',
                    'suspended'])
           .with_columns(pl.col('trade_date').cast(pl.Date))
           .filter(pl.col('trade_date') >= LOAD_START).collect()
           .unique([G, 'trade_date'], keep='last').rename({'trade_date': 'date'}))
    turn = (pl.scan_parquet(DATA / 'factors' / 'stock_turnover.parquet')
            .select([G, 'trade_date', 'turn']).with_columns(pl.col('trade_date').cast(pl.Date))
            .filter(pl.col('trade_date') >= LOAD_START).collect()
            .unique([G, 'trade_date'], keep='last').rename({'trade_date': 'date'}))
    val = (pl.scan_parquet(DATA / 'meta' / 'financials' / 'valuation_daily.parquet')
           .select([G, 'trade_date', 'circ_mv']).with_columns(pl.col('trade_date').cast(pl.Date))
           .filter(pl.col('trade_date') >= LOAD_START).collect()
           .unique([G, 'trade_date'], keep='last').rename({'trade_date': 'date'}))
    ind = (pl.scan_parquet(DATA / 'factors' / 'factor_panel.parquet')
           .select([G, 'signal_date', 'industry_l1']).with_columns(pl.col('signal_date').cast(pl.Date))
           .filter(pl.col('signal_date') >= LOAD_START).collect()
           .unique([G, 'signal_date'], keep='last').rename({'signal_date': 'date'}))
    sb = pl.read_parquet(DATA / 'meta' / 'stock_basic.parquet', columns=[G, 'delist_date'])
    delisted = sb.filter(pl.col('delist_date').is_not_null())[G].to_list()
    df = (df.join(lim, on=[G, 'date'], how='left').join(turn, on=[G, 'date'], how='left')
          .join(val, on=[G, 'date'], how='left').join(ind, on=[G, 'date'], how='left')
          .sort([G, 'date'])
          .with_columns([pl.col('industry_l1').fill_null(strategy='forward').over(G),
                         (pl.col(G).is_in(delisted)
                          & ((pl.len().over(G) - 1 - pl.int_range(pl.len()).over(G)) < 15))
                         .alias('delist_tail')]))
    log(f'[load] {df.height:,} 行 / {df[G].n_unique():,} 只；换手覆盖 {df["turn"].is_not_null().mean():.1%}；'
        f'流通市值 {df["circ_mv"].is_not_null().mean():.1%}；行业 {df["industry_l1"].is_not_null().mean():.1%}'
        f' ({time.perf_counter() - t0:.0f}s)')
    cal_d = cal['date'].to_list()
    weeks = (cal.with_columns(pl.col('date').dt.strftime('%G-%V').alias('wk'))
             .group_by('wk').agg(pl.col('date').max()).sort('date')['date'].to_list())
    return df, [d for d in weeks if START <= d <= END], cal_d


def features(df: pl.DataFrame) -> pl.DataFrame:
    fac = pl.col('hfq_factor').fill_null(strategy='forward').over(G).fill_null(1.0)
    df = df.with_columns([(pl.col('open') * fac).alias('a_open'), (pl.col('close') * fac).alias('a_close')])
    c = pl.col('a_close')
    d = c - c.shift(1).over(G)
    gain = pl.when(d > 0).then(d).otherwise(0.0).ewm_mean(alpha=1 / 14, adjust=False).over(G)
    loss = pl.when(d < 0).then(-d).otherwise(0.0).ewm_mean(alpha=1 / 14, adjust=False).over(G)
    df = df.with_columns([
        (c / c.rolling_mean(12, min_samples=12).over(G) - 1).alias('bias12'),
        (c / c.shift(5).over(G) - 1).alias('ret5'),
        (100 - 100 / (1 + gain / loss)).alias('rsi14'),
        (c.shift(21).over(G) / c.shift(252).over(G) - 1).alias('mom12_1'),
        pl.col('turn').rolling_mean(20, min_samples=15).over(G).alias('turn20'),
        (c / c.shift(20).over(G) - 1).alias('ret20'),
        pl.col('amount').rolling_mean(20, min_samples=15).over(G).alias('amt20'),
        ((pl.col('cidx') - pl.col('cidx').shift(20).over(G)) - 20).alias('gap20'),
        (pl.col('volume') <= 0).cast(pl.Int32).rolling_sum(20, min_samples=1).over(G).alias('zero20'),
        pl.int_range(pl.len()).over(G).alias('bar'),
    ])
    return df


def signal_frame(df: pl.DataFrame, weeks: list[date]) -> pl.DataFrame:
    s = df.filter(pl.col('date').is_in(weeks))
    pool = ((pl.col('st_status').fill_null('unknown') == 'non_st') & ~pl.col('delist_tail')
            & (pl.col('listing_days').fill_null(pl.col('bar') + 1) >= MIN_LISTED)
            & ~pl.col('suspended').fill_null(False) & (pl.col('volume') > 0)
            & ~pl.col('one_word_up').fill_null(False) & ~pl.col('one_word_down').fill_null(False)
            & ((pl.col('gap20').fill_null(99) + pl.col('zero20')) <= MAX_SUSP20)
            & (pl.col('amt20') >= MIN_AMT20))
    s = s.filter(pool.fill_null(False))

    def pct(col: str) -> pl.Expr:
        return (pl.col(col).rank('average').over('date') - 1) / (pl.col(col).count().over('date') - 1)
    ind_rs = (pl.col('ret20').mean().over(['date', 'industry_l1']) - pl.col('ret20').mean().over('date'))
    s = s.with_columns(ind_rs.alias('ind_rs20'))
    s = s.with_columns([
        ((pl.col('bias12') > 0.10) | (pl.col('ret5') > 0.10) | (pl.col('rsi14') > 70)).fill_null(False).alias('r1'),
        (pct('mom12_1') > 0.90).fill_null(False).alias('r2'),
        (pct('turn20') > 0.80).fill_null(False).alias('r3'),
        (pl.when(pl.col('industry_l1').is_not_null()).then(pct('ind_rs20') > 0.80).otherwise(False))
        .fill_null(False).alias('r4'),
        (pct('circ_mv') < 0.30).fill_null(False).alias('r5'),
    ])
    log(f'[signal] {len(weeks)} 个周末；平均池子 {s.height / len(weeks):,.0f} 只；命中率 '
        + ' '.join(f'{r}={float(s[r].mean()):.1%}' for r in ('r1', 'r2', 'r3', 'r4', 'r5')))
    return s.select(['date', G, 'r1', 'r2', 'r3', 'r4', 'r5'])


def holding_returns(df: pl.DataFrame, weeks: list[date]) -> pl.DataFrame:
    """每个 (信号周 w, 股票) 的持有收益：w 后第 1 根 bar 开盘买入 → w+1 后第 1 根可卖 bar 开盘卖出。"""
    buyable = (pl.col('volume') > 0) & ~pl.col('one_word_up').fill_null(False)
    sellable = (pl.col('volume') > 0) & ~pl.col('one_word_down').fill_null(False)
    d = df.select([G, 'date', 'a_open', 'a_close', buyable.alias('can_buy'), sellable.alias('can_sell')])
    wk = pl.DataFrame({'sig': weeks, 'nxt': weeks[1:] + [None]}).with_columns(pl.col('nxt').cast(pl.Date))
    # 2026-11-27 修正：无 by 键的 join_asof 要求左表按 date 全局有序；v1 按 (ts_code, date)
    # 排序并关闭了排序校验，匹配结果错乱，只剩 592 个持有收益（应为约 160 万）。
    first = (d.sort('date').join_asof(wk.sort('sig'), left_on='date', right_on='sig',
                                      strategy='backward')
             .filter(pl.col('date') > pl.col('sig')))
    entry = (first.group_by([G, 'sig']).agg(pl.all().sort_by('date').first())
             .filter(pl.col('can_buy')).select([G, 'sig', 'nxt', pl.col('a_open').alias('px_in')]))
    # 出场：nxt 之后第一根可卖 bar 的开盘；之后再无 bar（退市）按最后收盘
    exit_first = (d.sort('date').join_asof(
        wk.drop_nulls().select(pl.col('nxt').alias('ex')).sort('ex'),
        left_on='date', right_on='ex', strategy='backward')
        .filter(pl.col('date') > pl.col('ex')))
    sold = (exit_first.filter(pl.col('can_sell')).group_by([G, 'ex'])
            .agg(pl.col('a_open').sort_by('date').first().alias('px_out')))
    last = d.group_by(G).agg([pl.col('date').max().alias('last_date'),
                              pl.col('a_close').sort_by('date').last().alias('last_close')])
    h = (entry.join(sold, left_on=[G, 'nxt'], right_on=[G, 'ex'], how='left')
         .join(last, on=G, how='left')
         .with_columns(pl.when(pl.col('px_out').is_null() & (pl.col('last_date') <= pl.col('nxt')))
                       .then(pl.col('last_close')).otherwise(pl.col('px_out')).alias('px_out'))
         .filter(pl.col('nxt').is_not_null() & pl.col('px_out').is_not_null())
         .with_columns((pl.col('px_out') / pl.col('px_in') - 1).alias('r'))
         .select([G, pl.col('sig').alias('date'), 'r']))
    return h


def run_group(sig: pl.DataFrame, hold: pl.DataFrame, rules: tuple[str, ...] | None) -> pl.DataFrame:
    keep = sig if rules is None else sig.filter(~pl.any_horizontal([pl.col(r) for r in rules]))
    m = keep.select(['date', G]).join(hold, on=['date', G], how='inner')
    m = m.with_columns((1.0 / pl.len().over('date')).alias('w'))
    gross = m.group_by('date').agg([(pl.col('w') * pl.col('r')).sum().alias('gross'), pl.len().alias('n')])
    # 换手：相邻两期等权目标权重差（按信号日顺序）
    dates = sorted(gross['date'].to_list())
    wmap = {g['date'][0]: dict(zip(g[G].to_list(), g['w'].to_list()))
            for g in m.select(['date', G, 'w']).partition_by('date')}
    turn, prev = [], {}
    for d in dates:
        cur = wmap.get(d, {})
        keys = set(cur) | set(prev)
        turn.append(0.5 * sum(abs(cur.get(k, 0.0) - prev.get(k, 0.0)) for k in keys))
        prev = cur
    t = pl.DataFrame({'date': dates, 'turn': turn})
    out = gross.join(t, on='date').sort('date').with_columns(
        pl.when(pl.col('date') < STAMP_SWITCH).then(ROUND_TRIP_PRE).otherwise(ROUND_TRIP_POST).alias('rt'))
    return out.with_columns((pl.col('gross') - pl.col('turn') * pl.col('rt')).alias('net'))


def stats(net: np.ndarray, weeks_per_year: float = 52.0) -> dict:
    nav = np.cumprod(1 + net)
    yrs = net.size / weeks_per_year
    return {'annual': float(nav[-1] ** (1 / yrs) - 1),
            'mdd': float((nav / np.maximum.accumulate(nav) - 1).min()),
            'vol': float(net.std(ddof=1) * math.sqrt(weeks_per_year))}


def compare(a: pl.DataFrame, b: pl.DataFrame) -> dict:
    j = a.select(['date', 'net', 'turn', 'n']).join(
        b.select(['date', pl.col('net').alias('bnet'), pl.col('turn').alias('bturn'), pl.col('n').alias('bn')]),
        on='date', how='inner').sort('date')
    ex = (j['net'] - j['bnet']).to_numpy()
    sa, sb = stats(j['net'].to_numpy()), stats(j['bnet'].to_numpy())
    from src.backtest.performance.benchmark_eval import excess_deflated_sharpe
    out = {'A': sa, 'B': sb, 'excess_annual': sa['annual'] - sb['annual'], 't': nw_t(ex),
           'dsr': float(excess_deflated_sharpe(j['net'].to_list(), j['bnet'].to_list(),
                                               n_trials=N_TRIALS, periods_per_year=52)),
           'turn_A_annual': float(j['turn'].mean() * 52), 'turn_B_annual': float(j['bturn'].mean() * 52),
           'kept_ratio': {'mean': float((j['n'] / j['bn']).mean()), 'min': float((j['n'] / j['bn']).min()),
                          'max': float((j['n'] / j['bn']).max())}}
    out['segments'] = {}
    for name, s0, s1 in SEGS:
        k = j.filter((pl.col('date') >= s0) & (pl.col('date') <= s1))
        if k.height > 10:
            out['segments'][name] = (stats(k['net'].to_numpy())['annual']
                                     - stats(k['bnet'].to_numpy())['annual'])
    yearly = {}
    for y in sorted(j['date'].dt.year().unique().to_list()):
        k = j.filter(pl.col('date').dt.year() == y)
        yearly[str(y)] = float((1 + k['net']).product() - (1 + k['bnet']).product())
    out['yearly_excess'] = yearly
    return out


def main() -> int:
    global LOG_FH
    if (OUT / 'result.json').exists():
        print('[run] result.json 已存在：按预注册只跑一次，拒绝重跑')
        return 2
    LOG_FH = open(OUT / 'run.log', 'w', encoding='utf-8')
    t0 = time.perf_counter()
    df, weeks, _cal = load()
    df = features(df)
    sig = signal_frame(df, weeks)
    hold = holding_returns(df, weeks)
    log(f'[hold] {hold.height:,} 个（周 × 股）持有收益')
    # 健全性：每个池内（周 × 股）都应有持有收益（停牌/一字涨停买不到的少量除外）
    cover = sig.select(['date', G]).join(hold, on=['date', G], how='inner').height / sig.height
    log(f'[hold] 池内覆盖率 {cover:.1%}')
    if cover < 0.9:
        raise RuntimeError(f'持有收益覆盖率仅 {cover:.1%}，疑似计算错误，停止（不写 result.json）')
    base = run_group(sig, hold, None)
    res = {'weeks': len(weeks), 'B': {'stats': stats(base['net'].to_numpy()),
                                      'turn_annual': float(base['turn'].mean() * 52)}}
    for name, rules in GROUPS.items():
        res[name] = compare(run_group(sig, hold, rules), base)
        r = res[name]
        log(f'[{name}] 剔除 {"+".join(rules)}：年化 {r["A"]["annual"]:+.2%} vs B {r["B"]["annual"]:+.2%}，'
            f'超额 {r["excess_annual"]:+.2%}（t={r["t"]:.2f}，DSR={r["dsr"]:.3f}）；'
            f'三段 {", ".join(f"{k} {v:+.2%}" for k, v in r["segments"].items())}；'
            f'回撤 {r["A"]["mdd"]:.1%} vs {r["B"]["mdd"]:.1%}；保留 {r["kept_ratio"]["mean"]:.0%}')
    a = res['A']
    crit = {'K1': a['excess_annual'] >= 0.01, 'K2': a['t'] >= 2.0, 'K3': a['dsr'] >= 0.95,
            'K4': len(a['segments']) == 3 and all(v > 0 for v in a['segments'].values()),
            'K5': a['A']['mdd'] >= a['B']['mdd'] - 0.03}
    res['criteria'] = crit
    res['passed'] = all(crit.values())
    res['elapsed_s'] = round(time.perf_counter() - t0, 1)
    (OUT / 'result.json').write_text(json.dumps(res, ensure_ascii=False, indent=2, default=float),
                                     encoding='utf-8')
    log('[判定] ' + ', '.join(f'{k}={"✅" if v else "❌"}' for k, v in crit.items()))
    log(f'[判定] 主组（剔除 R1~R4）：{"有效" if res["passed"] else "剔除无效"}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
