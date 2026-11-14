#!/usr/bin/env python3
"""业绩预告「预增」事件驱动 alpha：只测信号有效性，不做组合优化（只读）。

规则
----
- 事件源 `data/local_store/meta/events/events.parquet`，取 `event_type=业绩预增`
  （表内无「略增/扭亏」类型、无预告幅度字段，故按事件类型二分类，不补造幅度）；
  同日同标的重复按 tushare 优先去重（2025 年读取层口径）。
- 公告日 T 的下一交易日开盘买入，等权持有 20 / 60 个交易日，到期开盘卖出；
  不做空、不加杠杆；开盘一字涨停（open/pre_close−1 ≥ 9.5%）视为买不进，剔除。
- 组合口径：每个事件建仓时投入等额资金（初始 1 单位），持有期内不调仓，日收益
  按当前市值加权 Σ(v·r)/Σv，避免日频等权再平衡的波动收割溢价；成本沿用 v1 默认：
  滑点 10bp、买卖各 2.5bp 佣金、卖出 0.1% 印花。
- 后复权价（close/open × hfq_factor）；事件自 2010 年起，无因子预热需求。

输出：两档年化、最大回撤、Calmar、Sharpe、年换手、事件笔数，与 v3 日收益的
Pearson/Spearman 相关、与等权全市场基准的 Pearson 相关。结论写入
`docs/基线_价量估值等权_2026-11-13.json` 的 `事件驱动_业绩预告_2026-11-14` 节。

用法
----
    .venv\\Scripts\\python.exe tools\\event_pe_forecast_alpha.py
"""
from __future__ import annotations

import json
import math
import sys
from bisect import bisect_left
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import risk_metrics_benchmark as r
from factor_turnover_lowvol import spearman

EVENTS = ROOT / 'data/local_store/meta/events/events.parquet'
QUOTES = ROOT / 'data/local_store/quotes/stock/**/*.parquet'
EW_CACHE = ROOT / '_staging/_ew_buyhold_cache.csv'
V3 = ROOT / '_staging/equity_v3_excl_vol_new_2026-11-14.csv'
DOCS_JSON = r.DOCS_JSON
HORIZONS = (20, 60)
POSITIVE = '业绩预增'
LIMIT_OPEN = 0.095
ENTRY_COST = 0.00125
EXIT_COST = 0.00225


def load_events() -> Tuple[List[str], List[np.datetime64]]:
    df = (pl.read_parquet(EVENTS)
          .filter(pl.col('event_type') == POSITIVE)
          .with_columns(pl.col('publish_time').dt.date().alias('ann_date'),
                        (pl.col('source') == 'tushare').alias('_ts'))
          .sort('_ts', descending=True)
          .unique(subset=['ts_code', 'ann_date'], keep='first')
          .sort('ann_date'))
    days = np.array(df['ann_date'].to_list(), dtype='datetime64[D]')
    return df['ts_code'].to_list(), list(days)


def global_calendar() -> Tuple[List[date], np.ndarray]:
    df = pl.read_csv(EW_CACHE)
    days = [date.fromisoformat(str(x)) for x in df['date'].to_list()]
    return days, np.array(days, dtype='datetime64[D]')


def load_quotes(codes: List[str]):
    df = (pl.scan_parquet(QUOTES)
          .select(['ts_code', 'date', 'open', 'close', 'pre_close', 'hfq_factor'])
          .filter(pl.col('ts_code').is_in(codes) & (pl.col('date') >= date(2009, 12, 1)))
          .collect()
          .sort(['ts_code', 'date']))
    cds = df['ts_code'].to_numpy()
    starts = np.flatnonzero(np.r_[True, cds[1:] != cds[:-1]])
    ends = np.r_[starts[1:], len(cds)]
    dts = np.array(df['date'].to_list(), dtype='datetime64[D]')
    fac = df['hfq_factor'].to_numpy().astype(float)
    op = df['open'].to_numpy().astype(float) * fac
    cl = df['close'].to_numpy().astype(float) * fac
    pre = df['pre_close'].to_numpy().astype(float) * fac
    return {str(cds[s]): (dts[s:t], op[s:t], cl[s:t], pre[s:t])
            for s, t in zip(starts, ends, strict=True)}


def build_windows(codes, anns, quotes, cal_np: np.ndarray):
    out: Dict[int, List[Tuple[int, int, str, int]]] = {h: [] for h in HORIZONS}
    skipped = {'no_quote': 0, 'limit_open': 0, 'no_exit': 0}
    n = len(cal_np)
    for code, ann in zip(codes, anns, strict=True):
        q = quotes.get(code)
        if q is None:
            skipped['no_quote'] += 1
            continue
        dts, op, cl, pre = q
        g = int(np.searchsorted(cal_np, ann, side='right')) - 1
        if g < 0 or g + 1 >= n:
            skipped['no_exit'] += 1
            continue
        entry = cal_np[g + 1]
        i0 = int(np.searchsorted(dts, entry))
        if i0 >= len(dts) or dts[i0] != entry or op[i0] <= 0:
            skipped['no_quote'] += 1
            continue
        if pre[i0] > 0 and op[i0] / pre[i0] - 1 >= LIMIT_OPEN:
            skipped['limit_open'] += 1
            continue
        for h in HORIZONS:
            if g + 1 + h >= n:
                skipped['no_exit'] += 1
                continue
            exit_day = cal_np[g + 1 + h]
            i1 = int(np.searchsorted(dts, exit_day))
            if i1 >= len(dts) or dts[i1] != exit_day or op[i1] <= 0:
                skipped['no_exit'] += 1
                continue
            out[h].append((i0, i1, code, g + 1))
    return out, skipped


def simulate(windows, quotes, cal_np: np.ndarray, h: int):
    """等权建仓、持有到期、不调仓：每日组合收益 = Σ(v·r)/Σv，v 为各仓位当前市值。"""
    deltas = np.zeros(len(cal_np) + 1)
    for _i0, _i1, _c, g0 in windows:
        deltas[g0] += 1
        deltas[g0 + h] -= 1
    count = np.cumsum(deltas)[:len(cal_np)]
    num = np.zeros(len(cal_np))
    den = np.zeros(len(cal_np))
    for i0, i1, code, _g0 in windows:
        dts, op, cl, _pre = quotes[code]
        v = 1.0
        for j in range(i0, i1 + 1):
            gi = int(np.searchsorted(cal_np, dts[j]))
            if j == i0:
                r = cl[j] / op[j] - 1 - ENTRY_COST
            elif j == i1:
                r = op[j] / cl[j - 1] - 1 - EXIT_COST
            else:
                r = cl[j] / cl[j - 1] - 1
            num[gi] += v * r
            den[gi] += v
            v *= (1.0 + r)
    daily = np.divide(num, den, out=np.zeros_like(num), where=den > 0)
    eq = np.cumprod(1.0 + daily)
    return daily, np.concatenate([[1.0], eq]), float(count.mean()), int(count.max())


def curve_stats(days: List[date], eq: np.ndarray) -> Dict[str, float]:
    ret = eq[1:] / eq[:-1] - 1
    years = (days[-1] - days[0]).days / 365.25
    annual = float(eq[-1] ** (1 / years) - 1)
    peak = np.maximum.accumulate(eq)
    mdd = float((eq / peak - 1).min())
    return {
        'annual': round(annual, 6),
        'max_drawdown': round(mdd, 6),
        'calmar': round(annual / abs(mdd), 4) if mdd else None,
        'sharpe': round(float(np.mean(ret) / np.std(ret, ddof=1) * math.sqrt(252)), 4),
        'vol': round(float(np.std(ret, ddof=1) * math.sqrt(252)), 4),
        'years': round(years, 2),
    }


def corr(a: Dict[date, float], b: Dict[date, float]) -> Dict[str, float]:
    common = sorted(set(a) & set(b))
    x = np.array([a[d] for d in common])
    y = np.array([b[d] for d in common])
    return {'pearson': round(float(np.corrcoef(x, y)[0, 1]), 4),
            'spearman': round(float(spearman(x, y)), 4),
            'days': len(common)}


def main() -> None:
    codes, anns = load_events()
    cal, cal_np = global_calendar()
    quotes = load_quotes(sorted(set(codes)))
    windows, skipped = build_windows(codes, anns, quotes, cal_np)

    v3_days, v3_eq = r.read_equity(V3)
    v3 = {v3_days[i]: float(v3_eq[i] / v3_eq[i - 1] - 1) for i in range(1, len(v3_days))}
    ew_df = pl.read_csv(EW_CACHE)
    ew_days = [date.fromisoformat(str(x)) for x in ew_df['date'].to_list()]
    ew_eq = ew_df['equity'].to_numpy().astype(float)
    ew = {ew_days[i]: float(ew_eq[i] / ew_eq[i - 1] - 1) for i in range(1, len(ew_days))}

    out: Dict[str, object] = {
        '口径': {
            '事件源': 'meta/events/events.parquet，event_type=业绩预增；表内无略增/扭亏与幅度字段，按类型二分类',
            '去重': '同一标的同一公告日重复按 source=tushare 优先',
            '预热': '无（事件自 2010 年起，价格直接用后复权，不需因子预热）',
            '复权': '后复权（open/close × hfq_factor）',
            '持有期口径': '公告日次一交易日开盘买入，持有 20 / 60 个交易日后开盘卖出；等权固定仓位、不调仓',
            '成本': '滑点10bp、买卖各2.5bp佣金、卖出0.1%印花（v1 默认）',
            '剔除': {'无行情': skipped['no_quote'], '开盘一字涨停': skipped['limit_open'],
                     '区间不足': skipped['no_exit']},
            '门槛': '与 v3 日收益相关 <0.5 且独立 Calmar ≥0.2',
        },
        '结果': {},
    }

    for h in HORIZONS:
        daily, eq, mean_pos, max_pos = simulate(windows[h], quotes, cal_np, h)
        stats = curve_stats(cal, eq)
        stats['events'] = len(windows[h])
        stats['avg_positions'] = round(mean_pos, 1)
        stats['max_positions'] = max_pos
        stats['turnover_two_way'] = round(len(windows[h]) * 2
                                          / max(mean_pos, 1e-9)
                                          / max(stats['years'], 1e-9), 3)
        series = {cal[i]: float(daily[i]) for i in range(len(daily))}
        stats['vs_v3'] = corr(series, v3)
        stats['vs_等权全市场'] = corr(series, ew)
        stats['通过'] = bool(stats['vs_v3']['pearson'] < 0.5
                            and (stats['calmar'] or 0) >= 0.2)
        out['结果'][f'持有{h}日'] = stats
        print(f'EVENT {h} ' + json.dumps(stats, ensure_ascii=False), flush=True)

    doc = json.loads(DOCS_JSON.read_text(encoding='utf-8'))
    doc['事件驱动_业绩预告_2026-11-14'] = out
    DOCS_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=1),
                         encoding='utf-8')
    print('WROTE ' + DOCS_JSON.name, flush=True)


if __name__ == '__main__':
    main()
