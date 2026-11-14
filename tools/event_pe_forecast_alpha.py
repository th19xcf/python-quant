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
  按当前市值加权 Σ(v·r)/Σv，避免日频等权再平衡的波动收割溢价。
- 成本按既有实测口径 `cost_bps=22.6432`（成本/双边成交额，源自 v1 基线）
  在买卖两侧按当日成交仓位占比各计提 22.64bps，不另设假设。
- 后复权价（close/open × hfq_factor）；事件自 2010 年起，无因子预热需求。
- 判定用超额口径：两档日收益减等权全市场买入持有日收益得超额序列，与 v3 超额
  序列比相关（Pearson/Spearman），门槛「超额相关 <0.5 且扣成本后超额年化 >3%」。

输出：两档年化、最大回撤、Calmar、Sharpe、年换手、事件笔数，与 v3 日收益的
Pearson/Spearman 相关、与等权全市场基准的 Pearson 相关。结论写入
`docs/基线_价量估值等权_2026-11-13.json` 的 `事件驱动_业绩预告_2026-11-14` 节。

用法
----
    .venv\\Scripts\\python.exe tools\\event_pe_forecast_alpha.py               # 原超额测度
    .venv\\Scripts\\python.exe tools\\event_pe_forecast_alpha.py --step tail   # ① 尾部剔除
    .venv\\Scripts\\python.exe tools\\event_pe_forecast_alpha.py --step combine # ② 80/20 组合
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from bisect import bisect_left
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
SECTION = '事件驱动_业绩预告_2026-11-14'
HORIZONS = (20, 60)
POSITIVE = '业绩预增'
LIMIT_OPEN = 0.095
COST_BPS = 22.6432          # 既有口径：基线 v1 实测 cost_bps（成本/双边成交额）
COST_RATE = COST_BPS / 1e4


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
    """等权建仓、持有到期、不调仓：每日组合收益 = Σ(v·r)/Σv，v 为各仓位当前市值。

    返回到手毛收益 daily（未扣成本）、当日成本拖累 cost（按成交仓位占比提 22.64bps）、
    扣成本后净值 eq，以及平均/最大同时持仓数。
    """
    deltas = np.zeros(len(cal_np) + 1)
    for _i0, _i1, _c, g0 in windows:
        deltas[g0] += 1
        deltas[g0 + h] -= 1
    count = np.cumsum(deltas)[:len(cal_np)]
    num = np.zeros(len(cal_np))
    den = np.zeros(len(cal_np))
    cnum = np.zeros(len(cal_np))
    for i0, i1, code, _g0 in windows:
        dts, op, cl, _pre = quotes[code]
        v = 1.0
        for j in range(i0, i1 + 1):
            gi = int(np.searchsorted(cal_np, dts[j]))
            if j == i0:
                r = cl[j] / op[j] - 1
                cnum[gi] += v * COST_RATE
            elif j == i1:
                r = op[j] / cl[j - 1] - 1
                cnum[gi] += v * COST_RATE
            else:
                r = cl[j] / cl[j - 1] - 1
            num[gi] += v * r
            den[gi] += v
            v *= (1.0 + r)
    gross = np.divide(num, den, out=np.zeros_like(num), where=den > 0)
    cost = np.divide(cnum, den, out=np.zeros_like(cnum), where=den > 0)
    daily = gross - cost
    eq = np.cumprod(1.0 + daily)
    return (daily, cost, np.concatenate([[1.0], eq]),
            float(count.mean()), int(count.max()))


def curve_stats(days: List[date], eq: np.ndarray) -> Dict[str, Any]:
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


def excess_stats(daily: Dict[date, float], ew: Dict[date, float]) -> tuple:
    """日收益减等权全市场基准得超额序列，返回（超额统计、超额序列 dict）。"""
    excess = {d: v - ew[d] for d, v in daily.items() if d in ew}
    days = sorted(excess)
    vals = np.array([excess[d] for d in days])
    eq = np.concatenate([[1.0], np.cumprod(1.0 + vals)])
    return curve_stats([days[0]] + days, eq), excess


def trade_rets(windows, quotes) -> np.ndarray:
    """单笔毛收益：次日开盘买入、到期开盘卖出的价格变动（成本两侧对称，不影响排序）。"""
    out = np.empty(len(windows))
    for i, (i0, i1, code, _g) in enumerate(windows):
        op = quotes[code][1]
        out[i] = op[i1] / op[i0] - 1.0
    return out


def run_tail(cal, cal_np, quotes, windows, skipped, v3_excess, ew) -> None:
    """① 尾部剔除检验：按单笔收益降序剔除最赚钱的 1% / 5%，重算指标。"""
    block: Dict[str, Any] = {
        '口径': {
            '剔除方式': '按单笔毛收益（次日开盘买、到期开盘卖）降序剔除最赚钱的 1% / 5% 交易，其余原样持有；不换样本区间',
            '成本': '既有 cost_bps=22.6432（成本/双边成交额），买卖两侧按当日成交仓位占比各计提',
            '预热': '无（事件自 2010 年起，价格直接用后复权，不需因子预热）',
            '复权': '后复权（open/close × hfq_factor）',
            '持有期口径': '公告日次一交易日开盘买入，持有 20 / 60 个交易日后开盘卖出；等权建仓、持有期内不调仓',
            '剔除计数': {'无行情': skipped['no_quote'], '开盘一字涨停': skipped['limit_open'],
                         '区间不足': skipped['no_exit']},
        },
        '结果': {},
    }
    results: Dict[str, Dict[str, Any]] = {}
    for h in HORIZONS:
        rets = trade_rets(windows[h], quotes)
        row: Dict[str, Any] = {}
        for label, frac in (('剔除1%', 0.01), ('剔除5%', 0.05)):
            k = int(len(rets) * frac)
            drop = set(np.argsort(-rets)[:k].tolist())
            keep = [w for i, w in enumerate(windows[h]) if i not in drop]
            daily, _cost, eq, mean_pos, max_pos = simulate(keep, quotes, cal_np, h)
            st = curve_stats(cal, eq)
            series = {cal[i]: float(daily[i]) for i in range(len(daily))}
            ex_stats, excess = excess_stats(series, ew)
            st['events'] = len(keep)
            st['剔除笔数'] = k
            st['avg_positions'] = round(mean_pos, 1)
            st['超额'] = {'annual': ex_stats['annual'],
                          'max_drawdown': ex_stats['max_drawdown'],
                          'calmar': ex_stats['calmar'],
                          'vs_v3_超额': corr(excess, v3_excess)}
            row[label] = st
            print(f'TAIL {h} {label} ' + json.dumps(st, ensure_ascii=False), flush=True)
        results[f'持有{h}日'] = row
    block['结果'] = results
    parts, alive = [], True
    for h in HORIZONS:
        row = results[f'持有{h}日']
        one, five = row['剔除1%'], row['剔除5%']
        parts.append(f"持有{h}日剔除1%后净年化 {one['annual']:.2%}、超额年化 "
                     f"{one['超额']['annual']:.2%}、超额回撤 {one['超额']['max_drawdown']:.2%}、"
                     f"超额Calmar {one['超额']['calmar']}；剔除5%后超额年化 "
                     f"{five['超额']['annual']:.2%}、超额Calmar {five['超额']['calmar']}")
        alive = alive and one['超额']['annual'] > 0.03
    block['结论'] = ('不依赖少数极端赢家：' if alive else '依赖少数极端赢家：') + '；'.join(parts)

    doc = json.loads(DOCS_JSON.read_text(encoding='utf-8'))
    doc[SECTION]['尾部剔除'] = block
    DOCS_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding='utf-8')
    print('WROTE ' + DOCS_JSON.name + ' 尾部剔除', flush=True)


def run_combine(cal, cal_np, quotes, windows, v3, ew) -> None:
    """② 组合层实测：v3 与事件 20 / 60 日档各按 80/20 合成日收益（同一口径一次跑完）。"""
    block: Dict[str, Any] = {
        '口径': {
            '权重': 'v3 0.8 + 事件 20 日档 0.2、v3 0.8 + 事件 60 日档 0.2，日收益线性合成，仅此一档不扫参数',
            '再平衡': '日频固定权重合成（隐含每日再平衡），未额外计再平衡成本；两腿各自成本已含',
            '成本': '既有 cost_bps=22.6432（成本/双边成交额），事件腿买卖两侧各计提；v3 沿用原净值',
            '预热': '无（两腿自 2010-01-05 起同区间，事件腿无需因子预热）',
            '复权': '后复权（事件腿 open/close × hfq_factor；v3 原口径）',
            '持有期口径': '事件腿公告日次一交易日开盘买入、持有 20 / 60 个交易日后开盘卖出；v3 月频调仓',
        },
        '结果': {},
    }
    results: Dict[str, Any] = {}
    for h in HORIZONS:
        daily, _cost, _eq, _mp, _xp = simulate(windows[h], quotes, cal_np, h)
        ev = {cal[i]: float(daily[i]) for i in range(len(daily))}
        common = sorted(set(ev) & set(v3))

        def curve(seq, days) -> Dict[str, Any]:
            vals = np.array(seq)
            eq = np.concatenate([[1.0], np.cumprod(1.0 + vals)])
            return curve_stats([days[0]] + days, eq)

        comb = curve([0.8 * v3[d] + 0.2 * ev[d] for d in common], common)
        base = curve([v3[d] for d in common], common)
        comb['days'] = len(common)
        base['days'] = len(common)
        results[f'事件{h}日'] = {'组合': comb, 'v3对照': base}
        print(f'COMBINE {h} ' + json.dumps({'组合': comb, 'v3对照': base}, ensure_ascii=False),
              flush=True)
    block['结果'] = results

    notes = []
    for h in HORIZONS:
        c = results[f'事件{h}日']['组合']
        b = results[f'事件{h}日']['v3对照']
        notes.append(f"事件{h}日 80/20：年化 {c['annual']:.2%}、回撤 {c['max_drawdown']:.2%}、"
                     f"Calmar {c['calmar']}、Sharpe {c['sharpe']}；v3 同区间 {b['annual']:.2%}、"
                     f"{b['max_drawdown']:.2%}、{b['calmar']}、{b['sharpe']}")
    block['结论'] = ('事件20日档不进组合（尾部剔除后归零，本组仅作反例留存）；'
                     '事件60日档仅作小权重试验：') + '；'.join(notes)

    doc = json.loads(DOCS_JSON.read_text(encoding='utf-8'))
    doc[SECTION]['组合层_80_20'] = block
    DOCS_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding='utf-8')
    print('WROTE ' + DOCS_JSON.name + ' 组合层_80_20', flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--step', choices=('alpha', 'tail', 'combine'), default='alpha',
                    help='alpha=原超额测度；tail=①尾部剔除；combine=②80/20 组合合成')
    args = ap.parse_args()
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
    v3_excess = {d: v - ew[d] for d, v in v3.items() if d in ew}

    if args.step == 'tail':
        run_tail(cal, cal_np, quotes, windows, skipped, v3_excess, ew)
        return
    if args.step == 'combine':
        run_combine(cal, cal_np, quotes, windows, v3, ew)
        return

    out: Dict[str, object] = {
        '口径': {
            '事件源': 'meta/events/events.parquet，event_type=业绩预增；表内无略增/扭亏与幅度字段，按类型二分类',
            '去重': '同一标的同一公告日重复按 source=tushare 优先',
            '预热': '无（事件自 2010 年起，价格直接用后复权，不需因子预热）',
            '复权': '后复权（open/close × hfq_factor）',
            '持有期口径': '公告日次一交易日开盘买入，持有 20 / 60 个交易日后开盘卖出；等权建仓、持有期内不调仓',
            '成本': '既有 cost_bps=22.6432（成本/双边成交额，源自基线 v1 实测），买卖两侧按当日成交仓位占比各计提',
            '净年化': 'annual 为扣成本后净年化；成本年拖累 ≈ 日均成本×252',
            '超额口径': '两档日收益减等权全市场买入持有日收益；相关性与 v3 超额日收益（v3−等权）比',
            '剔除': {'无行情': skipped['no_quote'], '开盘一字涨停': skipped['limit_open'],
                     '区间不足': skipped['no_exit']},
            '门槛': '超额相关（对 v3 超额日收益）<0.5 且扣成本后超额年化 >3%',
        },
        '结果': {},
    }
    results: Dict[str, Dict[str, Any]] = {}
    out['结果'] = results

    for h in HORIZONS:
        daily, cost, eq, mean_pos, max_pos = simulate(windows[h], quotes, cal_np, h)
        stats = curve_stats(cal, eq)
        stats['events'] = len(windows[h])
        stats['avg_positions'] = round(mean_pos, 1)
        stats['max_positions'] = max_pos
        stats['turnover_two_way'] = round(len(windows[h]) * 2
                                          / max(mean_pos, 1e-9)
                                          / max(stats['years'], 1e-9), 3)
        stats['cost_drag_annual'] = round(float(np.mean(cost)) * 252, 6)
        series = {cal[i]: float(daily[i]) for i in range(len(daily))}
        stats['vs_v3'] = corr(series, v3)
        stats['vs_等权全市场'] = corr(series, ew)
        ex_stats, excess = excess_stats(series, ew)
        ex_stats['vs_v3_超额'] = corr(excess, v3_excess)
        stats['超额'] = ex_stats
        stats['通过'] = bool(ex_stats['vs_v3_超额']['pearson'] < 0.5
                            and ex_stats['annual'] > 0.03)
        out['结果'][f'持有{h}日'] = stats
        print(f'EVENT {h} ' + json.dumps(stats, ensure_ascii=False), flush=True)

    parts, passed = [], True
    for h in HORIZONS:
        s = results[f'持有{h}日']
        e = s['超额']
        parts.append(f"持有{h}日净年化 {s['annual']:.2%}、超额年化 {e['annual']:.2%}、"
                     f"超额回撤 {e['max_drawdown']:.2%}、超额Calmar {e['calmar']}、"
                     f"超额相关 {e['vs_v3_超额']['pearson']:.3f}")
        passed = passed and s['通过']
    out['结论'] = ('可进组合：' if passed else '不进组合：') + '；'.join(parts)

    doc = json.loads(DOCS_JSON.read_text(encoding='utf-8'))
    doc[SECTION] = out
    DOCS_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=1),
                         encoding='utf-8')
    print('WROTE ' + DOCS_JSON.name, flush=True)


if __name__ == '__main__':
    main()
