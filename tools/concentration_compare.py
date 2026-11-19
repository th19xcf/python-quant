#!/usr/bin/env python3
"""集中度对比（只读）：v3 洁净集内按 60 日波动率排名取前 N 只等权。

口径
----
- 选股层完全复用 ``tools/factor_weighting.py::v3_path``：全市场 A 股 =
  有行情标的 − 60 日波动率最高 20% − 上市不满 250 个交易日，即正式 spec
  ``v3_monthly`` 的洁净集。
- 排名：同一份 60 日波动率面板，**升序**（低波在前）。这是 v3 自身唯一的
  横截面排序轴，所有档位用同一排名，不另造打分。无波动率的标的排末尾。
- 档位：``full``（v3 全部成分，≈4,284 只）、``top50`` / ``top100`` /
  ``top200``（当期排名最前的 N 只），档内等权，权重合计 0.99（1% 现金缓冲）。
- 变体：``top50_indcap5`` = 前 50 只，但同一申万一级行业最多 5 只；单独
  标注，不与主对比混算。
- 其余全部沿用 v3 冻结口径：月末 T 日收盘信号、T+1 开盘成交、两腿再平衡；
  hfq 出信号、原始价成交与盯市、现金分红入池；成本滑点 10bp、佣金万 2.5
  （最低 5 元）、卖出固定千 1 印花税、冲击 0.015×sqrt(参与率)；不加杠杆、
  不做空、不加行业/回撤等额外约束（变体除外）。
- 只读消费侧：不改引擎、不改数据、不改成本默认值；产物只写
  ``reports/集中度对比_20261117/``。

用法
----
    .venv\\Scripts\\python.exe tools\\concentration_compare.py
    .venv\\Scripts\\python.exe tools\\concentration_compare.py --tiers full top50
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_weighting as fw
import risk_metrics_benchmark as r
from excess_hedge_analysis import ew_series
from loguru import logger

WIN = (c.SEG1[0], c.SEG2[1])          # 2010-01-04 ~ 2026-11-13
BUDGET = 1.0 - f.CASH_BUFFER          # 0.99
PRINCIPAL = 1_000_000.0               # 可执行性按 100 万本金
LOT = 100
INDUSTRY_PIT = ROOT / 'data/local_store/meta/industry_pit/industry_pit_sw.parquet'
OUT_DIR = ROOT / 'reports/集中度对比_20261117'
TIERS = ('full', 'top50', 'top100', 'top200')
VARIANT = 'top50_indcap5'


# --------------------------------------------------------------------------
# 排名与选股
# --------------------------------------------------------------------------
def rank_by_vol(codes: List[str], vols: Dict[str, float]) -> List[str]:
    """同一排名：60 日波动率升序；无波动率者排末尾，再按代码稳定排序。"""
    inf = float('inf')
    return sorted(codes, key=lambda x: (vols.get(x, inf), x))


def pick_top(path: Dict[date, List[str]], panel: Dict[date, Dict[str, float]],
             n: int) -> Dict[date, List[str]]:
    out: Dict[date, List[str]] = {}
    for m, codes in path.items():
        out[m] = sorted(rank_by_vol(codes, panel.get(m, {}))[:n])
    return out


def load_industry_pit() -> Dict[str, List[Tuple[date, date | None, str]]]:
    rows = pl.read_parquet(
        INDUSTRY_PIT,
        columns=['ts_code', 'industry_l1_name', 'in_date', 'out_date'],
    ).to_dicts()
    by_code: Dict[str, List[Tuple[date, date | None, str]]] = {}
    for row in rows:
        by_code.setdefault(row['ts_code'], []).append(
            (row['in_date'], row['out_date'], row['industry_l1_name'] or '__UNKNOWN__'))
    return by_code


def industry_at(code: str, day: date,
                by_code: Dict[str, List[Tuple[date, date | None, str]]]) -> str:
    best: Tuple[date, str] | None = None
    for in_d, out_d, name in by_code.get(code, ()):
        if in_d <= day and (out_d is None or out_d > day):
            if best is None or in_d > best[0]:
                best = (in_d, name)
    return best[1] if best else '__UNKNOWN__'


def pick_top_industry(path: Dict[date, List[str]],
                      panel: Dict[date, Dict[str, float]], n: int, cap: int,
                      by_code: Dict[str, List[Tuple[date, date | None, str]]]
                      ) -> Dict[date, List[str]]:
    """前 n 只，单行业最多 cap 只；不足时按排名补足（补足部分不再受行业约束）。"""
    out: Dict[date, List[str]] = {}
    for m, codes in path.items():
        ranked = rank_by_vol(codes, panel.get(m, {}))
        count: Dict[str, int] = {}
        chosen: List[str] = []
        picked: set = set()
        for code in ranked:
            ind = industry_at(code, m, by_code)
            if count.get(ind, 0) >= cap:
                continue
            chosen.append(code)
            picked.add(code)
            count[ind] = count.get(ind, 0) + 1
            if len(chosen) >= n:
                break
        if len(chosen) < n:
            for code in ranked:
                if code not in picked:
                    chosen.append(code)
                    picked.add(code)
                    if len(chosen) >= n:
                        break
        out[m] = sorted(chosen)
    return out


# --------------------------------------------------------------------------
# 指标与可执行性
# --------------------------------------------------------------------------
def to_date(value: Any) -> date | None:
    return f.base.to_date(value)


def window_curve(result: Dict[str, Any]) -> Tuple[List[date], np.ndarray]:
    rows = [(d, e) for d, e in f.curve_of(result) if WIN[0] <= d <= WIN[1]]
    return [d for d, _ in rows], np.array([e for _, e in rows], dtype=float)


def trade_stats(result: Dict[str, Any]) -> Dict[str, Any]:
    trades = []
    for t in result.get('trades') or []:
        day = to_date(t.get('date'))
        if day is not None and WIN[0] <= day <= WIN[1]:
            trades.append(t)
    buy = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'buy')
    sell = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'sell')
    cost = sum(float(t.get('cost') or 0.0) for t in trades)
    return {'buy_notional': buy, 'sell_notional': sell, 'notional': buy + sell,
            'cost_total': cost, 'trades': len(trades)}


def raw_close_at(df: pl.DataFrame, day: date) -> float | None:
    dates = df['date'].to_numpy()
    want = np.datetime64(day, 'D')
    k = int(np.searchsorted(dates, want, side='right')) - 1
    if k < 0:
        return None
    px = float(df['raw_close'].to_numpy()[k])
    return px if np.isfinite(px) and px > 0 else None


def executability(data: Dict[str, pl.DataFrame], weights: Dict[date, Dict[str, float]],
                  principal: float = PRINCIPAL) -> Dict[str, Any]:
    """按 100 万本金逐期看：单只金额、买不到一手（100 股）的只数。"""
    amounts: List[float] = []
    below: List[int] = []
    names: List[int] = []
    final_day = None
    for m in sorted(weights):
        wmap = weights[m]
        if not (WIN[0] <= m <= WIN[1]) or not wmap:
            continue
        n = len(wmap)
        amount = principal * BUDGET / n
        cnt = 0
        for code in wmap:
            px = raw_close_at(data[code], m) if code in data else None
            if px is not None and amount < LOT * px:
                cnt += 1
        amounts.append(amount)
        below.append(cnt)
        names.append(n)
        final_day = m
    if not amounts:
        return {'missing': True}
    final_amt = amounts[-1]
    final_cnt = below[-1]
    final_names = names[-1]
    return {
        'principal': principal,
        'avg_names': round(float(np.mean(names)), 1),
        'avg_amount_per_name': round(float(np.mean(amounts)), 2),
        'final_signal_day': final_day.isoformat() if final_day else None,
        'final_amount_per_name': round(final_amt, 2),
        'avg_below_one_lot': round(float(np.mean(below)), 1),
        'max_below_one_lot': int(max(below)),
        'final_below_one_lot': final_cnt,
        'final_below_one_lot_pct': round(final_cnt / final_names * 100, 1) if final_names else None,
    }


def excess_vs_benchmark(days: List[date], eq: np.ndarray,
                        bdays: List[date], beq: np.ndarray) -> Dict[str, Any]:
    bmap = {d: float(e) for d, e in zip(bdays, beq, strict=False)}
    common = [d for d in days if d in bmap]
    if len(common) < 2:
        return {'missing': True}
    emap = {d: float(e) for d, e in zip(days, eq, strict=False)}
    tier_cum = emap[common[-1]] / emap[common[0]] - 1.0
    bench_cum = bmap[common[-1]] / bmap[common[0]] - 1.0
    years = (common[-1] - common[0]).days / 365.25
    return {
        'span': [common[0].isoformat(), common[-1].isoformat()],
        'tier_cum_return': round(tier_cum, 6),
        'bench_cum_return': round(bench_cum, 6),
        'cumulative_excess_pp': round((tier_cum - bench_cum) * 100, 2),
        'annual_excess_pp': round((
            (1 + tier_cum) ** (1 / years) - (1 + bench_cum) ** (1 / years)) * 100, 2)
        if years > 0 else None,
    }


def tier_metrics(result: Dict[str, Any], weights: Dict[date, Dict[str, float]],
                 data: Dict[str, pl.DataFrame],
                 bdays: List[date], beq: np.ndarray) -> Dict[str, Any]:
    days, eq = window_curve(result)
    m = r.metrics(days, eq)
    ts = trade_stats(result)
    years = (days[-1] - days[0]).days / 365.25
    avg_eq = float(np.mean(eq))
    m.update({
        'annual_turnover_two_way': round(ts['notional'] / avg_eq / years, 4)
        if avg_eq and years > 0 else None,
        'annual_turnover_one_way': round(ts['buy_notional'] / avg_eq / years, 4)
        if avg_eq and years > 0 else None,
        'cost_total': round(ts['cost_total'], 2),
        'cost_bps_of_turnover': round(ts['cost_total'] / ts['notional'] * 1e4, 2)
        if ts['notional'] else None,
        'cost_share_of_gross': round(ts['cost_total'] / (
            eq[-1] - eq[0] + ts['cost_total']), 4)
        if eq[-1] - eq[0] + ts['cost_total'] > 0 else None,
        'avg_members': round(float(np.mean(
            [len(weights[x]) for x in weights if WIN[0] <= x <= WIN[1]])), 1),
        'members_last': len(weights[max(x for x in weights if WIN[0] <= x <= WIN[1])]),
    })
    m.update(fw.concentration(weights))
    m.update(excess_vs_benchmark(days, eq, bdays, beq))
    m['executability'] = executability(data, weights)
    return m


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def main() -> None:  # noqa: C901
    parser = argparse.ArgumentParser()
    parser.add_argument('--tiers', nargs='+', default=list(TIERS) + [VARIANT])
    parser.add_argument('--out-dir', default=str(OUT_DIR))
    args = parser.parse_args()

    t0 = time.perf_counter()
    data = f.base.load_universe(fw.WARMUP_START, f.base.END)
    data, violations = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    path, panel = fw.v3_path(data, month_ends)
    sizes = [len(v) for v in path.values()]
    logger.info('v3 洁净集：{} 个调仓日，成分中位 {}（{}~{}）', len(path),
                int(np.median(sizes)), min(path).isoformat(), max(path).isoformat())

    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, fw.WARMUP_START,
                                  f.base.END, None, None)
    bdays, beq = ew_series()

    by_code = load_industry_pit() if VARIANT in args.tiers else {}

    # 档位 → 每月成分
    selected: Dict[str, Dict[date, List[str]]] = {}
    if 'full' in args.tiers:
        selected['full'] = path
    if 'top50' in args.tiers:
        selected['top50'] = pick_top(path, panel, 50)
    if 'top100' in args.tiers:
        selected['top100'] = pick_top(path, panel, 100)
    if 'top200' in args.tiers:
        selected['top200'] = pick_top(path, panel, 200)
    if VARIANT in args.tiers:
        selected[VARIANT] = pick_top_industry(path, panel, 50, 5, by_code)

    out: Dict[str, Any] = {
        'schema': 'concentration_compare/v1',
        'generated_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'script': 'tools/concentration_compare.py',
        'metadata': {
            '预热区间': '数据自 2009-08-01 起读，首个信号 2009-09-01；指标窗口 '
                        f'{WIN[0].isoformat()}~{WIN[1].isoformat()}',
            '复权方式': 'hfq 出信号（60 日波动率同源）；原始价成交与盯市；现金分红入池',
            '再平衡频率': '月频；T 日收盘信号、T+1 开盘成交；两腿（月末前一交易日卖腿、'
                          '月末买腿）；1% 现金缓冲，权重合计 0.99',
            '洁净集': 'v3 冻结口径：全市场 A 股 − 60 日波动率最高 20% − 上市不满 250 交易日',
            '排名': '同一份 60 日波动率面板升序（低波在前）；无波动率者排末尾',
            '成本': '滑点 10bp + 佣金万 2.5（最低 5 元）+ 卖出固定千 1 印花税 + '
                    '冲击 0.015×sqrt(参与率)；过户费引擎默认',
            '本金': f.base.INITIAL_CAPITAL,
            '可执行性本金': PRINCIPAL,
            '基准': '等权全市场买入持有（2010-01-04 建仓、不调仓、不补新上市）',
        },
        'data': {
            'universe_used': len(data),
            'universe_excluded_contract': len(violations),
            'rebalances': len(path),
            'members_median': int(np.median(sizes)),
            'members_min': int(min(sizes)),
            'members_max': int(max(sizes)),
        },
        '基准': {'metrics': r.metrics(bdays, beq)},
        '结果': {},
        '变体': {},
    }

    eq_rows: Dict[str, Dict[date, float]] = {}
    holding_rows: List[Dict[str, Any]] = []
    for name in args.tiers:
        if name not in selected:
            continue
        codes = selected[name]
        weights = fw.weights_equal({m: v for m, v in codes.items() if v})
        sched = fw.schedules_from_weights(weights, prev_day)
        logger.info('{} 回测开始：{} 只 / 成分中位 {}', name, len(sched),
                    int(np.median([len(v) for v in weights.values()])))
        result = f.run_portfolio(data, sched, dividends, st_status)
        item = tier_metrics(result, weights, data, bdays, beq)
        item['description'] = {
            'full': 'v3 全量等权（现有基线）',
            'top50': '同一排名前 50 只等权',
            'top100': '同一排名前 100 只等权',
            'top200': '同一排名前 200 只等权',
            VARIANT: '同一排名前 50 只、同一申万一级行业最多 5 只（单独变体）',
        }.get(name, name)
        out['结果' if name in TIERS else '变体'][name] = item
        curve = window_curve(result)
        eq_rows[name] = dict(zip(curve[0], curve[1].tolist(), strict=False))
        if name == 'full':
            eq_rows['benchmark'] = dict(zip(bdays, beq.tolist(), strict=False))
        for m in sorted(weights):
            if not (WIN[0] <= m <= WIN[1]) or not weights[m]:
                continue
            n = len(weights[m])
            amount = PRINCIPAL * BUDGET / n
            cnt = sum(1 for code in weights[m]
                      if (px := raw_close_at(data[code], m)) is not None
                      and amount < LOT * px)
            holding_rows.append({
                'signal_day': m.isoformat(), 'tier': name, 'members': n,
                'amount_per_name': round(amount, 2), 'below_one_lot': cnt,
                'below_one_lot_pct': round(cnt / n * 100, 1) if n else None,
            })
        print('TIER ' + json.dumps({'tier': name, **item}, ensure_ascii=False,
                                   default=str), flush=True)
        del result, sched
        gc.collect()

    base = out['结果'].get('full')
    verdict: Dict[str, Any] = {}
    if base:
        for name, item in out['结果'].items():
            if name == 'full':
                continue
            verdict[name] = {
                'annual_delta_pp': round((item['annual'] - base['annual']) * 100, 2),
                'max_drawdown_delta_pp': round(
                    (item['max_drawdown'] - base['max_drawdown']) * 100, 2),
                'sharpe_delta': round(item['sharpe'] - base['sharpe'], 4),
                'turnover_delta': round(
                    item['annual_turnover_two_way'] - base['annual_turnover_two_way'], 4),
                'cost_delta_pct': round(
                    item['cost_total'] / base['cost_total'] * 100 - 100, 1)
                if base['cost_total'] else None,
                'not_clearly_worse_after_cost': bool(
                    item['annual'] >= base['annual'] - 0.01
                    and item['max_drawdown'] >= base['max_drawdown'] - 0.03
                    and item['sharpe'] >= base['sharpe'] - 0.05),
            }
    out['对比_对全量基线'] = verdict
    out['判定口径'] = ('未明显劣于全量基线 = 净年化 ≥ 基线 −1.0pp 且最大回撤 ≥ 基线 −3.0pp '
                       '且 Sharpe ≥ 基线 −0.05；净年化与回撤均来自引擎扣成本后的净值')

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / '集中度对比_2026-11-17.json').write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding='utf-8')

    # 净值明细宽表：date + 各档 + 基准
    all_days = sorted(set().union(*[set(v) for v in eq_rows.values()])) if eq_rows else []
    cols: List[str] = ['date'] + list(eq_rows)
    with (out_dir / 'daily_nav.csv').open('w', encoding='utf-8', newline='') as fh:
        fh.write(','.join(cols) + '\n')
        for d in all_days:
            row = [d.isoformat()]
            for name in eq_rows:
                val = eq_rows[name].get(d)
                row.append('' if val is None else f'{val:.2f}')
            fh.write(','.join(row) + '\n')
    pl.DataFrame(holding_rows).write_csv(out_dir / 'monthly_holdings.csv')

    print('OUT ' + str(out_dir), flush=True)
    print(f'ELAPSED {time.perf_counter() - t0:.1f}s', flush=True)


if __name__ == '__main__':
    main()
