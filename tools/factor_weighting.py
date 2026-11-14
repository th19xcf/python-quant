#!/usr/bin/env python3
"""v3 权重维度四组对照：选股与剔除完全不动，只改「各买多少」。

选股口径就是 v3 冻结口径（与 ``tools/v3_risk_overlay.py`` 同一份路径构造）：
全市场 A 股，月末横截面剔除 60 日波动率最高 20% + 剔除上市不满 250 个交易日，
其余全部保留。四组方案共用同一份成分路径、同一两腿再平衡、同一成本模型，
只在「目标权重」这一维上不同：

1. ``equal``       等权（v3 基准，重跑作为一致性对照）；
2. ``inv_vol``     逆波动率，w ∝ 1/σ60（σ60 与剔除因子同一份 60 日对数收益标准差）；
3. ``risk_parity`` 等风险贡献。样本 60 日协方差在 N≈4000 ≫ T=60 时秩 ≤ 60、
   不可逆，等风险贡献解不唯一且数值不稳定；在可行的收缩结构（对角 / 等相关）
   下 ERC 解析解退化回 w ∝ 1/σ（等相关时 a_i=σ_i w_i 全相等 ⇒ w ∝ 1/σ），
   故本组退化为逆波动率近似，结论中显式标注「近似」；
4. ``cap``         自由流通市值加权，w ∝ circ_mv（估值表日频 PIT，万元 → 元）。

口径（三要素写进产物 metadata）：预热 2009-08-01 起读、2009-09 首个信号、
指标窗口 2010-01-04~2026-11-13；hfq 出信号与因子、原始价成交与盯市、分红入池；
月频、T 日收盘信号 / T+1 开盘成交、两腿再平衡（月末前一交易日卖腿、月末买腿）、
1% 现金缓冲（权重合计 0.99）。成本沿用引擎既有接线：佣金万 2.5（最低 5 元）、
卖出固定千 1 印花税、滑点 10bp、冲击 0.015×sqrt(参与率)，实测 cost_bps≈22.64。

判定门槛（预先定死）：Calmar 高于等权且年化降幅 ≤ 1pp 才算过。

用法
----
    .venv\\Scripts\\python.exe tools/factor_weighting.py
    .venv\\Scripts\\python.exe tools/factor_weighting.py --schemes equal cap
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from bisect import bisect_right
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

import factor_exclusion_list as e
import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = (c.SEG1[0], c.SEG2[1])
BUDGET = 1.0 - f.CASH_BUFFER
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
OUT = ROOT / '_staging/权重方案_2026-11-15.json'
SCHEMES = ('equal', 'inv_vol', 'risk_parity', 'cap')
# v3 基准（已入库口径），用于一致性核对
V3_BASE = {'annual': 0.091312, 'max_drawdown': -0.610778,
           'calmar': 0.1495, 'sharpe': 0.501}


def load_cap_arrays(codes: List[str]) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    """估值表 circ_mv（元）按代码切片，仅取 >= 预热起点。"""
    want = set(codes)
    df = pl.scan_parquet(VALUATION).select(
        ['ts_code', 'trade_date', 'circ_mv']).filter(
        pl.col('trade_date') >= date(2009, 8, 1)).collect()
    df = df.filter(pl.col('ts_code').is_in(list(want))).sort(['ts_code', 'trade_date'])
    codes_np = df['ts_code'].to_numpy()
    dts = df['trade_date'].to_numpy()
    mv = df['circ_mv'].to_numpy().astype('float64')
    starts = np.flatnonzero(np.r_[True, codes_np[1:] != codes_np[:-1]])
    ends = np.r_[starts[1:], len(codes_np)]
    out = {str(codes_np[s]): (dts[s:t], mv[s:t]) for s, t in zip(starts, ends)}
    logger.info('circ_mv：{} 只 / {} 行', len(out), len(codes_np))
    return out


def build_cap_panel(cap_arrays: Dict[str, Tuple[np.ndarray, np.ndarray]],
                    month_ends: List[date]) -> Dict[date, Dict[str, float]]:
    """各月末的自由流通市值（取 ≤ 月末的最近一条，PIT）。"""
    me = np.array(month_ends, dtype='datetime64[D]')
    out: Dict[date, Dict[str, float]] = {d: {} for d in month_ends}
    for code, (dts, vals) in cap_arrays.items():
        k = np.searchsorted(dts, me, side='right') - 1
        for j in np.nonzero(k >= 0)[0]:
            x = float(vals[k[j]])
            if np.isfinite(x) and x > 0:
                out[month_ends[j]][code] = x
    return out


def new_listing_set(data, quoted_day: List[str], list_dates, all_days: List[date],
                    day: date) -> set:
    """上市不满 250 个交易日（缺 list_date 时用面板首日，按全历史交易日计数）。"""
    idx_m = bisect_right(all_days, day)
    out = set()
    for code in quoted_day:
        ld = list_dates.get(code)
        if ld is None:
            ld = date.fromisoformat(str(e.np_dates(data[code])[0]))
        if idx_m - bisect_right(all_days, ld) < e.MIN_HISTORY:
            out.add(code)
    return out


def v3_path(data, month_ends) -> Tuple[Dict[date, List[str]],
                                       Dict[date, Dict[str, float]]]:
    """v3 成分路径：quoted - 波动率最高 20% - 上市不满 250 交易日。

    第二项返回同一份 60 日波动率面板，供逆波动率加权复用（同源、同窗口）。
    """
    days = [d for d in month_ends if d >= FIRST_SIGNAL]
    panel = f.compute_vol_panel(data, days)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, days)
    months = [m for m in days if m in members and m in quoted]
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    path: Dict[date, List[str]] = {}
    for m in months:
        q = set(quoted[m])
        vol = set(members[m][4] or ()) & q
        new = new_listing_set(data, sorted(q), list_dates, all_days, m)
        path[m] = sorted(q - vol - new)
    return path, panel


def weights_equal(path: Dict[date, List[str]]) -> Dict[date, Dict[str, float]]:
    return {m: {code: BUDGET / len(codes) for code in codes}
            for m, codes in path.items() if codes}


def _normalize(raw: Dict[str, float], codes: List[str],
               fallback: float) -> Dict[str, float]:
    total = sum(raw.values()) + fallback * (len(codes) - len(raw))
    if total <= 0:
        return {code: BUDGET / len(codes) for code in codes}
    return {code: raw.get(code, fallback) / total * BUDGET for code in codes}


def weights_inv_vol(path: Dict[date, List[str]],
                    panel: Dict[date, Dict[str, float]]) -> Tuple[Dict[date, Dict[str, float]], int]:
    """w ∝ 1/σ60；当月无 σ60 的标的分到该月中位 1/σ（计数回报）。"""
    out: Dict[date, Dict[str, float]] = {}
    missing = 0
    for m, codes in path.items():
        if not codes:
            continue
        sig = panel.get(m, {})
        raw = {code: 1.0 / sig[code] for code in codes
               if code in sig and sig[code] > 0}
        if not raw:
            out[m] = {code: BUDGET / len(codes) for code in codes}
            missing += len(codes)
            continue
        missing += len(codes) - len(raw)
        out[m] = _normalize(raw, codes, float(np.median(list(raw.values()))))
    return out, missing


def weights_cap(path: Dict[date, List[str]],
                caps: Dict[date, Dict[str, float]]) -> Tuple[Dict[date, Dict[str, float]], int]:
    """w ∝ circ_mv；当月无市值的标的分到该月中位市值（计数回报）。"""
    out: Dict[date, Dict[str, float]] = {}
    missing = 0
    for m, codes in path.items():
        if not codes:
            continue
        cm = caps.get(m, {})
        raw = {code: cm[code] for code in codes if code in cm}
        if not raw:
            out[m] = {code: BUDGET / len(codes) for code in codes}
            missing += len(codes)
            continue
        missing += len(codes) - len(raw)
        out[m] = _normalize(raw, codes, float(np.median(list(raw.values()))))
    return out, missing


def schedules_from_weights(weights: Dict[date, Dict[str, float]],
                           prev_day: Dict[date, date]
                           ) -> Dict[str, Dict[date, Tuple[str, float]]]:
    """按「信号日 → {标的分权重}」生成两腿调度表（退出票卖腿清零）。"""
    schedules: Dict[str, Dict[date, Tuple[str, float]]] = {}
    prev: set = set()
    for m in sorted(weights):
        cur = weights[m]
        if not cur:
            prev = set()
            continue
        sell_day = prev_day.get(m)
        for code, w in cur.items():
            sig = schedules.setdefault(code, {})
            if sell_day is not None:
                sig[sell_day] = ('sell', w)
            sig[m] = ('buy', w)
        for code in prev - set(cur):
            if sell_day is not None:
                schedules.setdefault(code, {})[sell_day] = ('sell', 0.0)
        prev = set(cur)
    return schedules


def curve_metrics(result: Dict[str, Any]) -> Dict[str, Any]:
    curve = [(d, e) for d, e in f.curve_of(result) if WIN[0] <= d <= WIN[1]]
    days = [d for d, _ in curve]
    eq = np.array([e for _, e in curve], dtype=float)
    ret = eq[1:] / eq[:-1] - 1
    years = (days[-1] - days[0]).days / 365.25
    annual = float((eq[-1] / eq[0]) ** (1 / years) - 1)
    peak = np.maximum.accumulate(eq)
    mdd = float((eq / peak - 1).min())
    m = c.seg_metrics(result, *WIN)
    return {
        'annual': round(annual, 6),
        'max_drawdown': round(mdd, 6),
        'calmar': round(annual / abs(mdd), 4) if mdd else None,
        'sharpe': round(float(np.mean(ret) / np.std(ret, ddof=1) * math.sqrt(252)), 4),
        'vol': round(float(np.std(ret, ddof=1) * math.sqrt(252)), 4),
        'annual_turnover_two_way': round(m['turnover_two_way'], 4),
        'cost_share_of_gross': m['cost_share_of_gross'],
        'cost_bps': m['cost_bps'],
        'final_equity': m['final_equity'],
        'trades': m['trades'],
    }


def concentration(weights: Dict[date, Dict[str, float]]) -> Dict[str, float]:
    """窗口内每个调仓日的有效持仓数 1/Σw² 与前十权重占比，取均值。"""
    eff, top10 = [], []
    for m, w in weights.items():
        if not (WIN[0] <= m <= WIN[1]) or not w:
            continue
        x = np.array(list(w.values()), dtype=float)
        eff.append(1.0 / float((x * x).sum()))
        top10.append(float(np.sort(x)[-10:].sum()))
    return {
        'avg_effective_n': round(float(np.mean(eff)), 1) if eff else None,
        'avg_top10_share': round(float(np.mean(top10)), 4) if top10 else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--schemes', nargs='+', default=list(SCHEMES),
                        choices=list(SCHEMES))
    parser.add_argument('--out', default=str(OUT))
    args = parser.parse_args()

    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, violations = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    path, panel = v3_path(data, month_ends)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)
    sizes = [len(x) for x in path.values()]
    logger.info('v3 成分：{} 个调仓日，成分数中位 {}（{}~{}）', len(path),
                int(np.median(sizes)), min(path).isoformat(), max(path).isoformat())

    caps = build_cap_panel(load_cap_arrays(list(data)), sorted(path))

    w_equal = weights_equal(path)
    w_invvol, miss_vol = weights_inv_vol(path, panel)
    w_cap, miss_cap = weights_cap(path, caps)
    # 等风险贡献在可行的收缩结构下退化为逆波动率，共用同一组权重
    weights = {'equal': w_equal, 'inv_vol': w_invvol,
               'risk_parity': w_invvol, 'cap': w_cap}

    out: Dict[str, Any] = {
        'schema': 'factor_weighting/v1',
        'generated_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'script': 'tools/factor_weighting.py',
        'metadata': {
            '预热区间': '数据自 2009-08-01 起读，首个信号 2009-09-01；指标窗口 '
                        f'{WIN[0].isoformat()}~{WIN[1].isoformat()}',
            '复权方式': 'hfq 出信号与因子（60 日波动率同源）；原始价成交与盯市；现金分红入池',
            '再平衡频率': '月频；T 日收盘出信号、T+1 开盘成交；两腿（月末前一交易日卖腿、月末买腿）；1% 现金缓冲，权重合计 0.99',
            '选股与剔除': 'v3 冻结口径：全市场 - 60 日波动率最高 20% - 上市不满 250 交易日（四组完全一致）',
            '成本': '成本模型与 v1/v3 同一套接线（佣金万2.5含最低5元、卖出固定千1印花税、滑点10bp、冲击0.015×sqrt(参与率)）；'
                    '22.64bps 是 v1 实测量级，本次为 v3 换手口径，实测 cost_bps 见各组结果（等权 28.43bp），未改任何成本参数',
            '本金': f.base.INITIAL_CAPITAL,
            '判定门槛': 'Calmar 高于等权且年化降幅 ≤ 1pp',
        },
        'data': {
            'universe_used': len(data),
            'universe_excluded_contract': len(violations),
            'rebalances': len(path),
            'members_median': int(np.median(sizes)),
            'members_min': int(min(sizes)),
            'members_max': int(max(sizes)),
            'vol_missing_fallback': miss_vol,
            'cap_missing_fallback': miss_cap,
        },
        'v3_base': V3_BASE,
        '结果': {},
        '说明': {
            'risk_parity': '近似：样本 60 日协方差 N≈4000≫T=60，秩≤60 不可逆，ERC 解不唯一；'
                           '对角/等相关收缩下 ERC 解析解退化回 w∝1/σ，故本组与逆波动率同一组权重，数值相同。',
            'cap': '自由流通市值取估值表 circ_mv（Tushare 流通市值，元，日频 PIT，取 ≤ 月末最近一条）；'
                   '真自由流通股本 free_float_share_filled 仅覆盖约半数标的，混用会改动持仓集合，故未混用。',
            'equal': 'v3 基准重跑，用于一致性核对（应与 9.1312% / -61.08% / Calmar 0.150 一致）。',
        },
    }

    done: Dict[int, Tuple[str, Dict[str, Any]]] = {}
    for name in args.schemes:
        w = weights[name]
        if id(w) in done:
            src, item = done[id(w)]
            item = dict(item, same_weights_as=src)
            out['结果'][name] = item
            print('WEIGHT ' + json.dumps({'scheme': name, **item}, ensure_ascii=False),
                  flush=True)
            continue
        sched = schedules_from_weights(w, prev_day)
        logger.info('{} 回测开始：{} 只 / 成分中位 {}', name, len(sched),
                    int(np.median([len(x) for x in w.values()])))
        result = f.run_portfolio(data, sched, dividends, st_status)
        item = curve_metrics(result)
        item['avg_members'] = round(float(np.mean(
            [len(w[m]) for m in w if WIN[0] <= m <= WIN[1]])), 1)
        item.update(concentration(w))
        item['vs_v3_annual'] = round(item['annual'] - V3_BASE['annual'], 6)
        item['vs_v3_calmar'] = round(item['calmar'] - V3_BASE['calmar'], 4)
        done[id(w)] = (name, item)
        out['结果'][name] = item
        print('WEIGHT ' + json.dumps({'scheme': name, **item}, ensure_ascii=False),
              flush=True)
        del result, sched
        gc.collect()

    base = out['结果'].get('equal') or {}
    judge: Dict[str, Any] = {}
    for name, item in out['结果'].items():
        if not base or name == 'equal':
            continue
        drop = base['annual'] - item['annual']
        judge[name] = {
            'calmar': item['calmar'], 'equal_calmar': base['calmar'],
            'calmar_higher': bool(item['calmar'] > base['calmar']),
            'annual_drop_pp': round(drop * 100, 3),
            'annual_drop_ok': bool(drop <= 0.01),
            'pass': bool(item['calmar'] > base['calmar'] and drop <= 0.01),
        }
    out['判定'] = judge
    print('JUDGE ' + json.dumps(judge, ensure_ascii=False), flush=True)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=1),
                              encoding='utf-8')
    print('OUT ' + str(args.out), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
