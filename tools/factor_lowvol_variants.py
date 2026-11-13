#!/usr/bin/env python3
"""低波因子两个应用变体（独立回测，复用 factor_lowvol_quantile 的因子与回测骨架）。

变体一「剔除式」：基线口径不变，每期横截面只剔除波动率最高的 20%（Q5），
其余等权。横截面取当日有行情且有 60 日波动率的标的，未能排名（新上市）的
标的无法判定为高波，保留在组合内，使标的池尽量贴近基线。

变体二「缓冲带」：原 Q1 多头（最低 20%），进入阈值为排名前 20%、退出阈值
为排名跌出 30%，中间持有不动；档内等权。

两变体共用同一份因子值（60 日 hfq 对数收益标准差，窗口末端为信号日）、
同一两腿再平衡与同一成本口径（佣金万 2.5、印花税千 1 卖出、滑点 10bp、
冲击 0.015×sqrt(参与率)、过户费引擎默认）。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_lowvol_variants.py
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Set, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import factor_lowvol_quantile as f

ENTRY_PCT = 0.20
EXIT_PCT = 0.30


def quoted_sets(data: Dict[str, object],
                month_ends: List[date]) -> Dict[date, Set[str]]:
    me = np.array(month_ends, dtype='datetime64[D]')
    out: Dict[date, Set[str]] = {d: set() for d in month_ends}
    for code, df in data.items():
        dates = df['date'].to_numpy()
        raw = np.searchsorted(dates, me)
        idx = np.minimum(raw, dates.size - 1)
        for j in np.nonzero((raw < dates.size) & (dates[idx] == me))[0]:
            out[month_ends[j]].add(code)
    return out


def schedules_from_path(path: Dict[date, List[str]],
                        prev_day: Dict[date, date]
                        ) -> Dict[str, Dict[date, Tuple[str, float]]]:
    schedules: Dict[str, Dict[date, Tuple[str, float]]] = {}
    prev: Set[str] = set()
    for m in sorted(path):
        cur = set(path[m])
        if not cur:
            prev = cur
            continue
        w = (1.0 - f.CASH_BUFFER) / len(cur)
        sell_day = prev_day.get(m)
        for code in cur:
            sig = schedules.setdefault(code, {})
            if sell_day is not None:
                sig[sell_day] = ('sell', w)
            sig[m] = ('buy', w)
        for code in prev - cur:
            if sell_day is not None:
                schedules.setdefault(code, {})[sell_day] = ('sell', 0.0)
        prev = cur
    return schedules


def variant_exclude(panel, members, quoted, months) -> Dict[date, List[str]]:
    path: Dict[date, List[str]] = {}
    for m in months:
        drop = set(members[m][4]) if m in members else set()
        path[m] = sorted(quoted[m] - drop)
        if not path[m]:
            raise RuntimeError(f'{m} 剔除后为空')
    return path


def variant_buffer(panel, quoted, months) -> Tuple[Dict[date, List[str]], List[int]]:
    path: Dict[date, List[str]] = {}
    counts: List[int] = []
    held: Set[str] = set()
    for m in months:
        ranked = sorted(panel[m].items(), key=lambda kv: kv[1])
        n = len(ranked)
        if n < f.MIN_NAMES:
            hold = set(held)
        else:
            pct = {code: rank / n for rank, (code, _) in enumerate(ranked)}
            hold = {c for c in held if pct.get(c, 0.0) <= EXIT_PCT}
            hold |= {c for c, p in pct.items() if p < ENTRY_PCT}
        path[m] = sorted(hold) if hold else []
        held = set(path[m])
        counts.append(len(path[m]))
    return path, counts


def summarize(tag: str, result, extra: Dict[str, object] | None = None) -> Dict[str, object]:
    summary = f.base.metrics(result)
    curve = f.curve_of(result)
    moved = [d for d, e in curve if abs(e - f.base.INITIAL_CAPITAL) > 1e-3]
    start_day = moved[0] if moved else curve[0][0]
    item = {
        'variant': tag,
        'annual_full_span': summary['annual'],
        'annual_matched': f.annual_between(curve, start_day)['annual'],
        'max_drawdown': summary['max_drawdown'],
        'max_dd_date': summary['max_dd_date'],
        'annual_turnover_two_way': summary['annual_turnover_two_way'],
        'cost_share_of_gross': summary['cost_share_of_gross'],
        'cost_bps_of_turnover': summary['cost_bps_of_turnover'],
        'final_equity': summary['final_equity'],
        'trades': summary['trades'],
        'expired': summary['expired_orders'],
    }
    item.update(extra or {})
    return item


def gross_annual(item: Dict[str, object]) -> float | None:
    share = item.get('cost_share_of_gross')
    if not share:
        return None
    net = float(item['final_equity']) - f.base.INITIAL_CAPITAL
    gross = net / (1.0 - float(share))
    years = 16.53
    return ((f.base.INITIAL_CAPITAL + gross) / f.base.INITIAL_CAPITAL) ** (1 / years) - 1


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(f.base.START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    panel = f.compute_vol_panel(data, month_ends)
    members, short = f.quantile_members(panel)
    quoted = quoted_sets(data, month_ends)
    months = sorted(members)
    logger.info('有效月末 {} 个 / 跳过 {} 个', len(months), len(short))

    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, f.base.START,
                                  f.base.END, None, None)

    path1 = variant_exclude(panel, members, quoted, months)
    logger.info('剔除式成分数：中位 {} / 首 {} / 末 {}',
                int(np.median([len(v) for v in path1.values()])),
                len(path1[months[0]]), len(path1[months[-1]]))
    sched1 = schedules_from_path(path1, prev_day)
    res1 = f.run_portfolio(data, sched1, dividends, st_status)
    s1 = f.base.metrics(res1)
    item1 = summarize('exclude_q5', res1, {
        'members_median': int(np.median([len(v) for v in path1.values()])),
        'gross_annual': gross_annual({'final_equity': s1['final_equity'],
                                      'cost_share_of_gross': s1['cost_share_of_gross']}),
    })
    print('V1 ' + json.dumps(item1, ensure_ascii=False, default=str), flush=True)
    del res1, sched1
    import gc
    gc.collect()

    path2, counts2 = variant_buffer(panel, quoted, months)
    logger.info('缓冲带成分数：中位 {} / 首 {} / 末 {}',
                int(np.median(counts2)), counts2[0], counts2[-1])
    sched2 = schedules_from_path(path2, prev_day)
    res2 = f.run_portfolio(data, sched2, dividends, st_status)
    s2 = f.base.metrics(res2)
    item2 = summarize('buffer_q1', res2, {
        'members_median': int(np.median(counts2)),
        'gross_annual': gross_annual({'final_equity': s2['final_equity'],
                                      'cost_share_of_gross': s2['cost_share_of_gross']}),
        'members_max': int(max(counts2)),
    })
    print('V2 ' + json.dumps(item2, ensure_ascii=False, default=str), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
