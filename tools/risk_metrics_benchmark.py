#!/usr/bin/env python3
"""v1/v2/v3 与两条基准的统一风险指标（只读）。

净值序列直接取 ``_staging/equity_v*``，不重跑回测；基准两条：

- 中证全指 000985.SH（本地覆盖 2011-08-02 起，2010~2011-08 缺，按实际区间单列）；
- 等权全市场：2010-01-04 全 A 等权建仓后买入持有、不再调仓、不补新上市，
  后复权（``close × hfq_factor``）、分红再投、停牌按前值平移、退市冻结。

指标：年化、最大回撤、Sharpe（rf=0、252 日）、Calmar、最长回撤持续天数、
逐年收益。结果就地写入 `docs/基线_价量估值等权_2026-11-13.json` 的
`风险指标_2026-11-14` 节。

用法
----
    .venv\\Scripts\\python.exe tools\\risk_metrics_benchmark.py
"""
from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
STAGING = ROOT / '_staging'
DOCS_JSON = ROOT / 'docs/基线_价量估值等权_2026-11-13.json'
QUOTES = ROOT / 'data/local_store/quotes/stock/**/*.parquet'
ADJ = ROOT / 'data/local_store/factors/stock_adj_factor.parquet'
INDEX = ROOT / 'data/local_store/quotes/index/sh/000985.SH.parquet'
START, END = date(2010, 1, 4), date(2026, 11, 13)
IDX_START = date(2011, 8, 2)
SERIES = {
    'v1': STAGING / 'equity_v1_pv_val_equal_weight_2026-11-14.csv',
    'v2': STAGING / 'equity_v2_excl_high_vol_2026-11-14.csv',
    'v3': STAGING / 'equity_v3_excl_vol_new_2026-11-14.csv',
}


def read_equity(path: Path) -> Tuple[List[date], np.ndarray]:
    df = pl.read_csv(path)
    days = [date.fromisoformat(str(x)) for x in df['date'].to_list()]
    eq = df['equity'].to_numpy().astype(float)
    return days, eq


def equal_weight_index() -> Tuple[List[date], np.ndarray]:
    """等权全市场买入持有：2010-01-04 全 A 等权建仓，此后不再调仓、不补新上市。

    后复权、分红再投；停牌按前值平移，退市后冻结在最后价格（不计退市损失）。
    """
    quotes = (pl.scan_parquet(QUOTES)
              .select(['ts_code', 'date', 'close'])
              .filter(pl.col('date') >= START))
    adj = (pl.scan_parquet(ADJ)
           .select(['ts_code', 'trade_date', 'hfq_factor'])
           .rename({'trade_date': 'date'}))
    panel = (quotes.join(adj, on=['ts_code', 'date'], how='left')
             .filter(pl.col('hfq_factor').is_not_null() & pl.col('close').is_not_null())
             .with_columns((pl.col('close') * pl.col('hfq_factor')).alias('adj'))
             .collect())
    first_day = panel['date'].min()
    cohort = set(panel.filter(pl.col('date') == first_day)['ts_code'].to_list())
    sub = (panel.filter(pl.col('ts_code').is_in(list(cohort)))
           .sort(['ts_code', 'date'])
           .with_columns((pl.col('adj') / pl.col('adj').first().over('ts_code') - 1)
                         .alias('rel')))
    wide = (sub.pivot(index='date', on='ts_code', values='rel')
            .sort('date')
            .fill_null(strategy='forward'))
    days = [date.fromisoformat(str(x)) for x in wide['date'].to_list()]
    cols = [c for c in wide.columns if c != 'date']
    mat = wide.select(cols).to_numpy().astype(float)
    mat = np.where(np.isnan(mat), 0.0, mat)
    eq = 1.0 + mat.mean(axis=1)
    return days, eq


def max_dd_days(eq: np.ndarray, days: List[date]) -> Tuple[float, int, str, str]:
    peak = float(eq[0])
    peak_idx = 0
    mdd, mdd_peak, trough = 0.0, 0, 0
    longest, l_start, l_end = 0, 0, 0
    for i in range(1, len(eq)):
        v = float(eq[i])
        if v >= peak:
            if i - peak_idx > longest:
                longest, l_start, l_end = i - peak_idx, peak_idx, i
            peak, peak_idx = v, i
        else:
            dd = v / peak - 1
            if dd < mdd:
                mdd, mdd_peak, trough = dd, peak_idx, i
    if len(eq) - 1 - peak_idx > longest:
        longest, l_start, l_end = len(eq) - 1 - peak_idx, peak_idx, len(eq) - 1
    span = (days[l_end] - days[l_start]).days
    return mdd, span, days[mdd_peak].isoformat(), days[trough].isoformat()


def metrics(days: List[date], eq: np.ndarray) -> Dict[str, Any]:
    eq = np.asarray(eq, dtype=float)
    ret = eq[1:] / eq[:-1] - 1
    years = (days[-1] - days[0]).days / 365.25
    annual = float((eq[-1] / eq[0]) ** (1.0 / years) - 1)
    vol = float(np.std(ret, ddof=1) * math.sqrt(252))
    sharpe = float(np.mean(ret) / np.std(ret, ddof=1) * math.sqrt(252)) if vol else None
    mdd, span, peak_day, trough_day = max_dd_days(eq, days)
    return {
        'annual': round(annual, 6),
        'max_drawdown': round(mdd, 6),
        'sharpe': round(sharpe, 4) if sharpe is not None else None,
        'calmar': round(annual / abs(mdd), 4) if mdd else None,
        'max_dd_days': span,
        'dd_peak': peak_day,
        'dd_trough': trough_day,
        'vol': round(vol, 4),
        'days': len(days),
        'span': [days[0].isoformat(), days[-1].isoformat()],
    }


def yearly(days: List[date], eq: np.ndarray) -> Dict[str, float]:
    out: Dict[str, float] = {}
    prev = eq[0]
    year = days[0].year
    for i, d in enumerate(days[1:], 1):
        if d.year != year:
            out[str(year)] = round((eq[i - 1] / prev - 1) * 100, 2)
            prev, year = eq[i - 1], d.year
    out[str(year)] = round((eq[-1] / prev - 1) * 100, 2)
    return out


def slice_window(days: List[date], eq: np.ndarray,
                 a: date, b: date) -> Tuple[List[date], np.ndarray]:
    idx = [i for i, d in enumerate(days) if a <= d <= b]
    if not idx:
        return [], np.array([])
    lo, hi = idx[0], idx[-1]
    return days[lo:hi + 1], eq[lo:hi + 1]


def main() -> None:
    series: Dict[str, Tuple[List[date], np.ndarray]] = {}
    for name, path in SERIES.items():
        series[name] = read_equity(path)
    series['中证全指'] = read_equity_index()
    series['等权全市场'] = equal_weight_index()

    out: Dict[str, Any] = {
        '口径': {
            '预热': '2009-08-01 起步、2009-09 首个信号（v1/v2/v3）',
            '复权': '后复权（hfq）',
            '再平衡': '月末信号、次日开盘成交、月频；v2/v3 剔除项同冻结记录',
            '基准_中证全指': '000985.SH 收盘，本地自 2011-08-02 起',
            '基准_等权全市场': '2010-01-04 全 A 等权建仓后不再调仓、不补新上市，后复权、停牌平移、退市冻结',
            'sharpe': 'rf=0，日收益均值/标准差×√252',
        },
        '全区间': {},
        '中证全指可比区间': {},
        '逐年收益': {},
    }
    for name, (days, eq) in series.items():
        m = metrics(days, eq)
        out['全区间'][name] = m
        out['逐年收益'][name] = yearly(days, eq)
        sub_days, sub_eq = slice_window(days, eq, IDX_START, END)
        out['中证全指可比区间'][name] = metrics(sub_days, sub_eq)
        print('RISK ' + json.dumps({'name': name, **m}, ensure_ascii=False),
              flush=True)

    doc = json.loads(DOCS_JSON.read_text(encoding='utf-8'))
    doc['风险指标_2026-11-14'] = out
    DOCS_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=1),
                         encoding='utf-8')
    print('WROTE ' + str(DOCS_JSON.relative_to(ROOT)), flush=True)


def read_equity_index() -> Tuple[List[date], np.ndarray]:
    df = pl.read_parquet(INDEX).select(['date', 'close']).sort('date')
    days = [date.fromisoformat(str(x)) for x in df['date'].to_list()]
    eq = df['close'].to_numpy().astype(float)
    keep = [i for i, d in enumerate(days) if START <= d <= END]
    days = [days[i] for i in keep]
    eq = eq[keep]
    return days, eq / eq[0]


if __name__ == '__main__':
    main()
