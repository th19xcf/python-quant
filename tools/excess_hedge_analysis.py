#!/usr/bin/env python3
"""v3 超额与对冲近似：判断 61% 回撤里有多少是 beta（只读）。

- 超额 = v3 日收益 − 等权全市场买入持有日收益（同一基准口径见
  `risk_metrics_benchmark.equal_weight_index`），累乘成超额净值；
- 对冲 = v3 日收益 − 中证 500（000905.SH）日收益 − 贴水成本/252，
  贴水取 3%/6%/9% 三档年化，100% 对冲、现金端收益记 0；
- 三档一次跑完，结果就地写入 `docs/基线_价量估值等权_2026-11-13.json`
  的 `超额与对冲_2026-11-14` 节。

用法
----
    .venv\\Scripts\\python.exe tools\\excess_hedge_analysis.py
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Dict

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import risk_metrics_benchmark as r

V3 = ROOT / '_staging/equity_v3_excl_vol_new_2026-11-14.csv'
CSI500 = ROOT / 'data/local_store/quotes/index/sh/000905.SH.parquet'
DOCS_JSON = r.DOCS_JSON
BASES = (0.03, 0.06, 0.09)
TRADING_DAYS = 252


def daily_returns(days, eq) -> Dict[date, float]:
    return {days[i]: float(eq[i] / eq[i - 1] - 1) for i in range(1, len(days))}


def curve_from(rets: Dict[date, float]):
    days = sorted(rets)
    eq = np.concatenate([[1.0], np.cumprod(1.0 + np.array([rets[d] for d in days]))])
    return [days[0]] + days, eq


def index_returns() -> Dict[date, float]:
    df = pl.read_parquet(CSI500).select(['date', 'close']).sort('date')
    days = [date.fromisoformat(str(x)) for x in df['date'].to_list()]
    close = df['close'].to_numpy().astype(float)
    return {days[i]: float(close[i] / close[i - 1] - 1) for i in range(1, len(days))}


def ew_series():
    cache = ROOT / '_staging/_ew_buyhold_cache.csv'
    if cache.exists():
        df = pl.read_csv(cache)
        days = [date.fromisoformat(str(x)) for x in df['date'].to_list()]
        return days, df['equity'].to_numpy().astype(float)
    days, eq = r.equal_weight_index()
    pl.DataFrame({'date': [d.isoformat() for d in days], 'equity': eq}).write_csv(cache)
    return days, eq


def main() -> None:
    v3_days, v3_eq = r.read_equity(V3)
    ew_days, ew_eq = ew_series()
    v3 = daily_returns(v3_days, v3_eq)
    ew = daily_returns(ew_days, ew_eq)
    csi = index_returns()
    common = sorted(set(v3) & set(ew))
    hedge_days = sorted(set(common) & set(csi))

    out = {
        '口径': {
            '预热': '2009-08-01 起步、2009-09 首个信号；后复权；月末信号、次日开盘成交、月频',
            '超额基准': '等权全市场买入持有（2010-01-04 建仓、不调仓、不补新上市）',
            '对冲空头': '中证 500（000905.SH）日收益，100% 名义对冲，现金端收益记 0',
            '贴水成本': '年化 3%/6%/9%，按 /252 逐日扣减',
            '区间': [common[0].isoformat(), common[-1].isoformat()],
        },
        '结果': {},
    }

    excess = {d: v3[d] - ew[d] for d in common}
    days, eq = curve_from(excess)
    out['结果']['超额_v3减等权全市场'] = r.metrics(days, eq)
    print('EXCESS ' + json.dumps(out['结果']['超额_v3减等权全市场'], ensure_ascii=False),
          flush=True)

    v3_common = {d: v3[d] for d in common}
    days_v3, eq_v3 = curve_from(v3_common)
    out['结果']['v3_同区间'] = r.metrics(days_v3, eq_v3)
    print('V3WIN ' + json.dumps(out['结果']['v3_同区间'], ensure_ascii=False), flush=True)

    ret_v3 = np.array([v3[d] for d in common])
    ret_ew = np.array([ew[d] for d in common])
    out['结果']['beta与相关'] = {
        'beta_vs_等权全市场': round(float(np.cov(ret_v3, ret_ew)[0, 1]
                                          / np.var(ret_ew, ddof=1)), 4),
        'corr_vs_等权全市场': round(float(np.corrcoef(ret_v3, ret_ew)[0, 1]), 4),
    }

    for b in BASES:
        hedged = {d: v3[d] - csi[d] - b / TRADING_DAYS for d in hedge_days}
        hd, he = curve_from(hedged)
        out['结果'][f'对冲_贴水{int(b * 100)}%'] = r.metrics(hd, he)
        print(f'HEDGE {int(b * 100)}% ' +
              json.dumps(out['结果'][f'对冲_贴水{int(b * 100)}%'], ensure_ascii=False),
              flush=True)

    hd, he = curve_from({d: v3[d] - csi[d] for d in hedge_days})
    out['结果']['对冲_零贴水'] = r.metrics(hd, he)
    out['结果']['对冲区间'] = [hedge_days[0].isoformat(), hedge_days[-1].isoformat()]

    doc = json.loads(DOCS_JSON.read_text(encoding='utf-8'))
    doc['超额与对冲_2026-11-14'] = out
    DOCS_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=1),
                         encoding='utf-8')
    print('WROTE ' + str(DOCS_JSON.name), flush=True)


if __name__ == '__main__':
    main()
