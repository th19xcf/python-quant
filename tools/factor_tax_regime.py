#!/usr/bin/env python3
"""真值税制 vs 现行固定千 1：v1 基线与 v2 剔除式的月频对照。

脚本层实现分段成本模型，不改引擎源码、不改引擎默认值：

- 印花税（仅卖出）：2023-08-28 前 0.1%，之后 0.05%；
- 过户费：2015-08-01 前沪市 0.06%，2015-08-01~2022-04-28 双边 0.002%，
  2022-04-29 起双边 0.001%。

其余口径与冻结记录完全一致：佣金万 2.5（最低 5 元）、滑点 10bp、冲击 0.015、
T 日收盘信号 / T+1 开盘成交、涨跌停停牌、复权与本金不动；月频两腿再平衡
与成分路径复用 factor_lowvol_quantile / factor_lowvol_variants。

四组：v1 固定千 1、v1 真值分段、v2 固定千 1、v2 真值分段；全区间指标切
2010-01-04~2026-11-13，另报 2023-08-28 起子区间（税制改动生效段）。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_tax_regime.py
"""
from __future__ import annotations

import gc
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import baseline_pv_val_equal_weight as base
import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_lowvol_variants as v
from src.backtest.engine.input_contract import BAR_CONTRACT
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.risk import NoSuspendedTrade, PriceLimitTradable, RiskEngine

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
TAX_CUT = date(2023, 8, 28)
WIN = c.SEG1[0], c.SEG2[1]


class RealTaxFlatCost(base.FlatCostModel):
    """冻结成本口径 + 真值分段印花税/过户费（唯一差异）。"""

    def _stamp_tax_rate(self, trade_date: Any, side: str, ts_code: str = '') -> float:
        if side != 'sell' or self._is_fund(ts_code):
            return 0.0
        day = base.to_date(trade_date)
        if day is not None and day >= TAX_CUT:
            return 0.0005
        return 0.001

    def _transfer_fee_rate(self, trade_date: Any, ts_code: str) -> float:
        if self._is_fund(ts_code):
            return 0.0
        day = base.to_date(trade_date)
        if day is None:
            return 0.00002
        if day >= date(2022, 4, 29):
            return 0.00001
        if day >= date(2015, 8, 1):
            return 0.00002
        return 0.0006 if self._is_shanghai(ts_code) else 0.0


def run_portfolio(data, schedules, dividends, st_status, cost_model):
    used = {code: data[code] for code in schedules if code in data}
    strategies = {code: base.MonthlyTargetWeightStrategy(sig)
                  for code, sig in schedules.items() if code in used and sig}
    used = {code: df for code, df in used.items() if code in strategies}
    engine = PortfolioBacktestEngine(used)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies(strategies)
    engine.set_cost_model(cost_model)
    return engine.run_backtest(
        initial_capital=base.INITIAL_CAPITAL, lot_size=100, industry_map={},
        st_status=st_status, dividends=dividends, pooled_cash=True)


def main() -> None:
    t0 = time.perf_counter()
    data = base.load_universe(WARMUP_START, base.END)
    data, _ = base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    panel = f.compute_vol_panel(data, month_ends)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, month_ends)
    months = [m for m in month_ends if m >= FIRST_SIGNAL and m in members]
    logger.info('有效月末 {} 个（{}~{}）', len(months),
                months[0].isoformat(), months[-1].isoformat())

    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START, base.END,
                                  None, None)

    paths: Dict[str, Dict[date, List[str]]] = {
        'v1': {m: sorted(quoted[m]) for m in months},
        'v2': v.variant_exclude(panel, members, quoted, months),
    }
    costs = {
        'fixed': base.FlatCostModel(10.0, impact_coef=0.015),
        'real': RealTaxFlatCost(10.0, impact_coef=0.015),
    }

    out: Dict[str, Dict[str, Any]] = {}
    for regime, cost in costs.items():
        for obj, path in paths.items():
            sched = v.schedules_from_path(path, prev_day)
            result = run_portfolio(data, sched, dividends, st_status, cost)
            item = {
                'full': c.seg_metrics(result, *WIN),
                'cut': c.seg_metrics(result, TAX_CUT, WIN[1]),
            }
            out[f'{obj}_{regime}'] = item
            logger.info('{} {} 完成', obj, regime)
            print('RUN ' + json.dumps({'obj': obj, 'regime': regime, **item},
                                      ensure_ascii=False), flush=True)
            del result, sched
            gc.collect()

    summary: Dict[str, Any] = {}
    for span in ('full', 'cut'):
        summary[span] = {}
        for regime in costs:
            v1, v2 = out[f'v1_{regime}'][span], out[f'v2_{regime}'][span]
            summary[span][regime] = {
                'v2_minus_v1_annual': round(v2['annual'] - v1['annual'], 6),
                'v2_minus_v1_dd': round(v2['max_drawdown'] - v1['max_drawdown'], 6),
                'v2_minus_v1_cost_share': (
                    round(v2['cost_share_of_gross'] - v1['cost_share_of_gross'], 4)
                    if v2['cost_share_of_gross'] is not None
                    and v1['cost_share_of_gross'] is not None else None),
            }
        for obj in paths:
            fixed, real = out[f'{obj}_fixed'][span], out[f'{obj}_real'][span]
            summary[span][f'{obj}_real_minus_fixed'] = {
                'annual': round(real['annual'] - fixed['annual'], 6),
                'dd': round(real['max_drawdown'] - fixed['max_drawdown'], 6),
                'turnover': round(real['turnover_two_way'] - fixed['turnover_two_way'], 4),
                'cost_share': (
                    round(real['cost_share_of_gross'] - fixed['cost_share_of_gross'], 4)
                    if real['cost_share_of_gross'] is not None
                    and fixed['cost_share_of_gross'] is not None else None),
            }
    print('SUMMARY ' + json.dumps(summary, ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
