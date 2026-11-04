"""推荐回放的单笔成交/调出：价格、成本、涨跌停、逐日盈亏。"""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.advice.config import TradingCosts, default_config
from src.advice.pipeline import AdviceEngine
from src.advice.portfolio import Allocation
from src.advice.replay import _ReplayTier
from src.advice.universe import build_meta
from src.backtest.engine.execution_core import ExecutionRules

CODE = '000001.SZ'
DATES = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)]


def _engine(tmp_path: Path) -> AdviceEngine:
    meta_dir = tmp_path / 'meta'
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / 'names.json').write_text(json.dumps({CODE: '平安银行'}),
                                         encoding='utf-8')
    return AdviceEngine(config=default_config(), store_root=tmp_path)


def _row(day: date, *, hfq_open: float, hfq_close: float,
         raw_prev: float, raw_open: float, raw_high: float,
         raw_low: float, raw_close: float, volume: float = 5000.0) -> dict:
    return {
        'date': day, 'ts_code': CODE, 'open': hfq_open, 'close': hfq_close,
        'raw_prev_close': raw_prev, 'raw_open': raw_open,
        'raw_high': raw_high, 'raw_low': raw_low, 'raw_close': raw_close,
        'raw_volume': volume, 'score_percentile': 0.99,
    }


def _tier(tmp_path: Path, rows: list[dict]) -> _ReplayTier:
    scored = pl.DataFrame(rows)
    meta = build_meta([CODE], {CODE: '平安银行'})
    return _ReplayTier(_engine(tmp_path), default_config().tier('short'),
                       scored, DATES, meta, 1_000_000.0)


def test_buy_and_sell_execution(tmp_path):
    signal = _row(DATES[0], hfq_open=10.0, hfq_close=10.0, raw_prev=10.0,
                  raw_open=10.0, raw_high=10.1, raw_low=9.9, raw_close=10.0)
    buy_day = _row(DATES[1], hfq_open=10.2, hfq_close=10.25, raw_prev=10.0,
                   raw_open=10.2, raw_high=10.3, raw_low=10.0, raw_close=10.25)
    sell_day = _row(DATES[2], hfq_open=10.5, hfq_close=10.6, raw_prev=10.25,
                    raw_open=10.5, raw_high=10.6, raw_low=10.4, raw_close=10.6)
    tier = _tier(tmp_path, [signal, buy_day, sell_day])
    allocation = Allocation(ts_code=CODE, target_weight=0.05, shares=1000,
                            price=10.0, industry='银行')

    tier._execute_buy(allocation, buy_day, signal, 1, DATES[1],
                      ExecutionRules())
    position = tier.positions[CODE]
    costs = TradingCosts()
    expected_entry = 10.2 * (1.0 + costs.impact_rate)
    expected_shares = int(math.floor(
        0.05 * 1_000_000.0 / expected_entry / 100) * 100)
    expected_commission = max(
        costs.min_commission, expected_shares * expected_entry * costs.commission_rate)
    assert position.entry_price == pytest.approx(expected_entry)
    assert position.entry_ref_price == pytest.approx(10.0)
    assert position.shares == expected_shares
    assert position.buy_cost == pytest.approx(expected_commission)
    assert tier.cash == pytest.approx(
        1_000_000.0 - expected_shares * expected_entry - expected_commission)
    assert tier.buy_count == 1

    tier._mark(DATES[1], {CODE: buy_day})
    # 当日盈亏 = 持仓浮盈 - 建仓佣金（净值从初始资金起算）
    assert tier.daily[-1]['pnl'] == pytest.approx(
        expected_shares * (10.25 - expected_entry) - expected_commission,
        abs=0.01)
    assert tier.position_daily[-1]['cum_return_pct'] == pytest.approx(
        (10.25 / expected_entry - 1.0) * 100.0)

    tier._execute_sell(CODE, sell_day, 2, ExecutionRules())
    assert CODE not in tier.positions
    trade = tier.closed[-1]
    expected_exit = 10.5 * (1.0 - costs.impact_rate)
    assert trade.exit_price == pytest.approx(expected_exit)
    assert trade.hold_days == 1
    assert trade.shares == expected_shares
    notional = expected_shares * expected_exit
    sell_cost = (max(costs.min_commission, notional * costs.commission_rate)
                 + notional * costs.stamp_duty_rate)
    assert trade.sell_cost == pytest.approx(sell_cost)
    basis = expected_shares * expected_entry + expected_commission
    assert trade.net_return_pct == pytest.approx(
        ((notional - sell_cost) / basis - 1.0) * 100.0)
    assert tier.sell_count == 1


def test_limit_up_and_suspended_are_skipped(tmp_path):
    signal = _row(DATES[0], hfq_open=10.0, hfq_close=10.0, raw_prev=10.0,
                  raw_open=10.0, raw_high=10.1, raw_low=9.9, raw_close=10.0)
    limit_up = _row(DATES[1], hfq_open=11.0, hfq_close=11.0, raw_prev=10.0,
                    raw_open=11.0, raw_high=11.0, raw_low=11.0, raw_close=11.0)
    suspended = dict(limit_up)
    suspended['raw_volume'] = 0.0
    tier = _tier(tmp_path, [signal, limit_up, suspended])
    allocation = Allocation(ts_code=CODE, target_weight=0.05, shares=1000,
                            price=10.0, industry='银行')
    rules = ExecutionRules()

    tier._execute_buy(allocation, limit_up, signal, 1, DATES[1], rules)
    assert CODE not in tier.positions
    assert tier.skipped == 1

    tier._execute_buy(allocation, suspended, signal, 1, DATES[1], rules)
    assert CODE not in tier.positions
    assert tier.skipped == 2
