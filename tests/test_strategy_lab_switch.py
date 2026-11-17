"""强弱比切换执行口径：信号日收盘生成，成交日为下一交易日、成交价为当日开盘价。

用合成行情跑完整链路（``build_switch_plan`` → ``pipeline.run_engine``），
不读本地真实行情；旧版没有 ``asset_switch`` 执行器，本用例在其上必红。
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.backtest.runner import pipeline
from src.quant.strategy_lab import switch_timing
from src.quant.strategy_lab.rules import validate_rule

NUM = '510999.SH'
DEN = '510998.SH'


def _weekdays(count: int) -> list[date]:
    days: list[date] = []
    day = date(2024, 1, 2)
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


def _frame(days: list[date], opens: list[float]) -> pl.DataFrame:
    """hfq_factor=2.0：复权价与原始价不同，引擎契约才接受；
    引擎成交用 raw_open，因此断言可直接对原始开盘价。"""
    return pl.DataFrame({
        'date': days,
        'open': opens,
        'high': [price * 1.005 for price in opens],
        'low': [price * 0.995 for price in opens],
        'close': opens,
        'volume': [1e8] * len(days),
        'hfq_factor': [2.0] * len(days),
    })


def _rule(days: list[date]) -> dict:
    return validate_rule({
        'rule_version': 'strategy_lab/v1',
        'rule_type': 'asset_switch',
        'name': '切换口径测试',
        'period': {'start': days[0].isoformat(), 'end': days[-1].isoformat()},
        'legs': [{'code': NUM, 'label': '分子'}, {'code': DEN, 'label': '分母'}],
        'switch': {'numerator': NUM, 'denominator': DEN, 'window': 3,
                   'entry': 0.02, 'exit': 0.02, 'min_hold_days': 2,
                   'initial': 'denominator'},
        'positions': {'mode': 'all_in'},
        'execution': {'price': 'next_open', 'delay_days': 1},
    })


def test_switch_trades_next_open(tmp_path, monkeypatch):
    days = _weekdays(40)
    num_open = [round(4.0 * 1.04 ** i, 4) for i in range(len(days))]
    den_open = [1.0] * len(days)
    paths = {}
    for code, opens in ((NUM, num_open), (DEN, den_open)):
        path = tmp_path / f'{code}.parquet'
        _frame(days, opens).write_parquet(path)
        paths[code] = path
    monkeypatch.setattr(switch_timing, '_quote_path', lambda code: paths[code])

    rule = _rule(days)
    plan = switch_timing.build_switch_plan(rule)

    # 计划层：首个信号出现在窗口填满当日，成交日为其下一交易日
    assert plan.markers[0]['signal_date'] == days[2].isoformat()
    assert plan.markers[0]['date'] == days[3].isoformat()

    spec = {'_resolved': {'portfolio': rule['portfolio'], 'cost': rule['cost']}}
    result, _, _ = pipeline.run_engine(plan.data, plan.schedules, spec, {}, {})
    trades = [{**trade, 'date': pipeline.to_date(trade['date'])}
              for trade in result['trades']]

    # 初始建仓：成交日 = 信号日下一交易日，成交价 = 当日原始开盘价
    first_buy = next(t for t in trades
                     if t['signal'] == 'buy' and t['date'] == days[3])
    assert first_buy['price'] == pytest.approx(den_open[3])

    # 切换建仓：同样落在信号日下一交易日，按当日开盘价成交
    switch_marker = next(m for m in plan.markers if m.get('from') == 'denominator')
    exec_day = date.fromisoformat(switch_marker['date'])
    index = days.index(exec_day)
    switch_buy = next(t for t in trades
                      if t['signal'] == 'buy' and t['date'] == exec_day)
    assert switch_buy['price'] == pytest.approx(num_open[index])
    # 同日卖出旧腿，证明换仓是 D+1 开盘同时完成
    assert any(t['signal'] == 'sell' and t['date'] == exec_day for t in trades)
