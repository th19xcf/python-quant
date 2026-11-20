"""定投补仓当日信号：最后一根 bar 也要能判定补仓/观察期/止盈并给出触发价。

用合成行情跑 ``build_dca_plan``，不读本地真实行情；历史调度只到倒数第二根 bar，
最后一根 bar 只产出 ``daily`` 信号，不写调度、不产生成交。
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.quant.strategy_lab import switch_timing
from src.quant.strategy_lab.rules import validate_rule

CODE = '510999.SH'


def _weekdays(count: int) -> list[date]:
    days: list[date] = []
    day = date(2024, 1, 2)
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


def _frame(days: list[date], prices: list[float]) -> pl.DataFrame:
    return pl.DataFrame({
        'date': days,
        'open': prices,
        'high': [price * 1.005 for price in prices],
        'low': [price * 0.995 for price in prices],
        'close': prices,
        'volume': [1e8] * len(days),
    })


def _rule(days: list[date], *, tranches: int = 3) -> dict:
    return validate_rule({
        'rule_version': 'strategy_lab/v1',
        'rule_type': 'dca_grid',
        'name': '定投当日信号测试',
        'period': {'start': days[0].isoformat(), 'end': days[-1].isoformat()},
        'target': {'code': CODE, 'label': '测试ETF'},
        'plan': {'tranches': tranches, 'add_drop_pct': 0.04,
                 'take_profit_pct': 0.15, 'trail_drop_pct': 0.04},
        'execution': {'price': 'next_open', 'delay_days': 1},
    })


def _plan(tmp_path, monkeypatch, prices: list[float]):
    days = _weekdays(len(prices))
    path = tmp_path / f'{CODE}.parquet'
    _frame(days, prices).write_parquet(path)
    monkeypatch.setattr(switch_timing, '_quote_path', lambda code: path)
    return days, switch_timing.build_dca_plan(_rule(days))


def test_dca_last_bar_add_signal(tmp_path, monkeypatch):
    """最后一根 bar 收盘跌到最近买入价 -4% 时，当日信号为补仓且不改历史调度。"""
    days, plan = _plan(
        tmp_path, monkeypatch, [10.0, 10.0, 9.5, 9.5, 9.6, 9.6, 9.6, 9.0])

    daily = plan.daily
    assert daily is not None
    assert daily['date'] == days[-1].isoformat()
    assert daily['state'] == 'add'
    assert daily['action'] == 'buy'
    assert daily['tranches'] == 2 and daily['tranches_total'] == 3
    # 下一次补仓价 = 最近一次买入价 9.5 × (1 - 4%)
    assert daily['next_add_price'] == pytest.approx(9.12)
    assert daily['observing'] is False
    assert daily['trail_trigger_price'] is None
    # 进入观察期价 = 平均成本 × (1 + 15%)
    avg_cost = 2 / (1 / 10.0 + 1 / 9.5)
    assert daily['observe_trigger_price'] == pytest.approx(
        round(avg_cost * 1.15, 4))

    # 最后一根 bar 不写调度：历史成交只到倒数第二根 bar
    assert days[-1] not in plan.schedules[CODE]
    assert all(marker['signal_date'] != days[-1].isoformat()
               for marker in plan.markers)


def test_dca_last_bar_enters_observation(tmp_path, monkeypatch):
    """最后一根 bar 收盘首次超过止盈阈值：当日信号为进入观察期，给出回撤触发价。"""
    days, plan = _plan(
        tmp_path, monkeypatch, [10.0, 10.0, 9.5, 9.5, 9.6, 9.6, 9.6, 11.3])

    daily = plan.daily
    assert daily is not None
    assert daily['date'] == days[-1].isoformat()
    assert daily['state'] == 'observe'
    assert daily['action'] is None
    assert daily['observing'] is True
    assert daily['peak'] == pytest.approx(11.3)
    assert daily['trail_trigger_price'] == pytest.approx(round(11.3 * 0.96, 4))
    assert daily['observe_trigger_price'] is None
    assert daily['next_add_price'] == pytest.approx(9.12)


def test_dca_last_bar_triggers_take_profit(tmp_path, monkeypatch):
    """观察期内最后一根 bar 自最高点回撤 4%：当日信号为触发止盈。"""
    days, plan = _plan(
        tmp_path, monkeypatch, [10.0, 10.0, 9.5, 9.5, 11.3, 11.5, 11.3, 10.9])

    daily = plan.daily
    assert daily is not None
    assert daily['date'] == days[-1].isoformat()
    assert daily['state'] == 'take_profit'
    assert daily['action'] == 'sell'
    assert daily['observing'] is True
    assert daily['peak'] == pytest.approx(11.5)
    assert daily['trail_trigger_price'] == pytest.approx(round(11.5 * 0.96, 4))
    assert daily['next_add_price'] is None


def test_dca_last_bar_does_not_change_history(tmp_path, monkeypatch):
    """把区间截到倒数第二根 bar，历史调度与标记必须与全区间完全一致。"""
    prices = [10.0, 10.0, 9.5, 9.5, 9.6, 9.6, 9.6, 9.0]
    days = _weekdays(len(prices))
    path = tmp_path / f'{CODE}.parquet'
    _frame(days, prices).write_parquet(path)
    monkeypatch.setattr(switch_timing, '_quote_path', lambda code: path)

    full = switch_timing.build_dca_plan(_rule(days))
    truncated = switch_timing.build_dca_plan(_rule(days[:-1]))
    assert full.schedules == truncated.schedules
    assert full.markers == truncated.markers
