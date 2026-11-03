#!/usr/bin/env python3

"""
除权除息记账回归测试。

口径：原始价成交/盯市 + 分红入现金 + 送转加股数 + 配股扣现金，
权益 = 现金 + Σ(持仓股数 × 原始收盘价)。所有用例使用零成本模型，
把交易成本对净值的干扰排除，只验证除权记账本身的净值中性。
"""

from collections.abc import Mapping
from datetime import date

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.corporate_actions import (
    CorporateAction,
    CorporateActionSchedule,
)
from src.backtest.strategies.base_strategy import BaseStrategy

ZERO_COST = CostModel(overrides={
    'commission_rate': 0.0,
    'min_commission': 0.0,
    'stamp_tax_rate': 0.0,
    'transfer_fee_rate': 0.0,
    'slippage': 0.0,
})

CODE = '600000.SH'


class _IndexSignalStrategy(BaseStrategy):
    """在指定 bar 索引发出固定信号；其余 bar 持有不动。"""

    def __init__(self, signals: Mapping[int, str]):
        super().__init__(name='索引信号策略')
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, 'hold')


def _make_df(opens, closes, volumes=None) -> pl.DataFrame:
    """构造单标日线；索引 0 = 2023-01-02（工作日序列）。

    信号在 T 日收盘产生、T+1 开盘成交，因此「索引 0 买入」对应
    「索引 1 开盘成交」，除权日按业务日序列对齐。
    """
    days = len(closes)
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    opens = np.asarray(opens, dtype=float)
    closes = np.asarray(closes, dtype=float)
    high = np.maximum(opens, closes) * 1.005
    low = np.minimum(opens, closes) * 0.995
    if volumes is None:
        volumes = np.full(days, 1_000_000.0)
    return pl.DataFrame({
        'date': dates,
        'open': opens,
        'high': high,
        'low': low,
        'close': closes,
        'volume': np.asarray(volumes, dtype=float),
    })


def _div_frame(events: list[dict]) -> pl.DataFrame:
    """构造 stock_dividend 快照同构的分红记录。"""
    rows = []
    for event in events:
        rows.append({
            'ts_code': CODE,
            'ex_date': event['ex_date'],
            'cash_div': event.get('cash_div', 0.0),
            'share_div': event.get('share_div', 0.0),
            'rights_issue_ratio': event.get('rights_issue_ratio', 0.0),
            'rights_issue_price': event.get('rights_issue_price', 0.0),
        })
    return pl.DataFrame(rows)


def _buy_trade(result) -> dict:
    """首笔买入成交（测试数据保证索引 0 的 buy 在索引 1 开盘成交）。"""
    trades = [trade for trade in result['trades'] if trade['signal'] == 'buy']
    assert trades, result['expired_orders']
    return trades[0]


def _run(data: pl.DataFrame, *, dividends, capital=1200.0, buy_index=0,
         tax_rate=0.0, rights_policy='partial'):
    engine = BacktestEngine(data)
    engine.cost_model = ZERO_COST
    engine.set_strategy(_IndexSignalStrategy({buy_index: 'buy'}))
    return engine.run_backtest(
        initial_capital=capital, ts_code=CODE,
        dividends=dividends, dividend_tax_rate=tax_rate,
        rights_policy=rights_policy,
    )


def test_cash_dividend_is_credited_on_ex_date():
    """现金分红在除权日入现金，价格同步下跌时净值必须中性。"""
    data = _make_df(
        opens=[10.0, 10.0, 9.0, 9.0, 9.0],
        closes=[10.0, 10.0, 9.0, 9.0, 9.0],
    )
    result = _run(data, dividends={CODE: _div_frame([
        {'ex_date': date(2023, 1, 4), 'cash_div': 1.0},
    ])})

    base = result['equity_curve'][1]['equity']   # 买入日收盘权益
    assert result['positions'] == {CODE: 100}
    assert result['dividend_cash_total'] == pytest.approx(100.0)
    assert result['final_equity'] == pytest.approx(base, abs=1e-9)
    ex_day = [row for row in result['equity_curve'] if str(row['date'])[:10] == '2023-01-04']
    assert ex_day and ex_day[0]['equity'] == pytest.approx(base, abs=1e-9)


def test_tax_rate_is_configurable():
    """税率 20%：现金分红按税后 0.8 元/股入账。"""
    data = _make_df(
        opens=[10.0, 10.0, 9.0, 9.0, 9.0],
        closes=[10.0, 10.0, 9.0, 9.0, 9.0],
    )
    result = _run(
        data,
        dividends={CODE: _div_frame([
            {'ex_date': date(2023, 1, 4), 'cash_div': 1.0},
        ])},
        tax_rate=0.2,
    )

    base = result['equity_curve'][1]['equity']
    assert result['dividend_cash_total'] == pytest.approx(80.0)
    # 相比免税口径，净值恰好少 20 元（100 股 × 1 元 × 20%）
    assert result['final_equity'] == pytest.approx(base - 20.0, abs=1e-9)


def test_ten_for_ten_bonus_keeps_equity_neutral():
    """硬要求：10 送 10 除权日净值必须不变（价格腰斩、股数翻倍）。"""
    data = _make_df(
        opens=[10.0, 10.0, 5.0, 5.0, 5.0],
        closes=[10.0, 10.0, 5.0, 5.0, 5.0],
    )
    result = _run(data, dividends={CODE: _div_frame([
        {'ex_date': date(2023, 1, 4), 'share_div': 1.0},
    ])})

    base = result['equity_curve'][1]['equity']
    assert result['positions'] == {CODE: 200}
    assert result['final_equity'] == pytest.approx(base, abs=1e-9)
    for row in result['equity_curve'][1:]:
        assert row['equity'] == pytest.approx(base, abs=1e-9)


def test_cash_and_bonus_same_day_keeps_equity_neutral():
    """同日现金分红 + 10 送 10：现金与股数同时记账，净值仍中性。"""
    data = _make_df(
        opens=[10.0, 10.0, 4.5, 4.5, 4.5],
        closes=[10.0, 10.0, 4.5, 4.5, 4.5],
    )
    result = _run(data, dividends={CODE: _div_frame([
        {'ex_date': date(2023, 1, 4), 'cash_div': 1.0, 'share_div': 1.0},
    ])})

    base = result['equity_curve'][1]['equity']
    assert result['positions'] == {CODE: 200}
    assert result['dividend_cash_total'] == pytest.approx(100.0)
    assert result['final_equity'] == pytest.approx(base, abs=1e-9)


def test_rights_issue_full_subscription_keeps_equity_neutral():
    """现金充足时全额配股：扣现金、加股数，按 10 配 1 的理论除权价净值中性。"""
    ex_price = (10.0 - 0.0 + 8.0 * 0.1) / (10.0 * 1.1) * 10.0  # 9.818181...
    data = _make_df(
        opens=[10.0, 10.0, ex_price, ex_price, ex_price],
        closes=[10.0, 10.0, ex_price, ex_price, ex_price],
    )
    result = _run(
        data,
        dividends={CODE: _div_frame([
            {'ex_date': date(2023, 1, 4), 'rights_issue_ratio': 0.1,
             'rights_issue_price': 8.0},
        ])})
    base = result['equity_curve'][1]['equity']
    log = result['corporate_actions']
    assert len(log) == 1
    assert log[0]['rights_shares'] == 10
    assert log[0]['rights_abandoned_shares'] == 0
    assert result['positions'] == {CODE: 110}
    assert result['final_equity'] == pytest.approx(base, abs=1e-6)


def test_rights_issue_partial_subscription_when_cash_short():
    """现金不足：默认按现金能力部分认购，未认购股数必须显式记录。"""
    ex_price = (10.0 - 0.0 + 8.0 * 0.1) / (10.0 * 1.1) * 10.0
    data = _make_df(
        opens=[10.0, 10.0, ex_price, ex_price, ex_price],
        closes=[10.0, 10.0, ex_price, ex_price, ex_price],
    )
    result = _run(
        data, capital=1050.0,
        dividends={CODE: _div_frame([
            {'ex_date': date(2023, 1, 4), 'rights_issue_ratio': 0.1,
             'rights_issue_price': 8.0},
        ])})
    cash_after_buy = _buy_trade(result)['capital']
    log = result['corporate_actions'][0]
    assert log['shares_before'] == 100
    assert log['rights_shares'] == 6          # 可用现金约 50 元，8 元/股 → 6 股
    assert log['rights_abandoned_shares'] == 4
    assert log['rights_cost'] == pytest.approx(48.0)
    assert result['positions'] == {CODE: 106}
    # 剩余现金 + 106 股 × 9.818181...（部分认购不享受未认购部分的折价）
    assert result['final_equity'] == pytest.approx(
        cash_after_buy - 48.0 + 106 * ex_price, abs=1e-6)


def test_rights_issue_abandon_policy_is_explicit():
    """abandon 策略：一股不配，并记录放弃股数（不静默跳过）。"""
    ex_price = (10.0 - 0.0 + 8.0 * 0.1) / (10.0 * 1.1) * 10.0
    data = _make_df(
        opens=[10.0, 10.0, ex_price, ex_price, ex_price],
        closes=[10.0, 10.0, ex_price, ex_price, ex_price],
    )
    result = _run(
        data, capital=1100.0, rights_policy='abandon',
        dividends={CODE: _div_frame([
            {'ex_date': date(2023, 1, 4), 'rights_issue_ratio': 0.1,
             'rights_issue_price': 8.0},
        ])})
    log = result['corporate_actions'][0]
    assert log['rights_shares'] == 0
    assert log['rights_abandoned_shares'] == 10
    assert result['positions'] == {CODE: 100}


def test_no_dividend_is_identical_to_previous_behavior():
    """不传/传空事件表时，结果与修复前完全一致（无分红对照）。"""
    data = _make_df(
        opens=[10.0, 10.0, 9.0, 9.0, 9.0],
        closes=[10.0, 10.0, 9.0, 9.0, 9.0],
    )
    no_schedule = _run(data, dividends=None)
    empty_schedule = _run(data, dividends={})

    assert no_schedule['equity_curve'] == empty_schedule['equity_curve']
    assert no_schedule['trades'] == empty_schedule['trades']
    assert no_schedule['equity_curve'][-1]['equity'] == _buy_trade(no_schedule)['capital'] + 900.0
    assert no_schedule['corporate_actions'] == []


def test_dividend_still_credited_when_suspended_on_ex_date():
    """除权日当天停牌（成交量为 0）：分红仍需按记录日持仓计提。"""
    data = _make_df(
        opens=[10.0, 10.0, 10.0, 9.0, 9.0],
        closes=[10.0, 10.0, 10.0, 9.0, 9.0],
        volumes=[1e6, 1e6, 0.0, 1e6, 1e6],
    )
    result = _run(data, dividends={CODE: _div_frame([
        {'ex_date': date(2023, 1, 4), 'cash_div': 1.0},
    ])})

    base = result['equity_curve'][1]['equity']
    assert result['dividend_cash_total'] == pytest.approx(100.0)
    ex_day = [row for row in result['equity_curve'] if str(row['date'])[:10] == '2023-01-04']
    # 停牌日价格未除权，分红先入账 → 当日权益暂时高出 100 元；复牌日回落
    assert ex_day and ex_day[0]['equity'] == pytest.approx(base + 100.0, abs=1e-9)
    assert result['final_equity'] == pytest.approx(base, abs=1e-9)
    assert not [t for t in result['trades'] if str(t['date'])[:10] == '2023-01-04']


def test_buying_on_ex_date_does_not_receive_dividend():
    """除权日开盘才买入（记录日不持仓）不得享有本次分红。"""
    data = _make_df(
        opens=[10.0, 10.0, 9.0, 9.0, 9.0],
        closes=[10.0, 10.0, 9.0, 9.0, 9.0],
    )
    result = _run(data, buy_index=1, dividends={CODE: _div_frame([
        {'ex_date': date(2023, 1, 4), 'cash_div': 1.0},
    ])})

    assert result['corporate_actions'] == []
    assert result['dividend_cash_total'] == 0.0
    assert result['final_equity'] == pytest.approx(
        _buy_trade(result)['capital'] + 900.0, abs=1e-9)


def test_multiple_same_day_events_are_aggregated():
    """同一除权日多条记录：现金分红相加，不能重复或遗漏。"""
    data = _make_df(
        opens=[10.0, 10.0, 9.0, 9.0, 9.0],
        closes=[10.0, 10.0, 9.0, 9.0, 9.0],
    )
    result = _run(data, dividends={CODE: _div_frame([
        {'ex_date': date(2023, 1, 4), 'cash_div': 0.5},
        {'ex_date': date(2023, 1, 4), 'cash_div': 0.5},
    ])})

    base = result['equity_curve'][1]['equity']
    assert len(result['corporate_actions']) == 1
    assert result['corporate_actions'][0]['cash_per_share'] == pytest.approx(1.0)
    assert result['dividend_cash_total'] == pytest.approx(100.0)
    assert result['final_equity'] == pytest.approx(base, abs=1e-9)


def test_index_codes_are_excluded_from_schedule():
    """指数没有分红：同号指数代码必须被排除，不能按个股分红记账。"""
    frame = _div_frame([{'ex_date': date(2023, 1, 4), 'cash_div': 1.0}])
    schedule = CorporateActionSchedule.from_frames({'000001.SH': frame})
    assert schedule.is_empty()
    assert schedule.codes() == ()
    assert schedule.events_for('000001.SH') == ()


def test_schedule_parses_event_and_skips_zero_rows():
    """share_div 的 0.0 是「无送转」而不是缺失，零事件行应被跳过。"""
    frame = _div_frame([
        {'ex_date': date(2023, 1, 4), 'cash_div': 1.0},
        {'ex_date': date(2023, 1, 5), 'cash_div': 0.0, 'share_div': 0.0},
    ])
    schedule = CorporateActionSchedule.from_frames({CODE: frame})
    events = schedule.events_for(CODE)
    assert len(events) == 1
    assert events[0] == CorporateAction(ex_date=date(2023, 1, 4), cash_per_share=1.0)


def test_invalid_rights_policy_is_rejected():
    """非法配股策略必须显式报错，不能静默按默认策略处理。"""
    data = _make_df(opens=[10.0, 10.0], closes=[10.0, 10.0])
    with pytest.raises(ValueError):
        _run(data, rights_policy='ignore', dividends=None)
