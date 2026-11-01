#!/usr/bin/env python3

"""
策略压力测试集：连续跌停 / 长期停牌 / 一字板，以及默认强制样本外（OOS）。

P1-14：单点涨跌停规则用例已存在，但它只能证明"某一天拦截正确"。本文件把
这些极端行情串成连续多日的完整回测，验证：

1. 连续跌停期间持仓无法卖出，挂单逐日作废且不幻想成交；
2. 长期停牌期间同样不成交，复牌后挂单才可执行；
3. 一字涨停连续封板时买入不可成交，开板后按开盘价成交；
4. 单票引擎与组合引擎在极端行情下仍逐笔一致；
5. optimize_strategy 默认带 OOS 验证，仅显式传 validation='none' 才关闭。
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.ma_strategy import MAStrategy


class _IndexSignalStrategy(BaseStrategy):
    """在指定 bar 索引发出固定信号（信号在下一交易日开盘执行）。"""

    def __init__(self, signals):
        super().__init__(name='压力测试信号策略')
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, 'hold')


def _stress_df(opens, closes, volumes=None):
    """构造无复权列的行情表：high/low 取开收盘的包络。"""
    days = len(closes)
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    opens = np.asarray(opens, dtype=float)
    closes = np.asarray(closes, dtype=float)
    return pl.DataFrame({
        'date': dates,
        'open': opens,
        'high': np.maximum(opens, closes),
        'low': np.minimum(opens, closes),
        'close': closes,
        'volume': np.full(days, 1_000_000.0) if volumes is None else np.asarray(volumes, dtype=float),
    })


def test_consecutive_limit_down_blocks_sell_until_board_opens():
    """连续跌停三日：卖单逐日作废，开板日才按开盘价成交。"""
    data = _stress_df(
        opens=[100.0, 100.0, 90.0, 81.0, 72.9, 75.0],
        closes=[100.0, 100.0, 90.0, 81.0, 72.9, 75.0],
    )
    dates = data['date'].to_list()

    engine = BacktestEngine(data)
    # 第 2/3/4 根 bar 生成的卖单分别在第 3/4/5 根开板执行
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 2: 'sell', 3: 'sell', 4: 'sell'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    assert [t['signal'] for t in result['trades']] == ['buy', 'sell']
    buy, sell = result['trades']
    assert buy['date'] == dates[1]
    assert abs(buy['price'] - 100.0) < 1e-9
    assert sell['date'] == dates[5]
    assert abs(sell['price'] - 75.0) < 1e-9
    assert sell['shares'] == buy['shares']

    # 连续跌停的两天必须留下作废记录，且不得出现 81/72.9 的成交价
    expired = result['expired_orders']
    assert [e['date'] for e in expired] == [dates[3], dates[4]]
    assert {e['reason'] for e in expired} == {'not_tradable'}
    assert all(e['side'] == 'sell' for e in expired)
    assert {t['price'] for t in result['trades']} == {100.0, 75.0}


def test_long_suspension_expires_orders_then_resumes_trading():
    """停牌三日：挂单逐日作废，复牌首日按开盘价成交。"""
    volumes = [1_000_000.0, 1_000_000.0, 0.0, 0.0, 0.0, 1_000_000.0]
    data = _stress_df(opens=[100.0] * 6, closes=[100.0] * 6, volumes=volumes)
    dates = data['date'].to_list()

    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 2: 'sell', 3: 'sell', 4: 'sell'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    assert [t['signal'] for t in result['trades']] == ['buy', 'sell']
    assert result['trades'][0]['date'] == dates[1]
    assert result['trades'][1]['date'] == dates[5]

    expired = result['expired_orders']
    assert [e['date'] for e in expired] == [dates[3], dates[4]]
    assert {e['reason'] for e in expired} == {'not_tradable'}


def test_consecutive_one_line_limit_up_blocks_buy_until_opened():
    """一字涨停连续两日：买入逐日作废，开板后按开盘价成交。"""
    data = _stress_df(
        opens=[100.0, 110.0, 121.0, 121.0],
        closes=[100.0, 110.0, 121.0, 121.0],
    )
    dates = data['date'].to_list()

    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 1: 'buy', 2: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    # 前两次挂单被一字板拦截，第三次在开板日（第 4 根 bar）成交
    assert len(result['trades']) == 1
    trade = result['trades'][0]
    assert trade['signal'] == 'buy'
    assert trade['date'] == dates[3]
    assert abs(trade['price'] - 121.0) < 1e-9

    expired = result['expired_orders']
    assert [e['date'] for e in expired] == [dates[1], dates[2]]
    assert {e['reason'] for e in expired} == {'not_tradable'}
    assert all(e['side'] == 'buy' for e in expired)


def test_single_and_portfolio_engines_agree_under_stress():
    """极端行情下 N=1 组合引擎与单票引擎仍必须逐笔一致。"""
    data = _stress_df(
        opens=[100.0, 100.0, 90.0, 81.0, 72.9, 75.0],
        closes=[100.0, 100.0, 90.0, 81.0, 72.9, 75.0],
    )
    signals = _IndexSignalStrategy({0: 'buy', 2: 'sell', 3: 'sell', 4: 'sell'})

    single = BacktestEngine(data)
    single.set_strategy(signals)
    single_result = single.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    portfolio = PortfolioBacktestEngine({'600000.SH': data})
    portfolio.set_strategy('600000.SH', signals)
    portfolio_result = portfolio.run_backtest(initial_capital=1_000_000.0)

    assert single_result['trades'] == portfolio_result['trades']
    assert single_result['expired_orders'] == portfolio_result['expired_orders']
    assert single_result['equity_curve'] == portfolio_result['equity_curve']
    assert single_result['final_equity'] == portfolio_result['final_equity']


def _oscillating(days=160):
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    close = 100.0 + 10.0 * np.sin(np.arange(days) / 10.0)
    return pl.DataFrame({
        'date': dates,
        'open': close * 1.0005,
        'high': close * 1.01,
        'low': close * 0.99,
        'close': close,
        'volume': np.full(days, 1_000_000.0),
    })


def test_optimize_strategy_enforces_oos_by_default():
    """默认必须执行 OOS 验证；只有显式 validation='none' 才关闭。"""
    data = _oscillating()

    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    default_result = engine.optimize_strategy(
        {'short_window': [3]}, initial_capital=1_000_000.0, max_workers=1,
    )
    assert 'oos_validation' in default_result
    assert default_result['oos_validation']['enabled'] is True
    # 未指定 walk_forward_windows 时必须显式标记未启用并给出原因
    walk_forward = default_result['oos_validation']['walk_forward']
    assert walk_forward['enabled'] is False
    assert 'walk_forward_windows' in walk_forward['reason']

    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    opt_out = engine.optimize_strategy(
        {'short_window': [3]}, initial_capital=1_000_000.0, max_workers=1,
        validation='none',
    )
    assert 'oos_validation' not in opt_out


def test_optimize_strategy_reports_disabled_oos_on_short_data():
    """数据不足以切分样本内外时必须显式标记未启用，而不是静默跳过。"""
    data = _oscillating(days=30)

    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    result = engine.optimize_strategy(
        {'short_window': [3]}, initial_capital=1_000_000.0, max_workers=1,
        oos_ratio=0.3,
    )

    oos = result['oos_validation']
    assert oos['enabled'] is False
    assert '不足以' in oos['reason']
