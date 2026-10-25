#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
执行内核一致性回归测试。

单票引擎与组合引擎必须共用同一套撮合语义：任何 A 股规则修复都只允许
落在 execution_core，两条路径不允许再出现独立实现。
"""

import numpy as np
import pandas as pd
import polars as pl

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.ma_strategy import MAStrategy
from src.risk import RiskEngine, MaxPositionWeight


class _IndexSignalStrategy(BaseStrategy):
    """在指定 bar 索引发出固定信号，用于验证 T+1 与涨跌停撮合。"""

    def __init__(self, signals):
        super().__init__(name='索引信号策略')
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, 'hold')


def _oscillating(days=160, phase=0.0):
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    close = 100.0 + 10.0 * np.sin(np.arange(days) / 10.0 + phase)
    return pl.DataFrame({
        'date': dates,
        'open': close * 1.0005,
        'high': close * 1.01,
        'low': close * 0.99,
        'close': close,
        'volume': np.full(days, 1_000_000.0),
    })


def _make_df(opens, closes, volumes=None):
    days = len(closes)
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    opens = np.asarray(opens, dtype=float)
    closes = np.asarray(closes, dtype=float)
    high = np.maximum(opens, closes) * 1.005
    low = np.minimum(opens, closes) * 0.995
    return pl.DataFrame({
        'date': dates,
        'open': opens,
        'high': high,
        'low': low,
        'close': closes,
        'volume': np.full(days, 1_000_000.0) if volumes is None else np.asarray(volumes, dtype=float),
    })


def test_single_and_portfolio_are_identical():
    """单票引擎与 N=1 组合引擎必须逐笔、逐日完全一致。"""
    data = _oscillating()

    single = BacktestEngine(data)
    single.set_strategy(MAStrategy(short_window=5, long_window=20))
    single_result = single.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    portfolio = PortfolioBacktestEngine({'600000.SH': data})
    portfolio.set_strategy('600000.SH', MAStrategy(short_window=5, long_window=20))
    portfolio_result = portfolio.run_backtest(initial_capital=1_000_000.0)

    assert single_result['trades'] == portfolio_result['trades']
    assert single_result['equity_curve'] == portfolio_result['equity_curve']
    assert single_result['final_equity'] == portfolio_result['final_equity']
    assert len(single_result['trades']) > 0


def test_risk_rejection_applies_to_both_engines():
    """风控全拦截时，两条路径都必须 0 成交且留下拒单日志。"""
    data = _oscillating()
    risk = RiskEngine([MaxPositionWeight(0.0)])

    single = BacktestEngine(data)
    single.set_strategy(MAStrategy(short_window=5, long_window=20))
    single.risk_engine = risk
    single_result = single.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    portfolio = PortfolioBacktestEngine({'600000.SH': data})
    portfolio.set_strategy('600000.SH', MAStrategy(short_window=5, long_window=20))
    portfolio.risk_engine = risk
    portfolio_result = portfolio.run_backtest(initial_capital=1_000_000.0)

    assert single_result['trades'] == []
    assert portfolio_result['trades'] == []
    assert single_result['risk_summary']['total_rejected'] > 0
    assert portfolio_result['risk_summary']['total_rejected'] > 0


def test_sell_proceeds_are_added_to_remaining_cash():
    """
    卖出所得必须叠加到剩余现金（回归：曾用 `=` 覆盖，吞掉未投入现金）。
    """
    data_dict = {'600000.SH': _oscillating(), '000001.SZ': _oscillating(phase=0.5)}
    engine = PortfolioBacktestEngine(data_dict)
    for code in data_dict:
        engine.set_strategy(code, MAStrategy(short_window=5, long_window=20))
    result = engine.run_backtest(initial_capital=1_000_000.0)

    per_code_initial = 1_000_000.0 / len(data_dict)
    cash = {code: per_code_initial for code in data_dict}

    for trade in result['trades']:
        code = trade['stock_code']
        if trade['signal'] == 'buy':
            cash[code] -= trade['shares'] * trade['price'] + trade['cost']
        else:
            cash[code] += trade['shares'] * trade['price'] - trade['cost']
        assert abs(cash[code] - trade['capital']) < 1e-6, f'{code} 现金记账错误'


def test_t_plus_one_open_price_and_lot_size():
    """t 日信号必须在 t+1 日开盘成交，且买入股数为 100 股整数倍。"""
    data = _oscillating(days=40)
    dates = data['date'].to_list()
    opens = dict(zip(dates, data['open'].to_list()))

    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({5: 'buy', 15: 'sell'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    assert len(result['trades']) == 2
    buy, sell = result['trades']
    assert buy['date'] == dates[6]
    assert sell['date'] == dates[16]
    assert abs(buy['price'] - opens[dates[6]]) < 1e-9
    assert abs(sell['price'] - opens[dates[16]]) < 1e-9
    assert buy['shares'] % 100 == 0


def test_limit_up_blocks_buy_and_limit_down_blocks_sell():
    """开盘触及涨停不可买、触及跌停不可卖。"""
    # 涨停：第 2 根 K 线开盘价相对前收 +10%，t 日买入信号无法成交
    up_df = _make_df(
        opens=[100.0, 110.0, 110.0],
        closes=[100.0, 110.0, 110.0],
    )
    up_engine = BacktestEngine(up_df)
    up_engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    up_result = up_engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    assert up_result['trades'] == []

    # 跌停：t=2 产生卖出信号，t=3 开盘相对前收 -10%，卖出无法成交
    down_df = _make_df(
        opens=[100.0, 100.0, 100.0, 90.0, 90.0],
        closes=[100.0, 100.0, 100.0, 90.0, 90.0],
    )
    down_engine = BacktestEngine(down_df)
    down_engine.set_strategy(_IndexSignalStrategy({0: 'buy', 2: 'sell'}))
    down_result = down_engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    assert len(down_result['trades']) == 1
    assert down_result['trades'][0]['signal'] == 'buy'


def test_pending_order_expires_after_one_day():
    """
    挂单当日有效：进入撮合即消费，不可成交或价格缺失时必须作废，不得残留到次日。

    回归：曾因 `exec_price is None` 提前 return 绕过清理，导致挂单在下一个交易日才成交。
    """
    # 价格缺失（open/close 为 null，成交量正常）：t=0 的买入信号在 t=1 作废，t=2 不得补成交
    missing_price = pl.DataFrame({
        'date': pd.date_range('2023-01-02', periods=4, freq='B'),
        'open': [100.0, None, 100.0, 100.0],
        'high': [101.0, None, 101.0, 101.0],
        'low': [99.0, None, 99.0, 99.0],
        'close': [100.0, None, 100.0, 100.0],
        'volume': [1_000_000.0] * 4,
    })
    engine = BacktestEngine(missing_price)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    assert engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')['trades'] == []

    # 持仓中再次触发 buy：该挂单当日作废，不得在卖出后幽灵成交
    data = _make_df(opens=[100.0] * 8, closes=[100.0] * 8)
    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 2: 'buy', 4: 'sell'}))
    trades = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')['trades']
    assert [t['signal'] for t in trades] == ['buy', 'sell']


def test_suspension_blocks_trading():
    """成交量为 0 的停牌日不可交易。"""
    df = _make_df(
        opens=[100.0, 100.0, 100.0],
        closes=[100.0, 100.0, 100.0],
        volumes=[1_000_000.0, 0.0, 1_000_000.0],
    )
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    assert result['trades'] == []


def test_expired_orders_are_audited():
    """挂单作废必须留痕：已持仓买 / 无持仓卖 / 资金不足 / 涨跌停均记入 expired_orders。

    回归：作废路径原先依赖巧合的控制流静默吞单，不进 trades / rejected_orders，
    无任何可审计痕迹。
    """
    # 已持仓再买 + 卖出后再卖
    data = _make_df(opens=[100.0] * 8, closes=[100.0] * 8)
    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 2: 'buy', 4: 'sell', 5: 'sell'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    reasons = [o['reason'] for o in result['expired_orders']]
    assert [t['signal'] for t in result['trades']] == ['buy', 'sell']
    assert reasons == ['already_holding', 'no_position']

    # 资金不足：本金连一手都买不起
    data = _make_df(opens=[100.0] * 4, closes=[100.0] * 4)
    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=5_000.0, ts_code='600000.SH')
    assert result['trades'] == []
    assert [o['reason'] for o in result['expired_orders']] == ['insufficient_cash']

    # 涨停不可买
    up_df = _make_df(opens=[100.0, 110.0, 110.0], closes=[100.0, 110.0, 110.0])
    engine = BacktestEngine(up_df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    assert result['trades'] == []
    assert [o['reason'] for o in result['expired_orders']] == ['not_tradable']
