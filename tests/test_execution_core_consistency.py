#!/usr/bin/env python3

"""
执行内核一致性回归测试。

单票引擎与组合引擎必须共用同一套撮合语义：任何 A 股规则修复都只允许
落在 execution_core，两条路径不允许再出现独立实现。
"""

import inspect

import numpy as np
import pandas as pd
import polars as pl

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.execution_core import EventBacktestRunner, ExecutionRules
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.ma_strategy import MAStrategy
from src.risk import MaxPositionWeight, RiskEngine


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
    cash = dict.fromkeys(data_dict, per_code_initial)

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
    opens = dict(zip(dates, data['open'].to_list(), strict=False))

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


def test_small_order_steps_down_one_lot_instead_of_expiring():
    """min_commission 下限使小单实际成本超费率估算时，应回退一手成交而非整笔作废。

    回归：估算用 get_buy_cost_rate() 不含 min_commission=5.0 下限，
    现金接近满仓的小单曾只做一次 total_pay <= available 检查就被吞掉。
    """
    # 价格 10 元/股、现金 10006：费率估算 10006/10.0051≈1000.09→1000 股，
    # 实际成本 max(10000*0.0003,5)+0.1+2.0=7.1 → 总价 10007.1 > 10006 超估算，
    # 回退一手后 900 股：9000 + max(2.7,5)+0.09+1.8 = 9006.89 ≤ 10006 成交
    data = _make_df(opens=[10.0] * 4, closes=[10.0] * 4)
    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=10_006.0, ts_code='600000.SH')

    assert result['expired_orders'] == []
    assert len(result['trades']) == 1
    assert result['trades'][0]['shares'] == 900


def test_limit_check_uses_raw_prices_on_ex_dividend_day():
    """除权日涨跌停判定必须用原始价：hfq 复权比值失真不得误放行。

    d2 为除权日：原始开盘 9.0 / 原始前收 10.0 = 一字跌停（-10%，不可卖）；
    hfq 因子 1.05 使复权开盘 9.45 / 复权前收 10.0 = -5.5%（看似可交易）。
    若误用复权价判定，卖出会被放行；正确行为是 not_tradable 作废。
    """
    days = 3
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    df = pl.DataFrame({
        'date': dates,
        'open': [10.0, 10.0, 9.45],
        'high': [10.0, 10.0, 9.45],
        'low': [10.0, 10.0, 9.45],
        'close': [10.0, 10.0, 9.45],
        'volume': [1_000_000.0] * days,
        'raw_open': [10.0, 10.0, 9.0],
        'raw_close': [10.0, 10.0, 9.0],
    })
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 1: 'sell'}))
    result = engine.run_backtest(initial_capital=100_000.0, ts_code='600000.SH')

    sells = [t for t in result['trades'] if t['signal'] == 'sell']
    assert not sells, '除权日一字跌停（按原始价判定）不得放行卖出'
    assert any(o['reason'] == 'not_tradable' for o in result['expired_orders'])


def test_limit_check_uses_raw_prices_blocks_buy_on_ex_dividend_limit_up():
    """除权日原始价涨停（+10%）不可买入，即使复权比值仅 +4.8% 看似可买。"""
    days = 3
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    df = pl.DataFrame({
        'date': dates,
        'open': [10.0, 10.0, 10.48],
        'high': [10.0, 10.0, 10.48],
        'low': [10.0, 10.0, 10.48],
        'close': [10.0, 10.0, 10.48],
        'volume': [1_000_000.0] * days,
        'raw_open': [10.0, 10.0, 11.0],
        'raw_close': [10.0, 10.0, 11.0],
    })
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({1: 'buy'}))
    result = engine.run_backtest(initial_capital=100_000.0, ts_code='600000.SH')

    buys = [t for t in result['trades'] if t['signal'] == 'buy']
    assert not buys, '除权日一字涨停（按原始价判定）不得放行买入'
    assert any(o['reason'] == 'not_tradable' for o in result['expired_orders'])


def test_limit_pct_resolves_by_board_prefix():
    """涨跌停幅度必须按代码前缀自适应推导，且不存在可覆盖的 limit_pct 假旋钮。"""
    resolve = ExecutionRules.resolve_limit_pct
    assert resolve('600000.SH') == 0.10                         # 沪主板
    assert resolve('000001.SZ') == 0.10                         # 深主板
    assert resolve('300001.SZ') == 0.20                         # 创业板 300
    assert resolve('301001.SZ') == 0.20                         # 创业板 301
    assert resolve('688001.SH') == 0.20                         # 科创板 688
    assert resolve('689001.SH') == 0.20                         # 科创板 689（CDR）
    assert resolve('680001.SH') == 0.10                         # 680 非科创板，回主板 10%
    assert resolve('830001.BJ') == 0.30                         # 北交所 8 开头
    assert resolve('430001.BJ') == 0.30                         # 北交所 4 开头
    assert resolve('900001.BJ') == 0.30                         # 北交所 9 开头
    assert resolve('920001.BJ') == 0.30                         # 北交所新号段 920
    assert resolve('920001') == 0.30                            # 无后缀时按 920 号段兜底
    assert resolve('900001.SH') == 0.10                         # 沪市 B 股：不得被 9 开头误判 30%
    assert resolve('200001.SZ') == 0.10                         # 深市 B 股
    assert resolve('430001') == 0.10                            # 无后缀老三板：不享受北交所 30%
    assert resolve('') == ExecutionRules.DEFAULT_LIMIT_PCT      # 无代码兜底
    assert resolve('600000.SH', is_st=True) == 0.05             # ST 优先于板块

    # 回归：limit_pct 曾被静默废弃（ts_code 非空时被忽略），现从调用链彻底移除
    assert 'limit_pct' not in inspect.signature(ExecutionRules.__init__).parameters
    assert 'limit_pct' not in inspect.signature(EventBacktestRunner.run).parameters
    assert 'limit_pct' not in inspect.signature(BacktestEngine.run_backtest).parameters
    assert 'limit_pct' not in inspect.signature(PortfolioBacktestEngine.run_backtest).parameters


def test_growth_board_allows_open_above_ten_percent():
    """创业板 20% 幅度：开盘 +15% 应可买入（误用 10% 阈值会被误判涨停而作废）。"""
    df = _make_df(opens=[100.0, 115.0, 115.0], closes=[100.0, 115.0, 115.0])
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='300001.SZ')

    assert [t['signal'] for t in result['trades']] == ['buy']
    assert result['expired_orders'] == []


# ------------------------------------------------------------------
# 缺陷 A/B/C 回归：确保三个可导致错误成交的路径被锁死
# ------------------------------------------------------------------


def test_volume_nan_blocks_trading():
    """缺陷 A 回归：volume 为 NaN（数据源缺字段、pivot 补空、复权对齐空洞）
    必须被视为停牌，禁止成交。"""
    rules = ExecutionRules()
    assert rules.is_tradable('buy', prev_close=100.0, open_price=100.0,
                             high=101.0, low=99.0, close=100.5,
                             volume=float('nan')) is False
    assert rules.is_tradable('sell', prev_close=100.0, open_price=100.0,
                             high=101.0, low=99.0, close=100.5,
                             volume=float('inf')) is False
    # volume=None 等价于缺失，同样视为停牌
    assert rules.is_tradable('buy', prev_close=100.0, open_price=100.0,
                             high=101.0, low=99.0, close=100.5,
                             volume=None) is False


def test_high_low_relative_tolerance_covers_reweighting_error():
    """缺陷 B 回归：复权后同一价位的 high / low 可能相差 1e-16（浮点误差），
    一字板判定必须用相对容差而非精确相等。"""
    rules = ExecutionRules()
    prev_close = 10.0
    # 一字涨停日：close = 11.0（+10%），复权后 high / low 相差 1e-10
    assert rules.is_tradable('buy', prev_close=prev_close, open_price=None,
                             high=11.0 + 1e-10, low=11.0, close=11.0,
                             volume=1_000_000.0, ts_code='600000.SH') is False
    # 跌停同理
    assert rules.is_tradable('sell', prev_close=prev_close, open_price=None,
                             high=9.0, low=9.0 - 1e-12, close=9.0,
                             volume=1_000_000.0, ts_code='600000.SH') is False
    # high-low 差 1e-4（千分之一）不是一字板，不应误判
    assert rules.is_tradable('buy', prev_close=prev_close, open_price=None,
                             high=11.0 + 1e-4, low=11.0, close=11.0,
                             volume=1_000_000.0, ts_code='600000.SH') is True


def test_fill_confidence_low_when_open_missing():
    """缺陷 C 回归：任一标的缺 open 列 → fill_confidence=low，
    且整次回测可正常执行（high≈low 退化路径被激活但不会崩）。"""
    days = 5
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    df_no_open = pl.DataFrame({
        'date': dates,
        'high': np.full(days, 105.0),
        'low': np.full(days, 95.0),
        'close': np.full(days, 100.0),
        'volume': np.full(days, 1_000_000.0),
    })
    df_full = _make_df(opens=[100.0] * days, closes=[100.0] * days)

    portfolio = PortfolioBacktestEngine({'NOOPEN.SH': df_no_open, 'FULL.SH': df_full})
    portfolio.set_strategy('NOOPEN.SH', MAStrategy(short_window=1, long_window=3))
    portfolio.set_strategy('FULL.SH', MAStrategy(short_window=1, long_window=3))
    result = portfolio.run_backtest(initial_capital=1_000_000.0)

    assert result['fill_confidence'] == 'low'
    # 完整列的标的 fill_confidence 不影响（整次回测按最弱的标的标 low）
    assert result is not None


# ------------------------------------------------------------------
# close 缺失 / NaN 回归：整行跳过撮合更新 + 挂单过期 + prev_close 保留
# ------------------------------------------------------------------


def test_close_none_row_expires_pending_order():
    """close=None 当日挂单必须过期，不得泄漏到下一个有效 close 日成交。

    语义：close 缺失 = 无有效行情，当日挂单作废；
    不是静默顺延到下一个有效 close 日。
    """
    # 4 天数据：day0 正常、day1 close=None、day2/3 正常
    # day0 的 buy 信号应该在 day1（close=None）过期，day2 补成交属于幽灵成交
    days = 4
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    df = pl.DataFrame({
        'date': dates,
        'open': [100.0, 100.0, 100.0, 100.0],
        'high': [101.0, 101.0, 101.0, 101.0],
        'low': [99.0, 99.0, 99.0, 99.0],
        'close': [100.0, None, 100.0, 100.0],
        'volume': [1_000_000.0] * days,
    })
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    # 无成交（day1 close=None 挂单过期）
    assert result['trades'] == []
    # expired_orders 包含 price_missing
    assert any(o['reason'] == 'price_missing' for o in result['expired_orders'])


def test_close_nan_inf_blocked():
    """close 为 NaN / inf / ≤0 时，与 close=None 同等处理（跳过更新 + 挂单过期）。"""
    for bad_close in (float('nan'), float('inf'), 0.0, -5.0):
        days = 3
        dates = pd.date_range('2023-01-02', periods=days, freq='B')
        df = pl.DataFrame({
            'date': dates,
            'open': [100.0, 100.0, 100.0],
            'high': [101.0, 101.0, 101.0],
            'low': [99.0, 99.0, 99.0],
            'close': [100.0, bad_close, 100.0],
            'volume': [1_000_000.0] * days,
        })
        engine = BacktestEngine(df)
        engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
        result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
        assert result['trades'] == [], f"close={bad_close!r} 不应成交"
        assert any(o['reason'] == 'price_missing' for o in result['expired_orders']), \
            f"close={bad_close!r} 应触发 price_missing"


# ------------------------------------------------------------------
# P0-2：open 列存在但值为 None 时，不得用 close 伪造成交价
# ------------------------------------------------------------------


def test_open_none_prevents_close_fallback_execution():
    """P0-2 回归：open 列存在但某 bar 为 None（停牌复牌日、除权日、脏数据），
    不得用 close 伪造开盘价成交。回测不应发明价格。"""
    # 4 天数据：day0 正常、day1 open=None、day2/3 正常
    days = 4
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    df = pl.DataFrame({
        'date': dates,
        'open': [100.0, None, 100.0, 100.0],
        'high': [101.0, 101.0, 101.0, 101.0],
        'low': [99.0, 99.0, 99.0, 99.0],
        'close': [100.0, 95.0, 100.0, 100.0],   # day1 close=95 若被伪造成交会是低价买入
        'volume': [1_000_000.0] * days,
    })
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    # 无成交（day1 open=None 挂单过期，不得用 close=95 伪造成交）
    assert result['trades'] == []
    # expired_orders 包含 price_missing
    assert any(o['reason'] == 'price_missing' for o in result['expired_orders'])


def test_open_none_col_existing_vs_col_missing_distinction():
    """P0-2 边界：open 列整体缺失时，列被回填 close + fill_confidence=low；
    open 列存在但值为 None 时，必须触发 price_missing 过期。"""
    days = 3
    dates = pd.date_range('2023-01-02', periods=days, freq='B')

    # 场景 A：open 列整体缺失 → 列回填 close，fill_confidence=low
    df_col_missing = pl.DataFrame({
        'date': dates,
        'high': [101.0] * days,
        'low': [99.0] * days,
        'close': [100.0] * days,
        'volume': [1_000_000.0] * days,
    })
    engine = BacktestEngine(df_col_missing)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    assert result['fill_confidence'] == 'low'
    # 列缺失 → open 回填 close → 用 close 成交（退化，但 fill_confidence 已提示）
    assert result['trades'] != []

    # 场景 B：open 列存在但 day1 为 None → 不成交，price_missing
    df_value_missing = pl.DataFrame({
        'date': dates,
        'open': [100.0, None, 100.0],
        'high': [101.0] * days,
        'low': [99.0] * days,
        'close': [100.0] * days,
        'volume': [1_000_000.0] * days,
    })
    engine2 = BacktestEngine(df_value_missing)
    engine2.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result2 = engine2.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    # fill_confidence=high（列存在，只是某 bar 值缺失）
    assert result2['fill_confidence'] == 'high'
    # 挂单过期，price_missing
    assert result2['trades'] == []
    assert any(o['reason'] == 'price_missing' for o in result2['expired_orders'])


# ------------------------------------------------------------------
# P0-4：pending 残留挂单被新信号覆盖时必须记账
# ------------------------------------------------------------------


def test_pending_no_overwrite_in_current_order():
    """P0-4 防御性验证：当前主循环顺序是 _execute_pending（先 pop pending）
    → 信号生成（后设置 pending），所以 pending 残留值总会在 pop 时被消费/过期，
    不会被新信号覆写。overwritten 分支是安全网，当前不可达。

    残留场景：标的某日整行缺失（不在 date_codes[date]）→ pending 残留 →
    下一有数据日 _execute_pending pop 消费 → 信号生成设置新值。
    残留值在 pop 时已处理，不会覆写。
    """
    from datetime import timedelta

    base = pd.date_range('2023-01-02', periods=4, freq='B')
    # 标的 A：day2 才上市（day0-1 无行）
    df_a = pl.DataFrame({
        'date': base[2:],
        'open': [100.0, 100.0],
        'high': [101.0, 101.0],
        'low': [99.0, 99.0],
        'close': [100.0, 100.0],
        'volume': [1_000_000.0, 1_000_000.0],
    })
    df_b = pl.DataFrame({
        'date': base,
        'open': [100.0] * 4,
        'high': [101.0] * 4,
        'low': [99.0] * 4,
        'close': [100.0] * 4,
        'volume': [1_000_000.0] * 4,
    })
    engine = PortfolioBacktestEngine({
        '000001.SZ': df_a,
        '000002.SZ': df_b,
    })
    # 标的 A 在它自己的 day0（全局 day2）发 buy，day1（全局 day3）发 sell
    engine.set_strategies({
        '000001.SZ': _IndexSignalStrategy({0: 'buy', 1: 'sell'}),
    })
    result = engine.run_backtest(initial_capital=1_000_000.0)

    # 当前代码结构下 overwritten 不会触发（_execute_pending 先 pop 再设置）
    overwritten = [o for o in result['expired_orders'] if o['reason'] == 'overwritten']
    assert overwritten == [], f"当前主循环顺序不应触发 overwritten，实际={overwritten}"
