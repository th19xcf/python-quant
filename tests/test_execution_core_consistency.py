#!/usr/bin/env python3

"""
执行内核一致性回归测试。

单票引擎与组合引擎必须共用同一套撮合语义：任何 A 股规则修复都只允许
落在 execution_core，两条路径不允许再出现独立实现。
"""

import inspect
import time

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine._parallel_worker import clone_strategy, run_backtest_worker
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.execution_core import (
    AccountBook,
    EventBacktestRunner,
    ExecutionRules,
)
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.ma_strategy import MAStrategy
from src.core.usecases import run_backtest
from src.risk import MaxPositionWeight, PriceLimitTradable, RiskEngine
from src.risk.rules import DrawdownTracker, MaxDrawdownCircuit, Portfolio


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
    """挂单作废必须留痕：资金不足 / 涨跌停均记入 expired_orders。

    P1-3 语义变更：策略输出的是"状态型"信号（持仓期间持续 buy、空仓期间持续
    sell），已持仓再买 / 无持仓再卖不构成新的委托意图，已在信号层过滤，不再
    生成挂单，因此不产生 already_holding / no_position 作废记录。
    """
    # 已持仓再买 + 卖出后再卖：信号层过滤，无挂单、无作废
    data = _make_df(opens=[100.0] * 8, closes=[100.0] * 8)
    engine = BacktestEngine(data)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy', 2: 'buy', 4: 'sell', 5: 'sell'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    reasons = [o['reason'] for o in result['expired_orders']]
    assert [t['signal'] for t in result['trades']] == ['buy', 'sell']
    assert reasons == []

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
    def resolve(code):
        return ExecutionRules.resolve_limit_pct(code, is_st=False)
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
    # ST 三态：True=ST 仅在常规 10% 板块收窄到 5%；20%/30% 板块 ST 与普通股同幅度；
    # False/None（未知）按板块默认幅度
    from datetime import date
    assert ExecutionRules.resolve_limit_pct('600000.SH', is_st=True) == 0.05   # 主板 ST → 5%
    assert ExecutionRules.resolve_limit_pct('200001.SZ', is_st=True) == 0.05   # B 股 ST → 5%
    assert ExecutionRules.resolve_limit_pct('300001.SZ', is_st=True) == 0.20   # 创业板 ST（改革后）→ 20%
    assert ExecutionRules.resolve_limit_pct('300001.SZ', is_st=True,
                                            trade_date=date(2020, 8, 21)) == 0.05  # 创业板 ST（改革前）→ 5%
    assert ExecutionRules.resolve_limit_pct('688001.SH', is_st=True) == 0.20   # 科创板 ST → 20%
    assert ExecutionRules.resolve_limit_pct('830001.BJ', is_st=True) == 0.30   # 北交所 ST → 30%
    assert ExecutionRules.resolve_limit_pct('600000.SH') == 0.10          # 未知 → 主板
    assert ExecutionRules.resolve_limit_pct('600000.SH', is_st=None) == 0.10
    assert ExecutionRules.resolve_limit_pct('300001.SZ') == 0.20          # 未知 → 创业板

    # 回归：limit_pct 曾被静默废弃（ts_code 非空时被忽略），现从调用链彻底移除
    assert 'limit_pct' not in inspect.signature(ExecutionRules.__init__).parameters
    assert 'limit_pct' not in inspect.signature(EventBacktestRunner.run).parameters
    assert 'limit_pct' not in inspect.signature(BacktestEngine.run_backtest).parameters
    assert 'limit_pct' not in inspect.signature(PortfolioBacktestEngine.run_backtest).parameters


def test_chinext_limit_pct_switches_at_registration_reform_date():
    """创业板涨跌停幅度必须按交易日区分改革前后：2020-08-24 前 10%，当日起 20%。

    回归：BOARD_RULES 曾把 300/301 固定映射为 20%，注册制改革前的历史行情
    会把实际已封板（>+10%）的开盘价误判为可成交，系统性虚增收益。
    """
    from datetime import date

    before = date(2020, 8, 21)   # 改革前最后一个周五
    reform = date(2020, 8, 24)   # 改革实施日
    after = date(2020, 8, 25)    # 改革后

    for code in ('300001.SZ', '301001.SZ'):
        assert ExecutionRules.resolve_limit_pct(code, is_st=False, trade_date=before) == 0.10
        assert ExecutionRules.resolve_limit_pct(code, is_st=False, trade_date=reform) == 0.20
        assert ExecutionRules.resolve_limit_pct(code, is_st=False, trade_date=after) == 0.20

    # trade_date 缺失时回退到 BOARD_RULES 的 20%（与旧行为一致，不引入新偏置）
    assert ExecutionRules.resolve_limit_pct('300001.SZ', is_st=False) == 0.20

    # 科创板 20% 不受该日期影响（注册制从上市首日即 20%）
    assert ExecutionRules.resolve_limit_pct(
        '688001.SH', is_st=False, trade_date=before) == 0.20
    # 主板 10% 不受该日期影响
    assert ExecutionRules.resolve_limit_pct(
        '600000.SH', is_st=False, trade_date=before) == 0.10


def test_chinext_pre_reform_open_between_ten_and_twenty_percent_not_tradable():
    """创业板改革前：开盘涨幅落在 (10%, 20%] 区间的买单必须被判为已封板、不可成交。

    这是本缺陷的核心验收：不能只验 resolve_limit_pct 返回 10%，还要验
    is_tradable 在新旧阈值之间的实际可成交性。
    """
    from datetime import date

    rules = ExecutionRules()
    # prev_close=100，改革前涨停价 110.00；开盘 115.00（+15%）实际已封板
    kw = {'prev_close': 100.0, 'high': 115.0, 'low': 100.0, 'close': 115.0,
          'volume': 1_000_000.0, 'ts_code': '300001.SZ', 'is_st': False,
          'trade_date': date(2020, 8, 21)}
    assert rules.is_tradable('buy', open_price=115.0, **kw) is False
    # 改革后同一价格应可成交（20% 涨停价 120.00）
    assert rules.is_tradable('buy', open_price=115.0,
                             **{**kw, 'trade_date': date(2020, 8, 25)}) is True
    # 改革前 +9.99% 仍可成交
    assert rules.is_tradable('buy', open_price=109.99, **kw) is True
    # 改革前跌停价 90.00：90.00 不可卖
    assert rules.is_tradable('sell', open_price=90.0, **kw) is False
    assert rules.is_tradable('sell', open_price=90.01, **kw) is True


def test_chinext_pre_reform_backtest_rejects_above_ten_percent_open():
    """端到端：创业板改革前历史区间内，开盘 +15% 的买单必须被作废（not_tradable）。"""
    dates = pd.to_datetime(['2020-08-19', '2020-08-20', '2020-08-21'])
    df = pl.DataFrame({
        'date': dates,
        'open': [100.0, 115.0, 115.0],
        'high': [100.5, 115.5, 115.5],
        'low': [99.5, 114.5, 114.5],
        'close': [100.0, 115.0, 115.0],
        'volume': [1_000_000.0] * 3,
    })
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='300001.SZ',
                                 is_st=False)

    buys = [t for t in result['trades'] if t['signal'] == 'buy']
    assert not buys, '创业板改革前 +15% 开盘已封板，不得成交'
    assert any(o['reason'] == 'not_tradable' for o in result['expired_orders'])


def test_growth_board_allows_open_above_ten_percent():
    """创业板 20% 幅度：开盘 +15% 应可买入（误用 10% 阈值会被误判涨停而作废）。"""
    df = _make_df(opens=[100.0, 115.0, 115.0], closes=[100.0, 115.0, 115.0])
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='300001.SZ',
                                 is_st=False)

    assert [t['signal'] for t in result['trades']] == ['buy']
    assert result['expired_orders'] == []


def test_st_unknown_uses_board_limit_and_marks_low_confidence():
    """ST 状态未知时按板块默认幅度判定（不按 5%），并保留数据质量标记。

    回归：未知曾按 ST 的 5% 判定，主板正常股开盘 +5% 以上即被判"已封板
    不可买"，跳空走强类信号被系统性剔除，动量/突破策略收益被系统性低估，
    且低波策略几乎不受影响，构成方向性偏置。
    """
    df = _make_df(opens=[100.0, 106.0, 106.0], closes=[100.0, 106.0, 106.0])
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    # 不传 is_st → 未知
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    # 主板 10%：+6% 开盘正常可成交，不再是 ST 5% 的封板
    assert [t['signal'] for t in result['trades']] == ['buy']
    assert result['trades'][0]['fill_confidence'] == 'low'
    assert result['expired_orders'] == []
    # 数据质量标记保留
    assert result['st_unknown'] is True
    assert result['fill_confidence'] == 'low'


def test_explicit_st_still_blocks_five_percent_gap_up():
    """明确 ST（is_st=True）仍按 5%：+6% 开盘已封板，不可买。"""
    df = _make_df(opens=[100.0, 106.0, 106.0], closes=[100.0, 106.0, 106.0])
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH',
                                 is_st=True)

    assert result['trades'] == []
    assert [o['reason'] for o in result['expired_orders']] == ['not_tradable']
    assert result['st_unknown'] is False
    assert result['fill_confidence'] == 'high'


def test_st_known_non_st_restores_board_limit():
    """明确非 ST（is_st=False）时回到板块 10%：同一 +6% 开盘可成交，
    逐笔与整体 fill_confidence 均为 high。"""
    df = _make_df(opens=[100.0, 106.0, 106.0], closes=[100.0, 106.0, 106.0])
    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({0: 'buy'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH',
                                 is_st=False)

    assert result['st_unknown'] is False
    assert result['fill_confidence'] == 'high'
    assert [t['signal'] for t in result['trades']] == ['buy']
    assert result['trades'][0]['fill_confidence'] == 'high'


def test_st_limit_is_board_aware_not_universally_five_percent():
    """ST 涨跌幅必须按板块处理，不能统一 5%。

    主板/B 股/改革前创业板 ST → 5%；改革后创业板/科创板/北交所 ST → 与普通股
    同幅度（20%/20%/30%）。旧实现 is_st=True 优先返回 5% 会系统性低估
    创业板/科创板/北交所 ST 股票的实际涨跌停边界。
    """
    from datetime import date

    r = ExecutionRules.resolve_limit_pct

    # 主板 ST：5%
    assert r('600000.SH', is_st=True) == 0.05
    assert r('000001.SZ', is_st=True) == 0.05
    # B 股 ST：5%
    assert r('900001.SH', is_st=True) == 0.05
    assert r('200001.SZ', is_st=True) == 0.05

    # 创业板 ST：改革前 5%，改革后 20%
    assert r('300001.SZ', is_st=True, trade_date=date(2020, 8, 21)) == 0.05
    assert r('300001.SZ', is_st=True, trade_date=date(2020, 8, 24)) == 0.20
    assert r('301001.SZ', is_st=True) == 0.20

    # 科创板 ST：20%（与普通股一致）
    assert r('688001.SH', is_st=True) == 0.20
    assert r('689001.SH', is_st=True) == 0.20

    # 北交所 ST：30%（与普通股一致）
    assert r('830001.BJ', is_st=True) == 0.30
    assert r('920001.BJ', is_st=True) == 0.30

    # 非 ST 对照：同板块常规幅度
    assert r('600000.SH', is_st=False) == 0.10
    assert r('300001.SZ', is_st=False) == 0.20
    assert r('688001.SH', is_st=False) == 0.20
    assert r('830001.BJ', is_st=False) == 0.30


def test_st_and_non_st_rule_switching_across_adjacent_days():
    """非 ST 与 ST 在相邻交易日的规则切换必须正确。

    模拟同一只股票相邻两日 ST 状态变化，验证幅度在 5%/10%/20% 之间正确切换，
    而不是被 ST=5% 永久覆盖。
    """
    from datetime import date

    rules = ExecutionRules()

    # 主板：相邻两日非 ST→ST，幅度 10% → 5%
    d1, d2 = date(2023, 6, 1), date(2023, 6, 2)
    assert rules.resolve_limit_pct('600000.SH', is_st=False, trade_date=d1) == 0.10
    assert rules.resolve_limit_pct('600000.SH', is_st=True, trade_date=d2) == 0.05
    # 次日摘帽恢复 10%
    assert rules.resolve_limit_pct('600000.SH', is_st=False, trade_date=date(2023, 6, 5)) == 0.10

    # 科创板：相邻两日非 ST→ST，幅度保持 20%（不切到 5%）
    assert rules.resolve_limit_pct('688001.SH', is_st=False, trade_date=d1) == 0.20
    assert rules.resolve_limit_pct('688001.SH', is_st=True, trade_date=d2) == 0.20

    # 创业板：ST 状态跨改革日切换——改革前 ST=5%，改革后 ST=20%
    assert rules.resolve_limit_pct('300001.SZ', is_st=True,
                                   trade_date=date(2020, 8, 21)) == 0.05
    assert rules.resolve_limit_pct('300001.SZ', is_st=True,
                                   trade_date=date(2020, 8, 24)) == 0.20

    # is_tradable 在 ST 切换日的实际可成交性：
    # 主板 prev_close=100，+6% 开盘：非 ST 可买，ST 已封板不可买
    base = {'prev_close': 100.0, 'high': 106.0, 'low': 100.0, 'close': 106.0,
            'volume': 1_000_000.0, 'ts_code': '600000.SH', 'trade_date': d1}
    assert rules.is_tradable('buy', open_price=106.0, is_st=False, **base) is True
    assert rules.is_tradable('buy', open_price=106.0, is_st=True, **base) is False


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


def test_limit_tolerance_compared_in_price_space():
    """回归：涨跌停判定必须在价格空间比较，不能在比率空间直接减 0.005。

    原实现 open_pct >= limit - 0.005 的容差被放大 prev_close 倍，
    会把涨幅落在 [limit-0.5%, limit) 的正常开盘价双向误判为封板：
    买入被拒（成交率低估）、卖出被拒（止损单滞留）。
    """
    rules = ExecutionRules()
    kw = {'prev_close': 10.0, 'high': 11.0, 'low': 9.0, 'volume': 1_000_000.0,
          'ts_code': '600000.SH', 'is_st': False}
    # 涨停价 11.00：10.99 仍可买（原实现从 10.95 起即拒绝）
    assert rules.is_tradable('buy', open_price=10.99, close=10.99, **kw) is True
    assert rules.is_tradable('buy', open_price=11.00, close=11.00, **kw) is False
    # 跌停价 9.00：9.01 仍可卖，9.00 不可卖
    assert rules.is_tradable('sell', open_price=9.01, close=9.01, **kw) is True
    assert rules.is_tradable('sell', open_price=9.00, close=9.00, **kw) is False

    # 3 元股：原实现容差放大 3 倍（+9.83% 即判涨停）
    kw3 = {'prev_close': 3.0, 'high': 3.3, 'low': 2.7, 'volume': 1_000_000.0,
           'ts_code': '600000.SH', 'is_st': False}
    assert rules.is_tradable('buy', open_price=3.29, close=3.29, **kw3) is True
    assert rules.is_tradable('buy', open_price=3.30, close=3.30, **kw3) is False

    # 200 元股：原实现容差被压缩到 0.025 元，现精确判到涨停价 220.00
    kw200 = {'prev_close': 200.0, 'high': 220.0, 'low': 180.0,
             'volume': 1_000_000.0, 'ts_code': '600000.SH', 'is_st': False}
    assert rules.is_tradable('buy', open_price=219.99, close=219.99, **kw200) is True
    assert rules.is_tradable('buy', open_price=220.00, close=220.00, **kw200) is False


def test_limit_price_rounded_to_cent_by_exchange_rule():
    """涨跌停价必须按交易所规则 round(prev_close×(1±limit), 2) 取整到分。

    回归：直接用浮点乘积比较时，prev_close=10.03 的涨停价算成 11.033，
    交易所公布 11.03，一字涨停开盘 11.03 判定 price >= 11.033 为 False
    → 该股被判可买入，连续一字板行情系统性虚增收益。
    """
    # 交易所涨跌停价：10.03×1.1=11.033 → 11.03；10.03×0.9=9.027 → 9.03
    assert ExecutionRules.resolve_limit_price(10.03, 0.10, 'buy') == 11.03
    assert ExecutionRules.resolve_limit_price(10.03, 0.10, 'sell') == 9.03
    # .005 边界必须四舍五入（不是银行家舍入）：10.05×1.1=11.055 → 11.06
    assert ExecutionRules.resolve_limit_price(10.05, 0.10, 'buy') == 11.06

    rules = ExecutionRules()
    base_kw = {
        'prev_close': 10.03, 'volume': 1_000_000.0,
        'ts_code': '600000.SH', 'is_st': False,
    }
    # 一字涨停 11.03 不可买，11.02 仍可买
    up_kw = {**base_kw, 'high': 11.03, 'low': 11.03}
    assert rules.is_tradable('buy', open_price=11.03, close=11.03, **up_kw) is False
    assert rules.is_tradable('buy', open_price=11.02, close=11.02, **up_kw) is True

    down_kw = {**base_kw, 'high': 9.03, 'low': 9.03}
    # 一字跌停 9.03 不可卖，9.04 仍可卖
    assert rules.is_tradable('sell', open_price=9.03, close=9.03, **down_kw) is False
    assert rules.is_tradable('sell', open_price=9.04, close=9.04, **down_kw) is True


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
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH',
                                 is_st=False)
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
    result2 = engine2.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH',
                                   is_st=False)
    # fill_confidence=high（列存在，只是某 bar 值缺失）
    assert result2['fill_confidence'] == 'high'
    # 挂单过期，price_missing
    assert result2['trades'] == []
    assert any(o['reason'] == 'price_missing' for o in result2['expired_orders'])


def test_slow_path_strategies_marked_in_result():
    """未实现 generate_signals_vectorized 的策略必须留痕：打 warning 并把
    (标的名, 策略名) 写进结果的 slow_path_strategies，避免调用方对
    "分钟级 → 小时级"的降级毫无感知；向量化策略则保持空列表。"""
    df = _make_df(opens=[10.0, 10.0, 10.0], closes=[10.0, 10.0, 10.0])

    slow = BacktestEngine(df)
    slow.set_strategy(_IndexSignalStrategy({0: 'buy'}))          # 仅逐 bar 接口
    slow_result = slow.run_backtest(initial_capital=1_000_000.0,
                                    ts_code='600000.SH', is_st=False)
    assert slow_result['slow_path_strategies'] == [
        {'ts_code': '600000.SH', 'strategy': '索引信号策略'}
    ]

    fast = BacktestEngine(df)
    fast.set_strategy(MAStrategy(short_window=1, long_window=2))  # 已覆写向量化接口
    fast_result = fast.run_backtest(initial_capital=1_000_000.0,
                                    ts_code='600000.SH', is_st=False)
    assert fast_result['slow_path_strategies'] == []


def test_snapshot_shares_equity_curve_without_copying():
    """快照必须共享 equity_curve 引用、不得整体复制。

    每笔候选订单复制一次净值曲线，累计开销 O(mT²)（10 年 × 50 标的约
    1.4 亿次元素复制），是长周期组合回测的首要纯开销；风控规则只读尾部
    window，共享引用无副作用。
    """
    book = AccountBook(100_000.0, ['AAA.SZ'])
    curve = [{'date': '2024-01-01', 'equity': 100_000.0}]
    assert book.snapshot(curve).equity_curve is curve


def test_idle_symbols_do_not_freeze_capital():
    """组合资金切分只覆盖活跃标的：universe 里加载了但未配策略的标的
    不得分走 initial_capital 的等权份额，否则冻结比例 = 无策略标的占比。"""
    df_a = _make_df(opens=[10.0, 10.0, 10.0], closes=[10.0, 10.0, 10.0])
    df_b = _make_df(opens=[10.0, 10.0, 10.0], closes=[10.0, 10.0, 10.0])
    engine = PortfolioBacktestEngine({'AAA.SZ': df_a, 'BBB.SZ': df_b})
    engine.set_strategies({'AAA.SZ': _IndexSignalStrategy({0: 'buy'})})
    result = engine.run_backtest(initial_capital=1_000_000.0)

    # 现金桶只分配给有策略的标的
    assert list(result['positions']) == ['AAA.SZ']
    # AAA 用满全部本金买入；若 BBB 分走一半，股数会不足 5 万
    assert result['trades'][0]['shares'] > 90_000


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


# ------------------------------------------------------------------
# P0-1：默认风控装配（涨跌停不可成交 + 停牌不可交易）不得误拒正常订单
# ------------------------------------------------------------------


def test_default_risk_engine_does_not_change_fills():
    """P0-1 验收：run_backtest 默认装配安全规则集后，成交与完全无风控时逐笔一致。

    回归：装配前 RiskOrder 未注入 prev_close / sector，PriceLimitTradable 按
    fail-closed 拒绝全部订单，表现为"开了风控就 0 成交"。本用例同时锁定
    risk_summary.rules 与 total_rejected==0，确保默认规则只是可审计闸门，
    不改变撮合结果。
    """
    data = _oscillating()

    no_risk = run_backtest(
        data=data, strategy=MAStrategy(short_window=5, long_window=20),
        ts_code='600000.SH', initial_capital=1_000_000.0,
        enable_default_risk=False,
    )
    with_risk = run_backtest(
        data=data, strategy=MAStrategy(short_window=5, long_window=20),
        ts_code='600000.SH', initial_capital=1_000_000.0,
    )

    assert no_risk['risk_summary'] is None
    assert with_risk['risk_summary']['rules'] == [
        'price_limit_tradable', 'no_suspended_trade',
    ]
    assert with_risk['risk_summary']['total_rejected'] == 0
    assert with_risk['trades'] == no_risk['trades']
    assert with_risk['final_equity'] == no_risk['final_equity']
    assert len(with_risk['trades']) > 0


def test_resolve_sector_matches_limit_tier():
    """P0-1 回归：风控板块标签必须与撮合幅度同源。

    PriceLimitTradable 按 order.sector 查阈值，因此任何代码/日期/ST 组合都
    必须满足 DEFAULT_LIMITS[resolve_sector(...)] == resolve_limit_pct(...)。
    旧实现把改革后创业板（20%）映射为 '主板'（10%），风控侧会漏放已封板的
    订单，与撮合判定分叉。
    """
    from datetime import date

    limits = PriceLimitTradable.DEFAULT_LIMITS
    cases = [
        ('600000.SH', False, None),                    # 沪主板 10%
        ('000001.SZ', False, None),                    # 深主板 10%
        ('688001.SH', False, None),                    # 科创板 20%
        ('689001.SH', False, None),                    # 科创板 CDR 20%
        ('300001.SZ', False, date(2020, 8, 24)),       # 创业板改革后 20%
        ('301001.SZ', False, date(2020, 8, 25)),       # 创业板 301
        ('300001.SZ', False, date(2020, 8, 21)),       # 创业板改革前 10%
        ('600000.SH', True, None),                     # 主板 ST 5%
        ('900001.SH', True, None),                     # 沪 B 股 ST 5%
        ('200001.SZ', True, None),                     # 深 B 股 ST 5%
        ('300001.SZ', True, date(2020, 8, 21)),        # 改革前创业板 ST 5%
        ('300001.SZ', True, date(2020, 8, 24)),        # 改革后创业板 ST 20%
        ('688001.SH', True, None),                     # 科创板 ST 20%
        ('830001.BJ', False, None),                    # 北交所 30%
        ('920001.BJ', False, None),                    # 北交所新号段 30%
        ('920001', False, None),                       # 无后缀号段兜底
        ('', False, None),                             # 空代码兜底
    ]
    for ts_code, is_st, trade_date in cases:
        sector = ExecutionRules.resolve_sector(ts_code, is_st, trade_date)
        pct = ExecutionRules.resolve_limit_pct(ts_code, is_st, trade_date=trade_date)
        assert sector in limits, f'{ts_code} 产出未知板块标签 {sector!r}'
        assert limits[sector] == pct, (
            f'{ts_code} is_st={is_st} date={trade_date}: '
            f'板块 {sector} 阈值 {limits[sector]:.0%} != 撮合幅度 {pct:.0%}'
        )


# ------------------------------------------------------------------
# P0-2：参数优化子回测必须复用主回测的执行上下文与完整策略参数
# ------------------------------------------------------------------


def test_clone_strategy_preserves_unoptimized_params():
    """P0-2 回归：克隆策略必须保留未参与优化的原参数，且不污染原实例。

    旧实现用 ``strategy.__class__()`` 重建，只优化 short_window 时
    long_window 会回落到类默认 20，子回测与主回测口径不一致。
    """
    original = MAStrategy(short_window=5, long_window=60)
    clone = clone_strategy(original, {'short_window': 3})

    assert clone.params['short_window'] == 3
    assert clone.params['long_window'] == 60
    assert original.params['short_window'] == 5
    assert clone is not original


def test_optimizer_subbacktests_match_direct_backtest():
    """P0-2 验收：同参数下优化子回测与直接回测 trades/final_equity 完全一致。

    子回测必须同时满足两条：保留未优化参数（long_window=20）与透传
    ts_code（创业板 20% 涨跌停档位）。任一缺失都会让结果与主回测不可比。
    """
    data = _oscillating()

    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    engine.run_backtest(initial_capital=1_000_000.0, ts_code='300001.SZ',
                        industry='计算机', is_st=False)

    optimized = engine.optimize_strategy(
        {'short_window': [3, 8]}, initial_capital=1_000_000.0, max_workers=2,
    )
    assert len(optimized['optimization_results']) == 2

    for row in optimized['optimization_results']:
        params = row['params']
        direct = BacktestEngine(data)
        direct.set_strategy(MAStrategy(short_window=int(params['short_window']),
                                       long_window=20))
        expected = direct.run_backtest(
            initial_capital=1_000_000.0, ts_code='300001.SZ',
            industry='计算机', is_st=False,
        )
        assert row['trades'] == expected['trades'], f'参数 {params} 成交不一致'
        assert row['final_equity'] == expected['final_equity'], f'参数 {params} 权益不一致'


def test_parallel_worker_receives_execution_context():
    """P0-2 回归：进程池 worker 必须透传 ts_code/ST 上下文。

    创业板（改革后）开盘 +15% 可成交；不带 context 时按 '__default__'
    主板 10% 推导，同一笔买单被判涨停而作废。
    """
    df = _make_df(opens=[100.0, 115.0, 115.0], closes=[100.0, 115.0, 115.0])

    with_context = run_backtest_worker(
        BacktestEngine, _IndexSignalStrategy({0: 'buy'}), df, CostModel(),
        {'tag': 0}, 1_000_000.0, None,
        {'ts_code': '300001.SZ', 'industry': '', 'is_st': False},
    )
    without_context = run_backtest_worker(
        BacktestEngine, _IndexSignalStrategy({0: 'buy'}), df, CostModel(),
        {'tag': 0}, 1_000_000.0, None,
    )

    assert [t['signal'] for t in with_context['trades']] == ['buy']
    assert without_context['trades'] == []


# ------------------------------------------------------------------
# P2-4：参数优化必须附带样本外（OOS）指标与参数稳定性报告
# ------------------------------------------------------------------


def test_optimize_strategy_reports_oos_metrics_and_stability():
    """样本内选参后必须在后段样本外给出独立指标，并输出稳定性报告。"""
    data = _oscillating()
    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))

    optimized = engine.optimize_strategy(
        {'short_window': [3, 5, 8]}, initial_capital=1_000_000.0,
        max_workers=2, oos_ratio=0.3,
    )

    oos = optimized['oos_validation']
    assert oos['enabled'] is True
    assert oos['split_index'] == int(data.height * 0.7)
    assert oos['in_sample_range'].split(' ~ ')[1] < oos['out_of_sample_range'].split(' ~ ')[0]

    for key in ('total_return', 'sharpe_ratio', 'max_drawdown', 'trade_count'):
        assert key in oos['in_sample']
        assert key in oos['out_of_sample']

    assert set(oos['in_sample_best_params']) == {'short_window'}
    assert set(oos['decay']) == {'total_return', 'sharpe_ratio'}

    stability = oos['param_stability']
    assert stability['param_stability'], '必须输出逐参数稳定性条目'
    assert 0.0 <= stability['stable_ratio'] <= 1.0
    # 全样本网格的报告与 OOS 内样本报告都应存在
    assert 'param_stability_report' in optimized


def test_walk_forward_validation_returns_per_fold_oos():
    """walk-forward 按折滚动选参，并对每折后一段给出样本外指标。"""
    data = _oscillating(days=300)
    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))

    optimized = engine.optimize_strategy(
        {'short_window': [3, 5]}, initial_capital=1_000_000.0,
        max_workers=1, walk_forward_windows=2,
    )

    walk_forward = optimized['oos_validation']['walk_forward']
    assert walk_forward['enabled'] is True
    assert walk_forward['windows'] == 2
    assert len(walk_forward['folds']) == 2

    for fold in walk_forward['folds']:
        assert fold['train_range'] and fold['test_range']
        assert 'total_return' in fold['out_of_sample']
        assert set(fold['best_params']) == {'short_window'}

    assert 0.0 <= walk_forward['positive_ratio'] <= 1.0


# ------------------------------------------------------------------
# P1-1：回撤改增量峰值结构，禁止每单 O(T) 重扫权益曲线
# ------------------------------------------------------------------


def _long_history(n: int = 5000) -> pl.DataFrame:
    """构造 n 根日线（含趋势与波动），用于长周期性能与挂单量验收。"""
    dates = pd.date_range('2005-01-03', periods=n, freq='B')
    base = 100.0 + 10.0 * np.sin(np.arange(n) / 40.0) + 0.01 * np.arange(n)
    return pl.DataFrame({
        'date': dates,
        'open': base * 1.001,
        'high': base * 1.01,
        'low': base * 0.99,
        'close': base,
        'volume': np.full(n, 1_000_000.0),
    })


def test_drawdown_tracker_matches_curve_slice_implementation():
    """P1-1 等价性：增量跟踪器与 Portfolio 原曲线切片实现逐步、逐窗口一致。

    覆盖随机游走、乱序无、以及 None/NaN/inf/≤0/非数值等非法权益值；任一窗口
    内出现非法值时必须同样返回 None（fail-closed 语义不能因增量实现而改变）。
    """
    rng = np.random.default_rng(20260305)
    bad_values = [None, float('nan'), float('inf'), 0.0, -3.0, 'oops', True]

    curve: list[dict] = []
    tracker = DrawdownTracker()
    equity = 100_000.0
    dates = pd.date_range('2020-01-01', periods=400, freq='B')
    windows = (1, 3, 20, 252)

    for day in dates:
        if rng.random() < 0.06:
            value = bad_values[int(rng.integers(len(bad_values)))]
        else:
            equity *= 1.0 + float(rng.normal(0.0, 0.02))
            value = equity
        curve.append({'date': day, 'equity': value})
        tracker.sync(curve)

        for window in windows:
            expected = Portfolio(equity_curve=curve).current_drawdown(window)
            actual = tracker.current_drawdown(window)
            if expected is None:
                assert actual is None, f'day={day} window={window}: 非法窗口应返回 None'
            else:
                assert actual is not None, f'day={day} window={window}: 不应返回 None'
                assert abs(actual - expected) < 1e-12, (
                    f'day={day} window={window}: {actual} != {expected}'
                )


def test_drawdown_tracker_keeps_5000_bar_two_rule_backtest_under_60ms():
    """P1-1 性能验收：5000 bar + 双规则（回撤熔断 + 单票权重）回测 ≤ 60ms。

    原实现每笔候选订单都把整条权益曲线转字符串排序，退化为 O(T²)，实测
    1545ms；改增量单调队列后单日增量消费为 O(1)，同一日内多次审查零成本。

    基线（1545ms）是独立进程单跑的墙钟口径，因此这里也放到子进程里测：
    共享 pytest 进程此前跑过大量大数据集用例，堆碎片/GC 压力会把 5000 行
    的固有 O(T) 预处理拖慢 2~3 倍，混入与回撤路径无关的噪声。
    """
    import subprocess
    import sys
    from pathlib import Path

    script = """
import time

import numpy as np
import pandas as pd
import polars as pl

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.strategies.ma_strategy import MAStrategy
from src.risk import MaxPositionWeight, RiskEngine
from src.risk.rules import MaxDrawdownCircuit

n = 5000
dates = pd.date_range('2005-01-03', periods=n, freq='B')
base = 100.0 + 10.0 * np.sin(np.arange(n) / 40.0) + 0.01 * np.arange(n)
df = pl.DataFrame({
    'date': dates,
    'open': base * 1.001,
    'high': base * 1.01,
    'low': base * 0.99,
    'close': base,
    'volume': np.full(n, 1_000_000.0),
})
risk = RiskEngine([MaxDrawdownCircuit(window=252), MaxPositionWeight(1.0)])

def run_backtest():
    engine = BacktestEngine(df)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    engine.risk_engine = risk
    return engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

result = run_backtest()  # 预热：指标预计算 / polars 懒加载
timings = []
for _ in range(5):
    engine = BacktestEngine(df)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    engine.risk_engine = risk
    started = time.perf_counter()
    engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')
    timings.append((time.perf_counter() - started) * 1000.0)

assert len(result['trades']) > 0
assert result['risk_summary']['total_rejected'] == 0
print(min(timings))
"""
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, '-c', script], cwd=root, capture_output=True,
        text=True, timeout=180, check=False,
    )
    assert completed.returncode == 0, f'基准子进程失败：{completed.stderr[-2000:]}'

    best = float(completed.stdout.strip().splitlines()[-1])
    assert best <= 60.0, f'5000 bar 双规则回测 {best:.1f}ms 超过 60ms 验收线（原实现 1545ms）'


# ------------------------------------------------------------------
# P1-3：信号层持仓状态过滤，避免每 bar 派单
# ------------------------------------------------------------------


def test_status_signals_do_not_generate_per_bar_expired_orders():
    """P1-3 验收：5000 bar 状态型策略的 expired_orders 降至信号切换次数级别。

    MAStrategy 在持仓期间持续输出 buy、空仓期间持续输出 sell（状态型而非
    委托意图）。逐 bar 派单时每次都会在次日以 already_holding / no_position
    作废，实测 4716 条；信号层按持仓过滤后不再生成这些无效挂单。
    """
    data = _long_history()
    engine = BacktestEngine(data)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    switches = len(result['trades'])
    assert switches > 0
    assert len(result['expired_orders']) <= switches, (
        f"expired_orders={len(result['expired_orders'])} 超出信号切换次数 {switches}"
        '（过滤前为 4716 条）'
    )
    # 状态过滤不得改变成交结果：仅移除无效挂单，有效切换仍照常成交
    assert [t['signal'] for t in result['trades'][:2]] == ['buy', 'sell']


# ------------------------------------------------------------------
# P1-2：成本模型按 trade_date 取历史印花税/过户费
# ------------------------------------------------------------------


def test_cost_model_uses_historical_stamp_tax_and_transfer_fee():
    """P1-2 验收：按 trade_date 取历史费率，2010 年卖出印花税为 0.1%。

    成交额 10 万元基准：佣金 max(30,5)=30、滑点 0.0002×10 万=20。
    - 2010-06-01 沪市卖出：印花税 0.1%=100 + 过户费 0.06%=60 → 210
    - 2010-06-01 深市卖出：深市 2015-08-01 前不收过户费 → 150
    - 2023-09 卖出：印花税 0.05%=50 + 统一过户费 0.002%=2 → 102（沪深一致）
    - 2007-06-01：印花税 0.3% 双边，买卖两侧都含 300
    """
    from datetime import date

    cm = CostModel()

    assert cm.calculate_sell_cost(
        10_000, 10.0, trade_date=date(2010, 6, 1), ts_code='600000.SH') == pytest.approx(210.0)
    assert cm.calculate_sell_cost(
        10_000, 10.0, trade_date=date(2010, 6, 1), ts_code='000001.SZ') == pytest.approx(150.0)
    assert cm.calculate_buy_cost(
        10_000, 10.0, trade_date=date(2010, 6, 1), ts_code='600000.SH') == pytest.approx(110.0)

    assert cm.calculate_sell_cost(
        10_000, 10.0, trade_date=date(2023, 9, 1), ts_code='600000.SH') == pytest.approx(102.0)
    assert cm.calculate_sell_cost(
        10_000, 10.0, trade_date=date(2023, 9, 1), ts_code='000001.SZ') == pytest.approx(102.0)
    assert cm.calculate_buy_cost(
        10_000, 10.0, trade_date=date(2023, 9, 1), ts_code='600000.SH') == pytest.approx(52.0)

    # 2007-05-30 至 2008-09-18 印花税 0.3% 双边征收
    assert cm.calculate_buy_cost(
        10_000, 10.0, trade_date=date(2007, 6, 1), ts_code='600000.SH') == pytest.approx(410.0)
    assert cm.calculate_sell_cost(
        10_000, 10.0, trade_date=date(2007, 6, 1), ts_code='600000.SH') == pytest.approx(410.0)

    # 不传 trade_date 保持原静态参数（向后兼容：印花税 0.05% 卖方单边）
    assert cm.calculate_sell_cost(10_000, 10.0) == pytest.approx(101.0)
    assert cm.get_buy_cost_rate(
        trade_date=date(2010, 6, 1), ts_code='600000.SH') == pytest.approx(0.0011)


