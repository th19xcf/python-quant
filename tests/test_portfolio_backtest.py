#!/usr/bin/env python3

"""
测试组合回测功能
"""

from typing import Any, Dict

import numpy as np
import pandas as pd
import polars as pl
import pytest
from loguru import logger

from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.ma_strategy import MAStrategy
from src.backtest.strategies.macd_strategy import MACDStrategy


def generate_test_data(stock_code, start_date, end_date, volatility=0.02, trend=0.001):
    """
    生成测试数据

    Args:
        stock_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        volatility: 波动率
        trend: 趋势

    Returns:
        pl.DataFrame: 测试数据
    """
    # 生成日期范围
    dates = pd.date_range(start=start_date, end=end_date, freq='B')

    # 生成价格数据
    np.random.seed(42)
    returns = np.random.normal(trend, volatility, len(dates))
    prices = [100.0]
    for ret in returns:
        prices.append(prices[-1] * (1 + ret))
    prices = prices[1:]

    # 生成成交量数据
    volumes = np.random.randint(100000, 1000000, len(dates))

    # 创建DataFrame
    data = {
        'date': dates,
        'open': [p * 0.999 for p in prices],
        'high': [p * 1.002 for p in prices],
        'low': [p * 0.998 for p in prices],
        'close': prices,
        'volume': volumes
    }

    return pl.DataFrame(data)


def test_portfolio_backtest():
    """
    测试组合回测功能
    """
    logger.info("=== 测试组合回测功能 ===")

    # 生成测试数据
    stock1_data = generate_test_data('600000.SH', '2023-01-01', '2023-12-31', volatility=0.02, trend=0.0005)
    stock2_data = generate_test_data('000001.SZ', '2023-01-01', '2023-12-31', volatility=0.025, trend=0.0003)
    stock3_data = generate_test_data('300001.SZ', '2023-01-01', '2023-12-31', volatility=0.03, trend=0.0008)

    # 创建数据字典
    data_dict = {
        '600000.SH': stock1_data,
        '000001.SZ': stock2_data,
        '300001.SZ': stock3_data
    }

    # 创建组合回测引擎
    engine = PortfolioBacktestEngine(data_dict)

    # 设置策略
    engine.set_strategy('600000.SH', MAStrategy(short_window=5, long_window=20))
    engine.set_strategy('000001.SZ', MACDStrategy())
    engine.set_strategy('300001.SZ', MAStrategy(short_window=10, long_window=30))

    # 运行回测
    logger.info("运行组合回测...")
    results = engine.run_backtest(initial_capital=1000000.0, allocation_strategy='equal')

    # 打印回测结果
    logger.info("\n回测结果:")
    logger.info(f"初始资金: {results['initial_capital']:.2f}")
    logger.info(f"最终资金: {results['final_equity']:.2f}")
    logger.info(f"总收益率: {results['total_return']:.2f}%")

    # 打印性能指标
    logger.info("\n性能指标:")
    performance = results['performance']
    for key, value in performance.items():
        # frequency/frequency_fallback 等口径元数据不是数值，不能按浮点格式化
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            logger.info(f"{key}: {value:.4f}")
        else:
            logger.info(f"{key}: {value}")

    # 打印交易记录
    logger.info("{} {}", "\n交易记录数量:", len(results['trades']))
    if results['trades']:
        logger.info("前5条交易记录:")
        for trade in results['trades'][:5]:
            logger.info(f"日期: {trade['date']}, 股票: {trade['stock_code']}, 信号: {trade['signal']}, 价格: {trade['price']:.2f}")

    logger.info("\n组合回测测试完成！")


def test_portfolio_backtest_precomputes_indicators():
    """
    回归测试：组合引擎必须为每只标的预计算策略依赖的指标列。

    历史缺陷：组合引擎从未调用 _precompute_indicators，策略读不到 ma_* /
    macd / rsi，generate_signal 恒返回 hold，回测静默产生 0 笔交易。
    """
    days = 300
    dates = pd.date_range('2023-01-02', periods=days, freq='B')

    def _oscillating(phase):
        close = 100.0 + 10.0 * np.sin(np.arange(days) / 10.0 + phase)
        return pl.DataFrame({
            'date': dates,
            'open': close * 1.001,
            'high': close * 1.01,
            'low': close * 0.99,
            'close': close,
            'volume': np.full(days, 1_000_000.0),
        })

    data_dict = {'600000.SH': _oscillating(0.0), '000001.SZ': _oscillating(0.5)}
    engine = PortfolioBacktestEngine(data_dict)
    for code in data_dict:
        engine.set_strategy(code, MAStrategy(short_window=5, long_window=20))

    results = engine.run_backtest(initial_capital=1_000_000.0)

    # 1. 必须产生交易（预计算链路生效）
    assert len(results['trades']) > 0, '组合回测产生 0 笔交易：指标预计算链路可能断链'

    # 2. 交易必须归属已配置策略的标的，且买卖股数为整手
    open_by_date = {
        code: dict(zip(df['date'].to_list(), df['open'].to_list(), strict=False))
        for code, df in data_dict.items()
    }
    for trade in results['trades']:
        assert trade['stock_code'] in data_dict
        assert trade['signal'] in ('buy', 'sell')
        assert trade['shares'] % 100 == 0
        # 3. 成交价必须是该标的当日开盘价（验证日期索引与行数据对齐）
        assert abs(trade['price'] - open_by_date[trade['stock_code']][trade['date']]) < 1e-9

    # 4. 权益曲线覆盖全部交易日且按日期升序
    equity_dates = [row['date'] for row in results['equity_curve']]
    assert equity_dates == sorted(equity_dates)
    assert set(equity_dates) == set(dates)


class _ScriptedStrategy(BaseStrategy):
    """按 bar 下标输出显式信号的策略，用于精确控制调仓日与目标权重。"""

    def __init__(self, script: Dict[int, Any]):
        super().__init__('scripted')
        self.script = dict(script)

    def generate_signal(self, data, index):  # noqa: ANN001
        return self.script.get(index)

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        return pl.Series(
            'signal', [self.script.get(i) for i in range(df.height)], dtype=pl.Object)


def _flat_panel(codes, days: int = 12, base: float = 10.0,
                overrides: Dict[str, Dict[str, list]] | None = None
                ) -> Dict[str, pl.DataFrame]:
    """确定性面板：默认价格逐 bar 微升（open==close），可按标的覆盖整列。"""
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    panel: Dict[str, pl.DataFrame] = {}
    for code in codes:
        close = np.array([base + 0.01 * i for i in range(days)])
        columns = {
            'date': dates,
            'open': close.copy(),
            'high': close * 1.001,
            'low': close * 0.999,
            'close': close,
            'volume': np.full(days, 1_000_000.0),
        }
        columns.update((overrides or {}).get(code, {}))
        panel[code] = pl.DataFrame(columns)
    return panel


def test_pooled_cash_express_equal_weight_full_investment():
    """池模式可表达「全市场等权、月度再平衡、T 收盘信号 T+1 开盘成交」。

    5 只标的每 20 个交易日收盘出一次目标权重信号，次一交易日开盘执行：
    首月建仓，之后把领涨标的减到目标权重、其余补到目标权重。
    对照：同一份等权目标权重在逐票桶模式（默认）下只能投出 1/N 仓位——
    每个桶按自身权益 20 万 × 0.199 建仓，这正是本次新增池模式要解决的
    表达力缺口。
    """
    codes = ['600000.SH', '000001.SZ', '300001.SZ', '600036.SH', '000002.SZ']
    rising = codes[0]
    days = 65
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    drifts = [0.003, -0.001, 0.0005, 0.0002, 0.001]
    panel: Dict[str, pl.DataFrame] = {}
    for code, drift in zip(codes, drifts, strict=False):
        close = 10.0 * np.cumprod(np.full(days, 1.0 + drift))
        panel[code] = pl.DataFrame({
            'date': dates, 'open': close, 'high': close * 1.001,
            'low': close * 0.999, 'close': close,
            'volume': np.full(days, 1_000_000.0),
        })

    weight = 0.995 / len(codes)
    build = {19: {'action': 'buy', 'target_weight': weight}}
    # 首月全部建仓；再平衡月领涨标的减到目标权重，其余标的买入到目标权重
    scripts = {
        code: {**build, 39: {'action': 'buy', 'target_weight': weight},
               59: {'action': 'buy', 'target_weight': weight}}
        for code in codes[1:]
    }
    scripts[rising] = {**build,
                       39: {'action': 'sell', 'target_weight': weight},
                       59: {'action': 'sell', 'target_weight': weight}}

    capital = 1_000_000.0
    pooled = PortfolioBacktestEngine(panel)
    pooled.set_strategies({code: _ScriptedStrategy(scripts[code]) for code in codes})
    pooled_result = pooled.run_backtest(initial_capital=capital, pooled_cash=True)

    def _ratio(result: dict) -> float:
        market_value = sum(qty * float(panel[code]['close'][-1])
                           for code, qty in result['positions'].items())
        return market_value / result['final_equity']

    trade_days = panel[codes[0]]['date'].to_list()
    execution_days = {trade_days[20], trade_days[40], trade_days[60]}
    assert {t['date'] for t in pooled_result['trades']} == execution_days, \
        'T 日收盘信号必须只在 T+1 开盘成交，且月内不得有额外成交'

    first_month = [t for t in pooled_result['trades'] if t['date'] == trade_days[20]]
    assert {t['stock_code'] for t in first_month} == set(codes)
    assert all(t['signal'] == 'buy' for t in first_month)

    for day in (trade_days[40], trade_days[60]):
        sells = [t for t in pooled_result['trades']
                 if t['date'] == day and t['stock_code'] == rising]
        assert len(sells) == 1 and sells[0]['signal'] == 'sell', '再平衡月应把领涨标的减到目标权重'
        assert 0 < sells[0]['shares'] < sells[0]['position'] + sells[0]['shares']

    pooled_ratio = _ratio(pooled_result)
    assert pooled_ratio > 0.95, f'池模式总仓位 {pooled_ratio:.4f} 未接近满仓'

    legacy = PortfolioBacktestEngine(panel)
    legacy.set_strategies({code: _ScriptedStrategy(scripts[code]) for code in codes})
    legacy_result = legacy.run_backtest(initial_capital=capital)
    legacy_ratio = _ratio(legacy_result)
    assert legacy_ratio < 0.30, f'逐票桶模式总仓位 {legacy_ratio:.4f} 不符合 1/N 预期'



def test_pooled_same_open_buys_scale_uniformly_under_cash_shortage():
    """池模式同开盘等权建仓资金不足：所有标的同一比例缩量，不整笔丢弃。

    4 只标的同日发 target_weight=0.3（合计 1.2 倍权益），现金只够 5/6。
    旧实现按代码序前序买单先吃光现金、后序买单整笔作废；修复后应在买单入口
    预扫描总缺口，卖出腿执行完后按 min(1, 池现金/总缺口) 统一缩量。
    """
    codes = ['600000.SH', '000001.SZ', '300001.SZ', '600036.SH']
    panel = _flat_panel(codes, days=6)
    weight = 0.3
    engine = PortfolioBacktestEngine(panel)
    engine.set_strategies(
        {code: _ScriptedStrategy({0: {'action': 'buy', 'target_weight': weight}})
         for code in codes})
    capital = 1_000_000.0
    result = engine.run_backtest(initial_capital=capital, pooled_cash=True)

    trade_day = panel[codes[0]]['date'].to_list()[1]
    buys = [t for t in result['trades'] if t['date'] == trade_day]
    assert {t['stock_code'] for t in buys} == set(codes)
    assert all(t['signal'] == 'buy' for t in buys)
    # 同一开盘、同一价格：缩量比例唯一 → 各标的股数完全一致（无代码序偏置）
    assert len({t['shares'] for t in buys}) == 1
    assert [o for o in result['expired_orders'] if o['side'] == 'buy'] == []
    # 比例 = 池现金 / 总缺口 = 1 / (4 × 0.3)
    assert result['scaled_buy_orders'] == len(codes)
    assert result['avg_buy_scale'] == pytest.approx(
        1.0 / (weight * len(codes)), abs=1e-12)
    invested = sum(t['shares'] * t['price'] for t in buys)
    assert invested >= capital * 0.98, '缩量后仍应接近满仓，不得只投出前序买单'


def test_pooled_rebalance_skips_limit_up_limit_down_and_suspension():
    """池模式调仓同样受 A 股约束：涨停不可买、跌停不可卖、停牌不调仓。"""
    codes = ['600000.SH', '000001.SZ', '300001.SZ', '600036.SH']
    a, b, c, d = codes
    days = 12
    flat = [10.0] * days
    overrides = {
        # A：调仓日开盘 +10% → 涨停不可买
        a: {'open': flat[:4] + [11.0] + [10.9] * (days - 5),
            'close': flat[:4] + [10.9] * (days - 4),
            'high': flat[:4] + [11.0] + [10.9] * (days - 5),
            'low': flat[:4] + [10.9] * (days - 4)},
        # B：调仓日成交量为 0 → 停牌不可交易
        b: {'volume': [1_000_000.0] * 4 + [0.0] + [1_000_000.0] * (days - 5)},
        # D：调仓日开盘 -10% → 跌停不可卖（卖单保留，次日重试）
        d: {'open': flat[:4] + [9.0] + [9.0] * (days - 5),
            'close': flat[:4] + [9.0] * (days - 4),
            'high': flat[:4] + [9.0] * (days - 4),
            'low': flat[:4] + [9.0] * (days - 4)},
    }
    panel = _flat_panel(codes, days=days, overrides=overrides)
    first = {0: {'action': 'buy', 'target_weight': 0.24}}
    second = {
        3: {'action': 'buy', 'target_weight': 0.24},
    }
    script_by_code = {code: {**first, **second} for code in codes}
    # C/D 在第二次调仓时减到目标权重 0（清仓）；A/B 仍按 0.24 加仓
    script_by_code[c] = {**first, 3: {'action': 'sell', 'target_weight': 0.0}}
    script_by_code[d] = {**first, 3: {'action': 'sell', 'target_weight': 0.0}}

    engine = PortfolioBacktestEngine(panel)
    engine.set_strategies(
        {code: _ScriptedStrategy(script) for code, script in script_by_code.items()})
    result = engine.run_backtest(initial_capital=1_000_000.0, pooled_cash=True)

    trade_days = panel[a]['date'].to_list()
    rebalance_day = trade_days[4]

    first_day_buys = [t for t in result['trades'] if t['date'] == trade_days[1]]
    assert {t['stock_code'] for t in first_day_buys} == set(codes)

    rebalance_trades = [t for t in result['trades'] if t['date'] == rebalance_day]
    # 调仓日只有 C 的清仓成交：A 涨停买不进、B 停牌不调仓、D 跌停卖不出
    assert {t['stock_code'] for t in rebalance_trades} == {c}
    assert rebalance_trades[0]['signal'] == 'sell'
    assert rebalance_trades[0]['position'] == 0
    assert {o['ts_code'] for o in result['blocked_orders']} == {d}
    assert result['blocked_orders'][0]['blocked_by'] == 'limit_down'
    assert result['blocked_orders'][0]['date'] == rebalance_day

    reasons = {(o['ts_code'], o['side']): o['reason']
               for o in result['expired_orders'] if o['date'] == rebalance_day}
    assert reasons[(a, 'buy')] == 'not_tradable'
    assert reasons[(b, 'buy')] == 'not_tradable'
    assert b not in {t['stock_code'] for t in rebalance_trades}

    # 跌停卖单次日重试成交（未被静默丢弃）
    retry_sells = [t for t in result['trades']
                   if t['stock_code'] == d and t['signal'] == 'sell']
    assert len(retry_sells) == 1
    assert retry_sells[0]['date'] == trade_days[5]


def test_pooled_sell_to_target_weight_trims_overweight():
    """sell + target_weight 把超配标的减到目标权重（不是清仓）。"""
    codes = ['600000.SH', '000001.SZ']
    rising, flat = codes
    days = 12
    panel = _flat_panel(codes, days=days, overrides={
        rising: {
            'open': [10.0] * 4 + [20.0] * (days - 4),
            'close': [10.0] * 4 + [20.0] * (days - 4),
            'high': [10.0] * 4 + [20.0] * (days - 4),
            'low': [10.0] * 4 + [20.0] * (days - 4),
        },
    })
    engine = PortfolioBacktestEngine(panel)
    engine.set_strategies({
        rising: _ScriptedStrategy({
            0: {'action': 'buy', 'target_weight': 0.5},
            5: {'action': 'sell', 'target_weight': 0.5},
        }),
        flat: _ScriptedStrategy({0: {'action': 'buy', 'target_weight': 0.5}}),
    })
    result = engine.run_backtest(initial_capital=1_000_000.0, pooled_cash=True)

    trade_days = panel[rising]['date'].to_list()
    sells = [t for t in result['trades']
             if t['stock_code'] == rising and t['signal'] == 'sell']
    assert len(sells) == 1
    assert sells[0]['date'] == trade_days[6]
    bought = next(t for t in result['trades']
                  if t['stock_code'] == rising and t['signal'] == 'buy')
    assert 0 < sells[0]['shares'] < bought['shares'], '目标权重减仓不应清仓'

    residual_weight = (sells[0]['position'] * 20.0) / result['final_equity']
    assert residual_weight == pytest.approx(0.5, abs=0.03)


def test_pooled_cash_conserved_against_trade_cashflow():
    """池模式资金守恒：现金 + 持仓市值 = 初始资金 + 盈亏 − 成本。"""
    codes = ['600000.SH', '000001.SZ', '300001.SZ']
    panel = _flat_panel(codes, days=16)
    script = {
        0: {'action': 'buy', 'target_weight': 0.32},
        8: {'action': 'buy', 'target_weight': 0.32},
        12: {'action': 'sell', 'target_weight': 0.1},
    }
    engine = PortfolioBacktestEngine(panel)
    engine.set_strategies({code: _ScriptedStrategy(script) for code in codes})
    capital = 1_000_000.0
    result = engine.run_backtest(initial_capital=capital, pooled_cash=True)

    trades = result['trades']
    buy_notional = sum(t['price'] * t['shares'] for t in trades if t['signal'] == 'buy')
    sell_notional = sum(t['price'] * t['shares'] for t in trades if t['signal'] == 'sell')
    total_cost = sum(t['cost'] for t in trades)
    market_value = sum(
        qty * float(panel[code]['close'][-1])
        for code, qty in result['positions'].items()
    )
    # 现金按成交流水独立复算（不使用账本内部状态）
    cash = capital + sell_notional - buy_notional - total_cost
    pnl = market_value + sell_notional - buy_notional

    assert result['final_equity'] == pytest.approx(cash + market_value, abs=1e-6)
    assert cash + market_value == pytest.approx(capital + pnl - total_cost, abs=1e-6)
    assert total_cost > 0


def test_default_mode_matches_explicit_pooled_false():
    """pooled_cash=False 是默认路径：显式传入必须与不传逐位一致。"""
    panel = _flat_panel(['600000.SH', '000001.SZ'])
    script = {0: {'action': 'buy', 'target_weight': 0.5},
              6: {'action': 'sell', 'fraction': 0.5}}

    def _run(**kwargs):
        engine = PortfolioBacktestEngine(panel)
        engine.set_strategies({code: _ScriptedStrategy(script) for code in panel})
        return engine.run_backtest(initial_capital=1_000_000.0, **kwargs)

    default_result = _run()
    explicit_result = _run(pooled_cash=False)
    assert default_result['trades'] == explicit_result['trades']
    assert default_result['equity_curve'] == explicit_result['equity_curve']
    assert default_result['positions'] == explicit_result['positions']
    assert default_result['final_equity'] == explicit_result['final_equity']


if __name__ == "__main__":
    test_portfolio_backtest()
    test_portfolio_backtest_precomputes_indicators()

