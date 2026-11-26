"""回测引擎极简基准（2026-11-26）：买入持有与随机策略不得「凭空赚钱」。

用合成数据，结果可以手算：
- 买入持有：首日收盘出信号、次日开盘按开盘价全仓买入（整手），之后不动；
  期末权益必须等于「现金余额 + 持股 × 末日收盘」，收益与标的自身涨幅只差首笔成本和零头现金。
- 随机策略：在一条**零漂移**的随机游走价格上随机买卖 100 次（不同种子）。
  价格本身没有趋势，任何稳定的正收益都只能来自撮合作弊（买在低点、卖在高点、偷看未来）。
  要求：平均收益 ≤ 0（成本让它略亏），且没有任何一次跑出不合理的大幅正收益。
"""
from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy

CODE = '600000.SH'


class _Scripted(BaseStrategy):
    def __init__(self, script: Dict[int, Any]):
        super().__init__('scripted')
        self.script = dict(script)

    def generate_signal(self, data, index):  # noqa: ANN001, ANN201
        return self.script.get(index)

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        return pl.Series('signal', [self.script.get(i) for i in range(df.height)],
                         dtype=pl.Object)


def _panel(close: np.ndarray, open_: np.ndarray | None = None) -> Dict[str, pl.DataFrame]:
    n = close.size
    open_ = close.copy() if open_ is None else open_
    dates = pd.date_range('2023-01-02', periods=n, freq='B')
    return {CODE: pl.DataFrame({
        'date': dates, 'open': open_,
        'high': np.maximum(open_, close) * 1.002, 'low': np.minimum(open_, close) * 0.998,
        'close': close, 'volume': np.full(n, 5_000_000.0),
    })}


def _run(panel: Dict[str, pl.DataFrame], script: Dict[int, Any],
         capital: float = 1_000_000.0) -> Dict[str, Any]:
    engine = PortfolioBacktestEngine(panel)
    engine.set_strategies({CODE: _Scripted(script)})
    return engine.run_backtest(initial_capital=capital, pooled_cash=True)


def test_buy_and_hold_matches_underlying() -> None:
    n = 120
    close = 10.0 * np.cumprod(np.full(n, 1.002))
    open_ = np.r_[close[0], close[:-1] * 1.001]       # 开盘价与收盘价不同，避免两者混淆被掩盖
    panel = _panel(close, open_)
    capital = 1_000_000.0
    res = _run(panel, {0: {'action': 'buy', 'target_weight': 0.99}}, capital)

    buys = [t for t in res['trades'] if t.get('signal') == 'buy']
    assert len(buys) == 1, 'T 日信号只应在 T+1 开盘成交一次'
    trade = buys[0]
    assert trade['price'] == pytest.approx(open_[1]), '必须按 T+1 开盘价成交，不能用信号日收盘价'
    assert trade['shares'] % 100 == 0 and trade['shares'] > 0

    shares = trade['shares']
    cash = capital - shares * trade['price'] - float(trade.get('cost') or 0.0)
    expected = cash + shares * close[-1]
    assert res['final_equity'] == pytest.approx(expected, rel=1e-9), '期末权益 = 现金 + 持股 × 末日收盘'

    # 与标的自身涨幅比较：收益 = 持仓市值涨幅 − 首笔成本（默认成本模型无市值输入时
    # 按小盘兜底档 30bp 滑点 + 5bp 佣金计，约 35bp），其余 1% 为现金不参与涨跌
    underlying = close[-1] / open_[1] - 1
    strat = res['final_equity'] / capital - 1
    cost = float(trade.get('cost') or 0.0)
    assert 0 < cost / (shares * open_[1]) < 0.005, '首笔成本应在 0~50bp 之间'
    assert strat == pytest.approx((underlying * shares * open_[1] - cost) / capital, abs=1e-9)


@pytest.mark.slow
def test_random_trading_cannot_beat_driftless_market() -> None:
    n = 250
    base_rng = np.random.default_rng(20261126)
    rets = base_rng.normal(0.0, 0.015, n)
    rets -= rets.mean()                                # 严格零漂移：价格首尾几乎持平
    close = 10.0 * np.cumprod(1 + rets)
    open_ = np.r_[close[0], close[:-1] * (1 + base_rng.normal(0, 0.003, n - 1))]
    panel = _panel(close, open_)
    market = close[-1] / close[0] - 1

    results = []
    for seed in range(100):
        rng = np.random.default_rng(seed)
        script: Dict[int, Any] = {}
        holding = False
        for i in range(n - 1):
            if rng.random() < 0.1:                      # 平均每 10 天换一次手
                script[i] = ({'action': 'sell', 'fraction': 1.0} if holding
                             else {'action': 'buy', 'target_weight': 0.99})
                holding = not holding
        res = _run(panel, script)
        results.append(res['final_equity'] / 1_000_000.0 - 1)
    arr = np.array(results)

    # 随机买卖在零漂移市场上，期望 ≈ 部分持仓 × 市场收益 − 成本；不应显著为正。
    # 单次结果受随机持仓区间影响可达 ±25%（日波动 1.5%、250 天），所以只检验均值：
    # 100 次的均值相对其标准误不得显著为正（撮合作弊会让均值系统性为正）。
    assert arr.mean() <= max(market, 0.0) + 0.005, (arr.mean(), market)
    t_mean = arr.mean() / (arr.std(ddof=1) / np.sqrt(arr.size))
    assert t_mean < 2.0, (arr.mean(), t_mean)
    # 正负结果都应出现（全部为正说明在偷看未来）
    assert (arr < 0).sum() >= 20 and (arr > 0).sum() >= 5, ((arr < 0).sum(), (arr > 0).sum())
