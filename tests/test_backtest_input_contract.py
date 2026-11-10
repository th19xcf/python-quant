#!/usr/bin/env python3

"""
回测输入契约与风控默认装配的回归测试（对应代码审查 2.1 / 2.2 / 6.3 / 6.5 / 6.8 / A1 / 3b）。

判别力要求：这些断言在「把校验改成恒等」或「把契约/闸门删掉」之后必须失败，
反向验证方式见本次改动的提交报告（运行时 monkeypatch 后跑本文件）。
"""

from __future__ import annotations

import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine import execution_core
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.input_contract import (
    BAR_CONTRACT,
    BacktestInputContractError,
    validate_universe,
)
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy

CODE = '600000.SH'
ENGINE_DIR = Path(inspect.getfile(execution_core)).parent


def _frame(days: int = 30, volume: float = 1_000_000.0,
           price: float = 10.0, **extra) -> pl.DataFrame:
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    data = {
        'date': dates,
        'open': np.full(days, price),
        'high': np.full(days, price * 1.02),
        'low': np.full(days, price * 0.98),
        'close': np.full(days, price),
        'volume': np.full(days, volume),
    }
    data.update(extra)
    return pl.DataFrame(data)


class _OneShotBuy(BaseStrategy):
    """首根 bar 买一次的极简策略（避免依赖策略实现细节）。"""

    def __init__(self, index: int = 0) -> None:
        super().__init__(name='一次性买入')
        self.index = index

    def generate_signal(self, data, index):
        return 'buy' if index == self.index else 'hold'


class _IndexSignals(BaseStrategy):
    """按 bar 序号返回固定信号（与生产策略接口一致）。"""

    def __init__(self, signals: dict[int, str]) -> None:
        super().__init__(name='索引信号')
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, 'hold')


# ---------------------------------------------------------------------------
# 1. 输入契约：缺列 / 改名 / 复权退化
# ---------------------------------------------------------------------------


def test_missing_required_column_raises_with_column_name():
    frame = _frame().drop('high')
    with pytest.raises(BacktestInputContractError, match='high'):
        validate_universe({CODE: frame})


def test_renamed_column_raises_instead_of_silently_degrading():
    """换数据源只把 date 改成 trade_date：契约必须拒绝，而不是当作无日期。"""
    frame = _frame().rename({'date': 'trade_date'})
    with pytest.raises(BacktestInputContractError, match='date'):
        validate_universe({CODE: frame})


def test_only_one_side_of_raw_pair_raises():
    frame = _frame().with_columns(pl.lit(9.0).alias('raw_open'))
    with pytest.raises(BacktestInputContractError, match='raw_open'):
        validate_universe({CODE: frame})


def test_identity_adjustment_is_rejected_by_default():
    """复权价与原始价逐值相同 = 复权因子恒 1.0，等价于用未复权价算收益。"""
    frame = _frame().with_columns([
        pl.lit(10.0).alias('raw_open'), pl.lit(10.0).alias('raw_close')])
    with pytest.raises(BacktestInputContractError, match='复权因子恒为 1.0'):
        validate_universe({CODE: frame})
    # 合法的原始层消费方必须显式声明，而不是放宽全局校验
    validate_universe({CODE: frame}, price_basis='raw')


def test_engine_run_rejects_contract_violation():
    """校验必须挂在真实撮合入口上，而不是只在工具函数里。"""
    engine = BacktestEngine(_frame().drop('volume'))
    engine.set_strategy(_OneShotBuy())
    with pytest.raises(BacktestInputContractError, match='volume'):
        engine.run_backtest(initial_capital=1_000_000.0, ts_code=CODE)


def test_engine_core_has_no_column_name_literals():
    """引擎内核不得用字面量取行情列：换源只改 input_contract。

    只检查「取列」形式（下标 / .get() / pl.col / in columns），
    结果字典里的 'date' 等字段名属于输出 schema，不在检查范围。
    """
    source = inspect.getsource(execution_core)
    for column in ('open', 'high', 'low', 'close', 'volume', 'amount',
                   'raw_open', 'raw_close', 'date', 'adv20', 'adv_volume',
                   'total_mv', 'total_mv_yi', 'category'):
        forms = (
            f"['{column}']", f'["{column}"]',
            f".get('{column}')", f'.get("{column}")',
            f"pl.col('{column}')", f'pl.col("{column}")',
            f"'{column}' in ", f"'{column}' not in ",
        )
        for form in forms:
            assert form not in source, (
                f'引擎内核仍以字面量取列：{form}（应改为 input_contract 角色）')


def test_result_records_input_contract():
    engine = BacktestEngine(_frame())
    engine.set_strategy(_OneShotBuy())
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code=CODE)
    bar = result['input_contract']['bar']
    assert bar['required']['date'] == BAR_CONTRACT.column('date')
    assert bar['required']['volume'] == BAR_CONTRACT.column('volume')


# ---------------------------------------------------------------------------
# 2. A1：风控默认不放行
# ---------------------------------------------------------------------------


def test_default_adv_gate_rejects_oversized_order():
    """只有 ADV 规则该拦：当日成交量很大（占比合规）、历史 ADV 很小。

    构造：前 29 日每日 1 万手，最后一日放量 100 万手；最后一根 bar 全仓买入
    约 100 万股。单日成交量占比 ≈1%（合规），但 20 日 ADV ≈4.3 万手，
    10% 上限 ≈43 万股 → 必须由 max_adv_participation 拒单。
    """
    days = 30
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    volumes = np.full(days, 1e4)
    volumes[-1] = 1e6
    frame = pl.DataFrame({
        'date': dates,
        'open': np.ones(days),
        'high': np.ones(days),
        'low': np.ones(days),
        'close': np.ones(days),
        'volume': volumes,
    })
    engine = PortfolioBacktestEngine({CODE: frame})
    engine.set_strategy(CODE, _OneShotBuy(index=days - 2))
    result = engine.run_backtest(initial_capital=1_000_000.0)

    gate = result['risk_gate']
    assert gate['enabled'] is True
    assert gate['assembled_by_default'] is True
    assert gate['bypassed_orders'] == 0
    assert result['trades'] == []
    reasons = {item['rule'] for item in result['rejected_orders']}
    assert reasons == {'max_adv_participation'}, reasons
    assert 'max_order_volume_ratio' in gate['rules']


def test_default_adv_gate_keeps_normal_sized_orders():
    """容量充足时不得误杀：小单照常成交，证明闸门是按量判定而非一律拒绝。"""
    engine = PortfolioBacktestEngine({CODE: _frame()})
    engine.set_strategy(CODE, _OneShotBuy())
    result = engine.run_backtest(initial_capital=1_000_000.0)
    assert len(result['trades']) == 1
    assert result['rejected_orders'] == []


# ---------------------------------------------------------------------------
# 3. 3b：跌停卖不出保留 intent 并可统计
# ---------------------------------------------------------------------------


def _limit_down_panel(pre_drop: int = 3) -> pl.DataFrame:
    days = pre_drop + 4
    opens = [100.0] * pre_drop + [90.0, 81.0, 72.9, 75.0]
    closes = list(opens)
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    return pl.DataFrame({
        'date': dates,
        'open': opens,
        'high': [max(o, c) for o, c in zip(opens, closes, strict=True)],
        'low': [min(o, c) for o, c in zip(opens, closes, strict=True)],
        'close': closes,
        'volume': np.full(days, 1_000_000.0),
    })


def test_locked_sell_is_retained_and_counted():
    data = _limit_down_panel(pre_drop=3)
    dates = data['date'].to_list()
    engine = BacktestEngine(data)
    # 第 0 根买入（次日 100 成交），第 2 根卖出 → 次日开始连续三天跌停
    engine.set_strategy(_IndexSignals({0: 'buy', 2: 'sell'}))
    result = engine.run_backtest(initial_capital=1_000_000.0, ts_code=CODE)

    assert [t['signal'] for t in result['trades']] == ['buy', 'sell']
    # 开板日按开盘价 75 成交，持仓没有被无声锁死
    assert result['trades'][1]['date'] == dates[-1]
    assert result['trades'][1]['price'] == 75.0
    # 「因跌停卖不出」逐日可统计，且不混入 expired_orders
    assert result['blocked_reason_counts'] == {'limit_down': 3}
    assert result['expired_reason_counts'] == {}
    assert {item['blocked_by'] for item in result['blocked_orders']} == {'limit_down'}
