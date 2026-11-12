"""成本模型冲击项（V-35/V-36）：默认系数 0 时与引入前逐位一致。

引入 ``impact_coef`` 之前的实测值于 2026-11-12 用改动前的 ``CostModel`` 复算，
见 ``2026-11-12/code-review/outputs/外部审查建议核对_v2_2026-11-12.md`` §4.1。
本文件断言四件事：
  1. 默认 ``impact_coef == 0.0``，且传 ``adv_amount`` 也不改变既有成本；
  2. 五个既有场景的返回值与改动前逐值相等（不是 approx）；
  3. 系数 > 0 时按 ``coef × sqrt(成交额/ADV)`` 精确叠加；
  4. V-36 接线：回测引擎把已算好的 ``adv20``（20 日均成交额，元）真的作为
     ``adv_amount`` 传进了成本模型——用「引擎实际成交成本差 == 公式值」证明，
     而不是只检查参数有没有被传进去。
"""

from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.costs.cost_model import COST_DEFAULTS, CostModel
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.strategies.ma_strategy import MAStrategy

#: 改动前实测值（精确相等断言，禁止改成 approx）
BASELINE: dict[str, float] = {
    'stock_plain_buy': 35.2,
    'stock_largecap_buy': 20.2,
    'stock_smallcap_sell': 40.2,
    'etf_deep_buy': 40.0,
    'legacy_2015_sell': 51.0,
}

CASES: dict[str, dict] = {
    'stock_plain_buy': {
        'side': 'buy', 'shares': 1000.0, 'price': 10.0,
        'trade_date': date(2024, 1, 2), 'ts_code': '600000.SH'},
    'stock_largecap_buy': {
        'side': 'buy', 'shares': 1000.0, 'price': 10.0,
        'trade_date': date(2024, 1, 2), 'ts_code': '600000.SH',
        'total_mv_yi': 800.0},
    'stock_smallcap_sell': {
        'side': 'sell', 'shares': 1000.0, 'price': 10.0,
        'trade_date': date(2024, 1, 2), 'ts_code': '000001.SZ',
        'total_mv_yi': 50.0},
    'etf_deep_buy': {
        'side': 'buy', 'shares': 10_000.0, 'price': 4.0,
        'trade_date': date(2024, 1, 2), 'ts_code': '510300.SH',
        'adv20': 2.0e8},
    'legacy_2015_sell': {
        'side': 'sell', 'shares': 1000.0, 'price': 10.0,
        'trade_date': date(2015, 3, 2), 'ts_code': '600000.SH'},
}


def _call(model: CostModel, name: str, **extra) -> float:
    case = dict(CASES[name])
    side = case.pop('side')
    fn = model.calculate_buy_cost if side == 'buy' else model.calculate_sell_cost
    return fn(**case, **extra)


def test_impact_coef_defaults_to_zero():
    """默认系数必须是 0：任何「顺手开一点」都会改掉全部历史回测的收益口径。"""
    assert COST_DEFAULTS['impact_coef'] == 0.0
    assert CostModel().params['impact_coef'] == 0.0


@pytest.mark.parametrize('name', sorted(CASES))
def test_default_coef_keeps_legacy_cost_bitwise(name):
    """默认 0 时返回值与引入冲击项之前逐值相等。"""
    assert _call(CostModel(), name) == BASELINE[name]


@pytest.mark.parametrize('name', sorted(CASES))
def test_adv_amount_is_inert_when_coef_is_zero(name):
    """系数为 0 时即使传 adv_amount，成本也必须逐值不变（调用方可以无条件传参）。"""
    assert _call(CostModel(), name, adv_amount=1.0e9) == BASELINE[name]


def test_impact_scales_with_sqrt_participation():
    """系数 > 0：新增成本 = 成交额 × coef × sqrt(成交额 / ADV)。"""
    model = CostModel({'impact_coef': 0.5})
    shares, price, adv_amount = 1000.0, 10.0, 1.0e6
    amount = shares * price
    participation = amount / adv_amount
    expected_extra = amount * 0.5 * participation ** 0.5

    base = _call(CostModel(), 'stock_plain_buy', adv_amount=adv_amount)
    with_impact = _call(model, 'stock_plain_buy', adv_amount=adv_amount)
    assert with_impact - base == pytest.approx(expected_extra, rel=1e-12, abs=0.0)


def test_impact_rate_returns_zero_without_inputs():
    """参与率缺失 / 非正时恒为 0.0，避免除零与 NaN 传播。"""
    model = CostModel({'impact_coef': 0.5})
    assert model.impact_rate(None) == 0.0
    assert model.impact_rate(0.0) == 0.0
    assert model.impact_rate(-0.2) == 0.0
    assert _call(model, 'stock_plain_buy', adv_amount=0.0) == BASELINE['stock_plain_buy']


def test_coef_positive_without_adv_amount_returns_zero_not_error():
    """对照组：系数 > 0 但 adv_amount 为 None → 返回 0，不抛错（既定选择）。

    选择写进 ``CostModel.impact_rate`` 的文档：ADV 缺失是数据覆盖问题，
    冲击项是可选叠加项，抛错会让整次回测因个别标的缺列而整体失败；
    缺 ADV 的标的会出现在回测结果 ``cost_inputs['impact_missing_symbols']`` 里。
    """
    model = CostModel({'impact_coef': 0.5})
    assert model._participation(10_000.0, None) is None
    assert model.impact_rate(None) == 0.0
    assert _call(model, 'stock_plain_buy', adv_amount=None) == BASELINE['stock_plain_buy']


# ---------------------------------------------------------------------------
# V-36 引擎接线：adv20（20 日均成交额，元）必须真正进入 CostModel
# ---------------------------------------------------------------------------

#: 合成面板的常数成交额（元）。adv20 = shift(1) 后的滚动均值，constant 序列
#: 在任一成交 bar 上都等于该常数，测试因此可以独立推出参与率分母。
ADV_AMOUNT = 1.0e12


def _panel(days: int = 80, with_amount: bool = True) -> pl.DataFrame:
    """单价 ~1 元的合成行情；有 amount 列时引擎才会构造 adv20。"""
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    close = 1.0 + 0.1 * np.sin(np.arange(days) / 5.0)
    data = {
        'date': dates,
        'open': close * 1.0005,
        'high': close * 1.01,
        'low': close * 0.99,
        'close': close,
        'volume': np.full(days, 1.0e8),
    }
    if with_amount:
        data['amount'] = np.full(days, ADV_AMOUNT)
    return pl.DataFrame(data)


def _run_engine(*, coef: float, with_amount: bool = True) -> dict:
    engine = BacktestEngine(_panel(with_amount=with_amount))
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    engine.set_cost_model(CostModel({'impact_coef': coef}))
    return engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')


def test_engine_reports_adv20_available_for_impact():
    """有 amount 列时 adv20 必须被识别为可用（默认系数下也是真实输入）。"""
    result = _run_engine(coef=0.0)
    cost_inputs = result['cost_inputs']
    assert cost_inputs['impact_adv20_symbols'] == ['600000.SH']
    assert cost_inputs['impact_missing_symbols'] == []
    assert cost_inputs['impact_missing_note'] is None


def test_default_coef_leaves_engine_result_identical():
    """反例（默认不变）：有 amount 列但 coef=0，结果必须与无 amount 列逐位相同。"""
    with_amount = _run_engine(coef=0.0, with_amount=True)
    without_amount = _run_engine(coef=0.0, with_amount=False)

    assert with_amount['total_return'] == without_amount['total_return']
    assert [t['cost'] for t in with_amount['trades']] == [
        t['cost'] for t in without_amount['trades']]
    assert [(t['date'], t['signal'], t['shares'])
            for t in with_amount['trades']] == [
        (t['date'], t['signal'], t['shares']) for t in without_amount['trades']]


def test_engine_impact_equals_amount_times_coef_times_sqrt_participation():
    """正例：coef>0 且 adv20 有值时，逐笔成交成本 == 零系数成本 + 冲击公式项。

    零系数成本由 ``CostModel()``（默认 impact_coef=0，已在上面的用例里与改动前
    逐值对齐）按同一笔成交独立重算；冲击项用测试自己的 ``ADV_AMOUNT``（常数
    amount 的 20 日均值）解析推导，相对误差 < 1e-9。
    若引擎没把 adv20 作为 adv_amount 传进去，差值恒为 0，断言必然失败。

    注：coef>0 时股数可能比 coef=0 少几个整手——买入的「可负担股数」估算费率
    （``get_buy_cost_rate``）不含冲击项，由逐手回退按实际成本兜底（见
    ``_execute_buy`` 的注释），因此这里不比较两次运行的股数。
    """
    coef = 0.5
    base_model = CostModel()
    base = _run_engine(coef=0.0)
    with_impact = _run_engine(coef=coef)

    assert base['trades'] and with_impact['trades']

    for trade in base['trades']:
        fn = (base_model.calculate_buy_cost if trade['signal'] == 'buy'
              else base_model.calculate_sell_cost)
        plain = fn(trade['shares'], trade['price'],
                   trade_date=trade['date'], ts_code=trade['stock_code'])
        # 系数 0：引擎成本必须与独立重算逐位相同（冲击项精确为 0）
        assert trade['cost'] == plain

    for trade in with_impact['trades']:
        fn = (base_model.calculate_buy_cost if trade['signal'] == 'buy'
              else base_model.calculate_sell_cost)
        plain = fn(trade['shares'], trade['price'],
                   trade_date=trade['date'], ts_code=trade['stock_code'])
        amount = trade['shares'] * trade['price']
        expected_impact = amount * coef * math.sqrt(amount / ADV_AMOUNT)
        diff = trade['cost'] - plain
        assert diff > 0.0
        assert diff == pytest.approx(expected_impact, rel=1e-9, abs=0.0)


def test_engine_without_amount_column_keeps_impact_at_zero():
    """对照组：面板无 amount 列（adv_amount=None）时冲击项为 0，且被显式记入审计。"""
    base = _run_engine(coef=0.0, with_amount=False)
    with_impact = _run_engine(coef=0.5, with_amount=False)

    assert with_impact['cost_inputs']['impact_missing_symbols'] == ['600000.SH']
    assert with_impact['cost_inputs']['impact_missing_note']
    assert [t['cost'] for t in with_impact['trades']] == [
        t['cost'] for t in base['trades']]

