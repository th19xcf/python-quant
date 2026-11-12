"""成本模型冲击项（V-35/V-36）：默认 0.015 的口径决定 + 零冲击路径的逐位不变量。

引入 ``impact_coef`` 之前的实测值于 2026-11-12 用改动前的 ``CostModel`` 复算，
见 ``2026-11-12/code-review/outputs/外部审查建议核对_v2_2026-11-12.md`` §4.1。

本文件锁两件互不冲突的事：
  1. **默认值本身**：``COST_DEFAULTS['impact_coef'] == 0.015``，是 2026-11-12 的
     显式口径决定（依据见该用例 docstring），不是随手打开的；
  2. **零冲击路径没被改动**：显式传 ``impact_coef=0.0`` 时，成本、成交笔数、
     逐笔冲击项与接线前的旧基线**精确相等**（``==``，不是 ``approx``）。
此外覆盖：冲击项公式、10% 参与率上限的实际行为、``adv_amount`` 缺失时的
行为（返回 0、不抛错、无 NaN 传染），以及 V-36 引擎接线（``adv20`` 真的作为
``adv_amount`` 进了成本模型）。
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


def _zero_model() -> CostModel:
    """显式零冲击模型：所有「与旧基线逐位相同」的断言都用它，不用默认参数。"""
    return CostModel({'impact_coef': 0.0})


def _call(model: CostModel, name: str, **extra) -> float:
    case = dict(CASES[name])
    side = case.pop('side')
    fn = model.calculate_buy_cost if side == 'buy' else model.calculate_sell_cost
    return fn(**case, **extra)


def test_impact_coef_default_is_explicit_decision():
    """默认 ``impact_coef`` = 0.015：显式口径决定，改它需要重新实测 + 用户批准。

    依据（2026-11-12 冲击成本系数扫描，36 次运行）：策略收益翻负点 1 亿档
    coef≈0.099、1,000 万档≈0.298；0.015 仅为翻负点的 1/6.6~1/20。旧默认 0
    的偏乐观幅度实测 100 万~1,000 万档 0.2~2.0 pp、1 亿档 1.8~6.0 pp。
    改这个值的条件：① 有新一版翻负点实测；② 用户显式批准；③ 同步更新
    report / golden 基线并留证。在此之前本断言失败即说明有人顺手改了口径。
    """
    assert COST_DEFAULTS['impact_coef'] == 0.015
    assert CostModel().params['impact_coef'] == 0.015


@pytest.mark.parametrize('name', sorted(CASES))
def test_explicit_zero_coef_keeps_unit_baseline_bitwise(name):
    """显式 coef=0 时返回值与引入冲击项之前逐值相等（精确 ==）。"""
    assert _call(_zero_model(), name) == BASELINE[name]


@pytest.mark.parametrize('name', sorted(CASES))
def test_adv_amount_is_inert_when_coef_is_explicitly_zero(name):
    """显式系数 0 时即使传 adv_amount，成本也必须逐值不变（调用方可无条件传参）。"""
    assert _call(_zero_model(), name, adv_amount=1.0e9) == BASELINE[name]


def test_impact_scales_with_sqrt_participation():
    """系数 > 0：新增成本 = 成交额 × coef × sqrt(成交额 / ADV)。"""
    model = CostModel({'impact_coef': 0.5})
    adv_amount = 1.0e6
    amount = 1000.0 * 10.0
    expected_extra = amount * 0.5 * math.sqrt(amount / adv_amount)

    base = _call(_zero_model(), 'stock_plain_buy', adv_amount=adv_amount)
    with_impact = _call(model, 'stock_plain_buy', adv_amount=adv_amount)
    assert with_impact - base == pytest.approx(expected_extra, rel=1e-12, abs=0.0)


def test_participation_above_10pct_is_not_truncated():
    """10% 流动性上限不在成本模型里截断，按实际参与率计价。

    当前实现（先读代码确认，非猜测）：``CostModel._participation`` 与
    ``impact_rate`` 都不做任何 clamp，参与率 50% 时冲击费率仍按 sqrt(0.5) 计。
    10% 的上限由风控规则 ``MaxAdvParticipation``
    （``DEFAULT_RISK_LIMITS['max_adv_participation'] == 0.10``）在**成交量**
    口径上拒单实现，属于「订单是否允许成交」，与「已成交订单的成本」是两件事，
    成本模型不重复截断——否则风控参数被调宽时成本会被静默低估。
    """
    model = CostModel({'impact_coef': 0.5})
    assert model.impact_rate(0.10) == pytest.approx(0.5 * math.sqrt(0.10), rel=1e-15)
    assert model.impact_rate(0.50) == pytest.approx(0.5 * math.sqrt(0.50), rel=1e-15)
    # 超过 10% 继续增长而不是被夹在同一水平
    assert model.impact_rate(0.50) > model.impact_rate(0.10)

    # 端到端：参与率 50% 的成交，多出的成本严格等于公式值
    adv_amount = 20_000.0        # 成交额 10,000 元 → 参与率 0.5
    base = _call(_zero_model(), 'stock_plain_buy', adv_amount=adv_amount)
    charged = _call(model, 'stock_plain_buy', adv_amount=adv_amount)
    assert charged - base == pytest.approx(
        10_000.0 * 0.5 * math.sqrt(0.5), rel=1e-12, abs=0.0)


def test_impact_rate_returns_zero_without_inputs():
    """参与率缺失 / 非正 / NaN 时恒为 0.0，避免除零与 NaN 传播。"""
    model = CostModel({'impact_coef': 0.5})
    assert model.impact_rate(None) == 0.0
    assert model.impact_rate(0.0) == 0.0
    assert model.impact_rate(-0.2) == 0.0
    assert model.impact_rate(float('nan')) == 0.0
    assert _call(model, 'stock_plain_buy', adv_amount=0.0) == BASELINE['stock_plain_buy']


def test_coef_positive_without_adv_amount_returns_zero_and_no_nan():
    """对照组：系数 > 0 但 adv_amount 为 None → 返回 0、不抛错、成本仍为有限值。

    选择写进 ``CostModel.impact_rate`` 的文档：ADV 缺失是数据覆盖问题，
    冲击项是可选叠加项，抛错会让整次回测因个别标的缺列而整体失败；
    缺 ADV 的标的会出现在回测结果 ``cost_inputs['impact_missing_symbols']`` 里。
    """
    model = CostModel({'impact_coef': 0.5})
    assert model._participation(10_000.0, None) is None
    assert model._participation(10_000.0, float('nan')) is None
    assert model.impact_rate(None) == 0.0

    for adv in (None, float('nan'), 0.0):
        cost = _call(model, 'stock_plain_buy', adv_amount=adv)
        assert math.isfinite(cost)
        assert not math.isnan(cost)
        assert cost == BASELINE['stock_plain_buy']


# ---------------------------------------------------------------------------
# V-36 引擎接线：adv20（20 日均成交额，元）必须真正进入 CostModel
# ---------------------------------------------------------------------------

#: 合成面板的常数成交额（元）。adv20 = shift(1) 后的滚动均值，constant 序列
#: 在任一成交 bar 上都等于该常数，测试因此可以独立推出参与率分母。
ADV_AMOUNT = 1.0e12

#: 显式零系数下该合成面板的旧基线（与接线前逐位相同，精确 == 断言）。
ZERO_IMPACT_BASELINE = {
    'n_trades': 3,
    'total_cost': 11114.93727729347,
    'total_return': -8.829667528597348,
}


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


def _run_engine(*, coef: float | None = None, with_amount: bool = True) -> dict:
    """``coef=None`` 用默认参数（0.015），否则显式传该系数。"""
    engine = BacktestEngine(_panel(with_amount=with_amount))
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    engine.set_cost_model(CostModel() if coef is None else CostModel({'impact_coef': coef}))
    return engine.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')


def test_engine_reports_adv20_available_for_impact():
    """有 amount 列时 adv20 必须被识别为可用（默认系数下也是真实输入）。"""
    cost_inputs = _run_engine()['cost_inputs']
    assert cost_inputs['impact_adv20_symbols'] == ['600000.SH']
    assert cost_inputs['impact_missing_symbols'] == []
    assert cost_inputs['impact_missing_note'] is None


def test_engine_default_coef_applies_impact():
    """默认 0.015 在引擎里真实生效：逐笔成本多出 成交额×0.015×sqrt(参与率)。"""
    coef = COST_DEFAULTS['impact_coef']
    base = _run_engine(coef=0.0)
    with_default = _run_engine()

    assert base['trades'] and with_default['trades']
    for trade in with_default['trades']:
        amount = trade['shares'] * trade['price']
        plain = (CostModel({'impact_coef': 0.0}).calculate_buy_cost
                 if trade['signal'] == 'buy'
                 else CostModel({'impact_coef': 0.0}).calculate_sell_cost)(
            trade['shares'], trade['price'],
            trade_date=trade['date'], ts_code=trade['stock_code'])
        diff = trade['cost'] - plain
        assert diff > 0.0
        assert diff == pytest.approx(
            amount * coef * math.sqrt(amount / ADV_AMOUNT), rel=1e-9, abs=0.0)


def test_explicit_zero_coef_engine_is_bitwise_identical_to_legacy_baseline():
    """显式 coef=0：引擎结果与接线前旧基线精确相等，逐笔冲击项精确为 0。"""
    result = _run_engine(coef=0.0)
    trades = result['trades']

    assert len(trades) == ZERO_IMPACT_BASELINE['n_trades']
    assert sum(t['cost'] for t in trades) == ZERO_IMPACT_BASELINE['total_cost']
    assert result['total_return'] == ZERO_IMPACT_BASELINE['total_return']

    for trade in trades:
        zero_cost = CostModel({'impact_coef': 0.0})
        fn = (zero_cost.calculate_buy_cost if trade['signal'] == 'buy'
              else zero_cost.calculate_sell_cost)
        plain = fn(trade['shares'], trade['price'],
                   trade_date=trade['date'], ts_code=trade['stock_code'])
        assert trade['cost'] - plain == 0.0


def test_engine_impact_equals_amount_times_coef_times_sqrt_participation():
    """正例：coef>0 且 adv20 有值时，逐笔成交成本 == 零系数成本 + 冲击公式项。

    零系数成本由显式 ``impact_coef=0.0`` 的模型按同一笔成交独立重算；冲击项用
    测试自己的 ``ADV_AMOUNT``（常数 amount 的 20 日均值）解析推导，相对误差
    < 1e-9。若引擎没把 adv20 作为 adv_amount 传进去，差值恒为 0，断言必然失败。

    注：coef>0 时股数可能比 coef=0 少几个整手——买入的「可负担股数」估算费率
    （``get_buy_cost_rate``）不含冲击项，由逐手回退按实际成本兜底（见
    ``_execute_buy`` 的注释），因此这里不比较两次运行的股数。
    """
    coef = 0.5
    base = _run_engine(coef=0.0)
    with_impact = _run_engine(coef=coef)

    assert base['trades'] and with_impact['trades']

    for trade in base['trades']:
        zero_cost = CostModel({'impact_coef': 0.0})
        fn = (zero_cost.calculate_buy_cost if trade['signal'] == 'buy'
              else zero_cost.calculate_sell_cost)
        plain = fn(trade['shares'], trade['price'],
                   trade_date=trade['date'], ts_code=trade['stock_code'])
        # 系数 0：引擎成本必须与独立重算逐位相同（冲击项精确为 0）
        assert trade['cost'] == plain

    for trade in with_impact['trades']:
        zero_cost = CostModel({'impact_coef': 0.0})
        fn = (zero_cost.calculate_buy_cost if trade['signal'] == 'buy'
              else zero_cost.calculate_sell_cost)
        plain = fn(trade['shares'], trade['price'],
                   trade_date=trade['date'], ts_code=trade['stock_code'])
        amount = trade['shares'] * trade['price']
        expected_impact = amount * coef * math.sqrt(amount / ADV_AMOUNT)
        diff = trade['cost'] - plain
        assert diff > 0.0
        assert diff == pytest.approx(expected_impact, rel=1e-9, abs=0.0)


def test_engine_explicit_zero_with_amount_equals_without_amount():
    """显式 coef=0：有无 amount 列的引擎结果逐位相同（adv20 此时不参与计价）。"""
    with_amount = _run_engine(coef=0.0, with_amount=True)
    without_amount = _run_engine(coef=0.0, with_amount=False)

    assert with_amount['total_return'] == without_amount['total_return']
    assert [t['cost'] for t in with_amount['trades']] == [
        t['cost'] for t in without_amount['trades']]
    assert [(t['date'], t['signal'], t['shares'])
            for t in with_amount['trades']] == [
        (t['date'], t['signal'], t['shares']) for t in without_amount['trades']]


def test_engine_without_amount_column_keeps_impact_at_zero():
    """对照组：面板无 amount 列（adv_amount=None）时冲击项为 0，且被显式记入审计。"""
    base = _run_engine(coef=0.0, with_amount=False)
    with_impact = _run_engine(coef=0.5, with_amount=False)

    assert with_impact['cost_inputs']['impact_missing_symbols'] == ['600000.SH']
    assert with_impact['cost_inputs']['impact_missing_note']
    assert [t['cost'] for t in with_impact['trades']] == [
        t['cost'] for t in base['trades']]
