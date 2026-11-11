#!/usr/bin/env python3
"""P0 结果可信度加固回归测试（2026-11-06 技术分析报告第 7 章）。

覆盖：分档滑点注入与因果性、参数优化样本外隔离、风控闸门显式化、
静默降级留痕、文档常量与代码绑定、撮合/挂单性能优化的等价性。
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import warnings
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.costs.cost_model import (
    COST_DEFAULTS,
    ETF_SLIPPAGE_TIERS,
    STOCK_SLIPPAGE_TIERS_YI,
    CostModel,
)
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.execution_core import (
    AccountBook,
    EventBacktestRunner,
    RiskDisabledWarning,
)
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.ma_strategy import MAStrategy
from src.core.usecases.run_backtest import run_backtest


class _FixedSignalStrategy(BaseStrategy):
    """在指定 bar 索引发出固定信号（只走逐行路径，便于控制成交日）。"""

    def __init__(self, signals):
        super().__init__(name='P0 固定信号')
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, 'hold')


def _panel(days=8, *, code='600000.SH', amount=None, total_mv_yi=None,
           total_mv=None, category=None, price=10.0):
    """构造价格恒定的平稳面板；可选注入分档滑点输入列。"""
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    columns = {
        'date': dates,
        'open': np.full(days, price),
        'high': np.full(days, price * 1.005),
        'low': np.full(days, price * 0.995),
        'close': np.full(days, price),
        'volume': np.full(days, 1_000_000.0),
    }
    if amount is not None:
        columns['amount'] = [amount] * days if np.isscalar(amount) else list(amount)
    if total_mv_yi is not None:
        columns['total_mv_yi'] = [total_mv_yi] * days
    if total_mv is not None:
        columns['total_mv'] = [total_mv] * days
    if category is not None:
        columns['category'] = [category] * days
    return {code: pl.DataFrame(columns)}


def _run(data, *, signals=None, capital=1_000_000.0, **kwargs):
    signals = signals or {0: 'buy', 4: 'sell'}
    engine = PortfolioBacktestEngine(data)
    code = next(iter(data))
    engine.set_strategies({code: _FixedSignalStrategy(signals)})
    return engine.run_backtest(initial_capital=capital, **kwargs)


# ---------------------------------------------------------------------------
# P0-1 分档滑点注入
# ---------------------------------------------------------------------------

def test_tiered_slippage_changes_buy_and_sell_cost():
    """大盘 15bp / 小盘 30bp 必须真实反映在买卖成本与逐笔日志上。"""
    big = _run(_panel(total_mv_yi=800.0))
    small = _run(_panel(total_mv_yi=50.0))
    missing = _run(_panel())

    big_buy = next(t for t in big['trades'] if t['signal'] == 'buy')
    small_buy = next(t for t in small['trades'] if t['signal'] == 'buy')
    missing_buy = next(t for t in missing['trades'] if t['signal'] == 'buy')

    assert big_buy['slippage_rate'] == STOCK_SLIPPAGE_TIERS_YI[0][1] == 0.0015
    assert small_buy['slippage_rate'] == STOCK_SLIPPAGE_TIERS_YI[-1][1] == 0.0030
    assert missing_buy['slippage_rate'] == 0.0030  # 缺输入退保守小盘档

    # 与成本模型直接调用一致（三处调用口径统一，估算/回退/扣费都不漏传）
    model = CostModel()
    expected = model.calculate_buy_cost(
        big_buy['shares'], big_buy['price'], trade_date=big_buy['date'],
        ts_code=big_buy['stock_code'], total_mv_yi=800.0)
    assert big_buy['cost'] == pytest.approx(expected)
    # 档位进入 get_buy_cost_rate 后会改变整手估算，股数不必相同；
    # 但每笔实际扣费必须与同档位的成本模型直接调用逐分一致
    expected_small = model.calculate_buy_cost(
        small_buy['shares'], small_buy['price'], trade_date=small_buy['date'],
        ts_code=small_buy['stock_code'], total_mv_yi=50.0)
    assert small_buy['cost'] == pytest.approx(expected_small)
    assert small_buy['shares'] < big_buy['shares']  # 30bp 档可买股数更少

    sell = next(t for t in big['trades'] if t['signal'] == 'sell')
    assert sell['slippage_rate'] == 0.0015


def test_etf_slippage_uses_adv20_and_category():
    """ETF 按成交额（元）分档，sector 类取相邻更高档。"""
    wide = _run(_panel(code='510300.SH', amount=2.0e8))
    narrow = _run(_panel(code='510300.SH', amount=6.0e7))
    sector = _run(_panel(code='510300.SH', amount=2.0e8, category='sector'))

    assert wide['trades'][0]['slippage_rate'] == 0.0005
    assert narrow['trades'][0]['slippage_rate'] == 0.0007
    assert sector['trades'][0]['slippage_rate'] == 0.0007
    assert wide['cost_inputs']['tiered_symbols'] == ['510300.SH']


def test_slippage_inputs_are_lagged_by_one_bar():
    """分档输入只能用 t-1 及之前信息：t 日开盘成交不得看到 t 日成交额。"""
    amounts = [1.0e8, 2.0e8, 3.0e8, 4.0e8]
    data = _panel(days=4, amount=amounts, total_mv_yi=100.0)
    code = next(iter(data))
    runner = EventBacktestRunner(CostModel(), PerformanceAnalyzer())
    _, cols, _ = runner._prepare_universe(data, {code: _FixedSignalStrategy({})})
    adv20 = cols[code]['adv20']

    assert adv20[0] is None
    assert adv20[1] == pytest.approx(amounts[0])
    assert adv20[2] == pytest.approx((amounts[0] + amounts[1]) / 2)
    assert adv20[3] == pytest.approx(sum(amounts[:3]) / 3)
    assert cols[code]['total_mv_yi'][0] is None
    assert cols[code]['total_mv_yi'][1] == 100.0


def test_total_mv_yuan_is_converted_to_yi():
    """估值快照 total_mv 常见单位为元，需换算为亿元后再分档。"""
    big = _run(_panel(total_mv=800.0 * 1e8))
    assert big['trades'][0]['slippage_rate'] == 0.0015


def test_cost_inputs_are_reported_in_result():
    """缺输入的标的必须出现在 fallback_symbols，不允许静默兜底。"""
    result = _run(_panel())
    assert result['cost_inputs']['fallback_symbols'] == ['600000.SH']
    assert result['cost_inputs']['tiered_count'] == 0
    assert result['cost_inputs']['lag_bars'] == 1


def test_cost_model_date_parsing_handles_third_party_timestamps():
    """历史印花税/过户费时间表在 Polars/Pandas 时间戳输入下不得失效。"""
    model = CostModel()
    stamp = pd.Timestamp('2010-01-04')  # 2008-09-19 ~ 2023-08-27：卖出 0.1%
    assert model._stamp_tax_rate(stamp, 'sell', '600000.SH') == 0.001
    # 2015-08-01 前沪市按成交额 0.06% 收过户费，深市不收
    assert model._transfer_fee_rate(stamp, '600000.SH') == 0.0006
    assert model._transfer_fee_rate(stamp, '000001.SZ') == 0.0
    assert model._stamp_tax_rate(datetime(2024, 1, 2), 'sell', '600000.SH') == 0.0005
    assert model._transfer_fee_rate(datetime(2024, 1, 2), '600000.SH') == 0.00002
    # ETF/LOF 免过户费：2015 前后、有无日期、沪深两市都必须为 0
    assert model._transfer_fee_rate(stamp, '510300.SH') == 0.0
    assert model._transfer_fee_rate(stamp, '159915.SZ') == 0.0
    assert model._transfer_fee_rate(datetime(2024, 1, 2), '510300.SH') == 0.0
    assert model._transfer_fee_rate(None, '501000.SH') == 0.0
    assert model._stamp_tax_rate('not-a-date', 'sell', '600000.SH') == (
        COST_DEFAULTS['stamp_tax_rate'])


# ---------------------------------------------------------------------------
# P0-2 参数优化信息泄漏
# ---------------------------------------------------------------------------

def _osc_frame(days=240, phase=0.0):
    dates = pd.date_range('2021-01-04', periods=days, freq='B')
    close = 100.0 + 12.0 * np.sin(np.arange(days) / 9.0 + phase)
    return pl.DataFrame({
        'date': dates,
        'open': close * 1.0005,
        'high': close * 1.01,
        'low': close * 0.99,
        'close': close,
        'volume': np.full(days, 1_000_000.0),
    })


def _optimize(frame, **kwargs):
    engine = BacktestEngine(frame)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    return engine, engine.optimize_strategy(
        {'short_window': [3, 5, 8]}, initial_capital=1_000_000.0,
        max_workers=1, **kwargs)


def test_best_params_ignore_out_of_sample_perturbation():
    """扰动 OOS 段行情不得改变 best_params —— 严格样本内选参的核心验收。"""
    base = _osc_frame()
    engine, baseline = _optimize(base)
    assert baseline['selection_sample'] == 'in_sample'
    assert baseline['leakage_guard']['strict_in_sample_selection'] is True
    assert baseline['leakage_guard']['oos_evaluations'] == 1

    split = baseline['oos_validation']['split_index']
    perturbed = base.with_row_index('idx').with_columns([
        pl.when(pl.col('idx') >= split + 20)
        .then(pl.col(col) * 3.0).otherwise(pl.col(col))
        for col in ('open', 'high', 'low', 'close')
    ]).drop('idx')
    _, after = _optimize(perturbed)

    assert after['best_params'] == baseline['best_params']
    assert after['best_value'] == pytest.approx(baseline['best_value'])
    # 写回策略的必须是 IS 选出的参数，而不是含 OOS 的全样本最优
    assert engine.strategy.params['short_window'] == baseline['best_params']['short_window']


def test_stability_report_uses_selection_sample():
    """稳定性报告必须与选参样本同源，不能再基于含 OOS 的全样本网格。"""
    _, optimized = _optimize(_osc_frame())
    assert optimized['param_stability_report'] == (
        optimized['oos_validation']['param_stability'])
    assert optimized['in_sample_optimization_results']
    assert optimized['leakage_guard']['multiple_testing_correction'] == 'not_implemented'


def test_full_sample_grid_is_reference_only():
    """全样本网格仍保留供兼容对账，但明确标注不参与选参。"""
    _, optimized = _optimize(_osc_frame())
    assert len(optimized['optimization_results']) == 3
    assert optimized['selection_sample'] == 'in_sample'
    assert 'full_sample_best_params' in optimized['leakage_guard']

    _, no_oos = _optimize(_osc_frame(), validation='none')
    assert no_oos['selection_sample'] == 'full_sample_no_oos'
    assert no_oos['leakage_guard']['strict_in_sample_selection'] is False
    assert no_oos['in_sample_optimization_results'] == []


# ---------------------------------------------------------------------------
# P0-3 风控闸门不可静默绕过
# ---------------------------------------------------------------------------

def _bare_engine(data=None):
    data = data or _panel()
    engine = PortfolioBacktestEngine(data)
    engine.set_strategies({
        next(iter(data)): _FixedSignalStrategy({0: 'buy', 4: 'sell'})})
    return engine


def test_walk_forward_disabled_warns_and_is_recorded():
    """单次 OOS 且未开启 walk-forward 时必须显式告警并写入 leakage_guard。"""
    from src.backtest.engine.backtest_engine import WalkForwardDisabledWarning

    with pytest.warns(WalkForwardDisabledWarning, match='walk-forward'):
        _, result = _optimize(_osc_frame())
    guard = result['leakage_guard']
    assert guard['walk_forward_enabled'] is False
    assert guard['walk_forward_disabled_warning'] is True
    assert guard['multiple_testing_correction'] == 'not_implemented'

    # 显式开启滚动验证后不得再发该告警（默认值保持不变，不改变历史输出）
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        _, enabled = _optimize(_osc_frame(), walk_forward_windows=2)
    assert not [w for w in caught
                if issubclass(w.category, WalkForwardDisabledWarning)]
    assert enabled['leakage_guard']['walk_forward_disabled_warning'] is False


def test_bare_engine_auto_assembles_adv_gate():
    """A1 回归：裸用引擎不得「未装配即全放行」，默认自动装 ADV 容量闸门。"""
    engine = _bare_engine()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        result = engine.run_backtest()
    assert not [w for w in caught if issubclass(w.category, RiskDisabledWarning)]
    gate = result['risk_gate']
    assert gate['enabled'] is True
    assert gate['policy'] == 'adv_gate'
    assert gate['assembled_by_default'] is True
    assert gate['bypassed_orders'] == 0
    assert set(gate['rules']) == {'max_order_volume_ratio', 'max_adv_participation'}
    assert result['risk_summary'] is not None


def test_explicit_off_policy_is_recorded_not_silent():
    """显式 on_missing_risk='off' 才允许关闭风控，且放行笔数必须在结果里可查。"""
    engine = _bare_engine()
    engine.risk_gate_policy = 'off'
    result = engine.run_backtest()
    gate = result['risk_gate']
    assert gate['enabled'] is False
    assert gate['policy'] == 'off'
    assert gate['bypassed_orders'] >= 1
    assert gate['rules'] == []
    assert result['risk_summary'] is None


def test_fail_closed_policy_blocks_unreviewed_orders():
    """生产路径可强制 fail-closed：未装配规则时不允许撮合继续。"""
    engine = _bare_engine()
    engine.risk_gate_policy = 'fail_closed'
    with pytest.raises(RuntimeError, match='风控闸门未装配规则'):
        engine.run_backtest()


def test_default_risk_gate_can_be_enabled_explicitly():
    """显式装配默认安全集后，闸门生效且不再有放行订单。"""
    engine = _bare_engine()
    engine.enable_default_risk_gate()
    result = engine.run_backtest()
    gate = result['risk_gate']
    assert gate['enabled'] is True
    assert gate['bypassed_orders'] == 0
    assert 'price_limit_tradable' in gate['rules']
    assert result['risk_summary'] is not None


def test_invalid_risk_gate_policy_is_rejected():
    """策略名拼错必须报错，不能静默退化成全放行。"""
    engine = _bare_engine()
    engine.risk_gate_policy = 'silently_allow_everything'
    with pytest.raises(ValueError, match='on_missing'):
        engine.run_backtest()


# ---------------------------------------------------------------------------
# P0-4 消除静默降级
# ---------------------------------------------------------------------------

def test_failed_grid_combinations_are_counted_and_block_selection(monkeypatch):
    """网格中失败的组合必须入台账；默认零容忍时不允许在残缺网格上选最优。"""
    engine = BacktestEngine(_osc_frame())
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    original = engine._run_single_backtest

    def flaky(params, capital, context, data):
        if params.get('short_window') == 5:
            raise RuntimeError('injected backend failure')
        return original(params, capital, context, data)

    monkeypatch.setattr(engine, '_run_single_backtest', flaky)
    relaxed = engine.optimize_strategy(
        {'short_window': [3, 5, 8]}, max_workers=1, validation='none',
        max_failed_ratio=1.0,
    )
    assert relaxed['failed_combinations']
    assert relaxed['failed_ratio'] > 0
    assert relaxed['optimization_attempts'] >= 3
    assert all(f['error'].startswith('RuntimeError') for f in relaxed['failed_combinations'])

    with pytest.raises(RuntimeError, match='失败组合占比'):
        engine.optimize_strategy(
            {'short_window': [3, 5, 8]}, max_workers=1, validation='none')


def test_dividend_snapshot_failure_marks_result_degraded(monkeypatch):
    """分红快照加载失败时必须在结果里标记 degraded 并下调置信度。"""
    data = _panel(days=6).copy()
    # 复权价 10.0 / 原始价 9.09（因子≈1.1）：刻意让两者不同，
    # 否则会被输入契约判为「复权因子恒为 1.0」而直接拒绝
    frame = data['600000.SH'].with_columns([
        pl.lit(9.09).alias('raw_open'), pl.lit(9.09).alias('raw_close')])
    data = {'600000.SH': frame}

    def boom(*_a, **_kw):
        raise OSError('injected corrupted snapshot')

    monkeypatch.setattr(
        'src.backtest.engine.corporate_actions.load_local_dividend_frames', boom)
    result = run_backtest(
        data=data, strategies={'600000.SH': _FixedSignalStrategy({0: 'buy'})},
        oos_ratio=0, enable_default_risk=False,
    )
    assert result['data_quality']['degraded'] is True
    assert result['fill_confidence'] == 'low'
    assert any(w['stage'] == 'dividend_local_snapshot'
               for w in result['data_quality']['warnings'])


def test_st_dimension_failure_marks_result_degraded(monkeypatch):
    """ST 时间维表失败按未知处理时同样必须标记 degraded。"""
    monkeypatch.setattr(
        'src.data.st_history.intervals_for_backtest', lambda *a, **k: {})

    def boom(*_a, **_kw):
        raise RuntimeError('injected DB outage')

    monkeypatch.setattr(
        'src.data.services.st_status_service.StStatusService.get_status_intervals', boom)
    result = run_backtest(
        data=_panel(days=6),
        strategies={'600000.SH': _FixedSignalStrategy({0: 'buy'})},
        oos_ratio=0, enable_default_risk=False,
        db_manager=SimpleNamespace(config=object()),
    )
    assert result['data_quality']['degraded'] is True
    assert any(w['stage'] == 'st_database_dimension'
               for w in result['data_quality']['warnings'])


class _UnclonableStrategy(_FixedSignalStrategy):
    def __deepcopy__(self, memo):
        raise TypeError('cannot deepcopy thread lock')


def test_unclonable_strategy_aborts_segmented_backtest():
    """IS/OOS 分段不得共享策略对象；深拷贝失败必须中断而非静默复用。"""
    with pytest.raises(RuntimeError, match='共享'):
        run_backtest(
            data=_panel(days=80),
            strategies={'600000.SH': _UnclonableStrategy({0: 'buy'})},
            oos_ratio=0.3, enable_default_risk=False,
        )


def test_ledger_rejection_does_not_abort_whole_backtest(monkeypatch):
    """账本层资金/股数校验失败只作废该单，不能中断整次回测。"""
    def reject(*_a, **_kw):
        raise ValueError('injected insufficient cash')

    monkeypatch.setattr(AccountBook, 'buy', reject)
    result = _run(_panel(), signals={0: 'buy', 4: 'sell'})
    assert result['equity_curve']
    assert any(o['reason'] == 'ledger_rejected' for o in result['expired_orders'])
    assert result['trades'] == []


# ---------------------------------------------------------------------------
# P0-5 文档口径与代码常量绑定
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (_ROOT / path).read_text(encoding='utf-8')


def test_readme_cost_constants_match_code():
    """README 费率必须由 COST_DEFAULTS 自动校验，禁止再次漂移。"""
    readme = _read('README.md')
    assert f"{COST_DEFAULTS['commission_rate'] * 100:.2f}%" in readme
    assert f"{COST_DEFAULTS['stamp_tax_rate'] * 100:.2f}%" in readme
    assert f"{COST_DEFAULTS['transfer_fee_rate'] * 100:.3f}%" in readme
    assert f"{COST_DEFAULTS['slippage'] * 100:.2f}%" in readme
    assert f"最低 {COST_DEFAULTS['min_commission']:.0f} 元" in readme

    # 个股 / ETF 分档 bp 也必须与成本模型常量一致
    for _floor, rate in STOCK_SLIPPAGE_TIERS_YI:
        assert f"**{rate * 10000:.0f}bp**" in readme
    for _floor, rate in ETF_SLIPPAGE_TIERS:
        assert f"**{rate * 10000:.0f}bp**" in readme

    # 审计前 README 的错误口径不得回流
    assert '0.03%' not in readme
    assert '0.001%' not in readme


def test_readme_data_source_switch_matches_code():
    """三源故障转移必须写明默认关闭，并与配置默认值绑定。"""
    from src.utils.config import DataSettings

    readme = _read('README.md')
    default = DataSettings.model_fields['allow_remote_sources'].default
    assert default is False
    assert 'allow_remote_sources' in readme
    assert '默认为 `False`' in readme
    assert 'DATA__ALLOW_REMOTE_SOURCES=true' in readme
    assert 'baostock' in readme.lower()
    assert '未写入 pyproject' not in readme  # 旧的错误描述


def test_research_docs_state_engine_boundary_and_rebaselining():
    """两份研究/历史性能文档必须声明与主引擎的边界与重测基线。"""
    perf = _read('docs/信号层向量化改造性能对比报告.md')
    assert '口径边界与重测基线' in perf
    assert '7.51 µs/bar' in perf
    assert '已过时' in perf
    assert 'execution_core.py' in perf

    protocol = _read('docs/个股端动量回测协议_2026-11-05.md')
    assert '边界声明' in protocol
    assert '独立研究回测链路' in protocol
    assert '不得外推' in protocol
    assert 'PRICE_EPS' in protocol


# ---------------------------------------------------------------------------
# A10 风控快照只读视图 / 增量现金
# ---------------------------------------------------------------------------

def test_risk_snapshot_is_live_readonly_view():
    """快照复用同一对象，positions 实时且只读；成交后无需重建。"""
    book = AccountBook(100_000.0, ['AAA.SZ', 'BBB.SZ'], {'AAA.SZ': '银行'})
    curve = [{'date': '2024-01-01', 'equity': 100_000.0}]
    first = book.snapshot(curve)
    assert dict(first.positions) == {}
    assert first.cash == 100_000.0
    with pytest.raises(TypeError):
        first.positions['BBB.SZ'] = 1  # MappingProxyType 只读

    book.buy('AAA.SZ', 10.0, 100, 5.0)
    second = book.snapshot(curve)
    assert second is first
    assert dict(second.positions) == {'AAA.SZ': 100}
    assert dict(second.last_prices) == {'AAA.SZ': 10.0}
    assert second.industry_map['AAA.SZ'] == '银行'
    assert second.cash == pytest.approx(sum(book.cash.values()))

    book.sell('AAA.SZ', 10.0, 100, 5.0)
    assert dict(book.snapshot(curve).positions) == {}


def test_corporate_action_keeps_incremental_cash_in_sync():
    """除权记账的现金变化必须同步到 O(1) 的 total_cash。"""
    book = AccountBook(100_000.0, ['AAA.SZ'])
    book.buy('AAA.SZ', 10.0, 100, 0.0)
    before = book.total_cash
    book.apply_corporate_action(
        'AAA.SZ', cash_per_share=1.0, share_ratio=0.5,
        rights_ratio=0.1, rights_price=5.0)
    assert book.total_cash == pytest.approx(sum(book.cash.values()))
    assert book.total_cash == pytest.approx(before + 100.0 - 10 * 5.0)
    # 100 股 + 送转 50 股 + 配股 10 股
    assert dict(book.snapshot([]).positions) == {'AAA.SZ': 160}


# ---------------------------------------------------------------------------
# A11 挂单到期堆 / ST 区间二分
# ---------------------------------------------------------------------------

def _two_symbol_panel(full_days=8, sparse_indices=(0, 3)):
    """A 全日期有行情；B 只在 sparse_indices 指定日有行情（制造数据空洞）。"""
    dates = pd.date_range('2023-01-02', periods=full_days, freq='B')

    def frame(ds):
        n = len(ds)
        return pl.DataFrame({
            'date': list(ds),
            'open': np.full(n, 10.0),
            'high': np.full(n, 10.05),
            'low': np.full(n, 9.95),
            'close': np.full(n, 10.0),
            'volume': np.full(n, 1_000_000.0),
        })

    return {
        '600000.SH': frame(dates),
        '000001.SZ': frame([dates[i] for i in sparse_indices]),
    }


def _run_sparse(sparse_indices, ttl=2):
    data = _two_symbol_panel(sparse_indices=sparse_indices)
    engine = PortfolioBacktestEngine(data)
    engine.set_strategies({
        '600000.SH': _FixedSignalStrategy({}),
        '000001.SZ': _FixedSignalStrategy({0: 'buy'}),
    })
    return engine.run_backtest(initial_capital=1_000_000.0,
                               pending_ttl_days=ttl)


def test_pending_heap_executes_before_ttl():
    """数据空洞短于 TTL 时挂单顺延到下一次行情日正常成交。"""
    result = _run_sparse((0, 3), ttl=2)
    b_trades = [t for t in result['trades'] if t['stock_code'] == '000001.SZ']
    assert len(b_trades) == 1
    assert b_trades[0]['signal'] == 'buy'
    assert not [o for o in result['expired_orders'] if o['reason'] == 'pending_ttl']


def test_pending_heap_expires_on_exact_day():
    """数据空洞超过 TTL 时必须在第 ttl+1 个无行情日作废（与旧逐日计数等价）。"""
    result = _run_sparse((0, 5), ttl=2)
    expired = [o for o in result['expired_orders'] if o['reason'] == 'pending_ttl']
    assert len(expired) == 1
    assert expired[0]['ts_code'] == '000001.SZ'
    assert str(expired[0]['date'])[:10] == '2023-01-05'  # day_index 3
    assert [t for t in result['trades'] if t['stock_code'] == '000001.SZ'] == []


def test_pending_heap_ttl_zero_and_long_gap():
    """TTL=0 与长时间无行情两个边界。"""
    result = _run_sparse((0, 7), ttl=0)
    expired = [o for o in result['expired_orders'] if o['reason'] == 'pending_ttl']
    assert len(expired) == 1
    assert str(expired[0]['date'])[:10] == '2023-01-03'  # 次日即过期

    result_long = _run_sparse((0,), ttl=2)
    assert len([o for o in result_long['expired_orders']
                if o['reason'] == 'pending_ttl']) == 1


def test_st_interval_bisect_matches_linear_reference():
    """预排序 + 二分必须与旧线性扫描逐日一致；重叠区间必须回退。"""
    intervals = [
        {'start_date': '2023-01-01', 'end_date': '2023-03-01', 'is_st': True},
        {'start_date': '2023-04-01', 'end_date': None, 'is_st': False},
    ]
    status = {'600000.SH': intervals}
    prepared, starts = EventBacktestRunner._prepare_st_intervals(status)
    assert '600000.SH' in prepared
    for day in pd.date_range('2022-12-01', '2023-06-01', freq='B'):
        d = day.date()
        assert EventBacktestRunner._resolve_st_intervals(
            starts['600000.SH'], prepared['600000.SH'], d
        ) == EventBacktestRunner._resolve_st_for_date(intervals, d)

    overlapping = {'000001.SZ': [
        {'start_date': '2023-01-01', 'end_date': '2023-06-01', 'is_st': True},
        {'start_date': '2023-02-01', 'end_date': '2023-03-01', 'is_st': False},
    ]}
    prepared2, _ = EventBacktestRunner._prepare_st_intervals(overlapping)
    assert '000001.SZ' not in prepared2  # 重叠 → 主循环回退线性语义


def test_shared_fetcher_close_releases_and_is_idempotent(monkeypatch):
    """P1-4：长驻取数资源必须能被显式释放，且重复调用不抛错、不重复释放。

    ``close_shared_data_fetcher`` 此前全仓 0 调用点：DataFetcher 的线程池/
    子进程池与 DatabaseManager 的会话只能靠进程退出兜底，测试进程内多次
    回测会一直占着连接与线程。本用例锁定释放契约本身，退出钩子的接线由
    下一个用例以子进程端到端验证。
    """
    # 包 __init__ 把 run_backtest 导出成同名函数，必须取模块对象而非包的属性
    rb = importlib.import_module('src.core.usecases.run_backtest')

    released: list[str] = []

    class _FakeFetcher:
        def close(self) -> None:
            released.append('fetcher')

    class _FakeDb:
        def disconnect(self) -> None:
            released.append('db')

    monkeypatch.setattr(rb, '_shared_fetcher', _FakeFetcher())
    monkeypatch.setattr(rb, '_shared_db_manager', _FakeDb())

    rb.close_shared_data_fetcher()
    assert released == ['fetcher', 'db']
    assert rb._shared_fetcher is None
    assert rb._shared_db_manager is None

    # 可重复调用：资源已置空，再调一次不得抛错也不得重复释放
    rb.close_shared_data_fetcher()
    assert released == ['fetcher', 'db']


def test_shared_fetcher_is_released_at_interpreter_exit():
    """P1-4：退出钩子必须真的挂在 atexit 上，进程退出时释放长驻资源。

    子进程内只导入用例模块、塞入假单例，不做任何显式 close；若没有
    ``atexit.register(close_shared_data_fetcher)``，两个释放标记都不会出现。
    """
    script = (
        'import importlib\n'
        "rb = importlib.import_module('src.core.usecases.run_backtest')\n"
        'class _F:\n'
        '    def close(self):\n'
        "        print('FETCHER_CLOSED')\n"
        'class _D:\n'
        '    def disconnect(self):\n'
        "        print('DB_CLOSED')\n"
        'rb._shared_fetcher = _F()\n'
        'rb._shared_db_manager = _D()\n'
    )
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True, text=True, cwd=str(root), timeout=180,
    )
    assert proc.returncode == 0, proc.stderr
    assert 'FETCHER_CLOSED' in proc.stdout
    assert 'DB_CLOSED' in proc.stdout
