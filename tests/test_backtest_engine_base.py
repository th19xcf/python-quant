"""回测引擎基类（``BaseBacktestEngine``）通用组件的单元测试。

覆盖的是基类自身提供、此前无人直测的通用能力，而不是某个具体引擎的回测
结果：数据格式归一、组件初始化与重置、策略装配、结果读取与可视化入口，
以及参数优化用的三个纯函数（最优参数选取 / 目标值抽取 / 参数稳定性报告）
和日期标签切片。这些方法都是「读起来没问题但一改就静默错」的那一类——
例如 ``_select_best_params`` 对未知目标名会退回 ``total_return``、
``_build_stability_report`` 对回撤目标用「越小越好」的反向比较，
没有用例盯着时改成 ``max`` 也不会有人发现。
"""

import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.base_engine import BaseBacktestEngine


def _engine() -> BaseBacktestEngine:
    return BaseBacktestEngine()


class TestConvertToPolars:
    """``convert_to_polars`` 必须覆盖四类输入，不能只认 Polars DataFrame"""

    def test_pandas_frame_converted(self):
        pdf = pd.DataFrame({'close': [1.0, 2.0]})
        out = BaseBacktestEngine.convert_to_polars(pdf)
        assert isinstance(out, pl.DataFrame)
        assert out['close'].to_list() == [1.0, 2.0]

    def test_lazy_frame_collected(self):
        lazy = pl.DataFrame({'close': [3.0]}).lazy()
        out = BaseBacktestEngine.convert_to_polars(lazy)
        assert isinstance(out, pl.DataFrame)
        assert out['close'].to_list() == [3.0]

    def test_polars_frame_passes_through(self):
        frame = pl.DataFrame({'close': [4.0]})
        assert BaseBacktestEngine.convert_to_polars(frame) is frame

    def test_fallback_constructs_from_records(self):
        out = BaseBacktestEngine.convert_to_polars([{'close': 5.0}, {'close': 6.0}])
        assert out['close'].to_list() == [5.0, 6.0]


class TestComponentLifecycle:
    """组件初始化 / 重置 / 装配：状态必须真的是新的，不能沿用旧对象"""

    def test_init_common_components_resets_state(self):
        engine = _engine()
        engine.backtest_results = {'stale': True}
        engine.trades = [{'symbol': 'X'}]
        engine.equity_curve = [{'date': '2020-01-01'}]
        old_cost_model = engine.cost_model

        engine.init_common_components()

        assert engine.backtest_results == {}
        assert engine.trades == []
        assert engine.equity_curve == []
        assert engine.cost_model is not old_cost_model
        assert engine.performance_analyzer is not None
        assert engine.visualizer is not None

    def test_set_cost_model_replaces_instance(self):
        engine = _engine()
        sentinel = object()
        engine.set_cost_model(sentinel)
        assert engine.cost_model is sentinel

    def test_empty_mapping_clears_strategy(self):
        engine = _engine()
        engine.strategy = object()
        engine.set_strategies({})
        assert engine.strategy is None

    def test_set_strategy_without_code_uses_default_key(self):
        engine = _engine()
        strategy = object()
        engine.set_strategy(strategy)
        assert engine.strategy is strategy

    def test_set_strategies_takes_first_of_mapping(self):
        engine = _engine()
        first, second = object(), object()
        engine.set_strategies({'A': first, 'B': second})
        assert engine.strategy is first

    def test_get_backtest_results_returns_live_dict(self):
        engine = _engine()
        engine.backtest_results = {'total_return': 0.1}
        results = engine.get_backtest_results()
        assert results is engine.backtest_results


class _StubVisualizer:
    def __init__(self):
        self.calls = []

    def visualize_backtest(self, results):
        self.calls.append(results)
        return 'figure'


class TestVisualizeBacktest:
    """未跑回测时应返回 None 而不是拿空 dict 去绘图"""

    def test_empty_results_returns_none(self):
        engine = _engine()
        engine.visualizer = _StubVisualizer()
        assert engine.visualize_backtest() is None
        assert engine.visualizer.calls == []

    def test_results_forwarded_to_visualizer(self):
        engine = _engine()
        stub = _StubVisualizer()
        engine.visualizer = stub
        engine.backtest_results = {'total_return': 0.2}
        assert engine.visualize_backtest() == 'figure'
        assert stub.calls == [{'total_return': 0.2}]


class TestSelectBestParams:
    """最优参数选取：三种目标 + 未知目标退回收益，空列表返回空"""

    def test_empty_results(self):
        assert _engine()._select_best_params([], 'total_return') == ({}, 0)

    def test_total_return_picks_maximum(self):
        results = [
            {'params': {'n': 5}, 'total_return': 0.1},
            {'params': {'n': 10}, 'total_return': 0.3},
            {'params': {'n': 20}, 'total_return': 0.2},
        ]
        best_params, best_value = _engine()._select_best_params(results, 'total_return')
        assert best_params == {'n': 10}
        assert best_value == 0.3

    def test_sharpe_ratio_picks_maximum(self):
        results = [
            {'params': {'n': 5}, 'total_return': 0.3, 'performance': {'sharpe_ratio': 0.5}},
            {'params': {'n': 10}, 'total_return': 0.1, 'performance': {'sharpe_ratio': 1.8}},
        ]
        best_params, best_value = _engine()._select_best_params(results, 'sharpe_ratio')
        assert best_params == {'n': 10}
        assert best_value == 1.8

    def test_max_drawdown_picks_minimum(self):
        results = [
            {'params': {'n': 5}, 'performance': {'max_drawdown': 0.12}},
            {'params': {'n': 10}, 'performance': {'max_drawdown': 0.04}},
        ]
        best_params, best_value = _engine()._select_best_params(results, 'max_drawdown')
        assert best_params == {'n': 10}
        assert best_value == 0.04

    def test_unknown_objective_falls_back_to_total_return(self):
        results = [
            {'params': {'n': 5}, 'total_return': 0.1},
            {'params': {'n': 10}, 'total_return': 0.4},
        ]
        best_params, best_value = _engine()._select_best_params(results, 'calmar')
        assert best_params == {'n': 10}
        assert best_value == 0.4

    def test_missing_fields_do_not_break_selection(self):
        """结果缺 params / 缺指标时必须仍能选出条目，不能抛 KeyError"""
        results = [
            {'params': {'n': 5}},
            {'total_return': 0.2},
        ]
        best_params, best_value = _engine()._select_best_params(results, 'total_return')
        assert best_params == {}
        assert best_value == 0.2


class TestObjectiveValue:
    """目标值抽取口径必须与 ``_select_best_params`` 一致"""

    def test_total_return_default(self):
        assert BaseBacktestEngine._objective_value({'total_return': 0.15}, 'total_return') == 0.15

    def test_sharpe_ratio(self):
        result = {'performance': {'sharpe_ratio': 2.5}}
        assert BaseBacktestEngine._objective_value(result, 'sharpe_ratio') == 2.5

    def test_max_drawdown(self):
        result = {'performance': {'max_drawdown': -0.2}}
        assert BaseBacktestEngine._objective_value(result, 'max_drawdown') == -0.2

    def test_none_values_coerced_to_zero(self):
        """指标为 None 时返回 0.0，不得让 None 参与后面的大小比较"""
        assert BaseBacktestEngine._objective_value({}, 'sharpe_ratio') == 0.0
        assert BaseBacktestEngine._objective_value({'total_return': None}, 'total_return') == 0.0


class TestBuildStabilityReport:
    """参数稳定性报告：收益类「越大越好」、回撤类「越小越好」"""

    def test_empty_results(self):
        report = _engine()._build_stability_report([], 'total_return')
        assert report == {'param_stability': [], 'stable_ratio': 0.0, 'best_value': 0.0}

    def test_return_objective_uses_relative_floor(self):
        results = [
            {'params': {'n': 5}, 'total_return': 0.10},
            {'params': {'n': 10}, 'total_return': 0.20},
        ]
        report = _engine()._build_stability_report(results, 'total_return')
        assert report['best_value'] == pytest.approx(0.20)
        entry = report['param_stability'][0]
        assert entry['param'] == 'n'
        # 0.10 < 0.20 * 0.9，判定为不稳定
        assert entry['stable'] is False
        assert report['stable_ratio'] == 0.0

    def test_drawdown_objective_uses_relative_ceiling(self):
        results = [
            {'params': {'n': 5}, 'performance': {'max_drawdown': 0.05}},
            {'params': {'n': 10}, 'performance': {'max_drawdown': 0.06}},
        ]
        report = _engine()._build_stability_report(results, 'max_drawdown')
        assert report['best_value'] == pytest.approx(0.05)
        entry = report['param_stability'][0]
        # 0.06 > 0.05 * 1.1 不成立？0.055 为界：0.06 超界 → 不稳定
        assert entry['stable'] is False

    def test_drawdown_non_positive_best_uses_absolute_compare(self):
        """最优回撤非正数时不能乘 1.1（负数放大会更宽松），必须直接比较"""
        results = [
            {'params': {'n': 5}, 'performance': {'max_drawdown': -0.10}},
            {'params': {'n': 10}, 'performance': {'max_drawdown': -0.05}},
        ]
        report = _engine()._build_stability_report(results, 'max_drawdown')
        assert report['best_value'] == pytest.approx(-0.10)
        # n=-0.05 比最优 -0.10 更大（回撤更小并不成立），判不稳定
        assert report['param_stability'][0]['stable'] is False

    def test_result_missing_param_is_skipped(self):
        """某次结果没有该参数时跳过，不得把 KeyError 抛给调用方"""
        results = [
            {'params': {'n': 5}, 'total_return': 0.20},
            {'params': {}, 'total_return': 0.05},
        ]
        report = _engine()._build_stability_report(results, 'total_return')
        entry = report['param_stability'][0]
        assert [point['value'] for point in entry['values']] == [5]
        assert report['best_value'] == pytest.approx(0.20)


class TestSliceLabel:
    """日期区间标签：空表或缺日期列时返回空串，不得抛错"""

    def test_empty_frame_returns_empty_label(self):
        assert _engine()._slice_label(pl.DataFrame({'date': []})) == ''

    def test_missing_column_returns_empty_label(self):
        frame = pl.DataFrame({'close': [1.0, 2.0]})
        assert _engine()._slice_label(frame, column='date') == ''

    def test_normal_frame_returns_range(self):
        frame = pl.DataFrame({'date': ['2020-01-02', '2020-12-31']})
        assert _engine()._slice_label(frame, column='date') == '2020-01-02 ~ 2020-12-31'
