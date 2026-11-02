"""第一批 P0（可信度）回归测试。

覆盖四类修复：
1. 报告导出使用真实结果、空结果拒绝生成；
2. 选股日期按最新交易日/固定 today 回溯；
3. 绩效波动率与夏普按 frequency 年化；
4. main 启动路径捕获 SQLAlchemyError 并降级。
"""

import importlib
from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
from sqlalchemy.exc import OperationalError

import src.main as main_module
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.core.usecases.screen_stocks import _months_before, _period_range, screen_stocks
from src.quant.reports.report_generator import ReportGenerator

# 包 __init__ 以同名函数导出 screen_stocks，需按模块路径取真实模块以便打桩
screen_stocks_module = importlib.import_module('src.core.usecases.screen_stocks')


# ----------------------------------------------------------------------
# P0-1 报告导出：真实结果非空；空结果明确拒绝
# ----------------------------------------------------------------------

@pytest.fixture
def report_env(monkeypatch, tmp_path):
    """把报告目录切到临时目录，并拦截浏览器预览。"""
    monkeypatch.chdir(tmp_path)
    opened = []
    monkeypatch.setattr(
        'src.quant.reports.report_generator.webbrowser.open',
        lambda url: opened.append(url),
    )
    return tmp_path, opened


def _read_report(path):
    with open(path, encoding='utf-8') as file:
        return file.read()


def test_backtest_report_contains_real_table_and_numbers(report_env):
    result = {
        'strategy_type': '趋势跟踪',
        'stock_code': '600519.SH',
        'start_date': '2024-01-02',
        'end_date': '2024-12-31',
        'initial_capital': 100000.0,
        'final_equity': 118500.0,
        'total_return': 18.5,
        'annual_return': 18.42,
        'sharpe_ratio': 1.25,
        'max_drawdown': 7.8,
        'trade_count': 12,
        'win_rate': 66.67,
        'equity_curve': [
            {'date': date(2024, 1, 2), 'equity': 100000.0},
            {'date': date(2024, 12, 31), 'equity': 118500.0},
        ],
    }
    tmp_path, opened = report_env

    path = ReportGenerator().generate_backtest_report(result)

    html = _read_report(path)
    assert path.startswith(str(tmp_path))
    assert '<table' in html
    assert '100,000.00' in html
    assert '18.50%' in html
    assert opened


def test_recommendation_report_contains_stock_rows(report_env):
    recommendations = [
        {
            'code': '600519.SH',
            'name': '贵州茅台',
            'industry': '食品饮料',
            'score': 88.5,
            'expected_return': 12.3,
            'risk': '低',
        },
    ]

    path = ReportGenerator().generate_recommendation_report(recommendations)

    html = _read_report(path)
    assert '<table' in html
    assert '贵州茅台' in html
    assert '88.50' in html
    assert '12.30%' in html


def test_factor_report_renders_validity_and_correlation_tables(report_env):
    validity = {
        'analysis_type': '因子有效性检验',
        'stock_code': '600519.SH',
        'start_date': '2024-01-02',
        'end_date': '2024-12-31',
        'factors': ['MACD', 'RSI'],
        'factor_results': {
            'MACD': {
                'ic': 0.0834,
                'long_return': 0.021,
                'short_return': -0.011,
                'long_short_return': 0.032,
            },
        },
    }
    correlation = {
        'analysis_type': '因子相关性分析',
        'correlation_matrix': [[1.0, 0.35], [0.35, 1.0]],
        'factor_columns': ['MACD', 'RSI'],
    }

    generator = ReportGenerator()
    validity_path = generator.generate_factor_analysis_report(validity)
    validity_html = _read_report(validity_path)
    # 两份同前缀报告在秒级时间戳下会重名，先读第一份再生成第二份
    correlation_path = generator.generate_factor_analysis_report(correlation)
    correlation_html = _read_report(correlation_path)

    assert '<table' in validity_html
    assert '0.0834' in validity_html
    assert '<table' in correlation_html
    assert '0.3500' in correlation_html


def test_reports_reject_empty_results(report_env):
    generator = ReportGenerator()

    with pytest.raises(ValueError):
        generator.generate_backtest_report({})
    with pytest.raises(ValueError):
        generator.generate_backtest_report(
            {'initial_capital': 100000.0, 'final_equity': 1.0, 'total_return': 1.0}
        )
    with pytest.raises(ValueError):
        generator.generate_recommendation_report([])
    with pytest.raises(ValueError):
        generator.generate_factor_analysis_report({})
    with pytest.raises(TypeError):
        generator.generate_backtest_report('not-a-dict')


# ----------------------------------------------------------------------
# P0-2 选股日期：按最新交易日回溯，固定 today 可复现
# ----------------------------------------------------------------------

def _make_panel(stocks: int = 4, days: int = 60) -> pl.DataFrame:
    """构造 60 天的合成行情面板，满足推荐器至少 20 行的要求。"""
    dates = [date(2024, 1, 1) + timedelta(days=offset) for offset in range(days)]
    rng = np.random.default_rng(7)
    frames = []
    for index in range(stocks):
        close = np.maximum(
            np.cumsum(rng.normal(0.05, 0.2, days)) + 10 + index,
            1.0,
        )
        frames.append(pl.DataFrame({
            'date': dates,
            'stock_code': [f'60000{index}.SH'] * days,
            'stock_name': [f'股票{index}'] * days,
            'industry': ['科技'] * days,
            'open': close,
            'high': close * 1.02,
            'low': close * 0.98,
            'close': close,
            'volume': [1_000_000.0] * days,
            'amount': close * 1_000_000.0,
        }))
    return pl.concat(frames)


def test_period_range_anchors_on_latest_trade_date():
    assert _period_range('12个月', date(2024, 12, 31)) == (
        date(2023, 12, 31), date(2024, 12, 31),
    )
    # 月末回溯夹取：3-31 往前一个月是 2-29（闰年）
    assert _months_before(date(2024, 3, 31), 1) == date(2024, 2, 29)
    # 未知周期回退 12 个月
    assert _period_range('未知周期', date(2024, 12, 31))[0] == date(2023, 12, 31)


def test_latest_trade_date_reads_database_and_falls_back():
    class _FakeSession:
        def query(self, *_args):
            return self

        def scalar(self):
            return date(2024, 12, 27)

    class _FakeDB:
        def get_session(self):
            return _FakeSession()

    assert screen_stocks_module._latest_trade_date(_FakeDB()) == date(2024, 12, 27)

    class _BrokenDB:
        def get_session(self):
            raise OperationalError('SELECT 1', {}, Exception('db down'))

    assert screen_stocks_module._latest_trade_date(_BrokenDB()) == date.today()


def test_screen_stocks_returns_non_empty_with_fixed_today(monkeypatch):
    panel = _make_panel()
    captured = {}

    def _fake_fetch(top_n, period, as_of=None):
        captured.update({'top_n': top_n, 'period': period, 'as_of': as_of})
        return panel

    monkeypatch.setattr(screen_stocks_module, '_fetch_panel', _fake_fetch)

    results = screen_stocks(top_n=3, period='12个月', today='2024-12-31')

    assert captured['as_of'] == date(2024, 12, 31)
    assert captured['top_n'] == 3
    assert results
    assert len(results) <= 3
    assert all(item['stock_code'] for item in results)


def test_screen_stocks_accepts_explicit_panel():
    results = screen_stocks(stocks_data=_make_panel(), top_n=2)

    assert len(results) == 2
    assert results[0]['risk_adjusted_score'] >= results[1]['risk_adjusted_score']


# ----------------------------------------------------------------------
# P0-4 绩效年化：波动率/夏普按 frequency 动态取年化因子
# ----------------------------------------------------------------------

def _equity_curve(days: int = 30):
    dates = [date(2024, 1, 1) + timedelta(days=offset) for offset in range(days)]
    equities = [100.0]
    for index in range(1, days):
        change = 0.01 if index % 2 else -0.005
        equities.append(equities[-1] * (1 + change))
    return [
        {'date': day, 'equity': equity}
        for day, equity in zip(dates, equities, strict=True)
    ]


def test_volatility_uses_frequency_specific_annualization():
    analyzer = PerformanceAnalyzer()
    curve = _equity_curve()

    daily = analyzer.analyze(curve, [], frequency='daily')
    weekly = analyzer.analyze(curve, [], frequency='weekly')
    monthly = analyzer.analyze(curve, [], frequency='monthly')

    assert daily['annualization_factor'] == 252
    assert weekly['annualization_factor'] == 52
    assert monthly['annualization_factor'] == 12
    assert daily['frequency_fallback'] is False

    assert daily['volatility'] / weekly['volatility'] == pytest.approx(
        np.sqrt(252 / 52)
    )
    assert daily['volatility'] / monthly['volatility'] == pytest.approx(
        np.sqrt(252 / 12)
    )


def test_sharpe_scales_with_frequency():
    analyzer = PerformanceAnalyzer()
    curve = _equity_curve()

    daily = analyzer.analyze(curve, [], frequency='daily')
    monthly = analyzer.analyze(curve, [], frequency='monthly')

    assert daily['sharpe_ratio'] / monthly['sharpe_ratio'] == pytest.approx(
        np.sqrt(252 / 12)
    )


@pytest.mark.parametrize(('frequency', 'expected'), [
    ('日线', 'daily'),
    ('周', 'weekly'),
    ('月线', 'monthly'),
])
def test_frequency_aliases_resolve(frequency, expected):
    resolved, factor, fallback = PerformanceAnalyzer._resolve_frequency(frequency)
    assert resolved == expected
    assert fallback is False


def test_unknown_frequency_falls_back_to_daily_and_is_marked():
    result = PerformanceAnalyzer().analyze(_equity_curve(), [], frequency='hourly')

    assert result['frequency'] == 'daily'
    assert result['annualization_factor'] == 252
    assert result['frequency_fallback'] is True


# ----------------------------------------------------------------------
# P0-6 main 启动异常：SQLAlchemyError 必须被捕获并降级
# ----------------------------------------------------------------------

def _operational_error() -> OperationalError:
    return OperationalError('SELECT 1', {}, Exception('db down'))


def test_initialize_data_manager_degrades_on_sqlalchemy_error(monkeypatch):
    created = []

    class _BrokenDatabaseManager:
        def __init__(self, config):
            self.config = config

        def connect(self):
            raise _operational_error()

    class _FakeDataManager:
        def __init__(self, config, db_manager, plugin_manager):
            created.append((config, db_manager, plugin_manager))

    monkeypatch.setattr(main_module, 'DatabaseManager', _BrokenDatabaseManager)
    monkeypatch.setattr(main_module, 'DataManager', _FakeDataManager)

    config = SimpleNamespace()
    plugin_manager = SimpleNamespace()
    data_manager = main_module._initialize_data_manager(config, plugin_manager)

    assert isinstance(data_manager, _FakeDataManager)
    assert created == [(config, None, plugin_manager)]


def test_update_stock_arg_swallows_sqlalchemy_error(monkeypatch):
    class _BrokenDatabaseManager:
        def __init__(self, config):
            self.config = config

        def connect(self):
            raise _operational_error()

    monkeypatch.setattr(main_module, 'DatabaseManager', _BrokenDatabaseManager)

    assert main_module._handle_update_stock_arg(SimpleNamespace()) is True


def test_main_exits_when_sqlalchemy_error_reaches_top_level(monkeypatch):
    cleanup = []
    monkeypatch.setattr(main_module.sys, 'argv', ['prog'])
    monkeypatch.setattr(main_module, 'get_config', lambda: SimpleNamespace())

    def _broken_system(config):
        raise _operational_error()

    monkeypatch.setattr(main_module, '_initialize_system', _broken_system)
    monkeypatch.setattr(
        main_module,
        '_cleanup_system',
        lambda plugin_manager, db_manager: cleanup.append((plugin_manager, db_manager)),
    )

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 1
    assert cleanup == [(None, None)]


# ----------------------------------------------------------------------
# P0-3 选股不再阻塞 UI 线程：参数组装后走 global_task_manager
# ----------------------------------------------------------------------

def test_stock_recommendation_dialog_dispatches_to_task_manager(monkeypatch, qapp):
    from src.ui.dialogs.stock_recommendation_dialog import StockRecommendationDialog

    calls = {}

    def _fake_create_task(name, func, args=(), **kwargs):
        calls.update({'name': name, 'func': func, 'args': args, 'kwargs': kwargs})
        return 'task-1'

    monkeypatch.setattr(
        'src.ui.dialogs.stock_recommendation_dialog.global_task_manager.create_task',
        _fake_create_task,
    )

    dialog = StockRecommendationDialog()
    try:
        dialog.generate_recommendation()

        assert calls['func'].__name__ == '_run_screen_task'
        assert calls['kwargs'].get('timeout_seconds') is None
        assert dialog.run_button.isEnabled() is False
        # 对话框未 show，isVisible 恒为 False；isHidden 反映显式显示状态
        assert dialog.progress_bar.isHidden() is False
    finally:
        dialog._disconnect_task_signals()
        dialog.close()


# ----------------------------------------------------------------------
# P0-5 因子分析用例入口 + 对话框导出真实结果
# ----------------------------------------------------------------------

def test_analyze_factors_delegates_and_validates(monkeypatch):
    module = importlib.import_module('src.core.usecases.analyze_factors')
    captured = {}

    class _FakeAnalyzer:
        def run_analysis(self, **kwargs):
            captured.update(kwargs)
            return {
                'stock_code': kwargs['stock_code'],
                'factor_results': {'MACD': {'ic': 0.1}},
            }

    monkeypatch.setattr(module, 'FactorAnalyzer', _FakeAnalyzer)

    result = module.analyze_factors(
        stock_code='600519.SH',
        start_date='2024-01-01',
        end_date='2024-12-31',
        factors=['MACD'],
        analysis_type='因子有效性检验',
    )

    assert result['stock_code'] == '600519.SH'
    assert captured['factors'] == ['MACD']
    with pytest.raises(ValueError):
        module.analyze_factors(
            '', '2024-01-01', '2024-12-31', ['MACD'], '因子有效性检验',
        )
    with pytest.raises(ValueError):
        module.analyze_factors(
            '600519.SH', '2024-01-01', '2024-12-31', [], '因子有效性检验',
        )


def test_factor_dialog_export_passes_real_result(monkeypatch, qapp):
    report_module = importlib.import_module('src.quant.reports.report_generator')
    from src.ui.dialogs.factor_analysis_dialog import FactorAnalysisDialog

    captured = []
    monkeypatch.setattr(
        report_module.ReportGenerator,
        'generate_factor_analysis_report',
        lambda self, result: captured.append(result) or 'report.html',
    )

    dialog = FactorAnalysisDialog()
    try:
        dialog.export_report()  # 空结果分支：只提示，不生成报告
        assert captured == []

        dialog._analysis_result = {'factor_results': {'MACD': {'ic': 0.1}}}
        dialog.export_report()
        assert captured == [dialog._analysis_result]
    finally:
        dialog.close()


def test_stock_dialog_export_passes_real_recommendations(monkeypatch, qapp):
    report_module = importlib.import_module('src.quant.reports.report_generator')
    from src.ui.dialogs.stock_recommendation_dialog import StockRecommendationDialog

    captured = []
    monkeypatch.setattr(
        report_module.ReportGenerator,
        'generate_recommendation_report',
        lambda self, result: captured.append(result) or 'report.html',
    )

    dialog = StockRecommendationDialog()
    try:
        dialog.export_report()  # 空结果分支
        assert captured == []

        dialog._recommendations = [{
            'code': '600519.SH',
            'name': '贵州茅台',
            'score': 88.5,
            'expected_return': 12.3,
            'risk': '低',
        }]
        dialog.export_report()
        assert captured == [dialog._recommendations]
    finally:
        dialog.close()


# ----------------------------------------------------------------------
# P0-2 自动取数路径：按最新交易日回溯后取后复权面板
# ----------------------------------------------------------------------

def test_fetch_panel_requests_date_range_from_trade_date(monkeypatch):
    from datetime import date as date_type

    from src.data import data_manager as data_manager_module
    from src.database import db_manager as db_manager_module
    from src.utils import config as config_module

    days = 60
    dates = [date(2023, 12, 1) + timedelta(days=offset) for offset in range(days)]
    bars = pl.DataFrame({
        'date': dates,
        'open': [10.0] * days,
        'high': [10.2] * days,
        'low': [9.8] * days,
        'close': [10.0 + (index % 5) * 0.1 for index in range(days)],
        'volume': [1_000_000.0] * days,
        'amount': [10_000_000.0] * days,
    })
    requested = {}

    class _FakeDB:
        def __init__(self, config):
            self.config = config
            self.disconnected = False

        def get_session(self):
            return self

        def query(self, *_args):
            return self

        def scalar(self):
            return date_type(2024, 12, 31)

        def disconnect(self):
            self.disconnected = True

    class _FakeDataManager:
        def __init__(self, config, db_manager):
            self.db_manager = db_manager

        def get_stock_basic(self):
            return pl.DataFrame({
                'ts_code': ['600000.SH'],
                'name': ['浦发银行'],
                'industry': ['银行'],
            })

        def get_stock_data(self, code, start_date, end_date, adjustment_type='hfq'):
            requested.update({
                'code': code,
                'start_date': start_date,
                'end_date': end_date,
                'adjustment_type': adjustment_type,
            })
            return bars

    monkeypatch.setattr(db_manager_module, 'DatabaseManager', _FakeDB)
    monkeypatch.setattr(data_manager_module, 'DataManager', _FakeDataManager)
    monkeypatch.setattr(config_module, 'Config', lambda: SimpleNamespace())

    panel = screen_stocks_module._fetch_panel(2, '12个月', as_of=date(2024, 12, 31))

    assert panel is not None
    assert panel.height == days
    assert panel['stock_code'][0] == '600000.SH'
    assert panel['stock_name'][0] == '浦发银行'
    assert requested['start_date'] == '2023-12-31'
    assert requested['end_date'] == '2024-12-31'
    assert requested['adjustment_type'] == 'hfq'
