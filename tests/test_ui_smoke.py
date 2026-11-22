"""主窗口离屏冒烟测试，作为 UI 重构的回归基线。"""

import pytest

# GUI 属于可选组 gui；未安装 PySide6 时整文件跳过
pytest.importorskip("PySide6.QtWidgets")

from types import SimpleNamespace

from PySide6.QtGui import QFont, QFontMetrics

from src.ui.window_event_manager import WindowEventManager


def _find_menu_action(window, name):
    """按菜单项名称查找 QAction（名称前可能带 Unicode 图标前缀）。"""
    for menu_action in window.menuBar().actions():
        menu = menu_action.menu()
        if menu is None:
            continue
        for action in menu.actions():
            if name in action.text():
                return action
    raise AssertionError(f'菜单项不存在: {name}')


# MainWindow 的 mixin 顺序即方法解析顺序：同名方法由更靠前者胜出，调换顺序会
# 静默改变 super() 链上的实际执行体。这里把顺序钉死成棘轮，任何重排都必须
# 同步修改本清单并在提交说明里写清理由。
EXPECTED_MIXIN_ORDER = (
    'MainWindowNavigationMixin',
    'MainWindowChartInteractionMixin',
    'MainWindowCandlestickMixin',
    'MainWindowIndicatorWindowMixin',
    'MainWindowDividendMixin',
    'MainWindowAdviceMixin',
    'MainWindowMarketTrendMixin',
    'MainWindowUiMixin',
    'MainWindowEventMixin',
    'MainWindowDrawingMixin',
)


def test_main_window_bases_order_is_pinned():
    """V-09：10 个 mixin 的相对顺序不得静默变化"""
    from src.ui.main_window import MainWindow

    bases = [base.__name__ for base in MainWindow.__bases__]
    assert bases == [*EXPECTED_MIXIN_ORDER, 'QMainWindow']


def test_qmainwindow_must_be_last_base():
    """V-09：QMainWindow 必须在所有 mixin 之后，否则 mixin 钩子被 Qt 基类遮蔽"""
    from src.ui.main_window import MainWindow

    mro = [cls.__name__ for cls in MainWindow.__mro__]
    assert len(mro) == len(set(mro)), 'MRO 中出现重复类，继承结构已损坏'
    qt_index = mro.index('QMainWindow')
    for mixin_name in EXPECTED_MIXIN_ORDER:
        assert mro.index(mixin_name) < qt_index, f'{mixin_name} 被排到 QMainWindow 之后'


def test_unimplemented_menu_items_are_disabled_with_tooltip(qapp, tmp_path):
    """未接实现的菜单项必须置灰并标注「未实现」，不得看起来可点。"""
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        for name in ('财经新闻', '条件单', '盘后数据下载'):
            action = _find_menu_action(window, name)
            assert not action.isEnabled(), name
            assert '未实现' in action.toolTip(), name
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_implemented_menu_items_stay_enabled(qapp, tmp_path):
    """已接实现的动作保持可用，避免误伤。"""
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        for name in ('沪深京指数', '策略回测', '退出'):
            assert _find_menu_action(window, name).isEnabled(), name
        # 工具栏中未实现的按钮同样置灰
        for button in (window.selected_btn, window.market_btn):
            assert not button.isEnabled()
            assert '未实现' in button.toolTip()
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_data_updated_does_not_call_missing_refresh_method():
    """数据更新事件只显式告警，不再调用不存在的 window.refresh_kline_chart。"""
    window = SimpleNamespace(current_stock_code='600519.SH')

    WindowEventManager(window).handle_data_updated(
        sender=None, data_type='stock_daily', ts_code='600519.SH'
    )


def test_main_window_constructs_with_empty_market_table(qapp, tmp_path):
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        assert window.advice_page is not None
        assert window.market_trend_page is not None
        assert window.industry_heat_page is not None
        assert window.style_rotation_page is not None
        assert window.portfolio_risk_page is not None
        assert window.action_list_page is not None
        assert window.attribution_page is not None
        assert window.etf_timing_page is not None
        texts = [
            window.tab_widget.tabText(index)
            for index in range(window.tab_widget.count())
        ]
        # 不写死标签总数：只锚定分析页尾部的名称与顺序，新增页不会打破用例
        assert texts[-7:] == ['市场趋势', '风格轮动', '行业热度', '仓位与风控', '收益归因',
                              '模型操作清单', 'ETF仓位择时']
        assert window.stock_table.columnCount() == 13
        # 示例假数据已移除，启动时行情表应为空
        assert window.stock_table.rowCount() == 0
        assert window.index_service is not None
    finally:
        # closeEvent 会弹出模态退出确认框，测试中改用 hide
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_stock_name_label_precedes_period_buttons(qapp, tmp_path):
    """股票名称标签位于工具栏最左侧，且在日线/周线/月线按钮之前。"""
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        layout = window.stock_name_label.parentWidget().layout()
        name_index = layout.indexOf(window.stock_name_label)
        first_period_index = layout.indexOf(window.period_buttons['日线'])
        assert name_index == 0
        assert name_index < first_period_index
        # 长名称不挤压按钮：标签宽度被限制
        assert window.stock_name_label.maximumWidth() == window._STOCK_NAME_MAX_WIDTH
        # 醒目样式：14px 加粗亮金色，与工具栏 12px 按钮文字形成层级
        style = window.stock_name_label.styleSheet()
        assert 'font-size: 14px' in style
        assert 'font-weight: bold' in style
        assert '#FFD04B' in style
        assert window._stock_name_font().pixelSize() == 14
        assert window._stock_name_font().weight() == QFont.Bold
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_stock_name_label_updates_and_elides(qapp, tmp_path):
    """切换股票时标签文本随之变化；超长名称以省略号截断且不超最大宽度。"""
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        window.update_stock_name_label('平安银行', '000001.SZ')
        assert window.stock_name_label.text() == '平安银行(000001.SZ)'

        window.update_stock_name_label('招商银行', '600036.SH')
        assert window.stock_name_label.text() == '招商银行(600036.SH)'

        long_name = '中华人民共和国超长股票名称测试股份有限公司'
        window.update_stock_name_label(long_name, '600036.SH')
        text = window.stock_name_label.text()
        assert long_name not in text
        assert text.endswith('…') or text.endswith('...')
        font = QFont('Microsoft YaHei')
        font.setPixelSize(14)
        font.setWeight(QFont.Bold)
        assert QFontMetrics(font).horizontalAdvance(text) <= (
            window._STOCK_NAME_MAX_WIDTH - window._STOCK_NAME_PADDING
        )
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_create_title_section_moves_name_to_toolbar(qapp, tmp_path):
    """标题行只显示周期，名称同步到工具栏；切换股票后两处都更新。"""
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        window._create_title_section('平安银行', '000001.SZ')
        assert window.stock_name_label.text() == '平安银行(000001.SZ)'
        assert window.chart_title_label.text() == '日线'

        window._create_title_section('招商银行', '600036.SH')
        assert window.stock_name_label.text() == '招商银行(600036.SH)'
        assert window.chart_title_label.text() == '日线'
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_vol_tdx_button_removed_from_indicator_bar(qapp, tmp_path):
    """vol_tdx 已随指标注册表注销，指标栏不得再出现该按钮。"""
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        assert 'VOL-TDX' not in window.indicator_buttons
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_analysis_tabs_tables_show_grid_and_columns(qapp, tmp_path):
    """三页分析表格开启网格线、列数固定；市场趋势页渲染后行列可读。"""
    from datetime import date as _date

    from PySide6.QtWidgets import QTableWidget

    from src.quant.analysis.market_trend_state import (
        IndicatorResult,
        LayerResult,
        TrendState,
    )
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)

    window = MainWindow(config, data_manager)
    try:
        pages = (
            window.market_trend_page,
            window.style_rotation_page,
            window.industry_heat_page,
            window.portfolio_risk_page,
            window.attribution_page,
        )
        for page in pages:
            tables = page.findChildren(QTableWidget)
            assert tables, page.objectName()
            for table in tables:
                assert table.showGrid()
                assert table.columnCount() > 0

        indicator = IndicatorResult(
            key='ma20', name='20 日均线', layer='trend', raw=0.01, raw_kind='pct',
            mode='quantile', percentile=62.0, score=0.24, asof=_date(2026, 11, 13),
            formula='close / ma20 - 1', judgment='分位越高越强', weight=0.5,
        )
        layer = LayerResult(
            key='trend', name='趋势', weight=1.0, score=0.24, contribution=0.24,
            indicators=(indicator,), description='趋势层',
        )
        window.market_trend_page.render_state(TrendState(
            asof=_date(2026, 11, 13), label='弱上行', total_score=0.24,
            layers=(layer,), notes=('测试',),
        ))
        assert window.market_trend_page.layer_table.rowCount() == 1
        assert window.market_trend_page.layer_table.columnCount() == 6
        assert (window.market_trend_page.layer_table.horizontalHeaderItem(0).text()
                == '序号')
        assert window.market_trend_page.indicator_table.rowCount() == 1
        assert window.market_trend_page.indicator_table.columnCount() == 7
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()
