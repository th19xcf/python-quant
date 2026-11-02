"""主窗口离屏冒烟测试，作为 UI 重构的回归基线。"""

from types import SimpleNamespace

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
        assert window.tab_widget.count() == 3
        assert window.stock_table.columnCount() == 13
        # 示例假数据已移除，启动时行情表应为空
        assert window.stock_table.rowCount() == 0
        assert window.index_service is not None
    finally:
        # closeEvent 会弹出模态退出确认框，测试中改用 hide
        window.hide()
        window.deleteLater()
        qapp.processEvents()
