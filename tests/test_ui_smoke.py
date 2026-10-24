"""主窗口离屏冒烟测试，作为 UI 重构的回归基线。"""

from types import SimpleNamespace


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
