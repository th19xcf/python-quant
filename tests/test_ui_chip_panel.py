"""筹码栏显隐单测：Ctrl+U 作用域、按钮同步、隐藏后主图铺满、视口保持。"""

from types import SimpleNamespace

from PySide6.QtCore import QEvent, QPointF, Qt
from PySide6.QtGui import QKeyEvent


def _make_window(tmp_path):
    from src.ui.main_window import MainWindow

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    data_manager = SimpleNamespace(db_manager=None)
    return MainWindow(config, data_manager)


def _ctrl_u() -> QKeyEvent:
    return QKeyEvent(
        QEvent.Type.KeyPress, Qt.Key.Key_U, Qt.KeyboardModifier.ControlModifier
    )


def test_ctrl_u_toggles_chip_panel_only_on_tech_tab(qapp, tmp_path):
    """快捷键注册在技术分析页签内；其它页签不触发，按钮/筹码栏状态同步。"""
    window = _make_window(tmp_path)
    try:
        window.show()
        qapp.processEvents()
        window.tab_widget.setCurrentWidget(window.tech_tab)
        qapp.processEvents()

        controller = window.chip_controller
        assert controller is not None
        initial = controller.is_visible()

        assert window._key_filter.eventFilter(window, _ctrl_u()) is True
        qapp.processEvents()
        assert controller.is_visible() is not initial
        assert window.chip_toggle_btn.isChecked() is controller.is_visible()
        assert window.chip_panel.isVisible() is controller.is_visible()
        assert '显示中' in window.chip_toggle_btn.text() or '已隐藏' in window.chip_toggle_btn.text()

        window.tab_widget.setCurrentIndex(0)
        qapp.processEvents()
        assert window._key_filter.eventFilter(window, _ctrl_u()) is False
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_chip_floor_combo_defaults_to_0p2_and_updates_controller(qapp, tmp_path):
    """下拉默认 0.2% 档，切换后控制器档位与下拉显示保持同步。"""
    window = _make_window(tmp_path)
    try:
        window.show()
        qapp.processEvents()
        combo = window.chip_floor_combo
        controller = window.chip_controller

        assert combo.currentData() == 'auto_p10'
        assert combo.currentText() == '自动 p10（0.02~0.2%，默认）'
        assert controller.floor_mode() == 'auto_p10'
        assert controller.turnover_source() == 'turn'
        assert combo.findData('floor_0p05') >= 0
        assert combo.findData('floor_0p2') >= 0

        combo.setCurrentIndex(combo.findData('floor_2'))
        qapp.processEvents()
        assert controller.floor_mode() == 'floor_2'

        controller.set_floor_mode('floor_0p2')
        assert combo.currentData() == 'floor_0p2'
        assert controller.floor_mode() == 'floor_0p2'

        controller.set_floor_mode('raw')
        assert combo.currentData() == 'raw'
        assert controller.floor_mode() == 'raw'
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_hide_chip_panel_expands_main_plot_and_restores_viewport(qapp, tmp_path):
    """隐藏后主图真正铺满；再显示后宽度与视口恢复。"""
    window = _make_window(tmp_path)
    try:
        window.resize(1200, 800)
        window.show()
        window.tab_widget.setCurrentWidget(window.tech_tab)
        qapp.processEvents()

        controller = window.chip_controller
        controller.set_visible(True)
        # 离屏平台不会自动分配顶层几何，手动给定容器尺寸后激活布局
        window.tech_container.resize(1000, 400)
        window.tech_container_layout.activate()
        qapp.processEvents()
        main_width_shown = window.tech_plot_widget.width()
        panel_width_shown = window.chip_panel.width()
        assert panel_width_shown > 0
        container_width = window.tech_container.width()
        assert abs(main_width_shown + panel_width_shown - container_width) <= 2
        assert 0.2 * container_width <= panel_width_shown <= 0.3 * container_width

        view_box = window.tech_plot_widget.getViewBox()
        view_box.setXRange(10, 60, padding=0)
        view_box.setYRange(6.0, 7.5, padding=0)
        before = view_box.viewRange()

        controller.set_visible(False)
        window.tech_container.resize(1000, 400)
        window.tech_container_layout.activate()
        qapp.processEvents()
        assert not window.chip_panel.isVisible()
        assert window.tech_plot_widget.width() == window.tech_container.width()

        controller.set_visible(True)
        window.tech_container.resize(1000, 400)
        window.tech_container_layout.activate()
        qapp.processEvents()
        assert window.chip_panel.isVisible()
        assert window.chip_panel.width() > 0
        assert (
            abs(
                window.tech_plot_widget.width()
                + window.chip_panel.width()
                - window.tech_container.width()
            )
            <= 2
        )
        after = view_box.viewRange()
        assert after[0] == before[0]
        assert after[1] == before[1]
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_price_axis_alignment_between_main_and_chip(qapp, tmp_path):
    """同一价位在左右两区的场景像素 y 一致（价格轴对齐）。"""
    window = _make_window(tmp_path)
    try:
        window.show()
        window.tab_widget.setCurrentWidget(window.tech_tab)
        window.tech_container.resize(1000, 400)
        window.tech_container_layout.activate()
        qapp.processEvents()

        controller = window.chip_controller
        controller.set_visible(True)
        window.tech_container.resize(1000, 400)
        window.tech_container_layout.activate()
        qapp.processEvents()

        view_box = window.tech_plot_widget.getViewBox()
        view_box.setYRange(6.0, 8.0, padding=0)
        window.chip_panel.sync_y_range(view_box.viewRange()[1])
        qapp.processEvents()

        for price in (6.2, 6.8, 7.5, 7.9):
            main_y = view_box.mapViewToScene(QPointF(0.0, price)).y()
            chip_y = window.chip_panel.map_price_to_scene_y(price)
            assert abs(main_y - chip_y) <= 1.0, (price, main_y, chip_y)
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_hidden_state_persists_when_source_changes(qapp, tmp_path):
    """隐藏后切换标的/周期重建数据，显隐状态保持用户上次选择。"""
    window = _make_window(tmp_path)
    try:
        controller = window.chip_controller
        controller.set_visible(False)
        assert controller.is_visible() is False

        controller.set_source(object(), '600519.SH', 'qfq')
        qapp.processEvents()
        assert controller.is_visible() is False
        assert not window.chip_toggle_btn.isChecked()
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def _viewbox_window_x(window, plot_widget) -> tuple[float, float]:
    """把 ViewBox 的实际矩形映射到主窗口坐标系，返回左右像素 x。"""
    rect = plot_widget.getViewBox().sceneBoundingRect()
    top_left = plot_widget.mapTo(window, plot_widget.mapFromScene(rect.topLeft()))
    bottom_right = plot_widget.mapTo(window, plot_widget.mapFromScene(rect.bottomRight()))
    return float(top_left.x()), float(bottom_right.x())


def test_all_plot_viewboxes_align_horizontally(qapp, tmp_path):
    """显示态与隐藏态下，主图及各副图的绘图区左右边界差均 ≤1px。"""
    window = _make_window(tmp_path)
    try:
        window.show()
        window.tab_widget.setCurrentWidget(window.tech_tab)
        window.chart_splitter.resize(1000, 600)
        qapp.processEvents()

        plots = (
            window.tech_plot_widget,
            window.volume_plot_widget,
            window.kdj_plot_widget,
        )

        def assert_aligned(stage):
            rects = [_viewbox_window_x(window, plot) for plot in plots]
            lefts = [r[0] for r in rects]
            rights = [r[1] for r in rects]
            assert max(lefts) - min(lefts) <= 1.0, (stage, rects)
            assert max(rights) - min(rights) <= 1.0, (stage, rects)

        window.chip_controller.set_visible(True)
        window.chart_splitter.resize(1000, 600)
        qapp.processEvents()
        assert window.volume_chip_gutter.width() > 0
        assert_aligned('显示态')

        window.chip_controller.set_visible(False)
        window.chart_splitter.resize(1000, 600)
        qapp.processEvents()
        assert window.volume_chip_gutter.width() == 0
        assert window.kdj_chip_gutter.width() == 0
        assert_aligned('隐藏态')

        window.chip_controller.set_visible(True)
        window.chart_splitter.resize(1000, 600)
        qapp.processEvents()
        assert_aligned('恢复显示态')
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()


def test_full_range_checkbox_syncs_controller(qapp, tmp_path):
    """全区间复选框直接驱动控制器状态。"""
    window = _make_window(tmp_path)
    try:
        controller = window.chip_controller
        assert controller._full_range is False
        window.chip_full_range_checkbox.setChecked(True)
        qapp.processEvents()
        assert controller._full_range is True
        window.chip_full_range_checkbox.setChecked(False)
        qapp.processEvents()
        assert controller._full_range is False
    finally:
        window.hide()
        window.deleteLater()
        qapp.processEvents()
