"""策略试算页面无头渲染冒烟：规则下拉、类型摘要、指标表与三张图。"""

from __future__ import annotations

import os

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

import pytest

pytest.importorskip('PySide6.QtWidgets')

from PySide6.QtWidgets import QApplication  # noqa: E402

from src.ui.strategy_lab.page import _METRIC_ROWS, StrategyLabPage  # noqa: E402


@pytest.fixture(scope='module')
def app():
    return QApplication.instance() or QApplication([])


def test_page_renders_headless(app):
    page = StrategyLabPage()
    labels = [page.rule_combo.itemText(i) for i in range(page.rule_combo.count())]
    assert any('选股' in label for label in labels)
    assert any('强弱比切换' in label for label in labels)
    assert page.rule_text_label.text()

    metrics = {key: 0.1 for key, _, _ in _METRIC_ROWS}
    metrics.update({'final_equity': 1.6e9, 'trades': 100, 'rebalances': 84,
                    'members_median': 50, 'members_min': 40, 'members_max': 60})
    page._fill_metrics(metrics)
    assert page.metrics_table.rowCount() == len(_METRIC_ROWS)
    assert page.metrics_table.columnWidth(0) > 0

    # 分红覆盖不足时指标表补一行提示；覆盖完整时不出现
    hint = '1 只标的分红覆盖不足，收益可能低估'
    page._fill_metrics(metrics, hint)
    assert page.metrics_table.rowCount() == len(_METRIC_ROWS) + 1
    assert page.metrics_table.item(len(_METRIC_ROWS), 0).text() == '分红覆盖'
    assert page.metrics_table.item(len(_METRIC_ROWS), 1).text() == hint
    page._fill_metrics(metrics)
    assert page.metrics_table.rowCount() == len(_METRIC_ROWS)

    # 数据新鲜度告警逐条补行，不阻断结果展示
    warn = '因子面板末日 2026-10-29 落后目标日 2026-11-19 15 个交易日'
    page._fill_metrics(metrics, '', [warn])
    assert page.metrics_table.rowCount() == len(_METRIC_ROWS) + 1
    assert page.metrics_table.item(len(_METRIC_ROWS), 0).text() == '数据新鲜度'
    page._fill_metrics(metrics)

    # 「到最近交易日」勾选后终点上限放宽到今天
    from PySide6.QtCore import QDate
    assert page.latest_check is not None and not page.latest_check.isChecked()
    page.latest_check.setChecked(True)
    assert page.start_date.maximumDate() == QDate.currentDate()
    page.latest_check.setChecked(False)

    nav = [['2024-01-31', 1.0e9], ['2024-02-29', 1.05e9], ['2024-03-29', 1.02e9]]
    bench = [['2024-01-31', 1.0e9], ['2024-02-29', 1.01e9], ['2024-03-29', 0.99e9]]
    page._plot_curves(nav, bench)
    page._plot_drawdown([['2024-01-31', 0.0], ['2024-02-29', -0.02],
                         ['2024-03-29', -0.05]])
    page._plot_markers([{'date': '2024-02-29', 'position': 1.0}])

    page.resize(1200, 800)
    pixmap = page.grab()
    assert not pixmap.isNull()
    assert pixmap.width() >= 1000
    page.deleteLater()


def test_empty_result_shows_reason(app):
    page = StrategyLabPage()
    page._on_result({'status': 'empty', 'reason': '条件筛选后没有成分'})
    assert '空结果' in page.status_label.text()
    page.deleteLater()
