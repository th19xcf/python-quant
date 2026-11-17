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
