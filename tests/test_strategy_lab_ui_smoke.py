"""策略试算页面无头渲染冒烟：规则载入、指标表、净值曲线与整体渲染。"""

from __future__ import annotations

import os

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

import pytest

pytest.importorskip('PySide6.QtWidgets')

from PySide6.QtWidgets import QApplication  # noqa: E402

from src.quant.strategy_lab.rules import validate_rule  # noqa: E402
from src.ui.strategy_lab.page import _METRIC_ROWS, StrategyLabPage  # noqa: E402


@pytest.fixture(scope='module')
def app():
    return QApplication.instance() or QApplication([])


def _rule():
    return validate_rule({
        'rule_version': 'strategy_lab/v1',
        'name': '界面冒烟规则',
        'universe': {'mode': 'all_market'},
        'conditions': [
            {'field': 'vol_60', 'op': '<=', 'operand': {'type': 'quantile', 'q': 0.5}},
            {'field': 'ep', 'op': '>=', 'operand': {'type': 'quantile', 'q': 0.7}},
        ],
        'sort': {'field': 'ep', 'direction': 'desc'},
        'top_n': 50,
        'rebalance': {'frequency': 'monthly'},
        'period': {'start': '2019-01-04', 'end': '2025-12-31'},
        'benchmark': '000300.SH',
    })


def test_page_renders_headless(app):
    page = StrategyLabPage()
    page.apply_rule(_rule())
    assert len(page._rows) == 2
    assert page.name_edit.text() == '界面冒烟规则'
    assert '全市场洁净集' in page.rule_text_label.text()

    metrics = {key: 0.1 for key, _, _ in _METRIC_ROWS}
    metrics.update({'final_equity': 1.6e9, 'trades': 100, 'rebalances': 84})
    page._fill_metrics(metrics)
    assert page.metrics_table.rowCount() == len(_METRIC_ROWS)
    assert page.metrics_table.columnWidth(0) > 0

    nav = [['2024-01-31', 1.0e9], ['2024-02-29', 1.05e9], ['2024-03-29', 1.02e9]]
    bench = [['2024-01-31', 1.0e9], ['2024-02-29', 1.01e9], ['2024-03-29', 0.99e9]]
    page._plot_curves(nav, bench)

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
