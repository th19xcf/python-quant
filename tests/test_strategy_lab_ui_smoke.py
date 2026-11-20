"""策略试算页面无头渲染冒烟：规则下拉、类型摘要、指标表与三张图。"""

from __future__ import annotations

import os
from datetime import date

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

import pytest

pytest.importorskip('PySide6.QtWidgets')

from PySide6.QtWidgets import QApplication  # noqa: E402

from src.ui.strategy_lab.page import (  # noqa: E402
    _METRIC_ROWS,
    StrategyLabPage,
    _next_rebalance_note,
)


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

    # 当前持仓：数量与权重逐只逐列展示，按权重降序；权重缺失不写成 0.00%
    page._fill_current({'date': '2024-03-29',
                        'positions': {'600000.SH': 10792600.0, '000001.SZ': 500.0},
                        'weights': {'600000.SH': 0.0312}})
    assert page.current_table.rowCount() == 2
    assert page.current_table.item(0, 0).text() == '600000.SH'
    assert page.current_table.item(0, 1).text() == '3.12%'
    assert page.current_table.item(0, 2).text() == '10,792,600'
    assert page.current_table.item(1, 1).text() == '—'
    assert '2 只' in page.current_title.text()
    assert page.current_note.text() == '估值日 2024-03-29'

    # 调仓目标：标题带最近一次生成的日期，并单列按频率推算的下次调仓日；
    # 名单明确标注「非下次预测」，不提前算下次名单。
    targets = [{'date': '2024-01-31', 'weights': {'600000.SH': 0.5, '000001.SZ': 0.5}},
               {'date': '2024-02-29', 'weights': {'600519.SH': 1.0}}]
    signal_rule = {'rebalance': {}}
    page._fill_targets(targets, '2024-02-29', signal_rule)
    assert page.target_table.rowCount() == 1
    assert page.target_table.item(0, 0).text() == '600519.SH'
    assert page.target_table.item(0, 1).text() == '100.00%'
    assert page.target_title.text() == '最近一次调仓目标（2024-02-29）· 1 只'
    assert page.target_note.text() == '该名单为最近一次生成，不是下次预测'
    assert page.target_next.text() == '该规则按信号触发，无固定调仓日'
    page._fill_targets([], '2024-03-29', signal_rule)
    assert page.target_table.rowCount() == 0
    assert page.target_title.text() == '最近一次调仓目标'
    assert '没有产生调仓目标' in page.target_note.text()

    # 下次调仓日：口径复用 compute.FREQUENCY_MONTHS，日历覆盖时给精确日期，
    # 最新交易日所在月未走完时只报月份、不猜日期。
    calendar = ([date(2024, 1, 31), date(2024, 2, 29)]
                + [date(2024, m, 28) for m in range(3, 13)]
                + [date(2025, 1, 27)])
    semi = {'rebalance': {'frequency': 'semi'}}
    monthly = {'rebalance': {'frequency': 'monthly'}}
    assert _next_rebalance_note(semi, '2024-06-28', calendar) == '下次调仓日：2024-12-28'
    assert _next_rebalance_note(semi, '2024-12-28', calendar) == (
        '下次调仓日：2025 年 06 月末最后一个交易日（日历未覆盖）')
    assert _next_rebalance_note(monthly, '2024-12-28', calendar) == '下次调仓日：2025-01-27'
    assert _next_rebalance_note(monthly, '2025-01-27', calendar) == (
        '下次调仓日：2025 年 01 月末最后一个交易日（日历未覆盖）')

    # 折叠区默认收起，展开后两块可见
    assert page.detail_panel.isHidden()
    assert page.detail_toggle.text().startswith('▸')
    page.detail_toggle.setChecked(True)
    assert not page.detail_panel.isHidden()
    assert page.detail_toggle.text().startswith('▾')
    page.detail_toggle.setChecked(False)
    assert page.detail_panel.isHidden()

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
