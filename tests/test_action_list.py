"""模型侧操作清单：纯函数、再平衡日与无头 Qt 渲染测试。"""

from __future__ import annotations

from datetime import date

from src.quant.analysis.action_list_local import next_rebalance_date
from src.quant.analysis.action_list_state import (
    ActionListState,
    ActionRow,
    direction_of,
    lots_from_amount,
)


def test_direction_of_mapping() -> None:
    assert direction_of('新建') == '买入'
    assert direction_of('加仓') == '加仓'
    assert direction_of('减仓') == '减持'
    assert direction_of('清仓') == '清仓'
    assert direction_of('不变') == '持有'
    assert direction_of('未知') == '持有'


def test_lots_from_amount_rounds_to_board_lot() -> None:
    assert lots_from_amount(100_000, 10.0) == 10_000
    assert lots_from_amount(1_099, 10.0) == 100
    assert lots_from_amount(100_000, 0) == 0
    assert lots_from_amount(-1, 10.0) == 0


def test_direction_counts() -> None:
    rows = (
        ActionRow('000001.SZ', 'A', '买入', 100, 10.0, 0.1, 'D', 'c', 'r'),
        ActionRow('000002.SZ', 'B', '买入', 200, 10.0, 0.1, 'D', 'c', 'r'),
        ActionRow('000003.SZ', 'C', '清仓', 300, 10.0, 0.0, 'D', 'c', 'r'),
    )
    state = ActionListState('2026-11-13', '2026-10-31', '2026-11-16', '2026-12-31',
                            1e9, '中性', '小盘', rows=rows)
    assert state.direction_counts() == {'买入': 2, '清仓': 1}


def test_next_rebalance_date_after_trade_day() -> None:
    from src.quant.analysis.action_list_local import _default_root

    nxt = next_rebalance_date(date(2026, 11, 13), _default_root())
    assert nxt is not None
    assert nxt > '2026-11-13'


def _sample_state() -> ActionListState:
    rows = (
        ActionRow('000001.SZ', '平安银行', '买入', 1000, 10.0, 0.10, '2026-11-16',
                  '开盘执行', '趋势中性'),
        ActionRow('600000.SH', '浦发银行', '持有', 0, 5.0, 0.05, '2026-11-16',
                  '持有至再平衡', '趋势中性'),
    )
    return ActionListState('2026-11-13', '2026-10-31', '2026-11-16', '2026-12-31',
                           1e9, '中性', '小盘', rows=rows,
                           notes=('模型侧清单，不含用户持仓',))


def test_page_renders_and_three_state_sort(qapp) -> None:
    from src.ui.action_list.page import ActionListPage

    page = ActionListPage()
    try:
        page.render_state(_sample_state())
        assert page.table.rowCount() == 2
        assert page.table.columnCount() == 10
        assert '2026-11-13' in page.status_label.text()
        assert '买入 1' in page.dist_label.text()

        # 参考价升序：5.0 在前
        page._on_header_clicked(5)
        assert page.table.item(0, 5).text() == '5.00'
        # 再点降序：10.0 在前
        page._on_header_clicked(5)
        assert page.table.item(0, 5).text() == '10.00'
        # 第三次取消排序：回到原序
        page._on_header_clicked(5)
        assert page._sort_col is None
        assert page.table.item(0, 5).text() == '10.00'
    finally:
        # 离屏退出前释放控件，避免 QApplication 析构时残留窗口导致进程崩溃
        page.deleteLater()
        qapp.processEvents()
