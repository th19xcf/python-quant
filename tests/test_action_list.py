"""模型侧操作清单：纯函数、再平衡日与无头 Qt 渲染测试。"""

from __future__ import annotations

from datetime import date

from src.quant.analysis.action_list_local import next_rebalance_dates
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


def test_next_rebalance_dates_follow_signal_month() -> None:
    """再平衡时点按信号日所在月份推导，执行日是其次一交易日。"""
    from src.quant.analysis.action_list_local import _default_root

    signal, exec_day = next_rebalance_dates(date(2026, 10, 30), _default_root())
    assert signal == '2026-11-30'
    assert exec_day == '2026-12-01'


def test_change_types_come_from_previous_ledger_day() -> None:
    """方向判定在 tools/target_holdings.py：有前一日台账时产出加仓/减仓/清仓/不变。"""
    from tools.target_holdings import classify, previous_targets

    assert classify(0.2, 0.1) == '加仓'
    assert classify(0.1, 0.2) == '减仓'
    assert classify(0.0, 0.1) == '清仓'
    assert classify(0.1, 0.0) == '新建'
    assert classify(0.1, 0.1) == '不变'

    ledger = [
        {'交易日': '2026-11-02', '代码': '000001.SZ', '目标权重': '0.1000000000',
         '目标金额': '100000.00', '名称': 'A'},
        {'交易日': '2026-11-13', '代码': '000001.SZ', '目标权重': '0.2000000000',
         '目标金额': '200000.00', '名称': 'A'},
    ]
    prev_day, prev = previous_targets(ledger, '2026-11-13')
    assert prev_day == '2026-11-02'
    assert prev['000001.SZ'][0] == 0.1
    assert classify(0.2, prev['000001.SZ'][0]) == '加仓'


def test_weight_rounding_noise_is_unchanged() -> None:
    """全精度权重对 1e-10 精度台账值只差存储舍入，应判「不变」而非加仓/减仓。

    实测 4284 只等权：0.99/4284 写回 10 位小数后差值约 3.8e-12，旧容差 1e-12
    会把全部标的判成「加仓」。真实增减一只成分的权重变化约 5e-8，仍须判出方向。
    """
    from tools.target_holdings import WEIGHT_TOL, classify

    weight = 0.99 / 4284
    ledger_weight = float(f'{weight:.10f}')
    assert 0 < abs(weight - ledger_weight) <= WEIGHT_TOL
    assert classify(weight, ledger_weight) == '不变'
    assert classify(0.99 / 4283, ledger_weight) == '加仓'
    assert classify(0.99 / 4285, ledger_weight) == '减仓'


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
