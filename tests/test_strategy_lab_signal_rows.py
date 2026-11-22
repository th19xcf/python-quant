"""策略试算信号块：当前分位 / 估值日期两行的文本与显示条件。"""

import pytest

# GUI 属于可选组 gui；未安装 PySide6 时整文件跳过
pytest.importorskip("PySide6.QtWidgets")
from datetime import date

from src.ui.strategy_lab.page import _switch_rows

TIMING_RULE = {
    'rule_type': 'timing',
    'signal': {'kind': 'index_pe_pct', 'metric': 'pb', 'index_code': '000015.SH',
               'window_months': 60, 'min_months': 36},
    'target': {'codes': ['510880.SH']},
}


def _payload(tail):
    return {
        'markers': [{'signal_date': '2026-07-31', 'date': '2026-08-03',
                     'level': 1, 'position': 0.5}],
        'signal_tail': tail,
    }


def test_rows_show_pct_threshold_and_lag():
    rows = dict(_switch_rows(_payload({'date': '2026-10-30', 'pct': 0.9,
                                       'threshold': 0.5}), TIMING_RULE))
    assert rows['当前分位'] == '90%（阈值 50%）'
    lag = (date.today() - date(2026, 10, 30)).days
    assert rows['估值日期'] == f'2026-10-30（滞后 {lag} 个自然日）'


def test_threshold_comes_from_rule_not_hardcoded():
    rows = dict(_switch_rows(_payload({'date': '2026-10-30', 'pct': 0.42,
                                       'threshold': 0.4}), TIMING_RULE))
    assert rows['当前分位'] == '42%（阈值 40%）'


def test_other_rules_have_no_signal_rows():
    labels = [row[0] for row in _switch_rows({}, {'rule_type': 'select'})]
    assert '当前分位' not in labels
    assert '估值日期' not in labels
