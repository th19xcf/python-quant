"""策略试算选股层：排序取前 N、条件链、空结果与缺值口径。"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from src.quant.strategy_lab.backtest import _passes, select_members

DAY = date(2024, 1, 31)
DAYS = [DAY]
UNIVERSE = ['A.SZ', 'B.SZ', 'C.SZ', 'D.SZ']


class FakePanels:
    def __init__(self, values_by_field):
        self.values_by_field = values_by_field

    def values(self, key, days=None):
        panel = self.values_by_field[key]
        return {d: panel.get(d, {}) for d in (days or DAYS)}


def _ctx():
    return SimpleNamespace(days=DAYS, quoted={DAY: set(UNIVERSE)}, allowed=None)


def _rule(**overrides):
    rule = {
        'conditions': [],
        'sort': {'field': 'ep', 'direction': 'desc'},
        'top_n': 2,
    }
    rule.update(overrides)
    return rule


def test_sort_desc_and_top_n():
    panels = FakePanels({'ep': {DAY: {'A.SZ': 3.0, 'B.SZ': 2.0, 'C.SZ': 1.0, 'D.SZ': 0.5}}})
    path, _ = select_members(_ctx(), panels, _rule(), DAYS)
    assert path[DAY] == ['A.SZ', 'B.SZ']


def test_sort_asc_order():
    panels = FakePanels({'ep': {DAY: {'A.SZ': 3.0, 'B.SZ': 2.0, 'C.SZ': 1.0, 'D.SZ': 0.5}}})
    path, _ = select_members(_ctx(), panels, _rule(sort={'field': 'ep', 'direction': 'asc'}), DAYS)
    assert path[DAY] == ['D.SZ', 'C.SZ']


def test_value_condition_filters_and_drops_missing():
    panels = FakePanels({
        'pe': {DAY: {'A.SZ': 10.0, 'B.SZ': 60.0, 'C.SZ': 30.0}},  # D 缺值
        'ep': {DAY: {'A.SZ': 3.0, 'B.SZ': 2.0, 'C.SZ': 1.0}},
    })
    rule = _rule(conditions=[{'field': 'pe', 'op': '<', 'operand': {'type': 'value', 'value': 40}}],
                 top_n=3)
    path, skipped = select_members(_ctx(), panels, rule, DAYS)
    assert path[DAY] == ['A.SZ', 'C.SZ']
    assert skipped.get('missing_value', 0) >= 1


def test_quantile_condition_keeps_bottom_half():
    panels = FakePanels({
        'vol_60': {DAY: {'A.SZ': 0.1, 'B.SZ': 0.2, 'C.SZ': 0.3, 'D.SZ': 0.4}},
        'ep': {DAY: {'A.SZ': 4.0, 'B.SZ': 3.0, 'C.SZ': 2.0, 'D.SZ': 1.0}},
    })
    rule = _rule(conditions=[{'field': 'vol_60', 'op': '<=',
                              'operand': {'type': 'quantile', 'q': 0.5}}], top_n=10)
    path, _ = select_members(_ctx(), panels, rule, DAYS)
    assert set(path[DAY]) == {'A.SZ', 'B.SZ'}


def test_empty_result_returns_no_path():
    panels = FakePanels({
        'pe': {DAY: {'A.SZ': 10.0, 'B.SZ': 20.0, 'C.SZ': 30.0, 'D.SZ': 40.0}},
        'ep': {DAY: {'A.SZ': 1.0}},
    })
    rule = _rule(conditions=[{'field': 'pe', 'op': '>',
                              'operand': {'type': 'value', 'value': 1000}}])
    path, skipped = select_members(_ctx(), panels, rule, DAYS)
    assert path == {}
    assert skipped['empty_after_conditions'] == 1


def test_passes_between_inclusive():
    operand = {'type': 'value', 'low': 1.0, 'high': 3.0}
    assert _passes(1.0, 'between', operand)
    assert _passes(3.0, 'between', operand)
    assert not _passes(3.1, 'between', operand)
    assert not _passes(None, 'between', operand)
