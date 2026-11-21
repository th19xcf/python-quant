"""策略试算选股层：排序取前 N、条件链、空结果与缺值口径。"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from src.quant.strategy_lab.backtest import (
    _current_snapshot,
    _freshness,
    _passes,
    _targets_from_markers,
    _targets_from_path,
    select_members,
)

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


def test_targets_from_path_uses_equal_weight():
    path = {date(2024, 1, 31): ['A.SZ', 'B.SZ'], date(2024, 2, 29): ['C.SZ']}
    targets = _targets_from_path(path, cash_buffer=0.02)
    assert targets[0]['date'] == '2024-01-31'
    assert targets[0]['members'] == 2
    assert targets[0]['weights'] == {'A.SZ': 0.49, 'B.SZ': 0.49}
    assert targets[1]['weights'] == {'C.SZ': 0.98}


def test_targets_from_markers_dedupes_same_weights():
    markers = [
        {'date': '2024-01-31', 'signal_date': '2024-01-30',
         'weights': {'510300.SH': 1.0}, 'direction': 'sell'},
        {'date': '2024-01-31', 'signal_date': '2024-01-30',
         'weights': {'510300.SH': 1.0}, 'direction': 'buy'},
        {'date': '2024-02-29', 'signal_date': '2024-02-28',
         'weights': {'588000.SH': 1.0}, 'direction': 'buy'},
    ]
    targets = _targets_from_markers(markers, code=None)
    assert [t['date'] for t in targets] == ['2024-01-31', '2024-02-29']
    # 定投没有 weights 时用份数占比回落
    dca = _targets_from_markers([{'date': '2024-03-01', 'position': 0.6}],
                                code='510300.SH')
    assert dca == [{'date': '2024-03-01', 'signal_date': None,
                    'weights': {'510300.SH': 0.6}}]


def test_current_snapshot_weights_from_valuation_close():
    snapshot = _current_snapshot(
        {'A.SZ': 1000.0, 'B.SZ': 0.0},
        {'date': '2024-01-31', 'close': {'A.SZ': 10.0, 'B.SZ': 5.0}},
        [(date(2024, 1, 31), 20000.0)])
    assert snapshot['date'] == '2024-01-31'
    assert snapshot['weights'] == {'A.SZ': 0.5}
    assert snapshot['positions'] == {'A.SZ': 1000.0, 'B.SZ': 0.0}


def test_freshness_flags_lagging_factor_panel(monkeypatch):
    import src.quant.strategy_lab.backtest as backtest

    monkeypatch.setattr(backtest, '_used_fields', lambda rule: {'vq_value'})
    monkeypatch.setattr(backtest, '_factor_panel_end', lambda: '2024-01-10')
    rule = {'rule_type': 'stock_select', 'period': {'start': '2024-01-02',
                                                   'end': '2024-01-31'}}
    calendar = [date(2024, 1, 10), date(2024, 1, 11), date(2024, 1, 12),
                date(2024, 1, 31)]
    fresh = _freshness(rule, calendar)
    assert fresh['panel_end'] == '2024-01-10'
    assert fresh['panel_lag_days'] == 3
    assert fresh['panel_fields'] == ['vq_value']
    assert '落后目标日' in fresh['warnings'][0]


def test_freshness_no_warning_when_panel_covers_target(monkeypatch):
    import src.quant.strategy_lab.backtest as backtest

    monkeypatch.setattr(backtest, '_used_fields', lambda rule: {'vol_60'})
    rule = {'rule_type': 'stock_select', 'period': {'start': '2024-01-02',
                                                   'end': '2024-01-31'}}
    fresh = _freshness(rule, [date(2024, 1, 31)])
    assert fresh['panel_fields'] == [] and fresh['warnings'] == []
    assert fresh['quotes_gap_days'] == 0


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


def test_quantile_by_industry_uses_group_thresholds():
    panels = FakePanels({
        'industry': {DAY: {'A.SZ': '银行', 'B.SZ': '银行', 'C.SZ': '医药', 'D.SZ': '医药'}},
        'vol_60': {DAY: {'A.SZ': 0.1, 'B.SZ': 0.2, 'C.SZ': 0.9, 'D.SZ': 0.8}},
        'ep': {DAY: {'A.SZ': 1.0, 'B.SZ': 4.0, 'C.SZ': 3.0, 'D.SZ': 2.0}},
    })
    rule = _rule(conditions=[{'field': 'vol_60', 'op': '<=',
                              'operand': {'type': 'quantile', 'q': 0.5,
                                          'group': 'industry'}}], top_n=10)
    path, _ = select_members(_ctx(), panels, rule, DAYS)
    # 银行组阈值 0.15 只留 A；医药组阈值 0.85 只留 D
    assert set(path[DAY]) == {'A.SZ', 'D.SZ'}


def test_two_stage_sort_with_keep_pct():
    panels = FakePanels({
        'vq_value': {DAY: {'A.SZ': 0.9, 'B.SZ': 0.8, 'C.SZ': 0.7, 'D.SZ': 0.6}},
        'ep': {DAY: {'A.SZ': 1.0, 'B.SZ': 5.0, 'C.SZ': 9.0, 'D.SZ': 0.0}},
    })
    rule = _rule(sort=[{'field': 'vq_value', 'direction': 'desc', 'keep_pct': 0.5},
                       {'field': 'ep', 'direction': 'desc'}], top_n=10)
    path, _ = select_members(_ctx(), panels, rule, DAYS)
    assert path[DAY] == ['B.SZ', 'A.SZ']


def test_factor_panel_end_reads_official_factor_panel(tmp_path, monkeypatch):
    """新鲜度核对读取正式因子面板（aaa3394 后为 panels.FACTOR_PANEL）。"""
    import polars as pl

    from src.quant.strategy_lab import panels as panels_module
    from src.quant.strategy_lab.backtest import _factor_panel_end

    panel = tmp_path / 'factor_panel.parquet'
    pl.DataFrame({'signal_date': [date(2026, 10, 30), date(2026, 11, 20)]}).write_parquet(panel)
    monkeypatch.setattr(panels_module, 'FACTOR_PANEL', panel)
    assert _factor_panel_end() == '2026-11-20'

    monkeypatch.setattr(panels_module, 'FACTOR_PANEL', tmp_path / 'missing.parquet')
    assert _factor_panel_end() is None
