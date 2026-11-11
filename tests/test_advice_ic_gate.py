#!/usr/bin/env python3

"""IC 失效门禁用例：``failed=true`` 时必须真正拦住新建仓，而不是只写备注。"""

from __future__ import annotations

from datetime import date

from _advice_synth import build_store

from src.advice import pipeline as pipeline_module
from src.advice.config import default_config
from src.advice.ic_monitor import ic_gate_decision
from src.advice.pipeline import AdviceEngine


def _codes() -> list[str]:
    return [f'6000{index:02d}.SH' for index in range(20)]


def _snapshot(tier: str, failed: bool, recent: list[float]) -> dict:
    return {
        'tier': tier, 'horizon_days': 5, 'window_days': 20,
        'recent_ic': recent,
        'status': {
            'failed': failed,
            'reason': ('连续3个窗口IC为负：' + str(recent)) if failed else '近窗IC正常',
        },
    }


def _passing_monitor(scored, tier, *, trade_date):  # noqa: ARG001
    return _snapshot(tier, False, [0.01, 0.02, 0.01])


def _failed_monitor(scored, tier, *, trade_date):  # noqa: ARG001
    return _snapshot(tier, True, [-0.05, -0.12, -0.13])


def test_ic_gate_decision_blocks_only_when_failed() -> None:
    assert ic_gate_decision(_snapshot('mid', False, [0.01, 0.02])) == (False, '')
    blocked, reason = ic_gate_decision(_snapshot('mid', True, [-0.1, -0.2]))
    assert blocked is True
    assert 'IC门禁触发' in reason
    assert 'mid' in reason
    assert '不出新建仓' in reason
    # 样本不足（未被判 failed）不得误拦
    insufficient = {'tier': 'long', 'horizon_days': 60, 'window_days': 20,
                    'recent_ic': [], 'status': {'failed': False, 'reason': '样本不足'}}
    assert ic_gate_decision(insufficient) == (False, '')


def test_failed_ic_blocks_new_buys_and_records_reason(tmp_path, monkeypatch):
    codes = _codes()
    root = build_store(tmp_path / 'local_store', codes,
                       end=date(2026, 6, 30), days=320)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    monkeypatch.setattr(pipeline_module, 'run_ic_monitor', _failed_monitor)

    result = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short'], codes=codes,
                             state_dir=tmp_path / 'state',
                             persist_state=False)
    advice = result.tiers['short']
    directions = {row.direction for row in advice.rows}
    assert 'BUY' not in directions, 'IC 失效时不得产出任何买入建议'
    assert any('IC门禁触发' in note for note in advice.notes)
    assert advice.filter_stats['ic_gate_blocked'] == 1
    # 门禁不是"只加备注"：候选池确实为空，没有买入行也没有延后买入
    assert not [row for row in advice.rows if row.direction == 'BUY']
    assert not [row for row in advice.rows if row.disposition == 'deferred']


def test_failed_ic_keeps_existing_holdings_under_management(tmp_path, monkeypatch):
    """门禁只拦新建仓：已持仓仍按最小持有期/风控继续管理。"""
    codes = _codes()
    root = build_store(tmp_path / 'local_store', codes,
                       end=date(2026, 6, 30), days=320)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    state_dir = tmp_path / 'state'

    monkeypatch.setattr(pipeline_module, 'run_ic_monitor', _passing_monitor)
    first = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                            tiers=['short'], codes=codes,
                            state_dir=state_dir)
    bought = {row.ts_code for row in first.tiers['short'].rows
              if row.direction == 'BUY'}
    assert bought, '对照组必须能正常买入，否则本用例不成立'

    monkeypatch.setattr(pipeline_module, 'run_ic_monitor', _failed_monitor)
    second = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short'], codes=codes,
                             state_dir=state_dir)
    advice = second.tiers['short']
    assert not [row for row in advice.rows if row.direction == 'BUY']
    held = {row.ts_code: row.direction for row in advice.rows}
    assert bought <= set(held), '已持仓不应因门禁而消失'
    assert {held[code] for code in bought} <= {'HOLD', 'SELL'}
