"""归因 run 选择：测试/冒烟 run 一律排除，不静默出数。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.quant.analysis import attribution_local as mod
from src.quant.analysis.attribution_state import AttributionState, RunSummary
from src.ui.attribution.page import AttributionPage


def _write_run(base: Path, run_id: str, *, name: str, created: str,
               nav_rows: int, positions: int = 0,
               experimental: bool = False) -> Path:
    run_dir = base / 'backtest_runs' / run_id
    run_dir.mkdir(parents=True)
    payload = {
        'run_id': run_id,
        'name': name,
        'created_at': created,
        'nav_rows': nav_rows,
        'experimental': experimental,
        'period': {'start': '2020-01-02', 'end': '2023-12-29'},
        'positions': {'file': 'positions.parquet' if positions else None,
                      'rows': positions},
    }
    (run_dir / 'metrics.json').write_text(
        json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    if positions:
        (run_dir / 'positions.parquet').write_bytes(b'')
    return run_dir


def test_index_weights_asof_normalizes_latest_month():
    frame = pl.DataFrame({
        'index_code': ['000300.SH'] * 3,
        'ts_code': ['A', 'B', 'C'],
        'trade_date': ['20200131', '20200131', '20200228'],
        'weight': [30.0, 70.0, 100.0],
    })
    weights = mod._index_weights_asof(frame, date(2020, 2, 15))
    assert weights == {'A': 0.3, 'B': 0.7}
    assert round(sum(weights.values()), 9) == 1.0


def test_latest_run_skips_test_names(tmp_path):
    _write_run(tmp_path, '20261116-142819_app_ui_click', name='app_ui_click',
               created='2026-11-16T14:28:19', nav_rows=147, positions=20)
    formal = _write_run(tmp_path, '20260101-090000_v3_monthly', name='v3_monthly',
                        created='2026-01-01T09:00:00', nav_rows=900)
    run_dir, metrics, skipped = mod._latest_run(tmp_path)
    assert skipped == 1
    assert run_dir == formal
    assert metrics['name'] == 'v3_monthly'


def test_latest_run_prefers_non_experimental(tmp_path):
    """零成本实验 run 更晚也必须让位给正式 run。"""
    formal = _write_run(tmp_path, '20260101-090000_v3_monthly', name='v3_monthly',
                        created='2026-01-01T09:00:00', nav_rows=900, positions=5)
    _write_run(tmp_path, '20261116-160000_v3_monthly_cost0',
               name='v3_monthly_cost0', created='2026-11-16T16:00:00',
               nav_rows=900, positions=5, experimental=True)
    run_dir, metrics, skipped = mod._latest_run(tmp_path)
    assert skipped == 0
    assert run_dir == formal
    assert metrics['name'] == 'v3_monthly'


def test_latest_run_prefers_positions_over_recency(tmp_path):
    _write_run(tmp_path, 'a_smoke', name='migrate_smoke',
               created='2026-11-16T10:00:00', nav_rows=300, positions=30)
    with_positions = _write_run(tmp_path, 'b_formal', name='v3_monthly',
                                created='2026-01-01T09:00:00', nav_rows=900,
                                positions=10)
    _write_run(tmp_path, 'c_newer', name='v4_monthly',
               created='2026-06-01T09:00:00', nav_rows=900)
    run_dir, _, skipped = mod._latest_run(tmp_path)
    assert skipped == 1
    assert run_dir == with_positions


def test_no_formal_run_returns_none(tmp_path):
    _write_run(tmp_path, 'x_app_ui_click', name='app_ui_click',
               created='2026-11-16T14:28:19', nav_rows=147, positions=20)
    run_dir, metrics, skipped = mod._latest_run(tmp_path)
    assert run_dir is None and metrics == {} and skipped == 1


def test_build_attribution_reports_no_formal_run(tmp_path):
    _write_run(tmp_path, 'x_app_ui_click', name='app_ui_click',
               created='2026-11-16T14:28:19', nav_rows=147, positions=20)
    state = mod.build_attribution(root=tmp_path)
    assert state.run is None
    assert state.degraded
    assert any('暂无可用正式回测' in note for note in state.notes)
    assert any('已跳过 1 条测试' in note for note in state.notes)


def test_test_run_guard_raises(tmp_path, monkeypatch):
    test_dir = _write_run(tmp_path, 'x_app_ui_click', name='app_ui_click',
                          created='2026-11-16T14:28:19', nav_rows=147, positions=20)
    monkeypatch.setattr(mod, '_latest_run',
                        lambda base: (test_dir, {'name': 'app_ui_click'}, 0))
    with pytest.raises(ValueError, match='测试/冒烟'):
        mod.build_attribution(root=tmp_path)


def test_page_shows_no_formal_run_and_blocks_test_run(qapp):
    page = AttributionPage()
    empty = AttributionState(
        asof=None, run=None, excess=None, industries=(), styles=(),
        degraded=True, missing=(), notes=('已跳过 1 条测试/冒烟 run。',))
    page.render_state(empty)
    assert '暂无可用正式回测' in page.banner_label.text()
    assert '暂无可用正式回测' in page.status_label.text()

    test_state = AttributionState(
        asof=None,
        run=RunSummary(run_id='x', name='app_ui_click', start='2024-01-02',
                       end='2024-06-28'),
        excess=None, industries=(), styles=(),
        degraded=True, missing=(), notes=())
    page.render_state(test_state)
    assert '归因失败' in page.status_label.text()
    assert '测试/冒烟' in page.banner_label.text()
    assert page.brinson_card.isVisible() is False
    page.close()
