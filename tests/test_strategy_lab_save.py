"""策略试算「保存结果」：落盘结构、重复保存不覆盖、按钮启用时机。"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

import pytest

from src.quant.strategy_lab.save import save_run

RULE = {
    'name': '示例低估值低波',
    'rule_type': 'timing',
    'codes': ['510300.SH'],
    'signal': {'field': 'pb_percentile', 'op': '<=', 'value': 0.3},
    'execution': {'timing': 'next_open'},
    'cost': {'one_way_pct': 0.0015},
}


def _payload() -> dict:
    return {
        'status': 'ok',
        'rule_text': '低估值满仓，否则空仓',
        'elapsed_seconds': 1.2,
        'engine_seconds': 0.8,
        'used_codes': 1,
        'metrics': {'annual': 0.1, 'max_drawdown': -0.2, 'sharpe': 1.1,
                    'final_equity': 1.5e9, 'trades': 10, 'rebalances': 12},
        'nav': [['2024-01-02', 1.0e9], ['2024-01-03', 1.01e9]],
        'benchmark_curve': [['2024-01-02', 1.0e9], ['2024-01-03', 1.0e9]],
        'drawdown': [['2024-01-02', 0.0], ['2024-01-03', -0.01]],
        'markers': [{'date': '2024-01-03', 'position': 1.0}],
        'dividend_coverage': {
            'ok': False, 'checked': 1,
            'issues': [{'code': '510300.SH', 'reason': '无分红记录'}],
            'message': '1 只标的分红覆盖不足，收益可能低估',
        },
    }


def _store(tmp_path: Path) -> Path:
    store = tmp_path / 'store'
    (store / 'meta').mkdir(parents=True)
    (store / 'meta' / 'data_version.json').write_text(json.dumps({
        'current': {'version': 'dv-test-1', 'digest': 'digest-abc',
                    'as_of': '2026-11-13'},
    }), encoding='utf-8')
    return store


def test_save_roundtrip_fields(tmp_path: Path) -> None:
    info = save_run(_payload(), RULE, root=tmp_path / 'runs',
                    store_root=_store(tmp_path),
                    now=datetime(2026, 11, 17, 10, 0, 0))
    run_dir = Path(info['run_dir'])
    assert run_dir.name == 'strategy_lab_示例低估值低波_20261117-100000'

    assert json.loads((run_dir / 'spec.json').read_text(encoding='utf-8')) == RULE
    doc = json.loads((run_dir / 'metrics.json').read_text(encoding='utf-8'))
    assert doc['run_id'] == run_dir.name
    assert doc['created_at'] == '2026-11-17T10:00:00'
    assert doc['metrics']['annual'] == 0.1
    assert doc['data_version']['version'] == 'dv-test-1'
    assert doc['dividend_coverage']['issues'][0]['code'] == '510300.SH'
    assert doc['code_head']['status'] in ('recorded', 'unknown')
    assert 'full' in doc['code_head']

    nav = (run_dir / 'daily_nav.csv').read_text(encoding='utf-8').splitlines()
    assert nav == ['date,equity', '2024-01-02,1000000000.00',
                   '2024-01-03,1010000000.00']
    dd = (run_dir / 'drawdown.csv').read_text(encoding='utf-8').splitlines()
    assert dd[0] == 'date,drawdown' and len(dd) == 3
    assert json.loads((run_dir / 'markers.json').read_text(
        encoding='utf-8')) == _payload()['markers']

    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    names = {item['name'] for item in manifest['files']}
    assert names == {'spec.json', 'metrics.json', 'daily_nav.csv',
                     'benchmark_nav.csv', 'drawdown.csv', 'markers.json'}
    assert manifest['index']['updated'] is False
    assert info['index_updated'] is False
    # 不碰工作台根索引
    assert not (tmp_path / 'runs' / 'index.json').exists()


def test_save_twice_creates_independent_runs(tmp_path: Path) -> None:
    kwargs = {'root': tmp_path / 'runs', 'store_root': _store(tmp_path),
              'now': datetime(2026, 11, 17, 10, 0, 0)}
    first = save_run(_payload(), RULE, **kwargs)
    second = save_run(_payload(), RULE, **kwargs)
    assert first['run_dir'] != second['run_dir']
    assert first['run_id'].endswith('20261117-100000')
    assert second['run_id'].endswith('-2')
    for info in (first, second):
        run_dir = Path(info['run_dir'])
        assert (run_dir / 'metrics.json').is_file()
        assert json.loads((run_dir / 'metrics.json').read_text(
            encoding='utf-8'))['run_id'] == info['run_id']


def test_save_rejects_empty_result(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_run({'status': 'empty'}, RULE, root=tmp_path / 'runs')


def test_save_button_disabled_until_result(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip('PySide6.QtWidgets')
    from PySide6.QtWidgets import QApplication

    import src.ui.strategy_lab.page as page_mod

    QApplication.instance() or QApplication([])
    calls: list[dict] = []

    def fake_save(payload, rule, **kwargs):
        calls.append(payload)
        return {'run_id': f'strategy_lab_stub_{len(calls)}'}

    monkeypatch.setattr(page_mod, 'save_run', fake_save)
    page = page_mod.StrategyLabPage()
    assert not page.save_btn.isEnabled()
    assert page.save_btn.text() != ''

    page._on_result(_payload())
    assert page.save_btn.isEnabled()
    page.save_btn.click()
    assert len(calls) == 1
    assert '已保存' in page.status_label.text()
    page.save_btn.click()
    assert len(calls) == 2

    page._on_result({'status': 'empty', 'reason': '条件筛选后没有成分'})
    assert not page.save_btn.isEnabled()
    page.deleteLater()
